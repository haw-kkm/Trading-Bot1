#!/usr/bin/env python3
"""Repeatable Stage 0 public-only live REST/WS verification with evidence logs.

This script NEVER reads environment files or API credentials. The subprocess has
only the flags needed to access PUBLIC unauthenticated market data.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import subprocess
import sys

SOURCES = {
    "binance-spot": ("BTCUSDT", ["--allow-binance-network"],
                     ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA")),
    "binance-futures": ("BTCUSDT", ["--allow-binance-network"],
                        ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA", "MARK_PRICE")),
    "toobit-futures": ("BTC-SWAP-USDT", ["--allow-toobit-network", "--i-approve-toobit-public-readonly"],
                       ("TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "MARK_PRICE")),
}


def read_json_stdout(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def run_command(name: str, argv: list[str], logdir: Path, timeout_s: float):
    out, err = logdir / f"{name}.stdout.json", logdir / f"{name}.stderr.log"
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s, check=False)
        out.write_text(result.stdout, encoding="utf-8")
        err.write_text(result.stderr, encoding="utf-8")
        return {"command": " ".join(argv), "exit_code": result.returncode, "timed_out": False,
                "stdout_file": out.name, "stderr_file": err.name}
    except subprocess.TimeoutExpired as e:
        out.write_text((e.stdout or b"").decode(errors="replace") if isinstance(e.stdout, bytes)
                       else (e.stdout or ""), encoding="utf-8")
        err.write_text((e.stderr or b"").decode(errors="replace") if isinstance(e.stderr, bytes)
                       else (e.stderr or ""), encoding="utf-8")
        return {"command": " ".join(argv), "exit_code": None, "timed_out": True,
                "stdout_file": out.name, "stderr_file": err.name}


def stream_evidence(jsonl: Path, source: str):
    counts, websocket = Counter(), Counter()
    total, invalid = 0, 0
    if not jsonl.is_file():
        return {"file_present": False, "events": 0, "invalid_lines": 0,
                "types": {}, "websocket_types": {}, "sha256": None}
    with jsonl.open(encoding="utf-8") as fp:
        for line in fp:
            total += 1
            try:
                event = json.loads(line)
                kind = event["event_type"]
                raw = event.get("raw_payload", {})
                if not isinstance(raw, dict):
                    raw = {}
                counts[kind] += 1
                if source in ("binance-spot", "binance-futures"):
                    # Binance REST only fetches reference data (no live trade/ticker/delta).
                    if kind in ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA", "MARK_PRICE"):
                        websocket[kind] += 1
                else:
                    # Toobit collection REST only fetches funding/OI/mark/index/contracts.
                    if kind in ("TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT"):
                        websocket[kind] += 1
                    elif kind == "MARK_PRICE" and "markPrice" in raw:
                        websocket[kind] += 1
            except (ValueError, KeyError, TypeError):
                invalid += 1
    return {"file_present": True, "events": total, "invalid_lines": invalid,
            "types": dict(sorted(counts.items())), "websocket_types": dict(sorted(websocket.items())),
            "sha256": hashlib.sha256(jsonl.read_bytes()).hexdigest()}


def verify(source: str, logdir: Path, seconds: int, timeout: int):
    symbol, flags, required = SOURCES[source]
    base = [sys.executable, "-m", "src.stage0.live", "--source", source, "--symbol", symbol, *flags]
    prefix = source.replace("-", "_")
    check = run_command(prefix + ".rest_check", base + ["--check"], logdir, timeout)
    check_result = read_json_stdout(logdir / check["stdout_file"])
    rows = check_result.get("endpoints", []) if isinstance(check_result, dict) else []
    verdicts = Counter(r.get("verdict", "UNKNOWN") for r in rows if isinstance(r, dict))
    required_pass = any(r.get("verdict") == "PASS" and r.get("endpoint", "").endswith("/time")
                        for r in rows if isinstance(r, dict))
    check_ok = (check["exit_code"] == 0 and bool(rows)
                and verdicts["PASS"] > 0 and verdicts["FAIL"] == 0 and required_pass)
    output = logdir / (prefix + ".events.jsonl")
    collect = run_command(prefix + ".websocket_collect", base + ["--seconds", str(seconds),
                                              "--out", str(output)], logdir, timeout)
    collect_result = read_json_stdout(logdir / collect["stdout_file"])
    evidence = stream_evidence(output, source)
    missing_ws = [k for k in required if evidence["websocket_types"].get(k, 0) <= 0]
    collect_ok = (collect["exit_code"] == 0 and isinstance(collect_result, dict)
                  and collect_result.get("exit_code") == 0 and evidence["events"] > 0
                  and evidence["invalid_lines"] == 0 and not missing_ws)
    return {"source": source, "symbol": symbol,
            "result": "PASS" if check_ok and collect_ok else "FAIL",
            "rest_check": {**check, "verdict_counts": dict(verdicts), "time_endpoint_pass": required_pass,
                           "passed": check_ok, "endpoints": rows},
            "websocket_collection": {**collect, "passed": collect_ok, "summary": collect_result,
                                     "evidence": evidence, "required_types": list(required),
                                     "missing_required_websocket_types": missing_ws},
            "notes": "PASS proves basic REST+WS public data in this runner only, not trading readiness."}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default="stage0-live-evidence")
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--timeout-seconds", type=int, default=150)
    p.add_argument("--approve-toobit-public-readonly", action="store_true",
                   help="explicitly approve unauthenticated Toobit public market data GET/WS")
    p.add_argument("--binance-only", action="store_true")
    args = p.parse_args(argv)
    if not 15 <= args.seconds <= 120:
        p.error("--seconds must be 15..120")
    logdir = Path(args.output_dir)
    logdir.mkdir(parents=True, exist_ok=True)
    sources = ["binance-spot", "binance-futures"]
    if not args.binance_only and args.approve_toobit_public_readonly:
        sources.append("toobit-futures")
    records = [verify(s, logdir, args.seconds, args.timeout_seconds) for s in sources]
    report = {"created_at_utc": datetime.now(timezone.utc).isoformat(),
              "python": sys.version.split()[0], "verification_seconds": args.seconds,
              "trading_or_account_requests": False, "tested_sources": sources,
              "overall": "PASS" if all(r["result"] == "PASS" for r in records) else "FAIL",
              "results": records}
    target = logdir / "verification-report.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    for r in records:
        print(f"{r['source']}: {r['result']}; REST={r['rest_check']['verdict_counts']}; "
              f"WS={r['websocket_collection']['evidence']['websocket_types']}; "
              f"missing={r['websocket_collection']['missing_required_websocket_types']}")
    print(f"Overall: {report['overall']}; evidence: {target}")
    return 0 if report["overall"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
