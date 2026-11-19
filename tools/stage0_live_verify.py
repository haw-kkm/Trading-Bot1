#!/usr/bin/env python3
"""Repeatable Stage 0 public-only live REST/WS verification with evidence logs.

This script NEVER reads environment files or API credentials. The subprocess has only the flags needed to
access PUBLIC unauthenticated market data. It never tries to work around regional/legal blocks (HTTP 451).

Every stage gets exactly one status:
  PASS          verified by this run
  FAIL          attempted and failed
  INCONCLUSIVE  attempted, no failure, but the evidence cannot establish success (empty data, docs leave the
                meaning open, a small share of unparsable messages)
  NOT_RUN       could not be attempted (e.g. required REST startup data unavailable)
A source whose REST requests ALL returned HTTP 451 is marked environment_blocked: its status stays FAIL/NOT_RUN
(never PASS), but it does not turn the exit code into 1 (the code is not at fault; the network location is).
Per source the stages are: rest | ws_received | ws_parsed | ws_fresh | gate_ready; the source status is the
worst of them (FAIL > NOT_RUN > INCONCLUSIVE > PASS). Exit code: 0 all PASS, 1 any FAIL (excluding
environment-blocked sources), 2 no such FAIL but some FAIL/NOT_RUN/INCONCLUSIVE remains.
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

PASS, FAIL, INCONCLUSIVE, NOT_RUN = "PASS", "FAIL", "INCONCLUSIVE", "NOT_RUN"
_ORDER = {FAIL: 3, NOT_RUN: 2, INCONCLUSIVE: 1, PASS: 0}

SOURCES = {
    "binance-spot": ("BTCUSDT", ["--allow-binance-network"], ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA")),
    "binance-futures": ("BTCUSDT", ["--allow-binance-network"], ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA", "MARK_PRICE")),
    "toobit-futures": ("BTC-SWAP-USDT", ["--allow-toobit-network", "--i-approve-toobit-public-readonly"],
                       ("TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "MARK_PRICE")),
}


def worst(statuses):
    return max(statuses, key=lambda s: _ORDER[s]) if statuses else NOT_RUN


def read_json_stdout(path: Path):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def run_command(name: str, argv: list, logdir: Path, timeout_s: float):
    out, err = logdir / f"{name}.stdout.json", logdir / f"{name}.stderr.log"
    try:
        result = subprocess.run(argv, capture_output=True, text=True, timeout=timeout_s, check=False)
        out.write_text(result.stdout, encoding="utf-8")
        err.write_text(result.stderr, encoding="utf-8")
        return {"command": " ".join(argv), "exit_code": result.returncode, "timed_out": False,
                "stdout_file": out.name, "stderr_file": err.name}
    except subprocess.TimeoutExpired as e:
        for path, data in ((out, e.stdout), (err, e.stderr)):
            path.write_text(data.decode(errors="replace") if isinstance(data, bytes) else (data or ""), encoding="utf-8")
        return {"command": " ".join(argv), "exit_code": None, "timed_out": True,
                "stdout_file": out.name, "stderr_file": err.name}


def file_evidence(jsonl: Path):
    if not jsonl.is_file():
        return {"file_present": False, "lines": 0, "invalid_lines": 0, "types": {}, "sha256": None}
    types, total, invalid = Counter(), 0, 0
    with jsonl.open(encoding="utf-8") as fp:
        for line in fp:
            total += 1
            try:
                types[json.loads(line)["event_type"]] += 1
            except (ValueError, KeyError, TypeError):
                invalid += 1
    return {"file_present": True, "lines": total, "invalid_lines": invalid, "types": dict(sorted(types.items())),
            "sha256": hashlib.sha256(jsonl.read_bytes()).hexdigest()}


def judge_rest(check, rows):
    """-> (status, reason)"""
    rows = [r for r in rows if isinstance(r, dict)]
    if not rows:
        return (FAIL, "--check produced no endpoint rows (crash or timeout)")
    v = Counter(r.get("verdict") for r in rows)
    statuses = Counter(r.get("http_status") for r in rows)
    if v["FAIL"]:
        if statuses[451] == len(rows):
            return (FAIL, "every request returned HTTP 451 (access restricted by region/legal; not bypassed)")
        return (FAIL, f"{v['FAIL']} of {len(rows)} endpoints FAIL")
    if not any(r.get("verdict") == "PASS" and r.get("endpoint", "").endswith("/time") for r in rows):
        return (FAIL, "server-time endpoint did not PASS")
    if v["INCONCLUSIVE"]:
        return (INCONCLUSIVE, f"{v['INCONCLUSIVE']} endpoint(s) INCONCLUSIVE (empty / meaning unverified)")
    return (PASS, "all endpoints PASS")


def judge_ws(collect, summary, required, evidence, stall_s):
    """-> dict of stage statuses with reasons"""
    stages = {}
    if collect["timed_out"]:
        for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready"):
            stages[k] = {"status": FAIL, "reason": "collection timed out"}
        return stages
    if not isinstance(summary, dict) or "exit_code" not in summary:
        for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready"):
            stages[k] = {"status": FAIL, "reason": "no valid collection summary (crash)"}
        return stages
    code = summary["exit_code"]
    if code in (2, 3):
        why = {2: "network permission refused", 3: "required REST startup data unavailable (e.g. HTTP 451); "
                                                   "streaming was never started"}[code]
        for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready"):
            stages[k] = {"status": NOT_RUN, "reason": why, "error": summary.get("error")}
        return stages
    ws = summary.get("ws_events_by_type") or {}
    missing = [t for t in required if ws.get(t, 0) <= 0]
    if code in (4, 5):
        stages["ws_received"] = {"status": FAIL, "reason": summary.get("error"), "missing_types": missing}
    elif missing:
        stages["ws_received"] = {"status": FAIL, "reason": "required WebSocket event types never arrived",
                                 "missing_types": missing}
    else:
        stages["ws_received"] = {"status": PASS, "reason": "all required types received over WebSocket",
                                 "counts": ws}
    n_ev, n_bad = summary.get("envelopes", 0), summary.get("rejected", 0)
    if n_ev + n_bad == 0:
        stages["ws_parsed"] = {"status": NOT_RUN, "reason": "no messages to parse"}
    elif n_bad == 0 and evidence["invalid_lines"] == 0:
        stages["ws_parsed"] = {"status": PASS, "reason": f"{n_ev} parsed, 0 rejected"}
    elif n_bad / (n_ev + n_bad) <= 0.01 and evidence["invalid_lines"] == 0:
        stages["ws_parsed"] = {"status": INCONCLUSIVE, "reason": f"{n_bad} of {n_ev + n_bad} rejected (<=1%)",
                               "rejected_by_error": summary.get("rejected_by_error")}
    else:
        stages["ws_parsed"] = {"status": FAIL, "reason": f"{n_bad} of {n_ev + n_bad} messages rejected",
                               "rejected_by_error": summary.get("rejected_by_error")}
    gap = summary.get("max_gap_s")
    if gap is None or not n_ev:
        stages["ws_fresh"] = {"status": NOT_RUN, "reason": "no data"}
    else:
        stages["ws_fresh"] = {"status": PASS if gap <= stall_s else FAIL,
                              "reason": f"largest gap between messages {gap}s (limit {stall_s}s)"}
    end = summary.get("end_of_collection")
    if not end:
        stages["gate_ready"] = {"status": NOT_RUN, "reason": "no end-of-collection gate state captured"}
    elif end["readiness"] == "READY":
        stages["gate_ready"] = {"status": PASS, "reason": f"core types {end['core_types']} READY at end of data",
                                "per_type": end["per_type"]}
    elif end["readiness"] == "UNVERIFIED":
        stages["gate_ready"] = {"status": INCONCLUSIVE, "reason": "a required type is UNVERIFIED",
                                "per_type": end["per_type"]}
    else:
        stages["gate_ready"] = {"status": FAIL, "reason": f"gate {end['readiness']} at end of data",
                                "per_type": end["per_type"]}
    return stages


def verify(source: str, logdir: Path, seconds: int, timeout: int):
    symbol, flags, required = SOURCES[source]
    base = [sys.executable, "-m", "src.stage0.live", "--source", source, "--symbol", symbol, *flags]
    prefix = source.replace("-", "_")
    check = run_command(prefix + ".rest_check", base + ["--check"], logdir, timeout)
    check_result = read_json_stdout(logdir / check["stdout_file"])
    rows = check_result.get("endpoints", []) if isinstance(check_result, dict) else []
    rest_status, rest_reason = judge_rest(check, rows)
    output = logdir / (prefix + ".events.jsonl")
    collect = run_command(prefix + ".websocket_collect", base + ["--seconds", str(seconds), "--out", str(output)],
                          logdir, timeout)
    summary = read_json_stdout(logdir / collect["stdout_file"])
    evidence = file_evidence(output)
    stages = {"rest": {"status": rest_status, "reason": rest_reason, "endpoints": rows}}
    stages.update(judge_ws(collect, summary, required, evidence, 20.0))
    blocked = bool(rows) and all(isinstance(r, dict) and r.get("http_status") == 451 for r in rows)
    return {"source": source, "symbol": symbol, "environment_blocked": blocked,
            "blocked_reason": "all REST requests returned HTTP 451 (restricted region/legal; not bypassed)" if blocked else None,
            "status": worst([s["status"] for s in stages.values()]),
            "stages": stages, "rest_check_run": check, "collection_run": collect,
            "collection_summary": summary, "events_file": evidence, "required_ws_types": list(required),
            "notes": "PASS proves public REST+WS data in THIS runner only; it is not trading readiness."}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default="stage0-live-evidence")
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--timeout-seconds", type=int, default=150)
    p.add_argument("--approve-toobit-public-readonly", action="store_true",
                   help="explicitly approve unauthenticated Toobit public market data GET/WS")
    p.add_argument("--binance-only", action="store_true")
    p.add_argument("--github-annotations", action="store_true", help="also print ::notice:: lines (CI summary)")
    p.add_argument("--sources", nargs="*", choices=list(SOURCES), help="restrict to these sources")
    args = p.parse_args(argv)
    if not 15 <= args.seconds <= 120:
        p.error("--seconds must be 15..120")
    logdir = Path(args.output_dir)
    logdir.mkdir(parents=True, exist_ok=True)
    sources = args.sources or ["binance-spot", "binance-futures"] + (
        ["toobit-futures"] if not args.binance_only and args.approve_toobit_public_readonly else [])
    if "toobit-futures" in sources and not args.approve_toobit_public_readonly:
        p.error("toobit-futures requires --approve-toobit-public-readonly")
    records = [verify(s, logdir, args.seconds, args.timeout_seconds) for s in sources]
    overall = worst([r["status"] for r in records])
    report = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "python": sys.version.split()[0],
              "verification_seconds": args.seconds, "trading_or_account_requests": False,
              "tested_sources": sources, "overall": overall, "results": records}
    target = logdir / "verification-report.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    for r in records:
        line = ", ".join(f"{k}={v['status']}" for k, v in r["stages"].items())
        print(f"{r['source']}: {r['status']} ({line})")
    print(f"Overall: {overall}; evidence: {target}")
    if args.github_annotations:
        for r in records:
            line = ", ".join(f"{k}={v['status']}" for k, v in r["stages"].items())
            blocked = " [environment_blocked: HTTP 451]" if r.get("environment_blocked") else ""
            print(f"::notice title=stage0 {r['source']}::{r['status']}{blocked} ({line})")
            for k, v in r["stages"].items():
                if v["status"] != PASS and k != "rest":
                    print(f"::notice title=stage0 {r['source']} {k}::{v['status']}: {str(v.get('reason'))[:200]}")
            rest = r["stages"]["rest"]
            for e in rest.get("endpoints", []):
                if isinstance(e, dict) and e.get("verdict") != PASS:
                    print(f"::notice title=stage0 {r['source']} REST {e.get('endpoint')}::{e.get('verdict')} http={e.get('http_status')} {str(e.get('reason'))[:160]}")
    counted = [r for r in records if not r.get("environment_blocked")]
    if any(r["status"] == FAIL for r in counted):
        return 1
    return 0 if overall == PASS else 2


if __name__ == "__main__":
    sys.exit(main())
