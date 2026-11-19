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
environment-blocked sources), 2 no such FAIL but some FAIL/NOT_RUN/INCONCLUSIVE remains. Exit 0 is the ONLY
code that means "everything required was proven"; 1 and 2 must both be treated as NOT verified by CI.

Extra stages: evidence_integrity (the events JSONL really exists, parses, and reconciles with the collector's
own counts/SHA-256) and runtime (a fatal/unhealthy collector exit is never hidden by a clean-looking log).
ws_fresh is judged PER REQUIRED EVENT TYPE (live, accepted events only), not from the overall message gap.
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


ENVELOPE_KEYS = {"source", "symbol", "event_type", "event_time", "received_at", "schema_version",
                 "raw_payload", "normalized_payload", "digest"}


def evidence_integrity(jsonl: Path, summary):
    """Reconcile the events JSONL with the collector summary. Distinguishes MISSING (cannot judge ->
    INCONCLUSIVE) from CORRUPT / TRUNCATED / COUNT MISMATCH (FAIL). Never PASS without real event evidence."""
    s = summary if isinstance(summary, dict) else {}
    summary_count = s.get("envelopes") if isinstance(s.get("envelopes"), int) else None
    out = {"status": FAIL, "kind": None, "file_present": False, "summary_count": summary_count, "lines": 0,
           "recorded_count": 0, "invalid_lines": 0, "counts_match": False, "types": {}, "sha256": None,
           "sha256_attested": False, "sha256_matches_attestation": None, "reason": ""}
    if not jsonl.is_file():
        out.update(status=INCONCLUSIVE, kind="missing", reason="events file is missing: parsing cannot be proven")
        return out
    raw = jsonl.read_bytes()
    out.update(file_present=True, sha256=hashlib.sha256(raw).hexdigest())
    parts = raw.split(b"\n")
    truncated = bool(parts[-1])            # last line lacks its newline: the writer always ends lines with LF
    lines = parts if truncated else parts[:-1]
    types, bad = Counter(), 0
    for i, ln in enumerate(lines):
        if truncated and i == len(lines) - 1:
            continue                       # reported as truncation below
        try:
            obj = json.loads(ln.decode("utf-8"))
            if not isinstance(obj, dict) or not ENVELOPE_KEYS <= set(obj) or not isinstance(obj["event_type"], str):
                raise ValueError("not an envelope")
            types[obj["event_type"]] += 1
        except (ValueError, UnicodeDecodeError):
            bad += 1
    recorded = sum(types.values())
    out.update(lines=len(lines), invalid_lines=bad + (1 if truncated else 0), recorded_count=recorded,
               types=dict(sorted(types.items())))
    att = s.get("events_file_written")
    if isinstance(att, dict) and att.get("sha256"):
        out.update(sha256_attested=True, sha256_matches_attestation=att["sha256"] == out["sha256"])
    problems = []
    if truncated:
        out["kind"] = "truncated"
        problems.append("last line is cut off (no terminating newline)")
    if bad:
        out["kind"] = out["kind"] or "corrupt"
        problems.append(f"{bad} line(s) are not valid envelopes")
    if summary_count is None:
        out["kind"] = out["kind"] or "no_summary_count"
        out["status"] = FAIL if problems else INCONCLUSIVE
        out["reason"] = "; ".join(problems) or "collector summary has no event count to reconcile"
        return out
    if recorded != summary_count:
        out["kind"] = out["kind"] or "count_mismatch"
        problems.append(f"summary says {summary_count} events, file holds {recorded}")
    ws_c, rest_c = s.get("ws_events_by_type"), s.get("rest_events_by_type")
    if ws_c is not None or rest_c is not None:
        expected = Counter(ws_c or {}) + Counter(rest_c or {})
        if expected != types:
            diff = {t: [expected.get(t, 0), types.get(t, 0)] for t in sorted(set(expected) | set(types))
                    if expected.get(t, 0) != types.get(t, 0)}
            out["kind"] = out["kind"] or "type_count_mismatch"
            out["type_count_diff"] = diff
            problems.append(f"per-type counts differ (summary vs file, [expected, recorded]): {diff}")
    if out["sha256_matches_attestation"] is False:
        out["kind"] = out["kind"] or "sha256_mismatch"
        problems.append("file SHA-256 differs from the hash the collector recorded when writing it")
    out["counts_match"] = recorded == summary_count and not problems
    if problems:
        out.update(status=FAIL, reason="; ".join(problems))
        return out
    out.update(status=PASS, reason=f"{recorded} events reconcile with the summary (count, per-type"
               + (", SHA-256 attested)" if out["sha256_attested"] else "; no SHA-256 attestation in summary)"))
    return out


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


DEFAULT_STREAM_LIMIT_S = 20.0


def judge_stream_freshness(summary, required, stall_s, limits=None):
    """Per required event type: live (accepted, non-BACKFILL, WebSocket) events must have kept arriving. The
    overall message gap is reported separately and can never rescue a silent required stream."""
    limits = limits or {}
    sf = summary.get("stream_freshness")
    overall_gap = summary.get("max_gap_s")
    all_msgs = ({"status": PASS if overall_gap <= stall_s else FAIL, "max_gap_s": overall_gap, "limit_s": stall_s}
                if overall_gap is not None else {"status": NOT_RUN, "max_gap_s": None})
    if not isinstance(sf, dict) or not isinstance(sf.get("by_type"), dict):
        return {"status": FAIL if all_msgs["status"] == FAIL else INCONCLUSIVE, "all_messages": all_msgs, "by_type": {},
                "reason": "no per-stream freshness data in the collector summary: freshness of each required stream "
                          "cannot be established from the overall message gap"}
    by_type = {}
    for t in required:
        lim = limits.get(t, stall_s)
        rec = sf["by_type"].get(t)
        if rec is None:
            by_type[t] = {"status": FAIL, "limit_s": lim, "live_count": 0,
                          "reason": "no live WebSocket event accepted (none arrived, or only BACKFILL/duplicate/"
                                    "out-of-order/bad-timestamp events)"}
            continue
        ok = rec["max_gap_s"] <= lim and rec["end_age_s"] <= lim
        by_type[t] = dict(rec, status=PASS if ok else FAIL, limit_s=lim,
                          reason=None if ok else f"max gap {rec['max_gap_s']}s / end age {rec['end_age_s']}s exceeds {lim}s")
    status = worst([v["status"] for v in by_type.values()] + [all_msgs["status"]])
    bad = [t for t, v in by_type.items() if v["status"] != PASS]
    return {"status": status, "all_messages": all_msgs, "by_type": by_type,
            "window": {k: sf.get(k) for k in ("window_start", "window_end", "window_s")},
            "reason": ("all required streams fresh" if status == PASS else
                       f"stream(s) stalled or silent: {bad}" if bad else f"overall message gap {overall_gap}s > {stall_s}s")}


def judge_ws(collect, summary, required, evidence, stall_s, limits=None):
    """-> dict of stage statuses with reasons. `evidence` is the result of evidence_integrity()."""
    stages = {}
    names = ("ws_received", "ws_parsed", "ws_fresh", "gate_ready")
    if collect["timed_out"]:
        for k in names:
            stages[k] = {"status": FAIL, "reason": "collection timed out"}
        return stages
    if not isinstance(summary, dict) or "exit_code" not in summary:
        for k in names:
            stages[k] = {"status": FAIL, "reason": "no valid collection summary (crash)"}
        return stages
    code = summary["exit_code"]
    if code in (2, 3):
        why = {2: "network permission refused", 3: "required REST startup data unavailable (e.g. HTTP 451); "
                                                   "streaming was never started"}[code]
        for k in names:
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
    ev_status = evidence.get("status") if isinstance(evidence, dict) else None
    if n_ev + n_bad == 0:
        stages["ws_parsed"] = {"status": NOT_RUN, "reason": "no messages to parse"}
    elif n_ev > 0 and ev_status != PASS:
        stages["ws_parsed"] = {"status": FAIL if ev_status == FAIL else INCONCLUSIVE,
                               "reason": "event evidence is not verified, so parsing cannot be PASS: "
                                         + str((evidence or {}).get("reason") or "no evidence integrity result")}
    elif n_bad == 0:
        stages["ws_parsed"] = {"status": PASS, "reason": f"{n_ev} parsed, 0 rejected, evidence reconciled"}
    elif n_bad / (n_ev + n_bad) <= 0.01:
        stages["ws_parsed"] = {"status": INCONCLUSIVE, "reason": f"{n_bad} of {n_ev + n_bad} rejected (<=1%)",
                               "rejected_by_error": summary.get("rejected_by_error")}
    else:
        stages["ws_parsed"] = {"status": FAIL, "reason": f"{n_bad} of {n_ev + n_bad} messages rejected",
                               "rejected_by_error": summary.get("rejected_by_error")}
    if summary.get("max_gap_s") is None or not n_ev:
        stages["ws_fresh"] = {"status": NOT_RUN, "reason": "no data"}
    else:
        stages["ws_fresh"] = judge_stream_freshness(summary, required, stall_s, limits)
    end = summary.get("end_of_collection")
    core = list((end or {}).get("core_types") or [])
    scope = {"gate_core_types": core, "required_but_outside_gate": [t for t in required if t not in core],
             "meaning": "READY = the listed core types are present, fresh, sane and verified at end of data. It does "
                        "NOT prove REST reference-data meaning, order-book sequence integrity, 24h stability or "
                        "trade safety."}
    if not end:
        stages["gate_ready"] = {"status": NOT_RUN, "reason": "no end-of-collection gate state captured", "scope": scope}
    elif end["readiness"] == "READY":
        stages["gate_ready"] = {"status": PASS, "reason": f"core types {end['core_types']} READY at end of data",
                                "per_type": end["per_type"], "scope": scope,
                                "unverified_outside_gate": sorted(t for t, v in end["per_type"].items()
                                                                  if v == "UNVERIFIED" and t not in core)}
    elif end["readiness"] == "UNVERIFIED":
        stages["gate_ready"] = {"status": INCONCLUSIVE, "reason": "a required type is UNVERIFIED",
                                "per_type": end["per_type"], "scope": scope}
    else:
        stages["gate_ready"] = {"status": FAIL, "reason": f"gate {end['readiness']} at end of data",
                                "per_type": end["per_type"], "scope": scope}
    ei = (ev_status or INCONCLUSIVE) if n_ev + n_bad else NOT_RUN
    stages["evidence_integrity"] = {"status": ei, **{k: v for k, v in (evidence or {}).items() if k != "status"}}
    if code in (4, 5, 6):
        stages["runtime"] = {"status": FAIL, "exit_code": code,
                             "reason": summary.get("error") or "; ".join(summary.get("unhealthy_reasons") or [])
                             or f"collector exit code {code}"}
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
    evidence = evidence_integrity(output, summary)
    stages = {"rest": {"status": rest_status, "reason": rest_reason, "endpoints": rows}}
    stages.update(judge_ws(collect, summary, required, evidence, 20.0))
    # Environment-blocked ONLY if every REST request was HTTP 451 AND streaming was therefore never run
    # (every non-REST stage NOT_RUN). Any other failure next to a 451 is a real failure and is not excused.
    all_451 = bool(rows) and all(isinstance(r, dict) and r.get("http_status") == 451 for r in rows)
    blocked = all_451 and all(v["status"] == NOT_RUN for k, v in stages.items() if k != "rest")
    return {"source": source, "symbol": symbol, "environment_blocked": blocked,
            "blocked_reason": "all REST requests returned HTTP 451 (restricted region/legal; not bypassed)" if blocked else None,
            "status": worst([s["status"] for s in stages.values()]),
            "stages": stages, "rest_check_run": check, "collection_run": collect,
            "collection_summary": summary, "evidence_integrity": evidence, "required_ws_types": list(required),
            "gate_scope": {"gate_ready_means": "core event types READY at end of data - NOT full source verification",
                           "core_types": (stages.get("gate_ready", {}).get("scope") or {}).get("gate_core_types"),
                           "required_types_outside_gate": (stages.get("gate_ready", {}).get("scope") or {}).get(
                               "required_but_outside_gate")},
            "notes": "PASS proves public REST+WS data in THIS runner only; it is not trading readiness."}


def exit_code(records, overall):
    """0 = every required stage PASS. 1 = a real FAIL (not excused by an environment block).
    2 = no such FAIL, but full success is NOT proven (environment block / NOT_RUN / INCONCLUSIVE)."""
    if any(r["status"] == FAIL for r in records if not r.get("environment_blocked")):
        return 1
    return 0 if overall == PASS else 2


EXIT_MEANING = {0: "ALL_REQUIRED_PASS", 1: "REAL_FAILURE", 2: "NOT_FULLY_VERIFIED (environment block / NOT_RUN / INCONCLUSIVE)"}


def markdown_summary(report):
    icon = {PASS: "PASS", FAIL: "FAIL", INCONCLUSIVE: "INCONCLUSIVE", NOT_RUN: "NOT_RUN"}
    code = report["exit_code"]
    lines = ["## Stage 0 public-data verification", "",
             f"**Overall: {report['overall']}** - verification exit code `{code}` ({EXIT_MEANING[code]}).", ""]
    if code != 0:
        lines += ["> **Not verified.** This run did NOT prove every required stage. "
                  "Evidence was collected and uploaded, but the verification gate does not pass.", ""]
    lines += ["| Market | Source status | REST | WS received | Parsed | Fresh (per stream) | Evidence | Gate (core only) | Note |",
              "|---|---|---|---|---|---|---|---|---|"]
    for r in report["results"]:
        st = r["stages"]
        g = lambda k: icon.get(st.get(k, {}).get("status"), "-")
        note = "environment_blocked: HTTP 451 (not bypassed)" if r.get("environment_blocked") else ""
        lines.append(f"| {r['source']} | **{r['status']}** | {g('rest')} | {g('ws_received')} | {g('ws_parsed')} | "
                     f"{g('ws_fresh')} | {g('evidence_integrity')} | {g('gate_ready')} | {note} |")
    lines += ["", "`Gate READY` only means the core event types were fresh at end of data; it is not full source "
              "verification and not trading approval. Live trading remains NOT APPROVED.", ""]
    for r in report["results"]:
        for k, v in r["stages"].items():
            if v.get("status") != PASS:
                lines.append(f"- {r['source']} `{k}` = {v['status']}: {str(v.get('reason'))[:200]}")
    return "\n".join(lines) + "\n"


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--output-dir", default="stage0-live-evidence")
    p.add_argument("--seconds", type=int, default=30)
    p.add_argument("--timeout-seconds", type=int, default=150)
    p.add_argument("--approve-toobit-public-readonly", action="store_true",
                   help="explicitly approve unauthenticated Toobit public market data GET/WS")
    p.add_argument("--binance-only", action="store_true")
    p.add_argument("--github-annotations", action="store_true", help="also print ::notice:: lines (CI summary)")
    p.add_argument("--summary-file", default=None, help="append a Markdown result table here (e.g. $GITHUB_STEP_SUMMARY)")
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
    code = exit_code(records, overall)
    report = {"created_at_utc": datetime.now(timezone.utc).isoformat(), "python": sys.version.split()[0],
              "verification_seconds": args.seconds, "trading_or_account_requests": False,
              "tested_sources": sources, "overall": overall, "exit_code": code, "exit_meaning": EXIT_MEANING[code],
              "fully_verified": code == 0, "results": records}
    target = logdir / "verification-report.json"
    target.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    md = markdown_summary(report)
    (logdir / "verification-summary.md").write_text(md, encoding="utf-8")
    if args.summary_file:
        with open(args.summary_file, "a", encoding="utf-8") as fh:
            fh.write(md)
    for r in records:
        line = ", ".join(f"{k}={v['status']}" for k, v in r["stages"].items())
        print(f"{r['source']}: {r['status']} ({line})")
    print(f"Overall: {overall} (exit {code}: {EXIT_MEANING[code]}); evidence: {target}")
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
    return code


if __name__ == "__main__":
    sys.exit(main())
