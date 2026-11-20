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

SCOPES (reported NEXT TO the unchanged `overall` / exit code, never instead of them):
  core_collection     Core Public Market Data Collection: REST startup/contract rows the core uses, required WS
                      types received, parsing, per-type freshness, clean collector exit, JSONL line count + SHA-256,
                      gate READY, and every type the core uses verified (no UNVERIFIED type in core).
  extended_semantics  Meaning of data the core does NOT use (Toobit index contract mapping, open-interest unit /
                      aggregation). Excluded from core ONLY if core never uses it; each exclusion is listed with
                      its status and reason. NOT_APPLICABLE where a source has no such data.
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

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from src.stage0.envelope import Source  # noqa: E402  (after sys.path setup)
from src.stage0.live import (CORE_TYPES, EXTENDED_SEMANTIC_ENDPOINTS,  # noqa: E402
                             required_rest_endpoints)  # single source of truth: check_rest's request table

PASS, FAIL, INCONCLUSIVE, NOT_RUN = "PASS", "FAIL", "INCONCLUSIVE", "NOT_RUN"
NOT_APPLICABLE = "NOT_APPLICABLE"          # scope only: the source has no extended-semantics data
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


MAX_LINE_BYTES = 16 * 1024 * 1024      # one envelope line larger than this is treated as corrupt (bounded memory)


def _scan_jsonl(path: Path):
    """Single streaming pass (constant memory): SHA-256, line count, valid-envelope counts by type, bad lines,
    and whether the last line is missing its newline. Never holds more than one line (<= MAX_LINE_BYTES)."""
    h, lines, bad, oversize, types, unterminated = hashlib.sha256(), 0, 0, 0, Counter(), False
    with path.open("rb") as fh:
        while True:
            chunk = fh.readline(MAX_LINE_BYTES + 1)
            if not chunk:
                break
            h.update(chunk)
            lines += 1
            unterminated = not chunk.endswith(b"\n")
            if len(chunk) > MAX_LINE_BYTES and unterminated:        # oversized line: drain it without keeping it
                oversize += 1
                bad += 1
                while True:
                    more = fh.readline(1 << 20)
                    if not more:
                        break
                    h.update(more)
                    unterminated = not more.endswith(b"\n")
                    if not unterminated:
                        break
                continue
            if unterminated:
                continue                                            # truncated last line, reported separately
            try:
                obj = json.loads(chunk.decode("utf-8"))
                if not isinstance(obj, dict) or not ENVELOPE_KEYS <= set(obj) or not isinstance(obj["event_type"], str):
                    raise ValueError("not an envelope")
                types[obj["event_type"]] += 1
            except (ValueError, UnicodeDecodeError):
                bad += 1
    return {"sha256": h.hexdigest(), "lines": lines, "bad": bad, "oversize": oversize, "types": types,
            "truncated": unterminated}


def evidence_integrity(jsonl: Path, summary):
    """Reconcile the events JSONL (streamed) with the collector summary. PASS needs ALL of: file present, every
    line a valid envelope, summary count == recorded count, per-type counts match (ws+rest), the collector's
    attestation (lines AND sha256) present and equal to what is on disk. MISSING evidence -> INCONCLUSIVE;
    CORRUPT / TRUNCATED / any mismatch -> FAIL."""
    s = summary if isinstance(summary, dict) else {}
    summary_count = s.get("envelopes") if isinstance(s.get("envelopes"), int) else None
    out = {"status": FAIL, "kind": None, "file_present": False, "summary_count": summary_count, "lines": 0,
           "recorded_count": 0, "invalid_lines": 0, "counts_match": False, "types": {}, "sha256": None,
           "sha256_attested": False, "sha256_matches_attestation": None, "attested_lines": None, "reason": ""}
    if not jsonl.is_file():
        out.update(status=INCONCLUSIVE, kind="missing", reason="events file is missing: parsing cannot be proven")
        return out
    sc = _scan_jsonl(jsonl)
    types, recorded = sc["types"], sum(sc["types"].values())
    out.update(file_present=True, sha256=sc["sha256"], lines=sc["lines"], recorded_count=recorded,
               invalid_lines=sc["bad"] + (1 if sc["truncated"] else 0), types=dict(sorted(types.items())))
    att = s.get("events_file_written")
    att = att if isinstance(att, dict) else {}
    att_sha = att.get("sha256") if isinstance(att.get("sha256"), str) and att.get("sha256") else None
    att_lines = att.get("lines") if isinstance(att.get("lines"), int) and not isinstance(att.get("lines"), bool) else None
    out.update(sha256_attested=att_sha is not None, attested_lines=att_lines,
               sha256_matches_attestation=None if att_sha is None else att_sha == sc["sha256"])
    problems, missing = [], []
    if sc["truncated"]:
        out["kind"] = "truncated"
        problems.append("last line is cut off (no terminating newline)")
    if sc["bad"]:
        out["kind"] = out["kind"] or ("oversized_line" if sc["oversize"] else "corrupt")
        problems.append(f"{sc['bad']} line(s) are not valid envelopes" + (f" ({sc['oversize']} oversized)" if sc["oversize"] else ""))
    if summary_count is None:
        missing.append("collector summary has no event count to reconcile")
    elif recorded != summary_count:
        out["kind"] = out["kind"] or "count_mismatch"
        problems.append(f"summary says {summary_count} events, file holds {recorded}")
    ws_c, rest_c = s.get("ws_events_by_type"), s.get("rest_events_by_type")
    if ws_c is None and rest_c is None:
        missing.append("collector summary has no per-type counts")
    else:
        expected = Counter(ws_c or {}) + Counter(rest_c or {})
        if expected != types:
            diff = {t: [expected.get(t, 0), types.get(t, 0)] for t in sorted(set(expected) | set(types))
                    if expected.get(t, 0) != types.get(t, 0)}
            out["kind"] = out["kind"] or "type_count_mismatch"
            out["type_count_diff"] = diff
            problems.append(f"per-type counts differ (summary vs file, [expected, recorded]): {diff}")
    if att_lines is None:
        missing.append("collector did not record the number of lines it wrote")
    elif att_lines != sc["lines"]:
        out["kind"] = out["kind"] or "attested_lines_mismatch"
        problems.append(f"collector wrote {att_lines} lines, file has {sc['lines']}")
    if att_sha is None:
        missing.append("collector did not record a SHA-256 of the file it wrote")
    elif att_sha != sc["sha256"]:
        out["kind"] = out["kind"] or "sha256_mismatch"
        problems.append("file SHA-256 differs from the hash the collector recorded when writing it")
    out["counts_match"] = summary_count is not None and recorded == summary_count and not problems
    if problems:                                   # real damage / contradiction beats merely absent evidence
        out.update(status=FAIL, reason="; ".join(problems + missing))
        return out
    if missing:
        out.update(status=INCONCLUSIVE, kind="attestation_missing" if att_sha is None or att_lines is None else "summary_incomplete",
                   reason="evidence cannot be fully verified: " + "; ".join(missing))
        return out
    out.update(status=PASS, reason=f"{recorded} events reconcile with the summary (count, per-type, collector-attested "
                                   f"line count and SHA-256 match the file)")
    return out


def rest_contract(rows, required):
    """Compare the reported REST rows with the endpoint contract. Unknown endpoints never count toward it."""
    names = [r.get("endpoint") for r in rows if isinstance(r, dict)]
    c = Counter(names)
    req = list(required or [])
    return {"required": req, "missing": [e for e in req if c[e] == 0], "duplicated": sorted(e for e in req if c[e] > 1),
            "unknown": sorted(str(e) for e in c if e not in req), "malformed": len(rows) - len(names)}


def judge_rest(check, rows, required=None):
    """-> (status, reason). Fail-closed: the check run itself must have completed, and EXACTLY the required
    endpoints must each be reported once. `required=None` means the contract is unknown -> never PASS."""
    check = check if isinstance(check, dict) else {}
    if check.get("timed_out"):
        return (FAIL, "REST check subprocess timed out: the check is incomplete")
    code = check.get("exit_code")
    if code not in (0, 1):
        return (FAIL, f"REST check subprocess exit code {code!r} (expected 0, or 1 when an endpoint FAILed): crashed, refused or not run")
    if not isinstance(rows, list) or not rows:
        return (FAIL, "--check produced no endpoint rows (crash or timeout)")
    if any(not isinstance(r, dict) for r in rows):
        return (FAIL, "--check output contains malformed endpoint rows")
    any_fail = any(r.get("verdict") == "FAIL" for r in rows)
    if (code == 1) != any_fail:
        return (FAIL, f"REST check exit code {code} contradicts its endpoint verdicts "
                      f"({'FAIL rows present' if any_fail else 'no FAIL row'})")
    if required is None:
        return (INCONCLUSIVE, "no endpoint contract supplied: completeness of the REST check cannot be established")
    ct = rest_contract(rows, required)
    if ct["duplicated"]:
        return (FAIL, f"required endpoint(s) reported more than once: {ct['duplicated']}")
    if ct["missing"]:
        return (FAIL, f"required endpoint(s) never reported (check incomplete): {ct['missing']}")
    req_set = set(required)
    extra = [r for r in rows if r.get("endpoint") not in req_set]           # outside the contract: cannot stand in for it...
    rows = [r for r in rows if r.get("endpoint") in req_set]
    bad_extra = [r.get("endpoint") for r in extra if r.get("verdict") == FAIL or r.get("verdict") not in (PASS, INCONCLUSIVE)]
    if bad_extra:                                                           # ...but its failure is never ignored
        return (FAIL, f"endpoint(s) outside the contract FAILed or have no valid verdict: {sorted(map(str, bad_extra))}")
    v = Counter(r.get("verdict") for r in rows)
    statuses = Counter(r.get("http_status") for r in rows)
    if v["FAIL"]:
        if statuses[451] == len(rows):
            return (FAIL, "every request returned HTTP 451 (access restricted by region/legal; not bypassed)")
        return (FAIL, f"{v['FAIL']} of {len(rows)} endpoints FAIL")
    if any(r.get("verdict") not in (PASS, INCONCLUSIVE) for r in rows):
        return (FAIL, "an endpoint has no valid verdict")
    if not any(r.get("verdict") == PASS and r.get("endpoint", "").endswith("/time") for r in rows):
        return (FAIL, "server-time endpoint did not PASS")
    if v["INCONCLUSIVE"] or any(r.get("verdict") == INCONCLUSIVE for r in extra):
        return (INCONCLUSIVE, f"{v['INCONCLUSIVE'] + sum(r.get('verdict') == INCONCLUSIVE for r in extra)} endpoint(s) INCONCLUSIVE (empty / meaning unverified)")
    return (PASS, f"all {len(required)} required endpoints reported once and PASS")


# 0 ok | 2 network permission refused | 3 required REST start data unavailable | 4 reconnect exhausted
# | 5 data integrity / internal error | 6 completed but unhealthy. Anything else is NOT a known outcome -> FAIL.
DEFINED_COLLECTOR_EXIT_CODES = frozenset({0, 2, 3, 4, 5, 6})
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
    proc = collect.get("exit_code")
    if proc != code:                         # the process and its own summary disagree: nothing here is trustworthy
        why = f"collection subprocess exit code {proc!r} contradicts summary exit_code {code!r}"
        for k in names:
            stages[k] = {"status": FAIL, "reason": why}
        stages["runtime"] = {"status": FAIL, "exit_code": proc, "reason": why}
        return stages
    if not (isinstance(code, int) and not isinstance(code, bool) and code in DEFINED_COLLECTOR_EXIT_CODES):
        why = f"undefined collector exit code {code!r} (defined: {sorted(DEFINED_COLLECTOR_EXIT_CODES)}): not a known outcome"
        for k in names:
            stages[k] = {"status": FAIL, "reason": why}
        stages["runtime"] = {"status": FAIL, "exit_code": code, "reason": why}
        return stages
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


def judge_rest_core(check, rows, required, excluded):
    """judge_rest restricted to the endpoints core uses. The integrity preconditions are checked on the FULL report
    (check finished, exit code consistent with ALL rows, rows well-formed); only then are the excluded endpoints
    set aside. A FAIL in an excluded row therefore cannot hide a broken check run."""
    check = check if isinstance(check, dict) else {}
    excluded = set(excluded or ())
    if check.get("timed_out") or check.get("exit_code") not in (0, 1) or not isinstance(rows, list) or not rows \
            or any(not isinstance(r, dict) for r in rows):
        return judge_rest(check, rows, required)                      # same FAIL reasons as the full judgement
    any_fail = any(r.get("verdict") == FAIL for r in rows)
    if (check.get("exit_code") == 1) != any_fail:
        return judge_rest(check, rows, required)                      # contradiction -> FAIL
    core_rows = [r for r in rows if r.get("endpoint") not in excluded]
    core_required = None if required is None else [e for e in required if e not in excluded]
    core_code = 1 if any(r.get("verdict") == FAIL for r in core_rows) else 0
    return judge_rest(dict(check, exit_code=core_code), core_rows, core_required)


def _ext_scope(source):
    from src.stage0.envelope import Source as _S
    return dict(EXTENDED_SEMANTIC_ENDPOINTS.get(_S(source), {}))


def judge_extended(source, rows, extended_evidence, semantic_questions):
    """Extended Data Semantics: worst of the semantic REST rows and the extra evidence rows. Never PASS while an
    official semantic question is open. A missing semantic row is FAIL (the check is incomplete)."""
    scope = _ext_scope(source)
    if not scope:
        return {"status": NOT_APPLICABLE, "items": [], "reason": "source has no extended-semantics data"}
    by_ep = {r.get("endpoint"): r for r in rows if isinstance(r, dict)}
    items = []
    for ep, etype in scope.items():
        r = by_ep.get(ep)
        st = r.get("verdict") if r and r.get("verdict") in (PASS, INCONCLUSIVE, FAIL) else FAIL
        items.append({"item": ep, "data_type": etype, "status": st,
                      "reason": (r or {}).get("reason") or "semantic row missing from the REST check"})
    for e in extended_evidence or []:
        st = e.get("verdict") if isinstance(e, dict) and e.get("verdict") in (PASS, INCONCLUSIVE, FAIL) else FAIL
        items.append({"item": (e or {}).get("endpoint"), "data_type": "INDEX_PRICE (components)", "status": st,
                      "reason": (e or {}).get("reason"), "proves_contract_mapping": False})
    qs = semantic_questions if isinstance(semantic_questions, dict) else {}
    open_q = sorted(q for q, v in qs.items() if not isinstance(v, dict) or v.get("status") != "VERIFIED")
    if not qs:
        open_q = ["semantic question registry missing from the REST check output"]
    status = worst([i["status"] for i in items] + ([INCONCLUSIVE] if open_q else []))
    if any(isinstance(v, dict) and v.get("status") == "INVALID_EVIDENCE" for v in qs.values()):
        status = FAIL
    return {"status": status, "items": items, "open_official_questions": open_q,
            "reason": ("all extended semantics verified" if status == PASS else
                       f"{sum(i['status'] != PASS for i in items)} item(s) not PASS; open official questions: {open_q}")}


DOWNSTREAM_RESTRICTIONS = {
    "toobit-futures": [
        "INDEX_PRICE: not a verified per-contract index price (contract->index mapping unconfirmed); envelopes carry "
        "UNVERIFIED_SCHEMA + UNVERIFIED_CONTRACT_MAPPING, the gate never reports them READY.",
        "OPEN_INTEREST: raw size only, unit/aggregation UNVERIFIED (no BASE_ASSET/CONTRACTS/USD unit), never rescaled "
        "by contractMultiplier, never compared or summed with another exchange's open interest.",
        "No strategy / signal / order input may use these types until the official answers are recorded.",
    ],
}


def judge_core(source, stages, rest_core, summary, required_ws):
    """Core Public Market Data Collection. PASS needs every core check PASS. An extended type may be excluded only
    if the core never uses it; otherwise its semantics are part of core."""
    s = summary if isinstance(summary, dict) else {}
    ext = _ext_scope(source)
    from src.stage0.envelope import Source as _S
    core_gate = list(CORE_TYPES.get(_S(source), ()))
    used_by_core = set(core_gate) | set(required_ws)
    checks = {"rest_core": {"status": rest_core[0], "reason": rest_core[1]}}
    for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready", "evidence_integrity"):
        st = stages.get(k, {}).get("status", NOT_RUN)
        checks[k] = {"status": st, "reason": stages.get(k, {}).get("reason")}
    code = s.get("exit_code")
    if "runtime" in stages:
        checks["collector_exit"] = {"status": stages["runtime"]["status"], "reason": stages["runtime"].get("reason")}
    elif code == 0 and not isinstance(code, bool):
        checks["collector_exit"] = {"status": PASS, "reason": "collector exit code 0"}
    elif code in (2, 3):
        checks["collector_exit"] = {"status": NOT_RUN, "reason": f"collector exit code {code}: streaming not run"}
    else:
        checks["collector_exit"] = {"status": FAIL, "reason": f"collector exit code {code!r}"}
    per_type = ((s.get("end_of_collection") or {}).get("per_type")) or {}
    unver = sorted(t for t in used_by_core if per_type.get(t) == "UNVERIFIED")
    unknown = sorted(t for t in used_by_core if t not in per_type)
    if unver:
        sem = {"status": INCONCLUSIVE, "reason": f"core uses UNVERIFIED type(s) {unver}: core cannot PASS"}
    elif unknown:
        sem = {"status": INCONCLUSIVE if per_type else NOT_RUN,
               "reason": f"verification level of core type(s) {unknown} not established at end of data"}
    else:
        sem = {"status": PASS, "reason": f"every type the core uses is verified: {sorted(used_by_core)}"}
    checks["core_types_verified"] = sem
    excluded, absorbed = [], []
    for ep, etype in ext.items():
        if etype in used_by_core:
            absorbed.append(etype)                         # core needs it: cannot be excluded
            continue
        row = next((r for r in stages.get("rest", {}).get("endpoints", []) if isinstance(r, dict) and r.get("endpoint") == ep), {})
        excluded.append({"item": ep, "data_type": etype, "status": row.get("verdict") or NOT_RUN,
                         "reason": f"{etype} is not a core gate type nor a required WebSocket type: core never uses it; "
                                   f"its meaning is judged under extended_semantics ({row.get('reason') or 'no row'})",
                         "excluded_from": "core_collection", "used_by_core": False})
    if absorbed:
        checks["core_types_verified"] = {"status": FAIL if sem["status"] == FAIL else INCONCLUSIVE,
                                         "reason": f"extended type(s) {absorbed} are core inputs: cannot be excluded"}
    status = worst([c["status"] for c in checks.values()])
    bad = [k for k, c in checks.items() if c["status"] != PASS]
    return {"status": status, "checks": checks, "core_gate_types": core_gate, "required_ws_types": list(required_ws),
            "excluded": excluded, "excluded_types_used_by_core": absorbed,
            "reason": "core public market data collection verified" if status == PASS else f"not PASS: {bad}"}


def verify(source: str, logdir: Path, seconds: int, timeout: int):
    symbol, flags, required = SOURCES[source]
    base = [sys.executable, "-m", "src.stage0.live", "--source", source, "--symbol", symbol, *flags]
    prefix = source.replace("-", "_")
    check = run_command(prefix + ".rest_check", base + ["--check"], logdir, timeout)
    check_result = read_json_stdout(logdir / check["stdout_file"])
    rows = check_result.get("endpoints", []) if isinstance(check_result, dict) else []
    required_rest = required_rest_endpoints(Source(source))
    rest_status, rest_reason = judge_rest(check, rows, required_rest)
    output = logdir / (prefix + ".events.jsonl")
    collect = run_command(prefix + ".websocket_collect", base + ["--seconds", str(seconds), "--out", str(output)],
                          logdir, timeout)
    summary = read_json_stdout(logdir / collect["stdout_file"])
    evidence = evidence_integrity(output, summary)
    stages = {"rest": {"status": rest_status, "reason": rest_reason, "endpoints": rows,
                       "contract": rest_contract(rows, required_rest)}}
    stages.update(judge_ws(collect, summary, required, evidence, 20.0))
    ext_scope = _ext_scope(source)
    ext_evidence = check_result.get("extended_evidence", []) if isinstance(check_result, dict) else []
    sem_q = check_result.get("semantic_questions") if isinstance(check_result, dict) else None
    excludable = [ep for ep, t in ext_scope.items() if t not in set(CORE_TYPES.get(Source(source), ())) | set(required)]
    core = judge_core(source, stages, judge_rest_core(check, rows, required_rest, excludable), summary, required)
    extended = judge_extended(source, rows, ext_evidence, sem_q)
    # Environment-blocked ONLY if every REST request was HTTP 451 AND streaming was therefore never run
    # (every non-REST stage NOT_RUN). Any other failure next to a 451 is a real failure and is not excused.
    ct = stages["rest"]["contract"]
    all_451 = (bool(rows) and all(isinstance(r, dict) and r.get("http_status") == 451 for r in rows)
               and not (ct["missing"] or ct["duplicated"] or ct["malformed"]) and rest_status == FAIL
               and check.get("exit_code") == 1 and not check.get("timed_out"))
    blocked = all_451 and all(v["status"] == NOT_RUN for k, v in stages.items() if k != "rest")
    return {"source": source, "symbol": symbol, "environment_blocked": blocked,
            "blocked_reason": "all REST requests returned HTTP 451 (restricted region/legal; not bypassed)" if blocked else None,
            "status": worst([s["status"] for s in stages.values()]),
            "core_collection": core, "extended_semantics": extended,
            "excluded_from_core": core["excluded"],
            "downstream_restrictions": DOWNSTREAM_RESTRICTIONS.get(source, []) if extended["status"] not in (PASS, NOT_APPLICABLE) else [],
            "stages": stages, "rest_check_run": check, "collection_run": collect,
            "collection_summary": summary, "evidence_integrity": evidence, "required_ws_types": list(required),
            "gate_scope": {"gate_ready_means": "core event types READY at end of data - NOT full source verification",
                           "core_types": (stages.get("gate_ready", {}).get("scope") or {}).get("gate_core_types"),
                           "required_types_outside_gate": (stages.get("gate_ready", {}).get("scope") or {}).get(
                               "required_but_outside_gate")},
            "notes": "PASS proves public REST+WS data in THIS runner only; it is not trading readiness."}


def scope_status(statuses):
    """Worst status, ignoring NOT_APPLICABLE; all NOT_APPLICABLE -> NOT_APPLICABLE."""
    real = [x for x in statuses if x != NOT_APPLICABLE]
    return worst(real) if real else NOT_APPLICABLE


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
    lines += ["", f"**Scope** - core_collection: **{report.get('core_collection_status')}** | extended_semantics: "
                  f"**{report.get('extended_semantics_status')}** | overall (full, unchanged): **{report['overall']}**", "",
              "| Market | Core collection | Extended semantics | Excluded from core |", "|---|---|---|---|"]
    for r in report["results"]:
        exc = ", ".join(f"{e['data_type']} ({e['status']})" for e in r.get("excluded_from_core", [])) or "-"
        lines.append(f"| {r['source']} | {r.get('core_collection', {}).get('status', '-')} | "
                     f"{r.get('extended_semantics', {}).get('status', '-')} | {exc} |")
    for src_name, items in (report.get("downstream_restrictions") or {}).items():
        for it in items:
            lines.append(f"- downstream restriction ({src_name}): {it}")
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
              "fully_verified": code == 0,
              "core_collection_status": worst([(r.get("core_collection") or {}).get("status", NOT_RUN) for r in records]),
              "extended_semantics_status": scope_status([(r.get("extended_semantics") or {}).get("status", NOT_RUN)
                                                         for r in records]),
              "scope_note": "overall / exit_code are the FULL verification (unchanged meaning). core_collection_status "
                            "covers only the data the core uses; extended_semantics_status covers excluded meanings. "
                            "A core PASS is not full verification and not trading approval.",
              "excluded_from_core": [dict(e, source=r["source"]) for r in records for e in r.get("excluded_from_core") or []],
              "downstream_restrictions": {r["source"]: r["downstream_restrictions"] for r in records
                                          if r.get("downstream_restrictions")},
              "results": records}
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
    print(f"Scope: core_collection={report['core_collection_status']} extended_semantics={report['extended_semantics_status']}"
          f" (overall/exit code unchanged; core PASS is not full verification)")
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
