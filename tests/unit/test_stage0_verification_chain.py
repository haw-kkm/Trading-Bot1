"""Verification chain: collector process -> collector summary -> JSONL evidence -> verifier verdict.
Any missing or contradictory link must make a final PASS impossible. Deterministic, no network.

Defect-reproducing tests (they FAIL on the previous verifier because it returned PASS):
  test_time_only_rest_*, test_duplicate_required_*, test_rest_exit_code_*, test_rest_timeout_*,
  test_evidence_without_sha_*, test_evidence_without_line_count_*, test_process_and_summary_exit_*.
Tests that only need new API/behaviour (would fail on old code only because the function signature/streaming
helper did not exist): test_large_jsonl_*, test_oversized_line_*, test_unknown_endpoint_*."""
import hashlib
import importlib.util
import json
import pathlib
import tracemalloc

import pytest

from src.stage0 import live
from src.stage0.envelope import Source

ROOT = pathlib.Path(live.__file__).parents[2]


def _tool():
    spec = importlib.util.spec_from_file_location("stage0_live_verify", ROOT / "tools" / "stage0_live_verify.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


T = _tool()
OK = {"exit_code": 0, "timed_out": False}
SPOT = live.required_rest_endpoints(Source.BINANCE_SPOT)


def rows(verdict="PASS", eps=SPOT, status=200):
    return [{"endpoint": e, "verdict": verdict, "http_status": status} for e in eps]


# ===================================================================== REST contract
def test_contract_is_derived_from_the_check_request_table():
    assert live.required_rest_endpoints(Source.BINANCE_FUTURES) == [
        "/fapi/v1/time", "/fapi/v1/ticker/price", "/fapi/v1/depth", "/fapi/v1/openInterest", "/fapi/v1/fundingRate",
        "/futures/data/globalLongShortAccountRatio"]
    assert live.required_rest_endpoints(Source.TOOBIT_FUTURES)[0] == "/api/v1/time" and len(
        live.required_rest_endpoints(Source.TOOBIT_FUTURES)) == 10


def test_time_only_rest_is_never_pass():
    st, why = T.judge_rest(OK, rows(eps=SPOT[:1]), SPOT)
    assert st == "FAIL" and "never reported" in why


def test_all_required_rest_endpoints_pass():
    assert T.judge_rest(OK, rows(), SPOT)[0] == "PASS"


def test_duplicate_required_endpoint_is_not_pass():
    st, why = T.judge_rest(OK, rows() + rows(eps=SPOT[:1]), SPOT)
    assert st == "FAIL" and "more than once" in why


def test_unknown_endpoint_cannot_replace_a_required_one_and_is_reported():
    extra = rows(eps=["/x/unknown"])
    assert T.judge_rest(OK, rows(eps=SPOT[:2]) + extra, SPOT)[0] == "FAIL"                 # still incomplete
    assert T.judge_rest(OK, rows() + extra, SPOT)[0] == "PASS"                             # complete: extras ignored
    assert T.rest_contract(rows() + extra, SPOT)["unknown"] == ["/x/unknown"]
    assert T.judge_rest(OK, rows() + rows("FAIL", ["/x/unknown"]), SPOT)[0] == "FAIL"      # exit 0 but FAIL row: contradiction


def test_rest_exit_code_abnormal_is_not_pass_even_with_good_rows():
    for code in (2, 3, -9, None, 139):
        assert T.judge_rest({"exit_code": code, "timed_out": False}, rows(), SPOT)[0] == "FAIL"


def test_rest_timeout_is_not_pass_even_with_good_rows():
    st, why = T.judge_rest({"exit_code": None, "timed_out": True}, rows(), SPOT)
    assert st == "FAIL" and "timed out" in why


def test_rest_exit_code_must_agree_with_verdicts():
    assert T.judge_rest({"exit_code": 1, "timed_out": False}, rows(), SPOT)[0] == "FAIL"     # exit 1 but no FAIL row
    assert T.judge_rest({"exit_code": 0, "timed_out": False}, rows("FAIL", status=500), SPOT)[0] == "FAIL"


def test_rest_without_a_contract_cannot_pass():
    assert T.judge_rest(OK, rows(), None)[0] == "INCONCLUSIVE"


def test_http_451_only_keeps_environment_block_semantics():
    st, why = T.judge_rest({"exit_code": 1, "timed_out": False}, rows("FAIL", status=451), SPOT)
    assert st == "FAIL" and "451" in why


def test_http_451_mixed_with_real_error_is_plain_failure():
    mixed = rows("FAIL", status=451)
    mixed[-1]["http_status"] = 500
    st, why = T.judge_rest({"exit_code": 1, "timed_out": False}, mixed, SPOT)
    assert st == "FAIL" and "451" not in why


# ===================================================================== evidence: attestation
def env_line(etype="TRADE", i=0):
    return json.dumps({"source": "s", "symbol": "x", "event_type": etype, "event_time": None, "received_at": "t",
                       "schema_version": "1", "raw_payload": {"i": i}, "normalized_payload": {}, "digest": "d"}) + "\n"


def write(path, n, etype="TRADE"):
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for i in range(n):
            fh.write(env_line(etype, i))
    b = pathlib.Path(path).read_bytes()
    return {"lines": n, "sha256": hashlib.sha256(b).hexdigest()}


def summ(n, att="auto", etype="TRADE"):
    s = {"exit_code": 0, "envelopes": n, "rejected": 0, "ws_events_by_type": {etype: n}, "rest_events_by_type": {}}
    if att != "auto":
        s["events_file_written"] = att
    return s


def test_evidence_good_with_full_attestation_passes(tmp_path):
    f = tmp_path / "e.jsonl"
    att = write(f, 7)
    ev = T.evidence_integrity(f, dict(summ(7, att)))
    assert ev["status"] == "PASS" and ev["sha256_matches_attestation"] is True and ev["attested_lines"] == 7


def test_evidence_without_sha_is_not_pass(tmp_path):
    f = tmp_path / "e.jsonl"
    att = write(f, 7)
    ev = T.evidence_integrity(f, summ(7, {"lines": att["lines"]}))
    assert ev["status"] == "INCONCLUSIVE" and ev["kind"] == "attestation_missing" and ev["sha256_attested"] is False


def test_evidence_without_any_attestation_is_not_pass(tmp_path):
    f = tmp_path / "e.jsonl"
    write(f, 7)
    assert T.evidence_integrity(f, summ(7, att="auto"))["status"] == "INCONCLUSIVE"
    assert T.evidence_integrity(f, summ(7, None))["status"] == "INCONCLUSIVE"


def test_evidence_without_line_count_is_not_pass(tmp_path):
    f = tmp_path / "e.jsonl"
    att = write(f, 7)
    ev = T.evidence_integrity(f, summ(7, {"sha256": att["sha256"]}))
    assert ev["status"] == "INCONCLUSIVE" and ev["attested_lines"] is None


def test_attested_line_count_mismatch_is_fail(tmp_path):
    f = tmp_path / "e.jsonl"
    att = write(f, 7)
    ev = T.evidence_integrity(f, summ(7, {"lines": 9, "sha256": att["sha256"]}))
    assert ev["status"] == "FAIL" and ev["kind"] == "attested_lines_mismatch"


def test_sha256_mismatch_is_fail_and_missing_file_is_distinct(tmp_path):
    f = tmp_path / "e.jsonl"
    att = write(f, 7)
    ev = T.evidence_integrity(f, summ(7, {"lines": 7, "sha256": "0" * 64}))
    assert ev["status"] == "FAIL" and ev["kind"] == "sha256_mismatch"
    gone = T.evidence_integrity(tmp_path / "nope.jsonl", summ(7, att))
    assert gone["status"] == "INCONCLUSIVE" and gone["kind"] == "missing"


def test_process_and_summary_exit_codes_must_agree():
    ev = {"status": "PASS", "reason": "x"}
    s = {"exit_code": 0, "envelopes": 5, "rejected": 0, "max_gap_s": 1.0, "ws_events_by_type": {"TRADE": 5}}
    st = T.judge_ws({"timed_out": False, "exit_code": 6}, s, ("TRADE",), ev, 20.0)
    assert st["runtime"]["status"] == "FAIL" and "contradicts" in st["runtime"]["reason"]
    assert all(v["status"] != "PASS" for v in st.values())
    st = T.judge_ws({"timed_out": False, "exit_code": None}, s, ("TRADE",), ev, 20.0)
    assert all(v["status"] != "PASS" for v in st.values())


# ===================================================================== evidence: streaming
def test_large_jsonl_is_validated_in_constant_memory(tmp_path):
    f = tmp_path / "big.jsonl"
    n = 120_000
    att = write(f, n)
    size = f.stat().st_size
    assert size > 20_000_000
    tracemalloc.start()
    ev = T.evidence_integrity(f, summ(n, att))
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    assert ev["status"] == "PASS" and ev["recorded_count"] == n and ev["types"] == {"TRADE": n}
    assert peak < size / 20                                        # nowhere near the file size
    assert ev["sha256"] == att["sha256"]


def test_corrupt_large_jsonl_is_fail(tmp_path):
    f = tmp_path / "big.jsonl"
    n = 30_000
    att = write(f, n)
    data = bytearray(f.read_bytes())
    mid = len(data) // 2
    data[mid:mid + 5] = b"#####"                                   # damage one line in the middle
    f.write_bytes(bytes(data))
    ev = T.evidence_integrity(f, summ(n, att))
    assert ev["status"] == "FAIL" and ev["invalid_lines"] >= 1


def test_oversized_line_is_bounded_and_fail(tmp_path, monkeypatch):
    f = tmp_path / "o.jsonl"
    f.write_bytes((env_line() + "x" * 5000 + "\n" + env_line()).encode())
    monkeypatch.setattr(T, "MAX_LINE_BYTES", 1000)
    ev = T.evidence_integrity(f, summ(2, {"lines": 3, "sha256": hashlib.sha256(f.read_bytes()).hexdigest()}))
    assert ev["status"] == "FAIL" and ev["kind"] == "oversized_line"


def test_truncated_last_line_in_streaming_scan(tmp_path):
    f = tmp_path / "t.jsonl"
    f.write_bytes((env_line() + env_line()).encode()[:-30])
    assert T.evidence_integrity(f, summ(2))["kind"] == "truncated"


# ===================================================================== whole chain through verify()
GOOD_SUMMARY = {
    "exit_code": 0, "error": None, "envelopes": 6, "rejected": 0, "max_gap_s": 1.0,
    "ws_events_by_type": {"TRADE": 2, "BOOK_TICKER": 2, "ORDERBOOK_DELTA": 2}, "rest_events_by_type": {},
    "end_of_collection": {"readiness": "READY", "core_types": ["TRADE", "BOOK_TICKER"], "per_type": {}},
    "stream_freshness": {"window_start": "a", "window_end": "b", "window_s": 30.0, "by_type": {
        t: {"live_count": 2, "first_at": "a", "last_at": "b", "max_gap_s": 1.0, "end_age_s": 1.0}
        for t in ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA")}}}


def chain(tmp_path, monkeypatch, rest_rows=None, summary=None, collect_code=None, write_file=True, drop_att=None):
    s = json.loads(json.dumps(summary or GOOD_SUMMARY))

    def fake_run(name, argv, logdir, timeout_s):
        out, err = logdir / f"{name}.stdout.json", logdir / f"{name}.stderr.log"
        err.write_text("", encoding="utf-8")
        if name.endswith("rest_check"):
            r = rest_rows if rest_rows is not None else rows()
            out.write_text(json.dumps({"endpoints": r}), encoding="utf-8")
            code = 1 if any(x["verdict"] == "FAIL" for x in r) else 0
        else:
            ev = logdir / "binance_spot.events.jsonl"
            if write_file:
                with open(ev, "w", encoding="utf-8", newline="\n") as fh:
                    for t in ("TRADE", "TRADE", "BOOK_TICKER", "BOOK_TICKER", "ORDERBOOK_DELTA", "ORDERBOOK_DELTA"):
                        fh.write(env_line(t))
                b = ev.read_bytes()
                s["events_file_written"] = {"lines": 6, "sha256": hashlib.sha256(b).hexdigest()}
                if drop_att:
                    s["events_file_written"].pop(drop_att)
            out.write_text(json.dumps(s), encoding="utf-8")
            code = s["exit_code"] if collect_code is None else collect_code
        return {"command": name, "exit_code": code, "timed_out": False, "stdout_file": out.name, "stderr_file": err.name}
    monkeypatch.setattr(T, "run_command", fake_run)
    rec = T.verify("binance-spot", tmp_path, 30, 10)
    return rec, T.exit_code([rec], T.worst([rec["status"]]))


def test_chain_all_links_present_passes(tmp_path, monkeypatch):
    rec, code = chain(tmp_path, monkeypatch)
    assert rec["status"] == "PASS" and code == 0, {k: v["status"] for k, v in rec["stages"].items()}


@pytest.mark.parametrize("label,kw", [
    ("rest incomplete", {"rest_rows": rows(eps=SPOT[:1])}),
    ("no events file", {"write_file": False}),
    ("no sha", {"drop_att": "sha256"}),
    ("no line count", {"drop_att": "lines"}),
    ("process exit != summary exit", {"collect_code": 6}),
])
def test_chain_any_broken_link_blocks_pass(tmp_path, monkeypatch, label, kw):
    rec, code = chain(tmp_path, monkeypatch, **kw)
    assert rec["status"] != "PASS" and code != 0, label


def test_chain_summary_claims_clean_but_runtime_unhealthy_blocks_pass(tmp_path, monkeypatch):
    s = dict(GOOD_SUMMARY, exit_code=6, unhealthy_reasons=["data stalled"])
    rec, code = chain(tmp_path, monkeypatch, summary=s)
    assert rec["stages"]["runtime"]["status"] == "FAIL" and code == 1


# ===================================================================== boundary conditions (restore pass)
def test_required_pass_plus_unknown_endpoint_fail_is_not_rest_pass():
    r = rows() + rows("FAIL", ["/x/unknown"], status=500)
    st, why = T.judge_rest({"exit_code": 1, "timed_out": False}, r, SPOT)         # consistent exit code, still not PASS
    assert st == "FAIL" and "outside the contract" in why


def test_required_pass_plus_clean_exit_zero_is_pass():
    assert T.judge_rest({"exit_code": 0, "timed_out": False}, rows(), SPOT)[0] == "PASS"


def test_required_missing_or_duplicated_is_fail():
    assert T.judge_rest(OK, rows(eps=SPOT[:-1]), SPOT)[0] == "FAIL"
    assert T.judge_rest(OK, rows() + rows(eps=SPOT[-1:]), SPOT)[0] == "FAIL"


def test_unknown_inconclusive_endpoint_keeps_rest_from_passing():
    assert T.judge_rest(OK, rows() + rows("INCONCLUSIVE", ["/x/unknown"]), SPOT)[0] == "INCONCLUSIVE"


GOOD_EV = {"status": "PASS", "reason": "x"}


def _ws(proc, summary_code):
    s = dict(GOOD_SUMMARY, exit_code=summary_code)
    return T.judge_ws({"timed_out": False, "exit_code": proc}, s, ("TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA"), GOOD_EV, 20.0)


def test_exit_code_zero_with_good_evidence_can_pass():
    st = _ws(0, 0)
    assert all(v["status"] == "PASS" for v in st.values()), {k: v["status"] for k, v in st.items()}


@pytest.mark.parametrize("code", [7, -1, 1, 8, 255, True])
def test_undefined_collector_exit_code_is_fail_even_when_process_and_summary_agree(code):
    st = _ws(code, code)
    assert st["runtime"]["status"] == "FAIL" and "undefined" in st["runtime"]["reason"]
    assert all(v["status"] == "FAIL" for v in st.values())


@pytest.mark.parametrize("proc,summ_code", [(6, 0), (0, 6)])
def test_process_summary_exit_mismatch_is_fail(proc, summ_code):
    st = _ws(proc, summ_code)
    assert st["runtime"]["status"] == "FAIL" and all(v["status"] != "PASS" for v in st.values())


@pytest.mark.parametrize("code,expect", [(2, "NOT_RUN"), (3, "NOT_RUN"), (4, "FAIL"), (5, "FAIL"), (6, "PASS")])   # 6: data did arrive; the runtime stage carries the failure
def test_defined_nonzero_codes_are_known_outcomes_but_never_pass(code, expect):
    st = _ws(code, code)
    assert "undefined" not in str(st.get("runtime", {}).get("reason", ""))
    assert st["ws_received"]["status"] == expect
    if code >= 4:
        assert st["runtime"]["status"] == "FAIL"
    assert st.get("runtime", {}).get("status", "n/a") != "PASS"


def test_workflow_is_manual_only_and_has_no_stale_branch_trigger():
    wf = (ROOT / ".github" / "workflows" / "stage0-public-live-verification.yml").read_text(encoding="utf-8")
    on = wf.split("\non:", 1)[1].split("\npermissions:", 1)[0]
    assert "workflow_dispatch" in on and "push" not in on and "verify-stage0-public-live" not in on
