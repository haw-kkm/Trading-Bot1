"""Regression tests: a failed/partial data collection must never be reported as PASS.

Covers (all deterministic, no network):
  * events JSONL evidence: missing / truncated / count mismatch / per-type mismatch / SHA mismatch / good
  * per-required-stream freshness (a live TRADE stream must not hide a dead BOOK_TICKER / order-book stream)
  * BACKFILL-only, duplicate-only and REST-only inputs never count as live WebSocket freshness
  * slow close handshake does not change the freshness verdict
  * runtime fatal error is not hidden by a clean-looking log
  * HTTP 451 handling and exit codes (all-451 -> 2, mixed with a real failure -> 1)
  * gate READY scope (core types only) and UNVERIFIED handling
Each defect test fails on the pre-fix code (judge_ws accepted a PASS from summary numbers alone)."""
import importlib.util
import json
import pathlib
from datetime import timedelta

import pytest

from src.stage0 import live
from src.stage0.adapters import toobit
from src.stage0.collector import Collector, PublicRestClient
from src.stage0.envelope import Source
from src.stage0.live import LiveRunner, PublicWsClient
from src.stage0.quality import DataQualityGate, Readiness
from tests.unit.test_stage0_live import APPROVED, T0, TSYM, Clock
from tests.unit.test_stage0_live import no_real_network  # noqa: F401  (autouse: sockets forbidden)

TB = Source.TOOBIT_FUTURES
REQ = ("TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "MARK_PRICE")


def _tool():
    spec = importlib.util.spec_from_file_location(
        "stage0_live_verify", pathlib.Path(live.__file__).parents[2] / "tools" / "stage0_live_verify.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


# ------------------------------------------------------------------ synthetic WebSocket
def _ms(clock):
    return int(clock().timestamp() * 1000)


class SynthConn:
    """One tick per second; each active type emits one message per tick. schedule: type -> options."""

    def __init__(self, clock, schedule, close_delay=0.0):
        self.clock, self.schedule, self.close_delay, self.queue, self.n = clock, schedule, close_delay, [], 0

    async def connect(self, url):
        pass

    async def send(self, text):
        pass

    def _msgs(self):
        t, ms = (self.clock() - T0).total_seconds(), _ms(self.clock)
        for typ, o in self.schedule.items():
            if not (o.get("start", 0) <= t < o.get("stop", 1e9)):
                continue
            self.n += 1
            if typ == "TRADE":
                tid = o.get("fixed_id") or 1000 + self.n
                yield {"symbol": TSYM, "topic": "trade", "params": {}, "f": bool(o.get("backfill")),
                       "data": [{"v": str(tid), "t": ms, "p": "62930", "q": "1.5", "m": True}]}
            elif typ == "BOOK_TICKER":
                yield {"topic": "bookTicker", "params": {}, "f": False,
                       "data": {"e": "bookTicker", "E": ms, "s": TSYM, "b": "62941", "bq": "1", "a": "62941.1", "aq": "1", "t": ms}}
            elif typ == "MARK_PRICE":
                yield {"topic": "markPrice", "params": {}, "f": False,
                       "data": {"exchangeId": 301, "symbolId": TSYM, "price": "62931.2", "time": ms}}
            elif typ == "ORDERBOOK_SNAPSHOT":
                yield {"topic": "depth", "symbol": TSYM, "f": False,
                       "data": [{"s": TSYM, "t": ms, "b": [["62930", "1"]], "a": [["62931", "1"]]}]}

    async def recv(self):
        if not self.queue:
            self.clock.advance(1.0)
            self.queue = list(self._msgs())
        return json.dumps(self.queue.pop(0)) if self.queue else json.dumps({"pong": 1})

    async def close(self):
        self.clock.advance(self.close_delay)        # slow close handshake (after end of data)


def run_toobit(schedule, seconds=30, close_delay=0.0):
    clock = Clock()
    rest = PublicRestClient(APPROVED, transport=lambda u, p: {})
    r = LiveRunner(Collector(), rest, PublicWsClient(APPROVED, lambda: SynthConn(clock, schedule, close_delay)),
                   clock=clock, sleep=clock.sleep)
    import asyncio
    asyncio.run(r.run_toobit(TSYM, seconds))
    return r, clock


ALL = {t: {} for t in REQ}


def judge(r, evidence=None, required=REQ, **summary_kw):
    t = _tool()
    s = {"exit_code": 0, "envelopes": r.c.envelope_count if hasattr(r.c, "envelope_count") else len(r.c.envelopes),
         "rejected": 0, "max_gap_s": round(r.max_gap_s, 1), "ws_events_by_type": dict(r.ws_counts),
         "rest_events_by_type": dict(r.rest_counts), "end_of_collection": r.end_state,
         "stream_freshness": r.stream_report()}
    s.update(summary_kw)
    return t.judge_ws({"timed_out": False, "exit_code": s["exit_code"]}, s, required, evidence or {"status": "PASS", "reason": "x"}, 20.0)


# ===================================================================== per-stream freshness
def test_all_streams_flowing_is_fresh_for_every_required_type():
    r, _ = run_toobit(ALL)
    st = judge(r)
    assert st["ws_fresh"]["status"] == "PASS"
    assert set(st["ws_fresh"]["by_type"]) == set(REQ)
    assert all(v["max_gap_s"] <= 2 and v["end_age_s"] <= 2 for v in st["ws_fresh"]["by_type"].values())


def test_trade_ok_but_book_ticker_stalled_fails_that_stream_not_hidden_by_overall_gap():
    r, _ = run_toobit({**ALL, "BOOK_TICKER": {"stop": 5}})
    assert r.max_gap_s <= 2                                     # overall message gap looks perfectly healthy
    f = judge(r)["ws_fresh"]
    assert f["all_messages"]["status"] == "PASS"
    assert f["by_type"]["BOOK_TICKER"]["status"] == "FAIL" and f["status"] == "FAIL"
    assert f["by_type"]["TRADE"]["status"] == "PASS"


def test_orderbook_stream_stalled_fails_required_stream():
    r, _ = run_toobit({**ALL, "ORDERBOOK_SNAPSHOT": {"stop": 4}})
    f = judge(r)["ws_fresh"]
    assert f["by_type"]["ORDERBOOK_SNAPSHOT"]["status"] == "FAIL" and f["status"] == "FAIL"
    assert f["by_type"]["ORDERBOOK_SNAPSHOT"]["end_age_s"] > 20


def test_stream_that_started_late_has_leading_gap_counted():
    r, _ = run_toobit({**ALL, "MARK_PRICE": {"start": 25}})
    assert judge(r)["ws_fresh"]["by_type"]["MARK_PRICE"]["status"] == "FAIL"


def test_slow_snapshot_stream_gets_its_own_limit_not_the_global_one():
    t = _tool()
    s = {"exit_code": 0, "envelopes": 10, "rejected": 0, "max_gap_s": 1.0,
         "stream_freshness": {"window_start": "a", "window_end": "b", "window_s": 30.0, "by_type": {
             "TRADE": {"max_gap_s": 1.0, "end_age_s": 1.0, "live_count": 5},
             "ORDERBOOK_SNAPSHOT": {"max_gap_s": 28.0, "end_age_s": 2.0, "live_count": 2}}}}
    assert t.judge_stream_freshness(s, ("TRADE", "ORDERBOOK_SNAPSHOT"), 20.0)["status"] == "FAIL"
    ok = t.judge_stream_freshness(s, ("TRADE", "ORDERBOOK_SNAPSHOT"), 20.0, {"ORDERBOOK_SNAPSHOT": 35.0})
    assert ok["status"] == "PASS" and ok["by_type"]["ORDERBOOK_SNAPSHOT"]["limit_s"] == 35.0


def test_summary_without_per_stream_data_cannot_be_fresh():
    t = _tool()
    st = t.judge_stream_freshness({"max_gap_s": 1.0}, REQ, 20.0)
    assert st["status"] == "INCONCLUSIVE"


def test_backfill_only_trades_are_not_live_freshness():
    r, _ = run_toobit({**ALL, "TRADE": {"backfill": True}})
    assert "TRADE" not in r.stream_report()["by_type"]
    assert r.ws_counts["TRADE"] > 0                              # received, but only as history
    assert judge(r)["ws_fresh"]["by_type"]["TRADE"]["status"] == "FAIL"


def test_repeated_duplicate_trade_does_not_fake_freshness():
    r, _ = run_toobit({**ALL, "TRADE": {"fixed_id": 4242}})
    rep = r.stream_report()["by_type"]["TRADE"]
    assert rep["live_count"] == 1 and rep["end_age_s"] > 20
    assert "DUPLICATE_EVENT" in r.c.gate.kinds()
    assert judge(r)["ws_fresh"]["by_type"]["TRADE"]["status"] == "FAIL"


def test_rest_events_never_count_as_ws_received_or_ws_fresh():
    r, clock = run_toobit({})                                     # nothing arrives over the socket
    envs = toobit.parse_ws({"topic": "markPrice", "params": {}, "f": False, "data": {
        "exchangeId": 301, "symbolId": TSYM, "price": "1", "time": _ms(clock)}}, clock())
    r._ingest_all(envs)                                           # default channel = REST
    assert r.rest_counts["MARK_PRICE"] == 1 and r.ws_counts["MARK_PRICE"] == 0
    assert "MARK_PRICE" not in r.stream_report()["by_type"]
    st = judge(r, required=("MARK_PRICE",))
    assert st["ws_received"]["status"] == "FAIL"


def test_slow_close_handshake_does_not_change_the_verdict():
    fast, _ = run_toobit(ALL)
    slow, clock = run_toobit(ALL, close_delay=12.0)
    assert clock() - T0 > timedelta(seconds=40)                   # the handshake really took 12 s
    assert slow.stream_report()["by_type"]["TRADE"]["end_age_s"] == fast.stream_report()["by_type"]["TRADE"]["end_age_s"]
    assert judge(slow)["ws_fresh"]["status"] == "PASS" and judge(slow)["gate_ready"]["status"] == "PASS"


def test_toobit_snapshot_only_refresh_is_judged_with_snapshot_cadence():
    r, _ = run_toobit({"ORDERBOOK_SNAPSHOT": {}})
    t = _tool()
    st = t.judge_stream_freshness({"max_gap_s": 1.0, "stream_freshness": r.stream_report()}, ("ORDERBOOK_SNAPSHOT",), 20.0)
    assert st["status"] == "PASS"


# ===================================================================== evidence integrity
def _lines(n, etype_flow=("BOOK_TICKER",)):
    ms = int(T0.timestamp() * 1000)
    out = []
    for i in range(n):
        msg = {"topic": "bookTicker", "params": {}, "f": False,
               "data": {"e": "bookTicker", "E": ms + i, "s": TSYM, "b": "62941", "bq": "1", "a": "62941.1", "aq": "1", "t": ms + i}}
        out.append(toobit.parse_ws(msg, T0 + timedelta(milliseconds=i))[0].to_json())
    return out


def _att(path):
    import hashlib
    b = pathlib.Path(path).read_bytes()
    return {"lines": b.count(b"\x0a"), "sha256": hashlib.sha256(b).hexdigest()}


def _summary(n, **kw):
    s = {"exit_code": 0, "envelopes": n, "rejected": 0, "ws_events_by_type": {"BOOK_TICKER": n}, "rest_events_by_type": {}}
    s.update(kw)
    return s


def test_missing_event_file_with_events_in_summary_is_never_pass(tmp_path):
    t = _tool()
    ev = t.evidence_integrity(tmp_path / "nope.jsonl", _summary(10))
    assert ev["status"] == "INCONCLUSIVE" and ev["kind"] == "missing" and ev["file_present"] is False
    st = t.judge_ws({"timed_out": False, "exit_code": 0}, dict(_summary(10), max_gap_s=1.0), ("BOOK_TICKER",), ev, 20.0)
    assert st["ws_parsed"]["status"] != "PASS" and st["evidence_integrity"]["status"] != "PASS"


def test_truncated_event_file_is_fail(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    body = "\n".join(_lines(5)) + "\n"
    f.write_text(body[: -40], encoding="utf-8")                   # cut in the middle of the last line
    ev = t.evidence_integrity(f, _summary(5))
    assert ev["status"] == "FAIL" and ev["kind"] == "truncated"
    st = t.judge_ws({"timed_out": False, "exit_code": 0}, dict(_summary(5), max_gap_s=1.0), ("BOOK_TICKER",), ev, 20.0)
    assert st["ws_parsed"]["status"] == "FAIL"


def test_truncation_at_a_line_boundary_is_caught_by_count(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(3)) + "\n", encoding="utf-8")
    ev = t.evidence_integrity(f, _summary(5))
    assert ev["status"] == "FAIL" and ev["kind"] == "count_mismatch" and not ev["counts_match"]
    assert ev["recorded_count"] == 3 and ev["summary_count"] == 5


def test_corrupt_line_and_non_envelope_lines_are_fail(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(2) + ["{not json", '{"event_type": "TRADE"}']) + "\n", encoding="utf-8")
    ev = t.evidence_integrity(f, _summary(4))
    assert ev["status"] == "FAIL" and ev["kind"] == "corrupt" and ev["invalid_lines"] == 2


def test_per_type_mismatch_between_summary_and_file_is_fail(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(4)) + "\n", encoding="utf-8")
    ev = t.evidence_integrity(f, _summary(4, ws_events_by_type={"TRADE": 4}))
    assert ev["status"] == "FAIL" and ev["kind"] == "type_count_mismatch"


def test_sha256_mismatch_with_collector_attestation_is_fail(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(2)) + "\n", encoding="utf-8")
    ev = t.evidence_integrity(f, _summary(2, events_file_written={"lines": 2, "sha256": "0" * 64}))
    assert ev["status"] == "FAIL" and ev["kind"] == "sha256_mismatch"


def test_good_file_and_summary_can_pass_with_sha256(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(6)) + "\n", encoding="utf-8")
    import hashlib
    sha = hashlib.sha256(f.read_bytes()).hexdigest()
    ev = t.evidence_integrity(f, _summary(6, events_file_written={"lines": 6, "sha256": sha}))
    assert ev["status"] == "PASS" and ev["counts_match"] and ev["sha256"] == sha and ev["sha256_matches_attestation"]
    assert ev["recorded_count"] == ev["summary_count"] == 6 and ev["file_present"]


def test_collector_writer_output_round_trips_through_the_verifier(tmp_path):
    """live._write is the real producer: its file + attestation must be accepted by the real verifier."""
    t = _tool()
    c = Collector()
    for line in _lines(3):
        from src.stage0.envelope import MarketEnvelope
        c._ingest(MarketEnvelope.from_dict(json.loads(line)))
    out = tmp_path / "w.jsonl"
    written = live._write(str(out), c)
    ev = t.evidence_integrity(out, _summary(3, events_file_written=written))
    assert ev["status"] == "PASS" and written["lines"] == 3


def test_runtime_fatal_error_is_not_hidden_by_a_clean_log(tmp_path):
    t = _tool()
    f = tmp_path / "e.jsonl"
    f.write_text("\n".join(_lines(4)) + "\n", encoding="utf-8")
    ev = t.evidence_integrity(f, _summary(4))
    s = dict(_summary(4, exit_code=5, error="data integrity: FatalDataError: Invalid Symbols!"), max_gap_s=1.0)
    st = t.judge_ws({"timed_out": False, "exit_code": 5}, s, ("BOOK_TICKER",), ev, 20.0)
    assert st["runtime"]["status"] == "FAIL" and st["ws_received"]["status"] == "FAIL"   # exit 5 stays a failure
    assert t.worst([v["status"] for v in st.values()]) == "FAIL"


# ===================================================================== gate READY scope
def test_gate_ready_reports_scope_and_does_not_claim_full_source_pass():
    r, _ = run_toobit(ALL)
    g = judge(r)["gate_ready"]
    assert g["status"] == "PASS"
    assert g["scope"]["gate_core_types"] == ["TRADE", "BOOK_TICKER", "MARK_PRICE"]
    assert g["scope"]["required_but_outside_gate"] == ["ORDERBOOK_SNAPSHOT"]
    assert "NOT prove" in g["scope"]["meaning"]


def test_unverified_schema_never_ready_for_a_consumer_that_needs_it():
    rx = T0 + timedelta(seconds=1)
    g = DataQualityGate()
    g.on_connect(TB)
    [oi] = toobit.parse_open_interest({"openInterestList": [{"symbol": TSYM, "size": "10", "time": int(T0.timestamp() * 1000)}]}, rx)
    assert "UNVERIFIED_SCHEMA" in oi.quality_flags
    g.observe(oi)
    for m in toobit.parse_ws({"topic": "markPrice", "params": {}, "f": False, "data": {
            "exchangeId": 1, "symbolId": TSYM, "price": "1", "time": int(rx.timestamp() * 1000)}}, rx):
        g.observe(m)
    common = "BTC/USDT:USDT"
    assert g.status(TB, common, rx, types=["MARK_PRICE", "OPEN_INTEREST"]) == Readiness.UNVERIFIED
    assert g.status(TB, common, rx) == Readiness.UNVERIFIED                    # no explicit types: everything seen
    assert g.status(TB, common, rx, types=["MARK_PRICE"]) == Readiness.READY   # scoped consumer: explicit, documented


# ===================================================================== HTTP 451 / exit codes / CI summary
def _fake_runs(t, monkeypatch, rest_rows, collect_summary=None):
    def fake_run(name, argv, logdir, timeout_s):
        out = logdir / f"{name}.stdout.json"
        err = logdir / f"{name}.stderr.log"
        err.write_text("", encoding="utf-8")
        if name.endswith("rest_check"):
            out.write_text(json.dumps({"endpoints": rest_rows}), encoding="utf-8")
        else:
            out.write_text(json.dumps(collect_summary), encoding="utf-8")
        code = (1 if any(r.get("verdict") == "FAIL" for r in rest_rows) else 0) if name.endswith("rest_check") else collect_summary["exit_code"]
        return {"command": name, "exit_code": code, "timed_out": False, "stdout_file": out.name, "stderr_file": err.name}
    monkeypatch.setattr(t, "run_command", fake_run)


SPOT_REQ = live.required_rest_endpoints(Source.BINANCE_SPOT)
R451 = [{"endpoint": e, "verdict": "FAIL", "http_status": 451} for e in SPOT_REQ]
S3 = {"exit_code": 3, "error": "required REST startup data unavailable: HTTP 451", "envelopes": 0, "rejected": 0}


def test_binance_451_rest_fail_ws_not_run_blocked_and_exit_2(tmp_path, monkeypatch):
    t = _tool()
    _fake_runs(t, monkeypatch, R451, S3)
    rec = t.verify("binance-spot", tmp_path, 30, 10)
    assert rec["stages"]["rest"]["status"] == "FAIL" and rec["stages"]["ws_received"]["status"] == "NOT_RUN"
    assert rec["environment_blocked"] is True and rec["status"] == "FAIL"
    assert t.exit_code([rec], t.worst([rec["status"]])) == 2


def test_451_mixed_with_a_real_failure_is_not_excused(tmp_path, monkeypatch):
    t = _tool()
    _fake_runs(t, monkeypatch, R451[:-1] + [{"endpoint": SPOT_REQ[-1], "verdict": "FAIL", "http_status": 500}], S3)
    rec = t.verify("binance-spot", tmp_path, 30, 10)
    assert rec["environment_blocked"] is False
    assert t.exit_code([rec], "FAIL") == 1


def test_451_rest_with_a_real_websocket_failure_is_not_excused(tmp_path, monkeypatch):
    t = _tool()
    _fake_runs(t, monkeypatch, R451, {"exit_code": 5, "error": "data integrity: boom", "envelopes": 0, "rejected": 0})
    rec = t.verify("binance-spot", tmp_path, 30, 10)
    assert rec["environment_blocked"] is False and t.exit_code([rec], "FAIL") == 1


def test_main_writes_markdown_table_and_never_reports_fail_as_success(tmp_path, monkeypatch):
    t = _tool()
    _fake_runs(t, monkeypatch, R451, S3)
    summary = tmp_path / "step_summary.md"
    code = t.main(["--output-dir", str(tmp_path / "ev"), "--sources", "binance-spot", "--summary-file", str(summary)])
    assert code == 2
    md = summary.read_text(encoding="utf-8")
    assert "| binance-spot |" in md and "environment_blocked: HTTP 451" in md and "Not verified" in md
    assert "Overall: FAIL" in md and "NOT APPROVED" in md
    rep = json.loads((tmp_path / "ev" / "verification-report.json").read_text(encoding="utf-8"))
    assert rep["exit_code"] == 2 and rep["fully_verified"] is False and rep["overall"] == "FAIL"
    assert (tmp_path / "ev" / "verification-summary.md").is_file()


def test_workflow_does_not_turn_exit_2_into_success():
    wf = (pathlib.Path(live.__file__).parents[2] / ".github" / "workflows" / "stage0-public-live-verification.yml").read_text(encoding="utf-8")
    assert "Not every stage PASSED" not in wf                      # the old warning-then-exit-0 path is gone
    assert "verification-gate" in wf and "GITHUB_STEP_SUMMARY" in wf and "if: always()" in wf
    gate = wf[wf.index("verification-gate:"):]
    assert "2) echo" in gate and gate.split("2) echo", 1)[1].split(";;")[0].rstrip().endswith("exit 1")
    assert "0) echo" in gate and "exit" not in gate.split("0) echo", 1)[1].split(";;")[0]
