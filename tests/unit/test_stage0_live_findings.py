"""Regression tests for the problems found in the real public-data runs (2026-11-19/20):
  * Toobit REST/WS index needs the BTCUSDT-style symbol (BTC-SWAP-USDT returns an empty map / "Invalid Symbols!")
  * Toobit WS markPrice pushes an empty ack, then a REST-like dict {exchangeId, symbolId, price, time}
  * Toobit first trade push (f=true) replays ~60 trades up to ~15 s old (history, not live data)
  * Binance HTTP 451 from a restricted region: reported as FAIL/NOT_RUN, never retried, never bypassed
  * verification tool statuses (PASS / FAIL / INCONCLUSIVE / NOT_RUN)
The message samples are copies of OBSERVED public Toobit messages (market data only; no credentials)."""
import importlib.util
import json
import pathlib
from datetime import datetime, timedelta, timezone

import pytest

from src.stage0 import live
from src.stage0.adapters import toobit
from src.stage0.collector import Collector, HttpError, NetworkPolicy, PublicRestClient
from src.stage0.envelope import EnvelopeError, Source
from src.stage0.live import FatalDataError, LiveRunner, PublicWsClient, RestPolicy, StartupFailure
from src.stage0.quality import DataQualityGate, Readiness
from tests.unit.test_stage0_hardening import _Sleeper, make_rest, run, seq  # noqa: F401
from tests.unit.test_stage0_live import APPROVED, MS, T0, TSYM, Clock, Factory, FakeRest, make  # noqa: F401

TB = Source.TOOBIT_FUTURES
COMMON = "BTC/USDT:USDT"

# ---- observed messages (shape verbatim, values from a real session) ----
OBS_MARK_ACK = {"symbol": TSYM, "topic": "markPrice", "params": {"realtimeInterval": "24h", "binary": "false"}, "data": [], "f": True}
OBS_MARK = {"topic": "markPrice", "params": {"realtimeInterval": "24h", "binary": "false"},
            "data": {"exchangeId": 301, "symbolId": TSYM, "price": "62931.2", "time": MS}, "f": False, "sendTime": MS}
OBS_BOOK_ACK = {"symbol": TSYM, "topic": "bookTicker", "params": {"realtimeInterval": "24h", "binary": "false"}, "data": [], "f": True}
OBS_BOOK = {"topic": "bookTicker", "params": {"realtimeInterval": "24h", "binary": "false"},
            "data": {"e": "bookTicker", "E": MS, "s": TSYM, "b": "62941", "bq": "12169.2", "a": "62941.1", "aq": "19047.3", "t": MS},
            "f": False, "sendTime": MS}
OBS_ERROR = {"code": "-100010", "desc": "Invalid Symbols!"}


def obs_trade(first: bool, ts, base_id=4900197869811334542):
    items = [{"v": str(base_id + i), "t": int(t.timestamp() * 1000), "p": "62930", "q": "1.5", "m": bool(i % 2)} for i, t in enumerate(ts)]
    return {"symbol": TSYM, "symbolName": TSYM, "topic": "trade", "params": {"realtimeInterval": "24h", "binary": "false"},
            "data": items, "f": first, "sendTime": MS}


# ===================================================================== Toobit index symbol
def test_index_symbol_form_and_ws_subscribe_uses_it():
    assert toobit.index_symbol(TSYM) == "BTCUSDT" and toobit.index_symbol("ETH-SWAP-USDT") == "ETHUSDT"
    with pytest.raises(EnvelopeError):
        toobit.index_symbol("BTCUSDT")
    assert toobit.ws_subscribe_message("index", TSYM)["symbol"] == "BTCUSDT"
    assert toobit.ws_subscribe_message("trade", TSYM)["symbol"] == TSYM


def test_index_rest_with_wrong_symbol_form_is_empty_and_inconclusive_not_a_crash():
    assert toobit.parse_index_price({"index": {}, "edp": {}}, T0) == []


def _toobit_check(resp):
    seen = []

    def transport(url, params):
        seen.append(("/" + url.split("/", 3)[3], dict(params)))
        v = resp["/" + url.split("/", 3)[3]]
        if isinstance(v, Exception):
            raise v
        return v
    pol = NetworkPolicy(allow_toobit=True, toobit_user_approved=True)
    rows = run(live.check_rest(PublicRestClient(pol, transport=transport), TB, TSYM, now=lambda: T0))
    return {r["endpoint"]: r for r in rows}, seen


def test_check_requests_index_with_index_symbol_and_empty_map_is_inconclusive():
    from tests.unit.test_stage0_hardening import GOOD_TB
    resp = dict(GOOD_TB)
    resp["/quote/v1/index"] = {"index": {}, "edp": {}}
    rep, seen = _toobit_check(resp)
    assert ("/quote/v1/index", {"symbol": "BTCUSDT"}) in seen
    assert rep["/quote/v1/index"]["verdict"] == "INCONCLUSIVE" and rep["/quote/v1/index"]["parsed_events"] == 0


def test_poll_reference_uses_index_symbol_and_flags_missing_index():
    resp = {"/api/v1/futures/fundingRate": [], "/quote/v1/openInterest": {"openInterestList": []},
            "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS},
            "/quote/v1/index": {"index": {}, "edp": {}}, "/api/v1/exchangeInfo": {"contracts": []}}
    r, c, fr, clock = make_rest(resp, pol=APPROVED)
    run(r.poll_reference(TB, TSYM))
    assert ("/quote/v1/index", {"symbol": "BTCUSDT"}) in fr.calls
    assert any(i.kind == "REFERENCE_UNAVAILABLE" and "INDEX_PRICE" in i.detail for i in c.gate.issues)


# ===================================================================== Toobit WS markPrice
def test_observed_markprice_ack_then_dict_push():
    assert toobit.parse_ws(OBS_MARK_ACK, T0) == []
    [e] = toobit.parse_ws(OBS_MARK, T0)
    assert e.event_type.value == "MARK_PRICE" and e.exchange_symbol == TSYM and e.symbol == COMMON
    assert str(e.normalized_payload["mark_price"]) == "62931.2" and "DOC_ONLY" in e.quality_flags


def test_markprice_docs_shape_still_accepted_and_garbage_rejected():
    assert len(toobit.parse_ws({"topic": "markPrice", "symbol": TSYM, "data": [{"symbol": TSYM, "markPrice": "5", "time": MS}]}, T0)) == 1
    for bad in ({"symbolId": TSYM, "time": MS}, {"price": "1", "time": MS}, {"symbolId": TSYM, "price": "1"}):
        with pytest.raises(EnvelopeError):
            toobit.parse_ws({"topic": "markPrice", "data": bad}, T0)


def test_observed_book_ticker_ack_and_dict():
    assert toobit.parse_ws(OBS_BOOK_ACK, T0) == []
    [e] = toobit.parse_ws(OBS_BOOK, T0)
    assert e.event_type.value == "BOOK_TICKER"


# ===================================================================== backfill (first trade push)
def test_first_trade_push_is_backfill_and_does_not_make_trade_fresh():
    rx = T0 + timedelta(seconds=20)
    old = [rx - timedelta(seconds=15 - i * 0.25) for i in range(60)]
    evs = toobit.parse_ws(obs_trade(True, old), rx)
    assert len(evs) == 60 and all("BACKFILL" in e.quality_flags for e in evs)
    g = DataQualityGate()
    g.on_connect(TB)
    flagged = [k for e in evs for k in g.observe(e)]
    assert "RECEIVE_LAG" not in flagged and "FUTURE_EVENT_TIME" not in flagged
    assert g.status(TB, COMMON, rx, types=["TRADE"]) == Readiness.STALE            # history is not live data
    live_trade = toobit.parse_ws(obs_trade(False, [rx], base_id=4900197869811339999), rx)[0]
    assert "BACKFILL" not in live_trade.quality_flags
    g.observe(live_trade)
    assert g.status(TB, COMMON, rx, types=["TRADE"]) == Readiness.READY


def test_backfill_duplicates_are_still_detected():
    rx = T0 + timedelta(seconds=20)
    g = DataQualityGate()
    g.on_connect(TB)
    t = [rx - timedelta(seconds=1)]
    g.observe(toobit.parse_ws(obs_trade(True, t), rx)[0])
    assert g.observe(toobit.parse_ws(obs_trade(False, t), rx)[0]) == ["DUPLICATE_EVENT"]


# ===================================================================== subscription error object
def test_server_error_object_is_a_subscription_error():
    with pytest.raises(toobit.SubscriptionError, match="-100010"):
        toobit.parse_ws(OBS_ERROR, T0)
    assert issubclass(toobit.SubscriptionError, EnvelopeError)


def test_rejected_subscription_is_fatal_not_silently_quarantined():
    r, c, fac, rest, clock = make([[OBS_ERROR]], {}, policy=APPROVED)
    with pytest.raises(FatalDataError, match="Invalid Symbols"):
        run(r.run_toobit(TSYM, 5))


# ===================================================================== end-of-collection gate state
def test_observed_session_reaches_ready_for_core_types_and_reports_channels():
    script = [OBS_BOOK_ACK, OBS_MARK_ACK, OBS_BOOK, OBS_MARK,
              obs_trade(False, [T0 + timedelta(seconds=0.5)])]
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED)
    run(r.run_toobit(TSYM, 0.45))
    assert not c.rejected and r.end_state["readiness"] == "READY"
    assert r.end_state["core_types"] == ["TRADE", "BOOK_TICKER", "MARK_PRICE"]
    assert r.ws_counts["MARK_PRICE"] == 1 and r.ws_counts["BOOK_TICKER"] == 1 and r.rest_counts == {}
    assert r.health()[0] == 0


def test_missing_core_type_makes_run_unhealthy_with_reason():
    script = [OBS_BOOK, obs_trade(False, [T0 + timedelta(seconds=0.2)])]            # mark price never arrives
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED)
    run(r.run_toobit(TSYM, 0.15))
    assert r.end_state["readiness"] == "STALE"
    code, why = r.health()
    assert code == 6 and any("core data types not READY" in w for w in why)


def test_end_state_is_captured_before_the_slow_close():
    """The socket close handshake can take seconds; readiness must be judged at the end of DATA."""
    script = [OBS_BOOK, OBS_MARK, obs_trade(False, [T0 + timedelta(seconds=0.3)])]
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED)
    orig = Factory.__call__

    def slow_close_factory(self):
        conn = orig(self)
        real_close = conn.close

        async def slow():
            clock.advance(10)                         # a 10 s close handshake
            await real_close()
        conn.close = slow
        return conn
    Factory.__call__ = slow_close_factory
    try:
        run(r.run_toobit(TSYM, 0.25))
    finally:
        Factory.__call__ = orig
    assert clock() - T0 > timedelta(seconds=10)
    assert r.end_state["readiness"] == "READY"


# ===================================================================== HTTP 451
def test_http_451_is_permanent_single_attempt_and_clearly_reported():
    r, c, fr, clock = make_rest({"/api/v3/time": seq(HttpError(451), {"serverTime": MS})})
    with pytest.raises(StartupFailure) as ei:
        run(r.sync_clock(Source.BINANCE_SPOT))
    assert len(fr.calls) == 1 and r.sleep.slept == []
    assert "451" in str(ei.value) and "regional" in r.rest_failures[0]["reason"] and r.rest_failures[0]["kind"] == "PERMANENT"


def test_check_rest_451_reason_and_no_bypass_attempt():
    calls = []

    def transport(url, params):
        calls.append(url)
        raise HttpError(451)
    rows = run(live.check_rest(PublicRestClient(NetworkPolicy(allow_binance=True), transport=transport), Source.BINANCE_SPOT,
                               "BTCUSDT", now=lambda: T0))
    assert all(r["verdict"] == "FAIL" and r["http_status"] == 451 and "not bypassed" in r["reason"] for r in rows)
    assert len(calls) == len(rows)                                # exactly one request per endpoint, no retries/proxies


# ===================================================================== verification tool statuses
def _tool():
    spec = importlib.util.spec_from_file_location("stage0_live_verify", pathlib.Path(live.__file__).parents[2] / "tools" / "stage0_live_verify.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_tool_all_451_is_fail_for_rest_and_not_run_for_ws():
    t = _tool()
    rows = [{"endpoint": "/api/v3/time", "verdict": "FAIL", "http_status": 451}] * 3
    st, why = t.judge_rest({}, rows)
    assert st == "FAIL" and "451" in why
    stages = t.judge_ws({"timed_out": False}, {"exit_code": 3, "error": "required REST data unavailable: HTTP 451"},
                        ("TRADE",), {"invalid_lines": 0}, 20.0)
    assert {v["status"] for v in stages.values()} == {"NOT_RUN"}
    assert t.worst(["PASS", "NOT_RUN", "FAIL"]) == "FAIL" and t.worst(["PASS", "NOT_RUN"]) == "NOT_RUN"


def test_tool_inconclusive_when_docs_leave_meaning_open_or_data_empty():
    t = _tool()
    rows = [{"endpoint": "/api/v1/time", "verdict": "PASS"}, {"endpoint": "/quote/v1/openInterest", "verdict": "INCONCLUSIVE"}]
    assert t.judge_rest({}, rows)[0] == "INCONCLUSIVE"
    assert t.judge_rest({}, [{"endpoint": "/api/v1/time", "verdict": "INCONCLUSIVE"}])[0] in ("FAIL",)      # time must PASS
    assert t.judge_rest({}, [])[0] == "FAIL"


def good_summary(**kw):
    s = {"exit_code": 0, "envelopes": 100, "rejected": 0, "max_gap_s": 1.0, "ws_events_by_type": {"TRADE": 5, "BOOK_TICKER": 9, "MARK_PRICE": 3},
         "end_of_collection": {"readiness": "READY", "core_types": ["TRADE"], "per_type": {}}}
    s.update(kw)
    return s


def test_tool_ws_stages_each_judged_separately():
    t = _tool()
    ev = {"invalid_lines": 0}
    req = ("TRADE", "BOOK_TICKER", "MARK_PRICE")
    ok = t.judge_ws({"timed_out": False}, good_summary(), req, ev, 20.0)
    assert {k: v["status"] for k, v in ok.items()} == {"ws_received": "PASS", "ws_parsed": "PASS", "ws_fresh": "PASS", "gate_ready": "PASS"}
    miss = t.judge_ws({"timed_out": False}, good_summary(ws_events_by_type={"TRADE": 1}), req, ev, 20.0)
    assert miss["ws_received"]["status"] == "FAIL" and set(miss["ws_received"]["missing_types"]) == {"BOOK_TICKER", "MARK_PRICE"}
    rej = t.judge_ws({"timed_out": False}, good_summary(rejected=31, envelopes=447), req, ev, 20.0)
    assert rej["ws_parsed"]["status"] == "FAIL"
    few = t.judge_ws({"timed_out": False}, good_summary(rejected=1, envelopes=447), req, ev, 20.0)
    assert few["ws_parsed"]["status"] == "INCONCLUSIVE"
    stall = t.judge_ws({"timed_out": False}, good_summary(max_gap_s=45.0), req, ev, 20.0)
    assert stall["ws_fresh"]["status"] == "FAIL"
    unv = t.judge_ws({"timed_out": False}, good_summary(end_of_collection={"readiness": "UNVERIFIED", "core_types": [], "per_type": {}}), req, ev, 20.0)
    assert unv["gate_ready"]["status"] == "INCONCLUSIVE"
    notready = t.judge_ws({"timed_out": False}, good_summary(end_of_collection={"readiness": "STALE", "core_types": [], "per_type": {}}), req, ev, 20.0)
    assert notready["gate_ready"]["status"] == "FAIL"
    assert t.judge_ws({"timed_out": True}, None, req, ev, 20.0)["ws_received"]["status"] == "FAIL"
    assert t.judge_ws({"timed_out": False}, None, req, ev, 20.0)["gate_ready"]["status"] == "FAIL"


def test_tool_main_exit_codes_and_report(tmp_path, monkeypatch):
    t = _tool()

    def fake_verify(src, logdir, seconds, timeout):
        st = {"binance-spot": "NOT_RUN", "binance-futures": "NOT_RUN", "toobit-futures": "PASS"}[src]
        return {"source": src, "status": st, "stages": {"rest": {"status": st}}}
    monkeypatch.setattr(t, "verify", fake_verify)
    assert t.main(["--output-dir", str(tmp_path), "--approve-toobit-public-readonly"]) == 2
    rep = json.loads((tmp_path / "verification-report.json").read_text())
    assert rep["overall"] == "NOT_RUN" and rep["trading_or_account_requests"] is False
    monkeypatch.setattr(t, "verify", lambda *a: {"source": "x", "status": "FAIL", "stages": {"rest": {"status": "FAIL"}}})
    assert t.main(["--output-dir", str(tmp_path), "--binance-only"]) == 1
    monkeypatch.setattr(t, "verify", lambda *a: {"source": "x", "status": "PASS", "stages": {"rest": {"status": "PASS"}}})
    assert t.main(["--output-dir", str(tmp_path), "--binance-only"]) == 0
    with pytest.raises(SystemExit):
        t.main(["--output-dir", str(tmp_path), "--sources", "toobit-futures"])      # needs explicit approval flag


# ===================================================================== default-type freshness (found in the real Binance run)
def test_default_status_ignores_bootstrap_snapshot_when_deltas_flow_but_not_for_snapshot_only_feeds():
    from src.stage0.adapters import binance
    g = DataQualityGate()
    g.on_connect(Source.BINANCE_SPOT)
    sym = "BTC/USDT"
    snap = binance.SPOT.parse_depth_snapshot("BTCUSDT", {"lastUpdateId": 1, "bids": [["99", "1"]], "asks": [["100", "1"]]}, T0)
    g.observe(snap)
    for i in range(1, 6):
        t = T0 + timedelta(seconds=6 * i)
        g.observe(binance.SPOT.parse({"e": "depthUpdate", "E": int(t.timestamp() * 1000), "s": "BTCUSDT", "U": 1 + i, "u": 1 + i,
                                      "b": [], "a": []}, t)[0])
    now = T0 + timedelta(seconds=31)
    assert g.status(Source.BINANCE_SPOT, sym, now) == Readiness.READY            # 30 s old snapshot is bootstrap only
    assert g.status(Source.BINANCE_SPOT, sym, now, types=["ORDERBOOK_SNAPSHOT"]) == Readiness.STALE   # explicit ask still honoured
    g2 = DataQualityGate()
    g2.on_connect(TB)
    g2.observe(toobit.parse_ws(OBS_BOOK_ACK, T0) or toobit.parse_depth(TSYM, {"t": MS, "b": [["99", "1"]], "a": [["100", "1"]]}, T0))
    assert g2.status(TB, COMMON, T0 + timedelta(seconds=30)) == Readiness.STALE   # snapshot-only feed: snapshot must be fresh
