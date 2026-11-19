"""Hardening: per-type freshness gate, REST error/rate-limit handling, --check verdicts, exit codes,
credential isolation. Fake transports only; no real network."""
import asyncio
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0 import collector as col
from src.stage0 import live
from src.stage0.adapters import binance, toobit
from src.stage0.collector import (Collector, HttpError, NetworkDisabled, NetworkPolicy, PublicRestClient,
                                  ResponseFormatError, TransportError)
from src.stage0.envelope import Source
from src.stage0.live import (FatalDataError, LiveRunner, PublicWsClient, ReconnectExhausted, RestFailure,
                             RestPolicy, StartupFailure)
from src.stage0.quality import DataQualityGate, Readiness
from tests.unit.test_stage0_live import (APPROVED, MS, TSYM, Clock, Factory, FakeRest, T0, depth, no_real_network,  # noqa: F401
                                         snap, trade, tws)

SPOT, FUT, TB = Source.BINANCE_SPOT, Source.BINANCE_FUTURES, Source.TOOBIT_FUTURES
SYM = "BTC/USDT"


def run(c):
    return asyncio.run(c)


def bt(rx, ms=MS, seq="1"):
    """spot bookTicker (no event time) received at rx"""
    return binance.SPOT.parse({"u": int(seq), "s": "BTCUSDT", "b": "99", "B": "1", "a": "100", "A": "1"}, rx)[0]


def tr(rx, i, ms=None):
    return binance.SPOT.parse({"e": "trade", "E": ms or int(rx.timestamp() * 1000), "s": "BTCUSDT", "t": i, "p": "100",
                               "q": "1", "T": 1, "m": False}, rx)[0]


def gate_connected():
    g = DataQualityGate()
    g.on_connect(SPOT)
    return g


# ===================================================================== P1: per-type freshness
def test_stale_trade_not_hidden_by_continuing_book_ticker():
    g = gate_connected()
    g.observe(tr(T0, 1))
    for i in range(1, 9):                                   # bookTicker keeps flowing for 40s, trades stopped
        g.observe(bt(T0 + timedelta(seconds=5 * i), seq=str(i)))
    now = T0 + timedelta(seconds=40)
    assert g.status(SPOT, SYM, now, types=["BOOK_TICKER"]) == Readiness.READY
    assert g.status(SPOT, SYM, now, types=["TRADE", "BOOK_TICKER"]) == Readiness.STALE
    assert g.status(SPOT, SYM, now) == Readiness.STALE      # default = every type ever seen
    assert g.explain(SPOT, SYM, now)["TRADE"].startswith("STALE")


def test_requested_type_never_received_is_not_ready():
    g = gate_connected()
    g.observe(bt(T0))
    assert g.status(SPOT, SYM, T0, types=["BOOK_TICKER"]) == Readiness.READY
    assert g.status(SPOT, SYM, T0, types=["BOOK_TICKER", "MARK_PRICE"]) == Readiness.STALE
    assert g.explain(SPOT, SYM, T0, types=["MARK_PRICE"]) == {"MARK_PRICE": "MISSING"}
    assert not g.usable(SPOT, SYM, T0, types=["OPEN_INTEREST"])


def test_only_one_of_several_types_stale():
    g = gate_connected()
    g.observe(tr(T0, 1))
    g.observe(bt(T0 + timedelta(seconds=9), seq="1"))
    now = T0 + timedelta(seconds=12)                        # trade is 12s old (> 10s), quote 3s old
    assert g.explain(SPOT, SYM, now, types=["TRADE", "BOOK_TICKER"]) == {"BOOK_TICKER": "OK", "TRADE": "STALE (12.0s)"}
    assert not g.usable(SPOT, SYM, now, types=["TRADE", "BOOK_TICKER"])


def test_future_timestamp_is_not_fresh_data():
    g = gate_connected()
    g.observe(tr(T0, 1))
    fut = binance.SPOT.parse({"e": "trade", "E": int((T0 + timedelta(hours=1)).timestamp() * 1000), "s": "BTCUSDT",
                              "t": 2, "p": "100", "q": "1", "T": 1, "m": False}, T0 + timedelta(seconds=20))[0]
    assert "FUTURE_EVENT_TIME" in g.observe(fut)
    now = T0 + timedelta(seconds=20)
    assert g.status(SPOT, SYM, now, types=["TRADE"]) == Readiness.INVALID      # not READY, not refreshed
    g.observe(tr(now, 3))                                                       # a sane trade recovers the type
    assert g.status(SPOT, SYM, now, types=["TRADE"]) == Readiness.READY


def test_excessive_receive_lag_is_not_fresh_data():
    g = gate_connected()
    laggy = tr(T0 + timedelta(seconds=30), 1, ms=int(T0.timestamp() * 1000))    # event 30s old on arrival
    assert "RECEIVE_LAG" in g.observe(laggy)
    assert g.status(SPOT, SYM, T0 + timedelta(seconds=30), types=["TRADE"]) == Readiness.INVALID
    assert g.explain(SPOT, SYM, T0 + timedelta(seconds=30), types=["TRADE"])["TRADE"].startswith("BAD_TIMESTAMP")


def test_missing_event_time_uses_receive_time_for_freshness_but_is_penalised():
    g = gate_connected()
    e = bt(T0)
    g.observe(e)
    assert "NO_EVENT_TIME" in e.quality_flags and e.reliability_score < 1.0
    assert g.usable(SPOT, SYM, T0 + timedelta(seconds=5), types=["BOOK_TICKER"])
    assert not g.usable(SPOT, SYM, T0 + timedelta(seconds=15), types=["BOOK_TICKER"])


def test_after_reconnect_ready_only_when_all_required_types_are_fresh_again():
    g = gate_connected()
    g.observe(tr(T0, 1))
    g.observe(bt(T0, seq="1"))
    assert g.usable(SPOT, SYM, T0, types=["TRADE", "BOOK_TICKER"])
    g.on_disconnect(SPOT)
    g.on_connect(SPOT, [SYM])
    g.mark_synced(SPOT, SYM)
    t1 = T0 + timedelta(seconds=2)
    assert g.status(SPOT, SYM, t1, types=["TRADE", "BOOK_TICKER"]) == Readiness.STALE   # pre-disconnect data is void
    g.observe(bt(t1, seq="2"))
    assert g.status(SPOT, SYM, t1, types=["TRADE", "BOOK_TICKER"]) == Readiness.STALE   # trade still missing
    g.observe(tr(t1, 2))
    assert g.status(SPOT, SYM, t1, types=["TRADE", "BOOK_TICKER"]) == Readiness.READY


def test_syncing_book_blocks_ready_even_with_fresh_ticks():
    g = gate_connected()
    g.on_connect(SPOT, [SYM])
    g.observe(tr(T0, 1))
    assert g.status(SPOT, SYM, T0, types=["TRADE"]) == Readiness.SYNCING


def test_slow_reference_types_do_not_trip_receive_lag_and_have_longer_freshness():
    g = gate_connected()
    old_funding = binance.FUTURES.parse_funding_history(
        [{"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingTime": int((T0 - timedelta(hours=8)).timestamp() * 1000),
          "markPrice": ""}], T0)[0]
    g.on_connect(FUT)
    assert g.observe(old_funding) == []
    assert g.usable(FUT, "BTC/USDT:USDT", T0 + timedelta(minutes=30), types=["FUNDING_RATE"])


def test_failed_reference_poll_makes_dependent_consumer_not_ready_but_others_ok():
    g = gate_connected()
    g.observe(bt(T0))
    g.on_reference_failed(SPOT, SYM, "OPEN_INTEREST", "HTTP 500")
    assert "REFERENCE_UNAVAILABLE" in g.kinds()
    assert g.usable(SPOT, SYM, T0, types=["BOOK_TICKER"])
    assert not g.usable(SPOT, SYM, T0, types=["BOOK_TICKER", "OPEN_INTEREST"])
    assert g.status(SPOT, SYM, T0) == Readiness.STALE        # "everything I have seen/needed" is not complete


def test_unverified_status_is_still_kept_per_type():
    g = DataQualityGate()
    g.on_connect(TB)
    g.observe(toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": TSYM}]}, T0)[0])
    g.observe(toobit.parse_book_ticker([{"t": MS, "s": TSYM, "b": "99", "bq": "1", "a": "100", "aq": "1"}], T0)[0])
    assert g.usable(TB, "BTC/USDT:USDT", T0, types=["BOOK_TICKER"])
    assert g.status(TB, "BTC/USDT:USDT", T0, types=["OPEN_INTEREST"]) == Readiness.UNVERIFIED


# ===================================================================== P1: REST errors / rate limits
def make_rest(responses, policy=None, rest_policy=None, pol=None):
    clock = Clock()
    pol = pol or NetworkPolicy(allow_binance=True)
    fr = FakeRest(responses)
    c = Collector()
    r = LiveRunner(c, PublicRestClient(pol, transport=fr), PublicWsClient(pol, Factory([], clock)), clock=clock,
                   sleep=_Sleeper(clock), rest_policy=rest_policy or RestPolicy(jitter=lambda d: 0.0))
    return r, c, fr, clock


class _Sleeper:
    def __init__(self, clock):
        self.clock, self.slept = clock, []

    async def __call__(self, s):
        self.slept.append(s)
        self.clock.advance(s)


def seq(*items):
    it = iter(items)

    def f():
        x = next(it)
        if isinstance(x, Exception):
            raise x
        return x
    return f


OK_TIME = {"serverTime": MS}


def test_429_uses_retry_after_header_then_succeeds():
    r, c, fr, clock = make_rest({"/api/v3/time": seq(HttpError(429, {"Retry-After": "7"}), OK_TIME)})
    run(r.sync_clock(SPOT))
    assert r.sleep.slept == [7.0] and r.rest_retries == 1 and len(fr.calls) == 2


def test_toobit_429_uses_reset_timestamp_header():
    r, c, fr, clock = make_rest({"/api/v1/time": seq(HttpError(429, {"X-Api-Limit-Reset-Timestamp": str(int(T0.timestamp() * 1000) + 5000)}),
                                                      OK_TIME)}, pol=APPROVED)
    run(r.sync_clock(TB))
    assert r.sleep.slept == [pytest.approx(5.0, abs=0.01)]


def test_5xx_and_timeouts_back_off_exponentially_and_are_bounded():
    r, c, fr, clock = make_rest({"/api/v3/time": seq(HttpError(503), TransportError("timeout"), HttpError(502), HttpError(500))})
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))
    assert r.sleep.slept == [1.0, 2.0, 4.0]                  # 4 attempts total, then stop
    assert len(fr.calls) == 4 == r.rest_policy.max_attempts
    assert r.rest_failures[0]["kind"] == "TRANSIENT" and r.rest_failures[0]["attempts"] == 4


def test_jitter_is_added_and_delays_are_capped():
    rp = RestPolicy(max_attempts=6, base_s=10, cap_s=30, jitter=lambda d: 1.0)
    r, c, fr, clock = make_rest({"/api/v3/time": seq(*[HttpError(500)] * 6)}, rest_policy=rp)
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))
    assert r.sleep.slept == [11.0, 21.0, 31.0, 31.0, 31.0]


@pytest.mark.parametrize("err", [HttpError(400), HttpError(403), HttpError(404), ResponseFormatError("x")])
def test_permanent_errors_are_not_retried(err):
    r, c, fr, clock = make_rest({"/api/v3/time": seq(err, OK_TIME)})
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))
    assert len(fr.calls) == 1 and r.sleep.slept == [] and r.rest_failures[0]["kind"] == "PERMANENT"


def test_server_asks_for_very_long_wait_we_give_up_instead_of_sleeping():
    r, c, fr, clock = make_rest({"/api/v3/time": seq(HttpError(429, {"retry-after": "600"}), OK_TIME)})
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))
    assert r.sleep.slept == [] and len(fr.calls) == 1


@pytest.mark.parametrize("bad", [{}, {"serverTime": "abc"}, {"serverTime": -5}, [], None])
def test_required_server_time_must_be_valid(bad):
    r, c, fr, clock = make_rest({"/api/v3/time": bad})
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))


def test_failure_records_hold_no_params_headers_or_secrets():
    r, c, fr, clock = make_rest({"/api/v3/time": seq(HttpError(500, {"Authorization": "Bearer SECRET", "Retry-After": "1"}),
                                                      HttpError(500, {"x-mbx-apikey": "KEY"}), HttpError(500), HttpError(500))})
    with pytest.raises(StartupFailure):
        run(r.sync_clock(SPOT))
    dump = json.dumps(r.rest_failures)
    assert "SECRET" not in dump and "KEY" not in dump and set(r.rest_failures[0]) == {"endpoint", "kind", "status", "attempts", "reason"}


def test_reference_failure_is_isolated_and_registered_with_the_gate():
    resp = {"/fapi/v1/openInterest": seq(*[HttpError(500)] * 4),
            "/fapi/v1/fundingRate": [{"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingTime": MS, "markPrice": ""}],
            "/futures/data/globalLongShortAccountRatio": []}
    r, c, fr, clock = make_rest(resp)
    run(r.poll_reference(FUT, "BTCUSDT"))                    # does not raise
    assert any(e.event_type.value == "FUNDING_RATE" for e in c.envelopes)
    assert [f["endpoint"] for f in r.rest_failures] == ["/fapi/v1/openInterest"]
    g = c.gate
    g.on_connect(FUT)
    assert not g.usable(FUT, "BTC/USDT:USDT", clock(), types=["OPEN_INTEREST"])
    assert not g.usable(FUT, "BTC/USDT:USDT", clock(), types=["LONG_SHORT_RATIO"])      # empty reply: nothing usable
    assert {i.kind for i in g.issues} >= {"REFERENCE_UNAVAILABLE"}


def test_full_run_survives_optional_failure_but_fails_closed_on_required_one():
    # optional OI fails, stream still runs and delivers
    resp = {"/api/v1/time": OK_TIME, "/api/v1/futures/fundingRate": seq(*[HttpError(500)] * 4),
            "/quote/v1/openInterest": seq(*[TransportError("timeout")] * 4),
            "/quote/v1/markPrice": seq(*[HttpError(500)] * 4), "/quote/v1/index": seq(*[HttpError(500)] * 4),
            "/api/v1/exchangeInfo": seq(*[HttpError(500)] * 4)}
    clock = Clock()
    late = MS + 35_000          # REST back-off consumed ~35 s of the fake clock before streaming began
    fac = Factory([[tws("bookTicker", {"e": "bookTicker", "E": late, "s": TSYM, "b": "99", "bq": "1", "a": "100", "aq": "1", "t": late}),
                    tws("trade", [{"v": "1", "t": late, "p": "100", "q": "1", "m": True}])]], clock)
    c = Collector()
    r = LiveRunner(c, PublicRestClient(APPROVED, transport=FakeRest(resp)), PublicWsClient(APPROVED, fac), clock=clock,
                   sleep=_Sleeper(clock), rest_policy=RestPolicy(jitter=lambda d: 0.0))
    run(r.run(TB, TSYM, 0.15))
    now = clock()
    assert c.gate.usable(TB, "BTC/USDT:USDT", now, types=["BOOK_TICKER", "TRADE"])
    assert not c.gate.usable(TB, "BTC/USDT:USDT", now, types=["OPEN_INTEREST"])
    assert len(r.rest_failures) == 5
    # required time unavailable -> never starts streaming
    resp2 = dict(resp)
    resp2["/api/v1/time"] = seq(*[HttpError(500)] * 4)
    fac2 = Factory([[]], clock)
    r2 = LiveRunner(Collector(), PublicRestClient(APPROVED, transport=FakeRest(resp2)), PublicWsClient(APPROVED, fac2),
                    clock=clock, sleep=_Sleeper(clock), rest_policy=RestPolicy(jitter=lambda d: 0.0))
    with pytest.raises(StartupFailure):
        run(r2.run(TB, TSYM, 5))
    assert fac2.conns == []


def test_depth_snapshot_permanent_error_is_fatal_transient_error_reconnects():
    r, c, fr, clock = make_rest({"/api/v3/depth": seq(HttpError(400))})
    r.ws.factory = Factory([[depth(101, 101)]], clock)
    with pytest.raises(FatalDataError):
        run(r.run_binance(SPOT, "BTCUSDT", 5))
    r, c, fr, clock = make_rest({"/api/v3/depth": seq(*[HttpError(500)] * 40)},
                                rest_policy=RestPolicy(max_attempts=2, jitter=lambda d: 0.0))
    r.ws.factory = Factory([[]] * 10, clock)
    r.reconnect = col.ReconnectPolicy(base_s=1, cap_s=2, max_attempts=2)
    with pytest.raises(ReconnectExhausted):
        run(r.run_binance(SPOT, "BTCUSDT", 600))
    assert "RESYNC_FAILED" in c.gate.kinds() and r.rest_failures


# ===================================================================== exit codes
class StubRunner:
    outcome = None
    health_result = (0, [])

    def __init__(self, c, rest, ws):
        self.c, self.reconnects, self.resyncs, self.rest_retries, self.rest_failures, self.max_gap_s = c, 0, 0, 0, [], 0.0

    def clock(self):
        return T0

    async def run(self, src, symbol, seconds):
        if StubRunner.outcome:
            raise StubRunner.outcome

    def health(self, stall):
        return StubRunner.health_result


@pytest.mark.parametrize("outcome,code", [(None, 0), (StartupFailure("t"), 3), (ReconnectExhausted("x"), 4),
                                          (live.ResyncExhausted("gap"), 5), (FatalDataError("x"), 5),
                                          (NetworkDisabled("no"), 2), (RuntimeError("boom"), 5)])
def test_cli_exit_codes(monkeypatch, tmp_path, outcome, code):
    monkeypatch.setattr(live, "LiveRunner", StubRunner)
    StubRunner.outcome, StubRunner.health_result = outcome, (0, [])
    out = tmp_path / "o.jsonl"
    rc = live.main(["--source", "binance-spot", "--symbol", "BTCUSDT", "--allow-binance-network", "--out", str(out)])
    assert rc == code and out.exists()                       # evidence file written even on failure


def test_cli_unhealthy_run_is_not_success(monkeypatch):
    monkeypatch.setattr(live, "LiveRunner", StubRunner)
    StubRunner.outcome, StubRunner.health_result = None, (6, ["no events collected"])
    assert live.main(["--source", "binance-spot", "--symbol", "BTCUSDT", "--allow-binance-network"]) == 6


def test_cli_refusals_are_nonzero_and_explained(capsys):
    assert live.main(["--source", "toobit-futures", "--symbol", TSYM, "--allow-toobit-network"]) == 2
    assert "REFUSED" in capsys.readouterr().err
    assert live.main(["--check", "--source", "toobit-futures", "--symbol", TSYM]) == 2
    assert col.TOOBIT_NETWORK_APPROVED is False


def runner_with(clock):
    return LiveRunner(Collector(), PublicRestClient(NetworkPolicy(allow_binance=True), transport=lambda u, p: {}),
                      PublicWsClient(NetworkPolicy(allow_binance=True), Factory([], clock)), clock=clock)


def test_health_flags_zero_events_mostly_unparsable_and_stalls():
    clock = Clock()
    r = runner_with(clock)
    r.started_at = clock()
    code, why = r.health()
    assert code == 6 and "no events" in why[0]
    r.c.envelopes.append(tr(T0, 1))
    assert r.health()[0] == 0
    r.c.rejected.extend([{"error": "x"}] * 5)
    assert r.health()[0] == 6 and "parsed" in r.health()[1][0]
    r2 = runner_with(clock)
    r2.started_at = clock()
    r2.c.envelopes.append(tr(T0, 1))
    r2.last_msg_at = clock()
    clock.advance(45)                                        # 45 s of silence at the end of the run
    assert r2.health(stall_s=20)[0] == 6 and "stalled" in r2.health(20)[1][0]
    r2.max_gap_s = 0
    assert r2.health(stall_s=60)[0] == 0


# ===================================================================== --check verdicts
def check(resp, src=SPOT, symbol="BTCUSDT", pol=None):
    transport = lambda url, params: _resp(resp, url)            # noqa: E731
    return {r["endpoint"]: r for r in run(live.check_rest(PublicRestClient(pol or NetworkPolicy(allow_binance=True, allow_toobit=True,
                                                                                               toobit_user_approved=True), transport=transport),
                                                         src, symbol, now=lambda: T0))}


def _resp(resp, url):
    v = resp["/" + url.split("/", 3)[3]]
    if isinstance(v, Exception):
        raise v
    return v


GOOD_SPOT = {"/api/v3/time": {"serverTime": MS}, "/api/v3/ticker/price": {"symbol": "BTCUSDT", "price": "50010.55"},
             "/api/v3/depth": {"lastUpdateId": 5, "bids": [["99", "1"]], "asks": [["100", "1"]]}}


def test_check_pass_requires_real_events_and_structure():
    rep = check(GOOD_SPOT)
    assert {k: v["verdict"] for k, v in rep.items()} == {k: "PASS" for k in GOOD_SPOT}
    assert rep["/api/v3/depth"]["parsed_events"] == 1 and rep["/api/v3/time"]["clock_offset_ms"] == 0
    assert rep["/api/v3/time"]["http_status"] == 200 and rep["/api/v3/ticker/price"]["request_ok"]
    assert "50010.55" not in json.dumps(rep)                 # no response values in the report


def test_check_empty_responses_are_inconclusive_not_pass():
    fut = {"/fapi/v1/time": {"serverTime": MS}, "/fapi/v1/ticker/price": {"symbol": "BTCUSDT", "price": "1"},
           "/fapi/v1/depth": {"lastUpdateId": 1, "bids": [["1", "1"]], "asks": [["2", "1"]]},
           "/fapi/v1/openInterest": {}, "/fapi/v1/fundingRate": [], "/futures/data/globalLongShortAccountRatio": []}
    rep = check(fut, FUT)
    for p in ("/fapi/v1/openInterest", "/fapi/v1/fundingRate", "/futures/data/globalLongShortAccountRatio"):
        assert rep[p]["verdict"] == "INCONCLUSIVE" and rep[p]["parsed_events"] == 0 and "empty" in rep[p]["reason"]


def test_check_request_failures_and_schema_mismatch_are_fail():
    resp = dict(GOOD_SPOT)
    resp["/api/v3/time"] = HttpError(429)
    resp["/api/v3/ticker/price"] = {"unexpected": "shape"}
    resp["/api/v3/depth"] = TransportError("timeout")
    rep = check(resp)
    assert rep["/api/v3/time"]["verdict"] == "FAIL" and rep["/api/v3/time"]["http_status"] == 429 and not rep["/api/v3/time"]["request_ok"]
    assert rep["/api/v3/ticker/price"]["verdict"] == "FAIL" and rep["/api/v3/ticker/price"]["request_ok"]
    assert rep["/api/v3/ticker/price"]["missing_keys"] == ["price", "symbol"]
    assert rep["/api/v3/depth"]["verdict"] == "FAIL" and "timeout" in rep["/api/v3/depth"]["reason"]


def test_check_time_field_is_really_validated():
    for bad in ({"time": 1}, {"serverTime": "x"}):
        assert check({**GOOD_SPOT, "/api/v3/time": bad})["/api/v3/time"]["verdict"] == "FAIL"
    assert check({**GOOD_SPOT, "/api/v3/time": {"serverTime": MS + 3_600_000}})["/api/v3/time"]["verdict"] == "INCONCLUSIVE"


GOOD_TB = {"/api/v1/time": {"serverTime": MS},
           "/quote/v1/contract/ticker/price": [{"s": TSYM, "p": "100"}],
           "/quote/v1/trades": [{"p": "100", "q": "1", "t": MS, "ibm": True}],
           "/quote/v1/depth": {"t": MS, "b": [["99", "1"]], "a": [["100", "1"]]},
           "/quote/v1/contract/ticker/bookTicker": [{"t": MS, "s": TSYM, "b": "99", "bq": "1", "a": "100", "aq": "1"}],
           "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "100", "time": MS},
           "/quote/v1/index": {"index": {"BTCUSDT": "100"}, "edp": {"BTCUSDT": "100"}},
           "/api/v1/futures/fundingRate": [{"symbol": TSYM, "rate": "0.0001", "period": "8H", "nextFundingTime": MS}],
           "/quote/v1/openInterest": {"openInterestList": [{"size": "5", "symbol": TSYM}]},
           "/api/v1/exchangeInfo": {"contracts": [{"symbol": TSYM, "contractMultiplier": "0.0001", "status": "TRADING", "inverse": False,
                                                    "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.1"},
                                                                {"filterType": "LOT_SIZE", "stepSize": "1"}]}]}}


def test_check_toobit_unverified_types_never_become_pass():
    rep = check(GOOD_TB, TB, TSYM)
    v = {k: x["verdict"] for k, x in rep.items()}
    assert v["/quote/v1/index"] == "INCONCLUSIVE" and v["/quote/v1/openInterest"] == "INCONCLUSIVE"
    assert "UNVERIFIED" in rep["/quote/v1/openInterest"]["reason"] and rep["/quote/v1/openInterest"]["structure"] == "PASS"
    assert all(x == "PASS" for k, x in v.items() if k not in ("/quote/v1/index", "/quote/v1/openInterest"))
    assert "DOC-level" in rep["/quote/v1/markPrice"]["reason"]


def test_check_toobit_wrong_shape_is_fail_and_exit_code(monkeypatch, capsys):
    resp = dict(GOOD_TB)
    resp["/quote/v1/markPrice"] = {"symbol": TSYM, "price": "1", "time": MS}          # WS/old shape instead of REST
    rep = check(resp, TB, TSYM)
    assert rep["/quote/v1/markPrice"]["verdict"] == "FAIL" and "symbolId" in rep["/quote/v1/markPrice"]["missing_keys"]
    monkeypatch.setattr(live, "PublicRestClient", lambda pol: PublicRestClient(pol, transport=lambda u, p: _resp(resp, u)))
    rc = live.main(["--check", "--source", "toobit-futures", "--symbol", TSYM, "--allow-toobit-network", "--i-approve-toobit-public-readonly"])
    assert rc == 1 and '"FAIL": 1' in capsys.readouterr().out


# ===================================================================== credentials are never used
def test_env_credentials_are_ignored_and_never_attached(monkeypatch):
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "TOOBIT_API_KEY", "TOOBIT_API_SECRET"):
        monkeypatch.setenv(k, "SHOULD-NEVER-APPEAR")
    seen = []
    PublicRestClient(NetworkPolicy(allow_binance=True), transport=lambda u, p: seen.append((u, p)) or {}).get(SPOT, "/api/v3/time")
    assert seen == [("https://api.binance.com/api/v3/time", {})]
    captured = {}

    class R:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def read(self): return b"{}"

    def fake_urlopen(req, timeout=None):
        captured["req"], captured["url"] = req, req.full_url
        return R()
    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    assert col._urllib_get("https://api.binance.com/api/v3/time", {"a": 1}) == {}
    hdrs = {k.lower(): v for k, v in captured["req"].header_items()}
    assert set(hdrs) <= {"user-agent", "accept"} and "SHOULD-NEVER-APPEAR" not in json.dumps(hdrs) + captured["url"]


def test_urllib_transport_maps_errors_without_bodies(monkeypatch):
    def raise_http(req, timeout=None):
        raise urllib.error.HTTPError(req.full_url, 429, "x", {"Retry-After": "3"}, None)
    monkeypatch.setattr(urllib.request, "urlopen", raise_http)
    with pytest.raises(HttpError) as ei:
        col._urllib_get("https://api.binance.com/x", {})
    assert ei.value.status == 429 and ei.value.headers == {"retry-after": "3"}

    def raise_timeout(req, timeout=None):
        raise urllib.error.URLError(TimeoutError("t"))
    monkeypatch.setattr(urllib.request, "urlopen", raise_timeout)
    with pytest.raises(TransportError) as te:
        col._urllib_get("https://api.binance.com/x", {})
    assert te.value.kind == "timeout"

    def raise_conn(req, timeout=None):
        raise urllib.error.URLError(ConnectionRefusedError("c"))
    monkeypatch.setattr(urllib.request, "urlopen", raise_conn)
    with pytest.raises(TransportError) as ce:
        col._urllib_get("https://api.binance.com/x", {})
    assert ce.value.kind == "connection"


def test_stage0_sources_never_read_env_or_dotenv():
    import pathlib
    text = "".join(p.read_text(encoding="utf-8") for p in pathlib.Path(live.__file__).parent.rglob("*.py"))
    for needle in ("os.environ", "getenv", "dotenv", ".env\"", "load_dotenv"):
        assert needle not in text, needle


def test_public_mode_works_without_any_credentials(monkeypatch):
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "TOOBIT_API_KEY", "TOOBIT_API_SECRET"):
        monkeypatch.delenv(k, raising=False)
    rep = check(GOOD_SPOT)
    assert all(r["verdict"] == "PASS" for r in rep.values())
