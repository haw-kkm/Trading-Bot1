"""Binance KLINE live-wiring tests (T1..T18 + end-state). FAKE REST + FAKE WebSocket only: sockets are patched
to fail the test if touched. Nothing here proves anything about the real Binance network (LIVE KLINE: NOT VERIFIED)."""
import asyncio
import hashlib
import json
import os
import socket
from datetime import datetime, timedelta, timezone

import pytest

from src.stage0 import live
from src.stage0.collector import Collector, NetworkPolicy, PublicRestClient, ReconnectPolicy
from src.stage0.envelope import Source
from src.stage0.live import (JsonlSink, LiveRunner, PublicWsClient, RejectedJsonlSink, SinkDurabilityError,
                             WsClosed, normalize_kline_intervals)

T0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
MS = 1795176000000          # == T0; divisible by 1m and 5m
assert int(T0.timestamp() * 1000) == MS
SPOT_BASE = "wss://stream.binance.com:9443/stream?streams=btcusdt@trade/btcusdt@bookTicker/btcusdt@depth@100ms"
FUT_PUBLIC = "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms"
FUT_MARKET = "wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s"


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    real_connect = socket.socket.connect

    def guarded(self, address, *a, **k):
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"real network touched: {address}")
        return real_connect(self, address, *a, **k)

    def boom(*a, **k):
        raise AssertionError("real network touched")
    monkeypatch.setattr(socket.socket, "connect", guarded)
    monkeypatch.setattr(socket, "create_connection", boom)


# ------------------------------------------------------------------ fakes
class Clock:
    def __init__(self):
        self.t = T0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += timedelta(seconds=s)

    async def sleep(self, s):
        self.advance(s)


class FakeConn:
    def __init__(self, script, clock, pick=None):
        self.script, self.clock, self.sent, self.closed, self._pick, self.url = list(script), clock, [], False, pick, None

    async def connect(self, url):
        self.url = url
        if self._pick is not None:
            self.script = list(self._pick(url))

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        await asyncio.sleep(0)                                   # let the other Futures connection run (fair interleaving)
        while self.script and isinstance(self.script[0], tuple) and self.script[0][0] == "ADV":
            self.clock.advance(self.script.pop(0)[1])           # jump the fake clock (no real sleeping)
        self.clock.advance(0.1)
        if not self.script:
            raise WsClosed("script exhausted")
        item = self.script.pop(0)
        if item == "CLOSE":
            raise WsClosed("closed by peer")
        return item if isinstance(item, str) else json.dumps(item)

    async def close(self):
        self.closed = True


class Factory:
    def __init__(self, scripts, clock):
        self.scripts, self.clock, self.conns = (scripts if isinstance(scripts, dict) else list(scripts)), clock, []

    def _pick(self, url):
        for key, lst in self.scripts.items():
            if key in url:
                return lst.pop(0) if lst else []
        return []

    def __call__(self):
        if isinstance(self.scripts, dict):
            c = FakeConn([], self.clock, self._pick)
        else:
            c = FakeConn(self.scripts.pop(0) if self.scripts else [], self.clock)
        self.conns.append(c)
        return c


class FakeRest:
    def __init__(self, responses):
        self.responses, self.calls = responses, []

    def __call__(self, url, params):
        path = "/" + url.split("/", 3)[3]
        self.calls.append((path, dict(params)))
        r = self.responses[path]
        return r() if callable(r) else r


def snap(last):
    return {"lastUpdateId": last, "bids": [["99", "1"]], "asks": [["101", "1"]]}


SPOT_REST = {"/api/v3/depth": snap(100), "/api/v3/time": {"serverTime": MS}}
FUT_REST = {"/fapi/v1/depth": snap(100)}


def make(scripts, rest_map, kline_intervals=(), collector=None, **kw):
    clock = Clock()
    pol = NetworkPolicy(allow_binance=True)
    rest = PublicRestClient(pol, transport=FakeRest(rest_map))
    fac = Factory(scripts, clock)
    c = collector or Collector()
    r = LiveRunner(c, rest, PublicWsClient(pol, fac), clock=clock, sleep=clock.sleep,
                   kline_intervals=kline_intervals, **kw)
    return r, c, fac, rest._transport, clock


def run(coro):
    return asyncio.run(coro)


def trade(i):
    return {"stream": "btcusdt@trade", "data": {"e": "trade", "E": MS, "s": "BTCUSDT", "t": i, "p": "100", "q": "1",
                                                "T": MS, "m": False}}


def agg(i):
    return {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": MS, "s": "BTCUSDT", "a": i, "p": "100", "q": "1",
                                                  "f": i, "l": i, "T": MS, "m": False}}


def mark():
    return {"stream": "btcusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": MS, "s": "BTCUSDT", "p": "100",
                                                        "i": "100", "P": "100", "r": "0.0001", "T": MS + 1}}


def depth(U, u, pu=None):
    m = {"e": "depthUpdate", "E": MS, "s": "BTCUSDT", "U": U, "u": u, "b": [["99", "5"]], "a": []}
    if pu is not None:
        m["pu"] = pu
    return {"stream": "btcusdt@depth@100ms", "data": m}


def spot_book_ticker(u):
    return {"stream": "btcusdt@bookTicker", "data": {"u": u, "s": "BTCUSDT", "b": "99", "B": "1", "a": "101", "A": "1"}}


def fut_book_ticker(u):
    return {"stream": "btcusdt@bookTicker", "data": {"e": "bookTicker", "u": u, "s": "BTCUSDT", "b": "99", "B": "1",
                                                     "a": "101", "A": "1", "E": MS, "T": MS}}


def kf(interval="1m", start=MS - 60_000, event_ms=None, closed=False, high="110", low="90", close="101", vol="5",
       n=5, bad_x=None):
    """A Binance-shaped kline frame (combined-stream wrapper, full k object)."""
    span = {"1m": 60_000, "5m": 300_000}.get(interval, 1000)
    event_ms = MS if event_ms is None else event_ms
    k = {"t": start, "T": start + span - 1, "s": "BTCUSDT", "i": interval, "f": 100, "L": 100 + n, "o": "100",
         "c": close, "h": high, "l": low, "v": vol, "n": n, "x": closed if bad_x is None else bad_x,
         "q": "1000.0", "V": "2", "Q": "200.0", "B": "0"}
    return {"stream": f"btcusdt@kline_{interval}",
            "data": {"e": "kline", "E": event_ms, "s": "BTCUSDT", "k": k}}


def dur(n_frames):          # loop exits right after the n-th recv (each fake recv advances 0.1s)
    return 0.1 * n_frames - 0.05


# ====================================================================== T1..T7 subscriptions
def test_t1_spot_default_subscription_unchanged():
    r, c, fac, rest, clock = make([[trade(1)]], SPOT_REST)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", dur(1)))
    assert [cn.url for cn in fac.conns] == [SPOT_BASE]
    assert r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["status"] == "NOT_REQUESTED"


def test_t2_spot_kline_subscription_exact():
    r, c, fac, rest, clock = make([[trade(1)]], SPOT_REST, kline_intervals=("1m", "5m"))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", dur(1)))
    assert [cn.url for cn in fac.conns] == [SPOT_BASE + "/btcusdt@kline_1m/btcusdt@kline_5m"]
    r1, *_ = make([[trade(1)]], SPOT_REST, kline_intervals="5m")
    assert r1.kline_intervals == ("5m",)


def test_t3_t4_futures_kline_only_on_market_stream():
    r, c, fac, rest, clock = make({"/public/": [[depth(99, 101, pu=98)] * 3], "/market/": [[agg(1)] * 3]},
                                  FUT_REST, kline_intervals=("1m", "5m"))
    run(r.run_binance(Source.BINANCE_FUTURES, "BTCUSDT", 0.45))
    urls = {cn.url for cn in fac.conns}
    assert urls == {FUT_MARKET + "/btcusdt@kline_1m/btcusdt@kline_5m", FUT_PUBLIC}      # T3 + T4 (public: no kline)
    assert not any("kline" in u for u in urls if "/public/" in u)
    assert not any(u.startswith("wss://fstream.binance.com/stream") or "/ws" in u.split("?")[0] for u in urls)


def test_t5_reconnect_restores_base_and_kline_streams_spot():
    r, c, fac, rest, clock = make([[trade(1), "CLOSE"], [trade(2)] * 5], SPOT_REST, kline_intervals=("1m", "5m"),
                                  reconnect=ReconnectPolicy(base_s=1, cap_s=2, max_attempts=3))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 1.5))
    assert r.reconnects >= 1 and len(fac.conns) >= 2
    want = SPOT_BASE + "/btcusdt@kline_1m/btcusdt@kline_5m"
    assert all(cn.url == want for cn in fac.conns)


def test_t5_reconnect_restores_kline_on_futures_market_only():
    r, c, fac, rest, clock = make({"/public/": [[depth(99, 101, pu=98)] * 30],
                                   "/market/": [[agg(1), "CLOSE"], [agg(2)] * 30]},
                                  FUT_REST, kline_intervals=("1m",), reconnect=ReconnectPolicy(base_s=1, cap_s=2, max_attempts=3))
    run(r.run_binance(Source.BINANCE_FUTURES, "BTCUSDT", 1.5))
    market = [cn.url for cn in fac.conns if "/market/" in cn.url]
    assert len(market) >= 2 and all(u == FUT_MARKET + "/btcusdt@kline_1m" for u in market)
    assert all(cn.url == FUT_PUBLIC for cn in fac.conns if "/public/" in cn.url)


@pytest.mark.parametrize("bad", ["1s", "15m", "1h", "1m,1h", "", "1M", "5m;1m", " "])
def test_t6_unsupported_interval_rejected_before_any_network(bad, monkeypatch):
    with pytest.raises(ValueError):
        normalize_kline_intervals(bad)
    with pytest.raises(ValueError):
        LiveRunner(Collector(), None, None, kline_intervals=bad)       # no REST/WS object is even touched

    def boom(*a, **k):
        raise AssertionError("network object created for an invalid --kline-intervals")
    monkeypatch.setattr(live, "PublicRestClient", boom)
    monkeypatch.setattr(live, "PublicWsClient", boom)
    monkeypatch.setattr(live, "NetworkPolicy", boom)
    assert live.main(["--source", "binance-spot", "--symbol", "BTCUSDT", "--allow-binance-network",
                      "--kline-intervals", bad, "--out", "x.jsonl"]) == live.EXIT_REFUSED


def test_t6_normalization_dedupes_and_orders():
    assert normalize_kline_intervals("5m,1m,5m") == ("1m", "5m")
    assert normalize_kline_intervals(" 1m ") == ("1m",)
    assert normalize_kline_intervals(None) == () and normalize_kline_intervals(()) == ()


def test_t7_kline_requires_out_and_binance_only(monkeypatch, capsys):
    def boom(*a, **k):
        raise AssertionError("network object created although the run must be refused")
    monkeypatch.setattr(live, "PublicRestClient", boom)
    monkeypatch.setattr(live, "PublicWsClient", boom)
    assert live.main(["--source", "binance-futures", "--symbol", "BTCUSDT", "--allow-binance-network",
                      "--kline-intervals", "1m,5m"]) == live.EXIT_REFUSED
    assert "requires --out" in capsys.readouterr().err
    assert live.main(["--source", "toobit-futures", "--symbol", "BTC-SWAP-USDT", "--allow-toobit-network",
                      "--i-approve-toobit-public-readonly", "--kline-intervals", "1m", "--out", "x.jsonl"]) == live.EXIT_REFUSED


def test_check_mode_never_reports_kline_streaming_success(monkeypatch, capsys):
    async def fake_check(rest, src, symbol, now=None):
        return [{"endpoint": "/fapi/v1/time", "verdict": "PASS"}]
    monkeypatch.setattr(live, "check_rest", fake_check)
    assert live.main(["--check", "--source", "binance-futures", "--symbol", "BTCUSDT", "--allow-binance-network",
                      "--kline-intervals", "1m,5m"]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["kline_streaming"]["status"].startswith("NOT_EXERCISED") and out["kline_streaming"]["requested_intervals"] == ["1m", "5m"]


# ====================================================================== T8..T11 parse / state / evidence
def spot_run(script, intervals=("1m", "5m"), frames=None, out=None, **kw):
    n = frames or len(script)
    return make([script], SPOT_REST, kline_intervals=intervals, **kw), dur(n)


def test_t8_open_open_closed_real_shape_accepted():
    (r, c, fac, rest, clock), d = spot_run([kf(event_ms=MS - 2000, vol="3"), kf(event_ms=MS - 1000, vol="4"),
                                            kf(event_ms=MS + 100, closed=True, vol="5")])
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    rep = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")
    one = rep["by_interval"]["1m"]
    assert (one["received"], one["parsed"], one["gate_accepted"], one["open_accepted"], one["closed_accepted"]) == (3, 3, 3, 2, 1)
    assert one["freshness"] == "CURRENT" and one["latest_approved_closed"]["open_time_ms"] == MS - 60_000
    assert one["close_delta_ms_E_minus_Tplus1"]["histogram"]["[100,1000)"] == 1       # E - (T+1) = +100 ms
    assert rep["by_interval"]["5m"]["freshness"] == "NO_DATA" and rep["status"] == "PARTIAL"
    assert all(e.sequence_id is None for e in c.envelopes if e.event_type.value == "KLINE")   # k.t is not a sequence id


def test_t9_1m_and_5m_same_start_time_are_independent():
    s = MS
    script = [kf("1m", s, MS, vol="1"), kf("5m", s, MS, vol="1"), ("ADV", 60), kf("1m", s, MS + 60_100, closed=True, vol="2")]
    (r, c, fac, rest, clock), _ = spot_run(script)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 62))
    rows = {k[3]: v.state for k, v in c.gate._klines.rows.items() if k[-1] == s}
    assert rows == {"1m": "CLOSED", "5m": "OPEN"}
    rep = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["by_interval"]
    assert rep["1m"]["closed_accepted"] == 1 and rep["5m"]["closed_accepted"] == 0


def test_t10_nonmonotone_open_does_not_block_valid_closed():
    script = [kf(event_ms=MS - 3000, vol="5"), kf(event_ms=MS - 2000, vol="4"), kf(event_ms=MS + 100, closed=True, vol="6")]
    (r, c, fac, rest, clock), d = spot_run(script, intervals=("1m",))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    st = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")
    one = st["by_interval"]["1m"]
    assert one["gate_rejected"] == 1 and one["gate_reject_reasons"] == {"KLINE_OPEN_NONMONOTONE": 1}
    assert one["closed_accepted"] == 1 and one["freshness"] == "CURRENT" and st["status"] == "GATE_CURRENT_AT_END"


def test_t11_early_closed_and_unsupported_interval_go_to_rejected_sidecar_only(tmp_path):
    early = kf(event_ms=MS - 2000, closed=True)            # closed 2 s before the bar end: beyond the 1000 ms tolerance
    one_s = kf("1s", MS - 1000, MS, closed=False)
    ok_open = kf(event_ms=MS - 500)
    ev, rj = tmp_path / "e.jsonl", tmp_path / "e.jsonl.rejected.jsonl"
    col = Collector(sink=JsonlSink(ev), rejected_sink=RejectedJsonlSink(rj))
    (r, c, fac, rest, clock), d = spot_run([early, one_s, ok_open], collector=col)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    col.sink.close(), col.rejected_sink.close()
    rows = [json.loads(x) for x in rj.read_text(encoding="utf-8").splitlines()]
    assert [x["interval"] for x in rows] == ["1m", "1s"] and rows[0]["raw_message"] == early and rows[1]["raw_message"] == one_s
    assert "KLINE_PREMATURE_CLOSE" in rows[0]["error"] and "KLINE_NOT_SUPPORTED" in rows[1]["error"]
    assert all(set(("event_time_ms", "open_time_ms", "close_time_ms", "is_closed", "received_at", "error", "raw_message")) <= set(x) for x in rows)
    envs = [json.loads(x) for x in ev.read_text(encoding="utf-8").splitlines()]
    kl = [x for x in envs if x["event_type"] == "KLINE"]
    assert len(kl) == 1 and kl[0]["normalized_payload"]["is_closed"] is False       # only the sane OPEN is in the main file
    rep = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["by_interval"]
    assert rep["1m"]["parse_rejected"] == 1 and rep["1m"]["parse_reject_reasons"] == {"KLINE_PREMATURE_CLOSE": 1}
    assert rep["1s"]["parse_rejected"] == 1 and "KLINE_NOT_SUPPORTED" in rep["1s"]["parse_reject_reasons"]
    assert rep["1m"]["closed_accepted"] == 0 and r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["by_interval"]["1m"]["freshness"] == "NO_APPROVED_CLOSED"


def test_close_early_and_late_finalization_notices_are_counted():
    early_ok = kf(event_ms=MS - 500, closed=True)                                   # 500 ms early: tolerated + noticed
    script = [early_ok, ("ADV", 67), kf(start=MS, event_ms=MS + 66_000, closed=True)]   # E - end = 6000 ms > 5000 warn
    (r, c, fac, rest, clock), _ = spot_run(script, intervals=("1m",))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 68))
    one = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")
    assert one["by_interval"]["1m"]["notices"] == {"CLOSE_EARLY_BY_MS": 1, "KLINE_LATE_FINALIZATION": 1}
    assert one["by_interval"]["1m"]["closed_accepted"] == 2
    assert one["gate_issue_totals_all_types"]["CLOSE_EARLY_BY_MS"] == 1


def test_receive_lag_rejection_is_per_interval_and_does_not_refresh():
    laggy = kf(event_ms=MS - 30_000)                       # arrives 30 s after its event time -> RECEIVE_LAG
    (r, c, fac, rest, clock), d = spot_run([laggy], intervals=("1m",))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    one = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["by_interval"]["1m"]
    assert one["gate_rejected"] == 1 and one["gate_reject_reasons"].get("RECEIVE_LAG") == 1
    assert one["gate_accepted"] == 0 and one["freshness"] == "NO_APPROVED_CLOSED"
    assert one["receive_lag_ms"]["max"] >= 30_000


# ====================================================================== T12..T15 evidence storage
def test_t12_attestation_matches_real_bytes_for_both_files(tmp_path):
    ev, rj = tmp_path / "e.jsonl", tmp_path / "e.jsonl.rejected.jsonl"
    col = Collector(sink=JsonlSink(ev), rejected_sink=RejectedJsonlSink(rj))
    script = [kf(event_ms=MS - 2000), kf(event_ms=MS - 2000, closed=True), kf("1s", MS - 1000, MS), trade(1), "{bad json"]
    (r, c, fac, rest, clock), d = spot_run(script, collector=col)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    a_ev, a_rj = col.sink.close(), col.rejected_sink.close()
    for att, path in ((a_ev, ev), (a_rj, rj)):
        raw = path.read_bytes()
        assert att["lines"] == len(raw.split(b"\n")) - 1 == len(raw.splitlines()) and raw.endswith(b"\n")
        assert hashlib.sha256(raw).hexdigest() == att["sha256"] and att["fsync"] == "OK" and b"\r" not in raw
    assert a_ev["lines"] == col.total_envelopes and a_rj["lines"] == 2          # early CLOSED + 1s (bad json is not KLINE)


def test_t13_small_memory_retention_does_not_lose_file_evidence(tmp_path):
    ev, rj = tmp_path / "e.jsonl", tmp_path / "e.jsonl.rejected.jsonl"
    col = Collector(max_envelopes=2, max_rejected=1, sink=JsonlSink(ev), rejected_sink=RejectedJsonlSink(rj))
    script = [kf("1s", MS - 1000, MS) for _ in range(6)] + [trade(i) for i in range(1, 8)]
    (r, c, fac, rest, clock), d = spot_run(script, collector=col)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    a_ev, a_rj = col.sink.close(), col.rejected_sink.close()
    assert len(col.envelopes) == 2 and len(col.rejected) == 1
    assert a_rj["lines"] == 6 == col.total_rejected and a_ev["lines"] == col.total_envelopes > 2
    assert len(rj.read_text(encoding="utf-8").splitlines()) == 6 and len(ev.read_text(encoding="utf-8").splitlines()) == a_ev["lines"]


def _env():
    from src.stage0.adapters import binance
    return binance.SPOT.parse(trade(1), T0)[0]


@pytest.mark.parametrize("cls,record", [(JsonlSink, None), (RejectedJsonlSink, {"error": "x", "raw_message": {"a": 1}})])
def test_t14_periodic_flush_makes_records_readable_before_close(tmp_path, cls, record):
    now = [0.0]
    p = tmp_path / "f.jsonl"
    sink = cls(p, flush_every_s=1.0, flush_every_n=3, monotonic=lambda: now[0])
    write = (lambda: sink(_env())) if record is None else (lambda: sink(record))
    write(), write()
    assert p.read_bytes() == b""                           # below both thresholds: still buffered
    write()                                                # 3rd record -> count threshold
    assert len(p.read_bytes().splitlines()) == 3
    write()
    assert len(p.read_bytes().splitlines()) == 3           # 4th buffered
    now[0] = 1.5
    sink.flush_if_due()                                    # time threshold (idle period, no new write)
    assert len(p.read_bytes().splitlines()) == 4 and sink.flushes == 2
    sink.close()


def test_t14_time_threshold_flushes_on_write_after_idle(tmp_path):
    now = [0.0]
    sink = JsonlSink(tmp_path / "t.jsonl", flush_every_s=1.0, flush_every_n=10 ** 6, monotonic=lambda: now[0])
    now[0] = 5.0
    sink(_env())
    assert len((tmp_path / "t.jsonl").read_bytes().splitlines()) == 1
    sink.close()


def test_runner_polls_sinks_for_time_based_flush_on_every_frame():
    class Probe:
        calls = 0

        def __call__(self, env):
            pass

        def flush_if_due(self):
            Probe.calls += 1
    col = Collector(sink=Probe(), rejected_sink=Probe())
    (r, c, fac, rest, clock), d = spot_run([trade(i) for i in range(1, 5)], collector=col)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    assert Probe.calls == 2 * 4


def test_t15_fsync_called_once_at_close_not_per_event_and_failure_detected(tmp_path, monkeypatch):
    calls = []
    real = os.fsync
    monkeypatch.setattr(os, "fsync", lambda fd: (calls.append(fd), real(fd))[1])
    sink = JsonlSink(tmp_path / "a.jsonl")
    for _ in range(50):
        sink(_env())
    assert calls == []                                     # never per event
    att = sink.close()
    assert len(calls) == 1 and att["fsync"] == "OK" and att["lines"] == 50

    def failing(fd):
        raise OSError(5, "simulated I/O error")
    monkeypatch.setattr(os, "fsync", failing)
    bad = JsonlSink(tmp_path / "b.jsonl")
    bad(_env())
    with pytest.raises(SinkDurabilityError) as ei:
        bad.close()
    assert ei.value.attestation["lines"] == 1 and bad._fh.closed           # file handle is not leaked


def test_t15_cli_reports_fsync_failure_as_integrity_error_not_success(tmp_path, monkeypatch, capsys):
    clock = Clock()
    pol_holder = {}
    fac = Factory([[kf(event_ms=MS - 2000), trade(1)]], clock)
    monkeypatch.setattr(live, "PublicRestClient", lambda pol: PublicRestClient(pol, transport=FakeRest(SPOT_REST)))
    monkeypatch.setattr(live, "PublicWsClient", lambda pol: PublicWsClient(pol, fac))
    real_runner = live.LiveRunner
    monkeypatch.setattr(live, "LiveRunner", lambda col, rest, ws, **k: real_runner(col, rest, ws, clock=clock, sleep=clock.sleep, **k))
    monkeypatch.setattr(os, "fsync", lambda fd: (_ for _ in ()).throw(OSError(5, "simulated")))
    code = live.main(["--source", "binance-spot", "--symbol", "BTCUSDT", "--allow-binance-network", "--seconds", str(dur(2)),
                      "--kline-intervals", "1m,5m", "--out", str(tmp_path / "o.jsonl")])
    out = json.loads(capsys.readouterr().out)
    assert code == live.EXIT_INTEGRITY and out["exit_code"] == live.EXIT_INTEGRITY
    assert out["events_file_written"]["fsync"] == "FAILED" and out["rejected_kline_file_written"]["fsync"] == "FAILED"
    assert len(out["durability_errors"]) == 2 and "durability" in out["error"]


def test_cli_end_to_end_report_and_attestation(tmp_path, monkeypatch, capsys):
    clock = Clock()
    script = [kf(event_ms=MS - 2000), kf("1s", MS - 1000, MS), kf(event_ms=MS + 100, closed=True, vol="9"), trade(1)]
    fac = Factory([script], clock)
    monkeypatch.setattr(live, "PublicRestClient", lambda pol: PublicRestClient(pol, transport=FakeRest(SPOT_REST)))
    monkeypatch.setattr(live, "PublicWsClient", lambda pol: PublicWsClient(pol, fac))
    real_runner = live.LiveRunner
    monkeypatch.setattr(live, "LiveRunner", lambda col, rest, ws, **k: real_runner(col, rest, ws, clock=clock, sleep=clock.sleep, **k))
    out_path = tmp_path / "o.jsonl"
    live.main(["--source", "binance-spot", "--symbol", "BTCUSDT", "--allow-binance-network", "--seconds", str(dur(4)),
               "--kline-intervals", "5m,1m,1m", "--out", str(out_path)])
    out = json.loads(capsys.readouterr().out)
    for key, path in (("events_file_written", out_path), ("rejected_kline_file_written", tmp_path / "o.jsonl.rejected.jsonl")):
        raw = path.read_bytes()
        assert out[key]["lines"] == len(raw.splitlines()) and out[key]["sha256"] == hashlib.sha256(raw).hexdigest()
    kd = out["kline_diagnostics"]
    assert kd["requested_intervals"] == ["1m", "5m"] and kd["core_pass_implies_kline_ready"] is False
    assert kd["stage1_trend_ready"] == "NOT_DECLARED" and kd["by_interval"]["1m"]["closed_accepted"] == 1
    assert out["ws_events_by_type"]["KLINE"] == 2 and out["envelopes"] == 4         # 2 KLINE + TRADE + REST depth snapshot; the 1s frame is parse-rejected
    assert out["rejected"] == 1 and out["unhealthy_reasons"].count("requested KLINE 5m produced no frame") == 1


# ====================================================================== T16..T18
def test_t16_core_ready_but_kline_never_arrives_is_not_kline_ready():
    script = [depth(98, 100), depth(101, 102), trade(1), spot_book_ticker(5)]
    (r, c, fac, rest, clock), d = spot_run(script, intervals=("1m", "5m"))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    assert r.end_state["readiness"] == "READY"                                          # CORE is fine ...
    rep = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")
    assert rep["status"] == "NO_DATA" and rep["core_pass_implies_kline_ready"] is False       # ... KLINE is not
    assert not c.gate.kline_ready_intervals(Source.BINANCE_SPOT.value, "BTC/USDT", clock())
    code, reasons = r.health()
    assert code == live.EXIT_UNHEALTHY and any("requested KLINE 1m produced no frame" in x for x in reasons)


def test_t17_unrequested_kline_changes_nothing():
    r, c, fac, rest, clock = make([[trade(1), trade(2)]], SPOT_REST)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", dur(2)))
    assert r.kline_stats == {} and not any("KLINE" in x for x in r.health()[1])
    assert r.kline_report(Source.BINANCE_SPOT, "BTCUSDT") == {"requested_intervals": [], "status": "NOT_REQUESTED",
                                                              "stage1_trend_ready": "NOT_DECLARED"}


def test_t18_counts_are_mutually_consistent(tmp_path):
    col = Collector(sink=JsonlSink(tmp_path / "e.jsonl"), rejected_sink=RejectedJsonlSink(tmp_path / "r.jsonl"))
    script = [kf(event_ms=MS - 3000, vol="5"), kf(event_ms=MS - 2000, vol="4"),               # OPEN, OPEN regression
              kf(event_ms=MS - 3000, vol="5"),                                              # same E as the accepted OPEN -> DUPLICATE
              kf(event_ms=MS - 2000, closed=True), kf("1s", MS - 1000, MS),                 # early CLOSED (parse reject), 1s
              kf(event_ms=MS + 50, closed=True, vol="7"),                                   # valid CLOSED
              kf(event_ms=MS + 50, closed=True, vol="7"),                                   # duplicate CLOSED
              kf("5m", MS - 300_000, MS - 1000, vol="1"), trade(1)]
    (r, c, fac, rest, clock), d = spot_run(script, collector=col)
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", d))
    col.sink.close(), col.rejected_sink.close()
    rep = r.kline_report(Source.BINANCE_SPOT, "BTCUSDT")["by_interval"]
    tot_parsed = tot_pr = 0
    for iv, s in rep.items():
        assert s["received"] == s["parsed"] + s["parse_rejected"]
        assert s["parsed"] == s["gate_accepted"] + s["gate_rejected"]
        assert s["gate_accepted"] == s["open_accepted"] + s["closed_accepted"]
        assert s["parse_rejected"] == sum(s["parse_reject_reasons"].values())
        assert all(v >= 1 for v in s["gate_reject_reasons"].values())
        tot_parsed += s["parsed"]
        tot_pr += s["parse_rejected"]
    assert r.ws_counts["KLINE"] == tot_parsed and c.total_rejected == tot_pr
    assert c.total_envelopes == tot_parsed + 1 + 1                # + TRADE + the REST depth snapshot bootstrap
    assert rep["1m"]["closed_accepted"] == 1 and rep["1m"]["gate_reject_reasons"]["DUPLICATE_EVENT"] == 2
    assert rep["1m"]["gate_reject_reasons"]["KLINE_OPEN_NONMONOTONE"] == 1


# ====================================================================== end state with two Futures connections
def test_futures_two_connections_final_state_is_the_latest_complete_one():
    r, c, fac, rest, clock = make({"/public/": [[depth(99, 101, pu=98), fut_book_ticker(1)] + [fut_book_ticker(i) for i in range(2, 30)]],
                                   "/market/": [[agg(1), mark()] + [agg(i) for i in range(2, 30)]]},
                                  FUT_REST, kline_intervals=("1m",))
    run(r.run_binance(Source.BINANCE_FUTURES, "BTCUSDT", 0.9))
    assert r.end_captures == 2                                           # BOTH connections reported end-of-data
    assert r.end_state["at"] == r.end_at.isoformat()                      # the stored state IS the latest capture
    assert r.end_state["readiness"] == "READY"
    assert set(r.end_state["core_types"]) == {"TRADE", "BOOK_TICKER", "MARK_PRICE"}
    assert all(r.end_state["per_type"][t] == "OK" for t in r.end_state["core_types"])      # market + public data both present in the one final state
    assert r.reconnects == 0 and r.c.gate.issue_counts.get("DISCONNECTED", 0) == 0
