"""LiveRunner / transports with FAKE REST + WebSocket (mock integration). No real network is ever opened:
sockets are patched to fail the test if touched. Real-connection checks are the user's job (src/stage0/live.py)."""
import asyncio
import json
import socket
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0 import collector as col
from src.stage0 import live
from src.stage0.collector import Collector, NetworkDisabled, NetworkPolicy, PublicRestClient, ReconnectPolicy
from src.stage0.envelope import Source
from src.stage0.live import LiveRunner, PublicWsClient, ReconnectExhausted, ResyncExhausted, WsClosed
from src.stage0.orderbook import BookState
from src.stage0.quality import Readiness

T0 = datetime(2026, 11, 20, 12, 0, 0, tzinfo=timezone.utc)
MS = 1795176000000


@pytest.fixture(autouse=True)
def no_real_network(monkeypatch):
    real_connect = socket.socket.connect

    def guarded(self, address, *a, **k):          # asyncio itself uses loopback socketpairs on Windows
        host = address[0] if isinstance(address, tuple) else address
        if host not in ("127.0.0.1", "::1", "localhost"):
            raise AssertionError(f"real network touched: {address}")
        return real_connect(self, address, *a, **k)

    def boom(*a, **k):
        raise AssertionError("real network touched")
    monkeypatch.setattr(socket.socket, "connect", guarded)
    monkeypatch.setattr(socket, "create_connection", boom)


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
        self.script, self.clock, self.sent, self.closed, self._pick = list(script), clock, [], False, pick

    async def connect(self, url):
        self.url = url
        if self._pick is not None:                 # per-URL scripts (futures uses two connections)
            self.script = list(self._pick(url))

    async def send(self, text):
        self.sent.append(json.loads(text))

    async def recv(self):
        self.clock.advance(0.1)
        if not self.script:
            raise WsClosed("script exhausted")
        item = self.script.pop(0)
        if item == "CLOSE":
            raise WsClosed("closed by peer")
        if item == "TIMEOUT1":
            raise asyncio.TimeoutError()
        if item == "TIMEOUT":
            self.clock.advance(30)
            raise asyncio.TimeoutError()
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
        path = url.split("/", 3)[3]
        self.calls.append(("/" + path, dict(params)))
        r = self.responses["/" + path]
        return r() if callable(r) else r


def depth(U, u, b=(), a=(), pu=None):
    m = {"e": "depthUpdate", "E": MS, "s": "BTCUSDT", "U": U, "u": u, "b": [list(x) for x in b], "a": [list(x) for x in a]}
    if pu is not None:
        m["pu"] = pu
    return {"stream": "btcusdt@depth@100ms", "data": m}


def snap(last):
    return {"lastUpdateId": last, "bids": [["99", "1"]], "asks": [["101", "1"]]}


def trade(i):
    return {"stream": "btcusdt@trade", "data": {"e": "trade", "E": MS, "s": "BTCUSDT", "t": i, "p": "100", "q": "1", "T": MS, "m": False}}


def make(scripts, rest_map, policy=None, **kw):
    clock = Clock()
    pol = policy or NetworkPolicy(allow_binance=True)
    rest = PublicRestClient(pol, transport=FakeRest(rest_map))
    fac = Factory(scripts, clock)
    c = Collector()
    r = LiveRunner(c, rest, PublicWsClient(pol, fac), clock=clock, sleep=clock.sleep, **kw)
    return r, c, fac, rest._transport, clock


def run(coro):
    return asyncio.run(coro)


# ------------------------------------------------------------------ Binance
def test_binance_snapshot_plus_buffered_deltas_then_ready():
    r, c, fac, rest, clock = make([[depth(98, 100), depth(101, 102, b=[("99", "5")]), trade(1)]],
                                  {"/api/v3/depth": snap(100)})
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 0.25))   # stop before the script runs dry
    b = c.books[(Source.BINANCE_SPOT, "BTC/USDT")]
    assert b.state == BookState.VALID and b.bids[Decimal(99)] == 5
    assert rest.calls[0] == ("/api/v3/depth", {"symbol": "BTCUSDT", "limit": 1000})
    assert "btcusdt@depth@100ms" in fac.conns[0].url and fac.conns[0].closed


def test_binance_rejected_kline_raw_is_attested_and_survives_eviction(tmp_path):
    """Parser-rejected KLINE evidence stays in its own JSONL, even with bounded rejects."""
    import hashlib

    def frame(event_ms=MS+500, interval="1m", closed=True):
        body = {"e": "kline", "E": event_ms, "s": "BTCUSDT",
                "k": {"t": MS, "T": MS + 59999, "s": "BTCUSDT",
                      "i": interval, "x": closed, "o": "100", "h": "110",
                      "l": "90", "c": "101", "v": "10"}}
        return {"stream": "btcusdt@kline_1m", "data": body}

    premature = frame()
    unsupported = frame(interval="1s", closed=False)
    bad_other = {"not": "a Binance stream"}
    r, c, fac, rest, clock = make([[premature, unsupported, bad_other, trade(1)]],
                                  {"/api/v3/depth": snap(100)})
    path = tmp_path / "raw.rejected.jsonl"
    c.rejected_sink = live.RejectedJsonlSink(path)
    c.max_rejected = 1
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 0.35))
    attestation = c.rejected_sink.close()

    raw_bytes = path.read_bytes()
    rows = [json.loads(line) for line in raw_bytes.splitlines()]
    assert c.total_rejected == 3 and len(c.rejected) == 1
    assert len(rows) == 2 and attestation["lines"] == 2
    assert hashlib.sha256(raw_bytes).hexdigest() == attestation["sha256"]
    assert rows[0]["raw_message"] == premature
    assert rows[1]["raw_message"] == unsupported
    assert rows[0]["event_time_ms"] == MS + 500
    assert rows[0]["close_time_ms"] == MS + 59999
    assert rows[0]["is_closed"] is True
    assert rows[1]["is_closed"] is False
    assert "KLINE_PREMATURE_CLOSE" in rows[0]["error"]
    assert "KLINE_NOT_SUPPORTED" in rows[1]["error"]
    assert all(row["source"] == "binance-spot" for row in rows)
    assert all(row["channel"] == "ws" for row in rows)
    assert not any("raw_message" in row for row in c.rejected)  # bad non-KLINE only
    assert any(e.event_type.value == "TRADE" for e in c.envelopes)


def test_binance_gap_triggers_resync_with_new_snapshot():
    seq = iter([snap(100), snap(200)])
    r, c, fac, rest, clock = make([[depth(101, 101), depth(105, 106), depth(201, 202)]],
                                  {"/api/v3/depth": lambda: next(seq)})
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 0.25))
    assert r.resyncs == 1 and len(rest.calls) == 2
    b = c.books[(Source.BINANCE_SPOT, "BTC/USDT")]
    assert b.state == BookState.VALID and b.last_update_id == 202
    assert "BOOK_INVALID" in c.gate.kinds()


def test_binance_disconnect_reconnects_with_backoff_and_resyncs():
    r, c, fac, rest, clock = make([[depth(101, 101), "CLOSE"], [depth(101, 101), trade(2)]],
                                  {"/api/v3/depth": snap(100)}, reconnect=ReconnectPolicy(base_s=1, cap_s=4, max_attempts=3))
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 3))
    assert r.reconnects >= 1 and len(fac.conns) >= 2 and fac.conns[0].closed
    assert "DISCONNECTED" in c.gate.kinds()
    assert len(rest.calls) >= 2                                   # a fresh snapshot after reconnect
    assert c.books[(Source.BINANCE_SPOT, "BTC/USDT")].state == BookState.VALID


def test_binance_reconnect_exhausted_marks_resync_failed():
    clock = Clock()
    pol = NetworkPolicy(allow_binance=True)

    class Failing:
        def __call__(self):
            raise OSError("connect refused")
    c = Collector()
    r = LiveRunner(c, PublicRestClient(pol, transport=lambda u, p: snap(1)), PublicWsClient(pol, Failing()),
                   clock=clock, sleep=clock.sleep, reconnect=ReconnectPolicy(base_s=1, cap_s=2, max_attempts=3))
    with pytest.raises(ReconnectExhausted):
        run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 600))
    assert "RESYNC_FAILED" in c.gate.kinds() and r.reconnects == 4


def test_resync_limit_stops_instead_of_looping_forever():
    r, c, fac, rest, clock = make([[depth(500 + 10 * i, 501 + 10 * i) for i in range(10)]], {"/api/v3/depth": snap(100)}, max_resyncs=2)
    with pytest.raises(ResyncExhausted):
        run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 100))
    assert c.gate.status(Source.BINANCE_SPOT, "BTC/USDT", clock()) != Readiness.READY


def test_idle_timeout_is_treated_as_disconnect():
    r, c, fac, rest, clock = make([["TIMEOUT"], [trade(1)]], {"/api/v3/depth": snap(100)})
    run(r.run_binance(Source.BINANCE_SPOT, "BTCUSDT", 40))
    assert "DISCONNECTED" in c.gate.kinds() and len(fac.conns) >= 2


def agg(i):
    return {"stream": "btcusdt@aggTrade", "data": {"e": "aggTrade", "E": MS, "s": "BTCUSDT", "a": i, "p": "100", "q": "1",
                                                  "f": i, "l": i, "T": MS, "m": False}}


def mark():
    return {"stream": "btcusdt@markPrice@1s", "data": {"e": "markPriceUpdate", "E": MS, "s": "BTCUSDT", "p": "100",
                                                        "i": "100", "P": "100", "r": "0.0001", "T": MS + 1}}


def test_binance_futures_uses_routed_public_and_market_endpoints():
    r, c, fac, rest, clock = make({"/public/": [[depth(99, 101, pu=98), depth(102, 103, pu=101)]],
                                   "/market/": [[agg(1), mark(), agg(2)]]},
                                  {"/fapi/v1/depth": snap(100)}, policy=NetworkPolicy(allow_binance=True))
    run(r.run_binance(Source.BINANCE_FUTURES, "BTCUSDT", 3))
    urls = {cn.url for cn in fac.conns}
    assert urls == {"wss://fstream.binance.com/market/stream?streams=btcusdt@aggTrade/btcusdt@markPrice@1s",
                    "wss://fstream.binance.com/public/stream?streams=btcusdt@bookTicker/btcusdt@depth@100ms"}
    assert not any("/stream?" in u and "fstream.binance.com/stream" in u for u in urls)      # no legacy URL
    kinds = {(e.event_type.value, e.sequence_id) for e in c.envelopes}
    assert ("ORDERBOOK_DELTA", "103") in kinds and ("TRADE", "2") in kinds
    assert any(k[0] == "MARK_PRICE" for k in kinds) and rest.calls[0][0] == "/fapi/v1/depth"


def test_clock_skew_detected_from_server_time():
    r, c, fac, rest, clock = make([[]], {"/api/v3/time": {"serverTime": MS + 5000}})
    off = run(r.sync_clock(Source.BINANCE_SPOT))
    assert 4900 <= off <= 5100 and "CLOCK_SKEW" in c.gate.kinds()


def test_reference_poll_quarantines_bad_responses():
    r, c, fac, rest, clock = make([[]], {"/fapi/v1/openInterest": {"symbol": "BTCUSDT", "openInterest": "5", "time": MS},
                                         "/fapi/v1/fundingRate": {"unexpected": 1},
                                         "/futures/data/globalLongShortAccountRatio": []})
    run(r.poll_reference(Source.BINANCE_FUTURES, "BTCUSDT"))
    assert any(e.event_type.value == "OPEN_INTEREST" for e in c.envelopes)
    assert any("fundingRate" in x["endpoint"] for x in c.rejected)


# ------------------------------------------------------------------ Toobit
TSYM = "BTC-SWAP-USDT"
APPROVED = NetworkPolicy(allow_toobit=True, toobit_user_approved=True)


def tws(topic, data, **x):
    m = {"symbol": TSYM, "topic": topic, "data": data, "f": True, "sendTime": MS}
    m.update(x)
    return m


def test_toobit_blocked_without_user_approval_even_if_allow_flag_set():
    clock = Clock()
    pol = NetworkPolicy(allow_toobit=True)             # no run-time approval
    called = []
    r = LiveRunner(Collector(), PublicRestClient(pol, transport=lambda u, p: called.append(u)),
                   PublicWsClient(pol, lambda: called.append("ws")), clock=clock, sleep=clock.sleep)
    with pytest.raises(NetworkDisabled):
        run(r.run_toobit(TSYM, 5))
    with pytest.raises(NetworkDisabled):
        run(r.sync_clock(Source.TOOBIT_FUTURES))
    assert called == [] and col.TOOBIT_NETWORK_APPROVED is False


def test_toobit_subscribes_parses_pings_and_gates():
    script = [tws("trade", [{"v": "1", "t": MS, "p": "100", "q": "2", "m": True}], f=False),
              tws("bookTicker", {"e": "bookTicker", "E": MS, "s": TSYM, "b": "99", "bq": "1", "a": "100", "aq": "1", "t": MS}),
              "TIMEOUT1", {"pong": 1}, tws("markPrice", [{"symbol": TSYM, "markPrice": "100", "time": MS}])]
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED, ping_interval_s=10)
    run(r.run_toobit(TSYM, 0.45))
    conn = fac.conns[0]
    subs = [m for m in conn.sent if "topic" in m]
    assert {m["topic"] for m in subs} == {"trade", "bookTicker", "markPrice", "depth"} and all(m["event"] == "sub" for m in subs)
    assert any("ping" in m for m in conn.sent)
    kinds = {e.event_type.value for e in c.envelopes}
    assert {"TRADE", "BOOK_TICKER", "MARK_PRICE"} <= kinds
    assert c.gate.usable(Source.TOOBIT_FUTURES, "BTC/USDT:USDT", clock(), types=["TRADE", "BOOK_TICKER", "MARK_PRICE"])


def test_toobit_unverified_data_keeps_gate_unverified():
    script = [tws("index", [{"symbol": "BTCUSDT", "index": "100", "edp": "100", "formula": "f", "time": MS}])]
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED)
    run(r.run_toobit(TSYM, 1))
    assert c.gate.status(Source.TOOBIT_FUTURES, "BTC/USDT:USDT", clock()) == Readiness.UNVERIFIED


def test_toobit_bad_message_quarantined_stream_continues():
    script = ["not json", tws("mystery", [{}]), tws("trade", [{"v": "9", "t": MS, "p": "1", "q": "1", "m": False}])]
    r, c, fac, rest, clock = make([script], {}, policy=APPROVED)
    run(r.run_toobit(TSYM, 1))
    assert len(c.rejected) == 2 and any(e.event_type.value == "TRADE" for e in c.envelopes)


def test_toobit_reconnect_resubscribes():
    r, c, fac, rest, clock = make([[tws("trade", [{"v": "1", "t": MS, "p": "1", "q": "1", "m": True}]), "CLOSE"], []],
                                  {}, policy=APPROVED)
    run(r.run_toobit(TSYM, 4))
    assert len(fac.conns) >= 2
    assert all(any(m.get("topic") == "trade" for m in cn.sent) for cn in fac.conns[:2])
    assert "DISCONNECTED" in c.gate.kinds()


def test_ws_client_refuses_foreign_url():
    pol = NetworkPolicy(allow_binance=True)
    with pytest.raises(NetworkDisabled):
        run(PublicWsClient(pol, lambda: None).open(Source.BINANCE_SPOT, "wss://evil.example.com/stream"))


# ------------------------------------------------------------------ REST client / CLI / safety
def test_toobit_rest_paths_are_public_market_data_only():
    paths = col.PUBLIC_PATHS[Source.TOOBIT_FUTURES]
    assert paths and not any(w in p.lower() for p in paths for w in ("order", "account", "position", "withdraw", "balance", "user"))
    for s, ps in col.PUBLIC_PATHS.items():
        assert not any("order" in p.lower() and "bookTicker" not in p for p in ps), s


def test_cli_refuses_without_flags_and_toobit_needs_both_flags(capsys):
    assert live.main(["--source", "binance-futures", "--symbol", "BTCUSDT"]) == 2
    assert live.main(["--source", "toobit-futures", "--symbol", TSYM, "--allow-toobit-network"]) == 2
    assert live.main(["--source", "toobit-futures", "--symbol", TSYM, "--i-approve-toobit-public-readonly"]) == 2
    assert col.TOOBIT_NETWORK_APPROVED is False


def test_live_modules_contain_no_order_or_auth_code():
    import pathlib
    text = "".join(p.read_text(encoding="utf-8") for p in pathlib.Path(live.__file__).parent.rglob("*.py"))
    for needle in ("create_order", "place_order", "new_order", "cancel_order", "hmac", "X-MBX-APIKEY", "api_secret",
                   "signature="):
        assert needle not in text, needle
