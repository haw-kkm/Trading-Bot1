"""Stage 0 order book, quality gate, collector, network policy, account interface. No network."""
import pathlib
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0 import collector as col
from src.stage0.account_interface import NotConfigured, UnconfiguredAccount
from src.stage0.adapters import binance
from src.stage0.collector import (Collector, Mode, NetworkDisabled, NetworkPolicy, PublicRestClient,
                                  ReconnectPolicy, SubscriptionManager)
from src.stage0.envelope import EnvelopeError, Source
from src.stage0.orderbook import BookState, LocalOrderBook
from src.stage0.quality import DataQualityGate, Readiness

T0 = datetime(2026, 3, 31, 12, 0, 0, tzinfo=timezone.utc)
MS = 1774958400000
FIX = pathlib.Path(__file__).resolve().parents[1] / "fixtures" / "stage0" / "session_basic.jsonl"


def spot(msg, rx):
    return binance.SPOT.parse(msg, rx)[0]


def trade(seq, ms, price="100", rx=None, parser=binance.SPOT):
    return parser.parse({"e": "trade", "E": ms, "s": "BTCUSDT", "t": seq, "p": price, "q": "1", "T": ms, "m": False},
                        rx or T0 + timedelta(milliseconds=ms - MS))[0]


# ---------------------------------------------------------------- order book
def synced_book(futures=False):
    b = LocalOrderBook(futures)
    b.start_sync()
    b.apply_snapshot(100, [["99", "1"]], [["101", "1"]])
    return b


def test_book_snapshot_then_continuous_deltas():
    b = synced_book()
    assert b.state == BookState.VALID
    b.on_delta(101, 102, [["99", "2"]], [])
    b.on_delta(103, 103, [["98", "1"]], [["101", "0"]])
    assert b.state == BookState.VALID and b.bids[Decimal(99)] == 2 and Decimal(101) not in b.asks


def test_book_buffered_events_dropped_or_applied_after_snapshot():
    b = LocalOrderBook(False)
    b.start_sync()
    b.on_delta(90, 99, [["50", "9"]], [])         # entirely before snapshot -> dropped
    b.on_delta(100, 102, [["99", "5"]], [])        # spans lastUpdateId+1 -> applied
    b.apply_snapshot(100, [["99", "1"]], [["101", "1"]])
    assert b.state == BookState.VALID and b.bids[Decimal(99)] == 5 and Decimal(50) not in b.bids


def test_spot_sequence_gap_invalidates_then_resync_recovers():
    b = synced_book()
    b.on_delta(101, 101, [], [])
    assert b.on_delta(103, 104, [["99", "7"]], []) == BookState.INVALID and "gap" in b.reason
    assert b.on_delta(105, 105, [["99", "8"]], []) == BookState.INVALID     # ignored while invalid
    assert b.bids[Decimal(99)] != 8
    b.resync()
    b.apply_snapshot(200, [["99", "1"]], [["101", "1"]])
    assert b.state == BookState.VALID and b.on_delta(201, 201, [], []) == BookState.VALID


def test_futures_uses_pu_chain():
    b = synced_book(True)
    assert b.on_delta(100, 105, [], [], pu=99) == BookState.VALID        # U<=101<=u at start
    assert b.on_delta(106, 107, [], [], pu=105) == BookState.VALID
    assert b.on_delta(108, 109, [], [], pu=100) == BookState.INVALID


def test_first_event_gap_after_snapshot_invalidates():
    b = synced_book()
    assert b.on_delta(150, 160, [], []) == BookState.INVALID


def test_duplicate_delta_is_not_applied_twice():
    b = synced_book()
    b.on_delta(101, 101, [["99", "2"]], [])
    b.on_delta(101, 101, [["99", "9"]], [])
    assert b.bids[Decimal(99)] == 2 and b.state == BookState.VALID


def test_crossed_book_and_abnormal_levels_invalidate():
    b = synced_book()
    assert b.on_delta(101, 101, [["102", "1"]], []) == BookState.INVALID and "crossed" in b.reason
    b2 = synced_book()
    assert b2.on_delta(101, 101, [["-1", "1"]], []) == BookState.INVALID


# ---------------------------------------------------------------- quality gate
def ready_gate():
    g = DataQualityGate()
    g.on_connect(Source.BINANCE_SPOT)
    return g


def test_ready_requires_connection_and_fresh_data():
    g = DataQualityGate()
    e = trade(1, MS)
    g.observe(e)
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0) == Readiness.DISCONNECTED
    g.on_connect(Source.BINANCE_SPOT)
    assert g.usable(Source.BINANCE_SPOT, "BTC/USDT", T0)
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0 + timedelta(seconds=60)) == Readiness.STALE
    assert not g.usable(Source.BINANCE_SPOT, "BTC/USDT", T0 + timedelta(seconds=60))
    assert g.status(Source.BINANCE_SPOT, "ETH/USDT", T0) == Readiness.STALE          # never seen


def test_disconnect_then_reconnect_is_syncing_until_synced():
    g = ready_gate()
    g.observe(trade(1, MS))
    g.on_disconnect(Source.BINANCE_SPOT)
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0) == Readiness.DISCONNECTED
    g.on_connect(Source.BINANCE_SPOT, ["BTC/USDT"])
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0) == Readiness.SYNCING
    g.mark_synced(Source.BINANCE_SPOT, "BTC/USDT")
    assert g.usable(Source.BINANCE_SPOT, "BTC/USDT", T0)


def test_resync_failure_makes_invalid():
    g = ready_gate()
    g.observe(trade(1, MS))
    g.on_connect(Source.BINANCE_SPOT, ["BTC/USDT"])
    g.on_resync_failed(Source.BINANCE_SPOT, "BTC/USDT", "snapshot timeout")
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0) == Readiness.INVALID and "RESYNC_FAILED" in g.kinds()


def test_duplicate_out_of_order_future_lag_detected():
    g = ready_gate()
    assert g.observe(trade(1, MS + 1000)) == []
    assert g.observe(trade(1, MS + 1000)) == ["DUPLICATE_EVENT"]
    assert g.observe(trade(2, MS + 500)) == ["OUT_OF_ORDER"]
    fut = binance.SPOT.parse({"e": "trade", "E": MS + 600000, "s": "BTCUSDT", "t": 3, "p": "1", "q": "1", "T": 1, "m": False}, T0)[0]
    assert "FUTURE_EVENT_TIME" in g.observe(fut)
    lag = trade(4, MS + 2000, rx=T0 + timedelta(seconds=60))
    assert "RECEIVE_LAG" in g.observe(lag)


def test_out_of_order_event_does_not_overwrite_newer_price():
    g = ready_gate()
    g.observe(trade(1, MS + 1000, "100"))
    g.observe(trade(2, MS + 100, "50"))
    assert g._prices[("binance-spot", "BTC/USDT", "last")].normalized_payload["price"] == 100


def test_crossed_book_ticker_invalid_then_sane_quote_recovers():
    g = ready_gate()
    bad = spot({"u": 1, "s": "BTCUSDT", "b": "101", "B": "1", "a": "100", "A": "1"}, T0)
    assert g.observe(bad) == ["CROSSED_BOOK"]
    assert g.status(Source.BINANCE_SPOT, "BTC/USDT", T0) == Readiness.INVALID
    g.observe(spot({"u": 2, "s": "BTCUSDT", "b": "99", "B": "1", "a": "100", "A": "1"}, T0))
    assert g.usable(Source.BINANCE_SPOT, "BTC/USDT", T0)


def test_toobit_is_unverified_never_ready():
    g = DataQualityGate()
    g.on_connect(Source.TOOBIT_FUTURES)
    from src.stage0.adapters import toobit
    g.observe(toobit.parse_open_interest({"openInterestList": [{"size": "5", "symbol": "BTC-SWAP-USDT"}]}, T0)[0])
    assert g.status(Source.TOOBIT_FUTURES, "BTC/USDT:USDT", T0) == Readiness.UNVERIFIED
    assert not g.usable(Source.TOOBIT_FUTURES, "BTC/USDT:USDT", T0)


def _two_markets(g, fut_price, skew_ms=0):
    g.on_connect(Source.BINANCE_SPOT)
    g.on_connect(Source.BINANCE_FUTURES)
    g.observe(trade(1, MS, "100"))
    g.observe(binance.FUTURES.parse({"e": "trade", "E": MS + skew_ms, "s": "BTCUSDT", "t": 1, "p": fut_price, "q": "1", "T": 1, "m": False},
                                    T0 + timedelta(milliseconds=skew_ms))[0])


def test_divergence_blocks_spot_vs_futures_without_basis_flag():
    g = DataQualityGate()
    _two_markets(g, "100")
    r = g.divergence(Source.BINANCE_SPOT, Source.BINANCE_FUTURES, "BTC/USDT", "last", T0)
    assert isinstance(r, str) and "BASIS" in r


def test_divergence_computed_only_for_matching_symbol_kind_and_fresh_pair():
    g = DataQualityGate()
    _two_markets(g, "100")
    # symbols differ in common form (spot vs perp) -> use the perp's own key for futures side
    g.observe(binance.FUTURES.parse({"e": "trade", "E": MS, "s": "BTCUSDT", "t": 2, "p": "101", "q": "1", "T": 1, "m": False}, T0)[0])
    r = g.divergence(Source.BINANCE_SPOT, Source.BINANCE_FUTURES, "BTC/USDT", "last", T0, allow_basis=True)
    assert isinstance(r, str)            # futures price lives under "BTC/USDT:USDT": not the same key


def test_divergence_flags_large_gap_between_comparable_feeds():
    g = DataQualityGate(divergence_pct=Decimal("0.5"))
    g.on_connect(Source.BINANCE_SPOT)
    g.on_connect(Source.BINANCE_FUTURES)
    sym = "BTC/USDT"
    a = trade(1, MS, "100")
    b = binance.FUTURES.parse({"e": "trade", "E": MS, "s": "BTCUSDT", "t": 1, "p": "102", "q": "1", "T": 1, "m": False}, T0)[0]
    g.observe(a)
    g.observe(b)
    g._prices[("binance-futures", sym, "last")] = b      # same common key to compare (test wiring)
    g._last_rx[("binance-futures", sym)] = T0
    d = g.divergence(Source.BINANCE_SPOT, Source.BINANCE_FUTURES, sym, "last", T0, allow_basis=True)
    assert d.exceeds and d.pct > Decimal("1.9") and "PRICE_DIVERGENCE" in g.kinds()


def test_divergence_refuses_stale_or_skewed_inputs():
    g = DataQualityGate(max_pair_skew=timedelta(seconds=1))
    g.on_connect(Source.BINANCE_SPOT)
    g.on_connect(Source.BINANCE_FUTURES)
    sym = "BTC/USDT"
    a = trade(1, MS, "100")
    b = binance.FUTURES.parse({"e": "trade", "E": MS + 5000, "s": "BTCUSDT", "t": 1, "p": "150", "q": "1", "T": 1, "m": False},
                              T0 + timedelta(seconds=5))[0]
    g.observe(a); g.observe(b)
    g._prices[("binance-futures", sym, "last")] = b
    g._last_rx[("binance-futures", sym)] = T0 + timedelta(seconds=5)
    r = g.divergence(Source.BINANCE_SPOT, Source.BINANCE_FUTURES, sym, "last", T0 + timedelta(seconds=5), allow_basis=True)
    assert isinstance(r, str) and "NOT_COMPARABLE" in r      # spot feed is 5s old -> stale/skewed
    r2 = g.divergence(Source.BINANCE_SPOT, Source.BINANCE_FUTURES, sym, "mark", T0, allow_basis=True)
    assert isinstance(r2, str)


# ---------------------------------------------------------------- collector / network
def test_fixture_replay_is_deterministic_and_makes_no_network_calls(monkeypatch):
    import socket
    def boom(*a, **k):
        raise AssertionError("network used in FIXTURE mode")
    monkeypatch.setattr(socket, "socket", boom)
    monkeypatch.setattr(socket, "create_connection", boom)
    c1 = col.run(Mode.FIXTURE, str(FIX))
    c2 = col.run(Mode.FIXTURE, str(FIX))
    assert [e.digest for e in c1.envelopes] == [e.digest for e in c2.envelopes]
    assert c1.network_calls == 0 and len(c1.envelopes) >= 10
    assert len(c1.rejected) == 1 and "bogus" in c1.rejected[0]["error"] or "unrecognised" in c1.rejected[0]["error"]
    kinds = {e.event_type.value for e in c1.envelopes}
    assert {"TRADE", "BOOK_TICKER", "ORDERBOOK_DELTA", "ORDERBOOK_SNAPSHOT", "MARK_PRICE", "FUNDING_RATE",
            "OPEN_INTEREST", "LIQUIDATION", "TICKER"} <= kinds


def test_fixture_book_synced_and_gate_states():
    c = col.run(Mode.FIXTURE, str(FIX))
    book = c.books[(Source.BINANCE_SPOT, "BTC/USDT")]
    assert book.state == BookState.VALID and book.bids[Decimal(50000)] == 2 and book.last_update_id == 103
    assert c.gate.status(Source.BINANCE_SPOT, "BTC/USDT", T0 + timedelta(seconds=1)) == Readiness.READY
    assert c.gate.status(Source.TOOBIT_FUTURES, "BTC/USDT:USDT", T0) == Readiness.UNVERIFIED


def test_fixture_gap_session_invalidates_book(tmp_path):
    f = tmp_path / "gap.jsonl"
    f.write_text("\n".join([
        '{"t":"2026-03-31T12:00:00Z","source":"binance-spot","kind":"control","action":"connect","symbol":"BTCUSDT"}',
        '{"t":"2026-03-31T12:00:00Z","source":"binance-spot","kind":"control","action":"start_sync","symbol":"BTCUSDT"}',
        '{"t":"2026-03-31T12:00:00Z","source":"binance-spot","kind":"depth_snapshot","symbol":"BTCUSDT","msg":{"lastUpdateId":100,"bids":[["99","1"]],"asks":[["101","1"]]}}',
        '{"t":"2026-03-31T12:00:01Z","source":"binance-spot","kind":"ws","msg":{"e":"depthUpdate","E":1774958400900,"s":"BTCUSDT","U":101,"u":101,"b":[],"a":[]}}',
        '{"t":"2026-03-31T12:00:02Z","source":"binance-spot","kind":"ws","msg":{"e":"depthUpdate","E":1774958401900,"s":"BTCUSDT","U":105,"u":106,"b":[],"a":[]}}',
    ]), encoding="utf-8")
    c = col.run(Mode.FIXTURE, str(f))
    assert c.books[(Source.BINANCE_SPOT, "BTC/USDT")].state == BookState.INVALID
    assert c.gate.status(Source.BINANCE_SPOT, "BTC/USDT", T0 + timedelta(seconds=2)) == Readiness.INVALID
    assert "BOOK_INVALID" in c.gate.kinds()


def test_default_network_policy_blocks_everything_and_transport_never_called():
    calls = []
    cl = PublicRestClient(NetworkPolicy(), transport=lambda u, p: calls.append(u))
    for s in (Source.BINANCE_SPOT, Source.BINANCE_FUTURES, Source.TOOBIT_FUTURES):
        with pytest.raises(NetworkDisabled):
            cl.get(s, "/api/v3/time")
    assert calls == []


def test_binance_allow_is_independent_of_toobit_and_toobit_stays_blocked():
    calls = []
    cl = PublicRestClient(NetworkPolicy(allow_binance=True, allow_toobit=True), transport=lambda u, p: calls.append(u) or {})
    cl.get(Source.BINANCE_SPOT, "/api/v3/time")
    assert calls == ["https://api.binance.com/api/v3/time"]
    with pytest.raises(NetworkDisabled):
        cl.get(Source.TOOBIT_FUTURES, "/anything")       # not approved even with allow_toobit=True
    assert col.TOOBIT_NETWORK_APPROVED is False


def test_only_public_paths_and_no_auth_params():
    cl = PublicRestClient(NetworkPolicy(allow_binance=True), transport=lambda u, p: {})
    for path in ("/api/v3/order", "/fapi/v1/order", "/api/v3/account", "/sapi/v1/capital/config/getall"):
        with pytest.raises(NetworkDisabled):
            cl.get(Source.BINANCE_SPOT, path)
    with pytest.raises(NetworkDisabled):
        cl.get(Source.BINANCE_SPOT, "/api/v3/time", {"signature": "x"})
    assert not hasattr(cl, "api_key") and "secret" not in PublicRestClient.__init__.__code__.co_varnames


def test_public_mode_requires_explicit_allow():
    with pytest.raises(NetworkDisabled):
        col.run(Mode.PUBLIC_READ_ONLY)
    with pytest.raises(ValueError):
        col.run(Mode.FIXTURE)


def test_reconnect_backoff_and_resubscription():
    d = ReconnectPolicy(base_s=1, cap_s=10, max_attempts=6).delays()
    assert d == [1, 2, 4, 8, 10, 10]
    m = SubscriptionManager()
    for s in SubscriptionManager.binance_streams("BTCUSDT"):
        m.add(Source.BINANCE_SPOT, s)
    assert m.resubscribe_plan(Source.BINANCE_SPOT) == ["btcusdt@bookTicker", "btcusdt@depth@100ms", "btcusdt@trade"]
    assert m.resubscribe_plan(Source.TOOBIT_FUTURES) == []


def test_no_order_api_in_stage0_sources():
    root = pathlib.Path(col.__file__).parent
    text = "".join(p.read_text(encoding="utf-8") for p in root.rglob("*.py"))
    for needle in ("create_order", "place_order", "new_order", "cancel_order", "/order\"", "ExecutionEngine", "api_secret"):
        assert needle not in text.replace("tests", ""), needle


def test_account_interface_not_configured_never_fakes_data():
    a = UnconfiguredAccount()
    for fn in (a.get_account_equity, a.get_all_positions, a.get_open_orders, a.get_conditional_orders, a.get_position_mode):
        with pytest.raises(NotConfigured, match="NOT_CONFIGURED"):
            fn()


def test_default_mode_needs_no_api_keys(monkeypatch):
    for k in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "TOOBIT_API_KEY", "TOOBIT_API_SECRET"):
        monkeypatch.delenv(k, raising=False)
    assert len(col.run(Mode.FIXTURE, str(FIX)).envelopes) > 0


def test_bad_fixture_line_quarantined_not_used(tmp_path):
    f = tmp_path / "bad.jsonl"
    f.write_text('{"t":"2026-03-31T12:00:00Z","source":"kraken","kind":"ws","msg":{}}\n{not json\n', encoding="utf-8")
    c = col.run(Mode.FIXTURE, str(f))
    assert c.envelopes == [] and len(c.rejected) == 2
