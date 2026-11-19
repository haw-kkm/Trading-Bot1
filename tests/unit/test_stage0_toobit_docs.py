"""Toobit parsers vs the OFFICIAL documented structure (api-docs.toobit.com, read 2026-11-20).

The samples below reproduce the documented FIELD STRUCTURE with synthetic values (the docs were read via a
summarising fetch, so verbatim examples were unavailable). They are regression tests against the docs,
NOT proof about real responses. REST and WebSocket shapes are deliberately kept apart."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.stage0.adapters import toobit
from src.stage0.envelope import EnvelopeError
from src.stage0.quality import DataQualityGate, Readiness
from src.stage0.envelope import Source

RX = datetime(2026, 11, 20, 12, 0, 1, tzinfo=timezone.utc)
MS = 1795176000000
SYM = "BTC-SWAP-USDT"


# ----------------------------------------------------------------------------- REST (documented shapes)
def test_rest_mark_price_uses_symbolId_and_price_object_or_list():
    raw = {"exchangeId": 301, "symbolId": SYM, "price": "50010.5", "time": MS}
    [e] = toobit.parse_mark_price(raw, RX)
    assert e.exchange_symbol == SYM and e.normalized_payload["mark_price"] == Decimal("50010.5")
    assert len(toobit.parse_mark_price([raw, dict(raw)], RX)) == 2
    with pytest.raises(EnvelopeError):            # old (wrong) shape: "symbol"
        toobit.parse_mark_price({"symbol": SYM, "price": "1", "time": MS}, RX)


def test_rest_index_is_a_symbol_keyed_map_and_unverified():
    [e] = toobit.parse_index_price({"index": {"BTCUSDT": "50008.1"}, "edp": {"BTCUSDT": "50007.9"}}, RX)
    assert e.normalized_payload["index_price"] == Decimal("50008.1") and e.normalized_payload["edp_10m_avg"] == Decimal("50007.9")
    assert e.symbol == "BTC/USDT:USDT" and "UNVERIFIED_SCHEMA" in e.quality_flags
    with pytest.raises(EnvelopeError):
        toobit.parse_index_price({"price": "1"}, RX)


def test_rest_funding_is_array_without_time():
    [e] = toobit.parse_funding_rate([{"symbol": SYM, "rate": "-0.0001", "period": "8H", "nextFundingTime": MS + 1,
                                      "interest": "0.0001", "fundingRateCap": "0.003", "fundingRateFloor": "-0.003"}], RX)
    assert e.normalized_payload["funding_rate"] == Decimal("-0.0001")
    assert e.event_time is None and "NO_EVENT_TIME" in e.quality_flags       # docs give no observation time
    assert e.normalized_payload["period"] == "8H" and "DOC_ONLY" in e.quality_flags


def test_rest_open_interest_list_with_size_is_unverified_unit():
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "1234.5", "symbol": SYM}]}, RX)
    assert e.normalized_payload["open_interest"] == Decimal("1234.5") and e.normalized_payload["unit"] == "UNDOCUMENTED"
    assert "UNVERIFIED_SCHEMA" in e.quality_flags
    with pytest.raises(EnvelopeError):
        toobit.parse_open_interest({"symbol": SYM, "openInterest": "1"}, RX)       # old (wrong) shape


def test_rest_contracts_filters_and_multiplier():
    info = {"timezone": "UTC", "symbols": [], "contracts": [
        {"symbol": SYM, "status": "TRADING", "inverse": False, "contractMultiplier": "0.0001",
         "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.1", "minPrice": "0.1", "maxPrice": "1e6"},
                     {"filterType": "LOT_SIZE", "stepSize": "1", "minQty": "1", "maxQty": "1000000"}]},
        {"symbol": "BTC-SWAP", "status": "TRADING", "inverse": True, "contractMultiplier": "1.0",
         "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.5"}, {"filterType": "LOT_SIZE", "stepSize": "1"}]}]}
    evs = toobit.parse_contracts(info, RX)
    assert [e.exchange_symbol for e in evs] == [SYM]          # inverse BTC-SWAP is not a linear USDT perp: skipped
    n = evs[0].normalized_payload
    assert n["tick_size"] == Decimal("0.1") and n["step_size"] == 1 and n["contract_size"] == Decimal("0.0001")
    assert toobit.contracts_to_base_qty("30", n["contract_size"]) == Decimal("0.0030")
    bad = {"contracts": [{"symbol": SYM, "contractMultiplier": "0.0001", "filters": [{"filterType": "LOT_SIZE", "stepSize": "1"}]}]}
    with pytest.raises(EnvelopeError):
        toobit.parse_contracts(bad, RX)
    with pytest.raises(EnvelopeError):
        toobit.parse_contracts({"contracts": [{"symbol": SYM, "filters": []}]}, RX)      # multiplier mandatory


def test_rest_book_ticker_trades_depth_price():
    [b] = toobit.parse_book_ticker([{"t": MS, "s": SYM, "b": "99", "bq": "2", "a": "100", "aq": "3"}], RX)
    assert b.normalized_payload["ask_qty"] == 3
    [t] = toobit.parse_trades(SYM, [{"p": "100", "q": "5", "t": MS, "ibm": True}], RX)
    assert t.normalized_payload["buyer_is_maker"] is True and t.sequence_id is None
    d = toobit.parse_depth(SYM, {"t": MS, "b": [["99", "1"]], "a": [["100", "1"]]}, RX)
    assert d.normalized_payload["no_sequence_ids"] is True
    [p] = toobit.parse_ticker_price([{"s": SYM, "p": "100"}], RX)
    assert p.symbol == "BTC/USDT:USDT"


# ----------------------------------------------------------------------------- WebSocket (documented wrapper)
def ws(topic, data, **extra):
    m = {"symbol": SYM, "topic": topic, "data": data, "f": True, "sendTime": MS}
    m.update(extra)
    return m


def test_ws_trade_wrapper_and_fields():
    evs = toobit.parse_ws(ws("trade", [{"v": "1001", "t": MS, "p": "100.5", "q": "2", "m": True},
                                       {"v": "1002", "t": MS + 1, "p": "100.6", "q": "1", "m": False}]), RX)
    assert [e.sequence_id for e in evs] == ["1001", "1002"] and evs[0].normalized_payload["is_buy"] is True
    assert "DOC_ONLY" in evs[0].quality_flags and "UNVERIFIED_SCHEMA" not in evs[0].quality_flags


def test_ws_mark_price_book_ticker_depth_kline_index():
    [m] = toobit.parse_ws(ws("markPrice", [{"symbol": SYM, "markPrice": "50000", "time": MS}]), RX)
    assert m.event_type.value == "MARK_PRICE"
    [b] = toobit.parse_ws(ws("bookTicker", {"e": "bookTicker", "E": MS, "s": SYM, "b": "1", "bq": "2", "a": "3", "aq": "4", "t": MS}), RX)
    assert b.normalized_payload["bid_price"] == 1
    [d] = toobit.parse_ws(ws("depth", [{"s": SYM, "t": MS, "v": "112801745_18", "b": [["99", "1"]], "a": [["100", "1"]]}]), RX)
    assert d.event_type.value == "ORDERBOOK_SNAPSHOT" and d.normalized_payload["partial_depth"] is True
    [k] = toobit.parse_ws(ws("kline", [{"t": MS, "s": SYM, "sn": SYM, "o": "1", "h": "3", "l": "0.5", "c": "2", "v": "10"}],
                             params={"klineType": "1m"}), RX)
    assert k.normalized_payload["interval"] == "1m"
    with pytest.raises(EnvelopeError):
        toobit.parse_ws(ws("kline", [{"t": MS, "s": SYM, "o": "1", "h": "3", "l": "0.5", "c": "2", "v": "10"}]), RX)
    [i] = toobit.parse_ws(ws("index", [{"symbol": "BTCUSDT", "index": "50001", "edp": "50000", "formula": "x", "time": MS}]), RX)
    assert "UNVERIFIED_SCHEMA" in i.quality_flags


def test_ws_diff_depth_is_unverified_and_never_a_synced_book():
    [e] = toobit.parse_ws(ws("diffDepth", [{"e": 0, "s": SYM, "t": MS, "v": "1_18", "b": [["99", "0"]], "a": []}]), RX)
    assert e.event_type.value == "ORDERBOOK_DELTA" and "UNVERIFIED_SCHEMA" in e.quality_flags
    g = DataQualityGate()
    g.on_connect(Source.TOOBIT_FUTURES)
    g.observe(e)
    assert g.status(Source.TOOBIT_FUTURES, e.symbol, RX) == Readiness.UNVERIFIED


def test_ws_ping_pong_ignored_unknown_topic_rejected():
    assert toobit.parse_ws({"pong": 1}, RX) == [] and toobit.parse_ws({"ping": 1}, RX) == []
    for bad in ({"topic": "mystery", "data": [{}]}, {"data": []}, [], "x"):
        with pytest.raises(EnvelopeError):
            toobit.parse_ws(bad, RX)


def test_ws_and_rest_shapes_are_not_interchangeable():
    with pytest.raises(EnvelopeError):
        toobit.parse_mark_price({"symbol": SYM, "markPrice": "1", "time": MS}, RX)     # WS item given to REST parser
    with pytest.raises(EnvelopeError):
        toobit.parse_ws(ws("markPrice", [{"symbolId": SYM, "price": "1", "time": MS}]), RX)  # REST item given to WS parser


def test_subscribe_message_matches_docs():
    assert toobit.ws_subscribe_message("trade", SYM) == {"symbol": SYM, "topic": "trade", "event": "sub", "params": {"binary": False}}
    assert toobit.ws_subscribe_message("kline_1m", SYM)["topic"] == "kline_1m"
    with pytest.raises(EnvelopeError):
        toobit.ws_subscribe_message("placeOrder", SYM)


# ----------------------------------------------------------------------------- verification levels in the gate
def test_documented_types_can_be_ready_but_unverified_types_never_are():
    g = DataQualityGate()
    g.on_connect(Source.TOOBIT_FUTURES)
    sym = "BTC/USDT:USDT"
    g.observe(toobit.parse_book_ticker([{"t": MS, "s": SYM, "b": "99", "bq": "2", "a": "100", "aq": "3"}], RX)[0])
    now = RX
    assert g.usable(Source.TOOBIT_FUTURES, sym, now)                              # doc-level type only
    g.observe(toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": SYM}]}, RX)[0])
    assert g.status(Source.TOOBIT_FUTURES, sym, now) == Readiness.UNVERIFIED       # any seen type unverified
    assert g.usable(Source.TOOBIT_FUTURES, sym, now, types=["BOOK_TICKER"])        # consumer needs only quotes
    assert not g.usable(Source.TOOBIT_FUTURES, sym, now, types=["OPEN_INTEREST"])


def test_every_parser_key_has_a_verification_level():
    assert set(toobit.VERIFICATION.values()) <= {toobit.DOC, toobit.UNVERIFIED}
    for k in ("rest_index_price", "rest_open_interest", "ws_index", "ws_diff_depth"):
        assert toobit.VERIFICATION[k] == toobit.UNVERIFIED
