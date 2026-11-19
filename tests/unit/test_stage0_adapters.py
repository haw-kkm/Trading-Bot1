"""Stage 0 exchange adapters: Binance Spot/Futures (shapes per Binance public docs, not live-checked),
Toobit (UNVERIFIED synthetic shapes)."""
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from src.stage0.adapters import binance
from src.stage0.envelope import EnvelopeError
from src.stage0.symbols import (binance_to_common, common_to_binance, common_to_toobit, toobit_to_common)

RX = datetime(2026, 3, 31, 12, 0, 1, tzinfo=timezone.utc)
MS = 1774958400000


def test_symbol_mapping_separates_common_and_exchange():
    assert binance_to_common("BTCUSDT", False) == "BTC/USDT"
    assert binance_to_common("BTCUSDT", True) == "BTC/USDT:USDT"
    assert toobit_to_common("BTC-SWAP-USDT") == "BTC/USDT:USDT"
    assert common_to_binance("BTC/USDT:USDT") == "BTCUSDT" and common_to_toobit("BTC/USDT:USDT") == "BTC-SWAP-USDT"
    for bad in ("BTC-USDT", "BTC-SPOT-USDT", "", "BTC-SWAP-"):
        with pytest.raises(EnvelopeError):
            toobit_to_common(bad)
    with pytest.raises(EnvelopeError):
        binance_to_common("btcusdt", False)
    with pytest.raises(EnvelopeError):
        common_to_toobit("BTC/USDT")        # spot symbol is not a perpetual


def test_spot_trade_and_combined_wrapper():
    m = {"e": "trade", "E": MS, "s": "BTCUSDT", "t": 5, "p": "50000.10", "q": "0.5", "T": MS, "m": True}
    [e] = binance.SPOT.parse({"stream": "btcusdt@trade", "data": m}, RX)
    assert e.source.value == "binance-spot" and e.symbol == "BTC/USDT" and e.exchange_symbol == "BTCUSDT"
    assert e.normalized_payload["price"] == Decimal("50000.10") and e.raw_payload == m and e.sequence_id == "5"
    assert e.event_time.timestamp() * 1000 == MS


def test_spot_book_ticker_has_no_event_time():
    [e] = binance.SPOT.parse({"u": 1, "s": "ETHUSDT", "b": "10", "B": "1", "a": "11", "A": "2"}, RX)
    assert e.event_time is None and "NO_EVENT_TIME" in e.quality_flags


def test_spot_kline_snapshot_price_time():
    k = {"e": "kline", "E": MS, "s": "BTCUSDT", "k": {"t": MS, "T": MS + 59999, "s": "BTCUSDT", "i": "1m",
         "o": "1", "c": "2", "h": "3", "l": "0.5", "v": "10", "x": False}}
    [e] = binance.SPOT.parse(k, RX)
    assert e.normalized_payload["interval"] == "1m" and e.normalized_payload["high"] == 3
    s = binance.SPOT.parse_depth_snapshot("BTCUSDT", {"lastUpdateId": 7, "bids": [["1", "1"]], "asks": [["2", "1"]]}, RX)
    assert s.normalized_payload["last_update_id"] == 7
    assert binance.SPOT.parse_price({"symbol": "BTCUSDT", "price": "5"}, RX).normalized_payload["price"] == 5
    assert binance.SPOT.parse_server_time({"serverTime": MS}, RX).event_type.value == "SERVER_TIME"


def test_unknown_messages_raise_not_guess():
    for bad in ({"e": "wat"}, [], "x", {"foo": 1}, {"e": "markPriceUpdate", "s": "BTCUSDT"}):
        with pytest.raises((EnvelopeError, KeyError)):
            binance.SPOT.parse(bad, RX)


def test_futures_mark_index_funding():
    m = {"e": "markPriceUpdate", "E": MS, "s": "BTCUSDT", "p": "50010.1", "i": "50008.2", "P": "1", "r": "-0.0001", "T": MS + 1}
    evs = binance.FUTURES.parse(m, RX)
    assert [e.event_type.value for e in evs] == ["MARK_PRICE", "INDEX_PRICE", "FUNDING_RATE"]
    assert evs[2].normalized_payload["funding_rate"] == Decimal("-0.0001")
    assert evs[0].symbol == "BTC/USDT:USDT"


def test_futures_depth_update_has_pu():
    [e] = binance.FUTURES.parse({"e": "depthUpdate", "E": MS, "T": MS, "s": "BTCUSDT", "U": 5, "u": 7, "pu": 4,
                                 "b": [["1", "0"]], "a": []}, RX)
    assert e.normalized_payload["prev_final_update_id"] == 4


def test_futures_oi_funding_ratio_liquidation_exchangeinfo():
    oi = binance.FUTURES.parse_open_interest({"symbol": "BTCUSDT", "openInterest": "10.5", "time": MS}, RX)
    assert oi.normalized_payload["open_interest"] == Decimal("10.5")
    [f] = binance.FUTURES.parse_funding_history([{"symbol": "BTCUSDT", "fundingRate": "0.0001", "fundingTime": MS, "markPrice": ""}], RX)
    assert f.event_type.value == "FUNDING_RATE"
    [r] = binance.FUTURES.parse_long_short_ratio([{"symbol": "BTCUSDT", "longShortRatio": "1.2", "longAccount": "0.55",
                                                   "shortAccount": "0.45", "timestamp": MS}], RX)
    assert r.normalized_payload["ratio"] == Decimal("1.2")
    [l] = binance.FUTURES.parse({"e": "forceOrder", "E": MS, "o": {"s": "BTCUSDT", "S": "SELL", "q": "1", "p": "10", "ap": "9", "z": "1"}}, RX)
    assert l.event_type.value == "LIQUIDATION" and l.provenance.value == "OBSERVED" and l.normalized_payload["price"] == 9
    spec = binance.FUTURES.parse_exchange_info_symbol({"symbol": "BTCUSDT", "status": "TRADING", "contractType": "PERPETUAL",
        "filters": [{"filterType": "PRICE_FILTER", "tickSize": "0.10"}, {"filterType": "LOT_SIZE", "stepSize": "0.001"}]}, RX)
    assert spec.normalized_payload["tick_size"] == Decimal("0.10")


def test_spot_parser_rejects_futures_only_messages():
    with pytest.raises(EnvelopeError):
        binance.SPOT.parse({"e": "forceOrder", "o": {}}, RX)


def test_estimated_liquidation_map_is_not_implemented_not_faked():
    with pytest.raises(NotImplementedError, match="ESTIMATED_DATA_UNAVAILABLE"):
        binance.estimated_liquidation_map()


def test_book_imbalance_derived_value():
    b = [[Decimal(1), Decimal(3)]]
    a = [[Decimal(2), Decimal(1)]]
    assert binance.book_imbalance(b, a) == Decimal("0.5")
    with pytest.raises(EnvelopeError):
        binance.book_imbalance([], [])
