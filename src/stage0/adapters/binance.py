"""Binance public-data parsers (Spot = DATA-01, USDT-M Futures = DATA-02).

Pure functions: raw JSON (already decoded) -> list[MarketEnvelope]. No I/O.
Field layouts below are Binance's public stream/REST shapes as known at implementation time;
they were NOT re-checked against live responses (no network access in this task). Official docs:
  Spot    https://developers.binance.com/docs/binance-spot-api-docs  (checked: NOT checked online)
  Futures https://developers.binance.com/docs/derivatives/usds-margined-futures (NOT checked online)
Unknown message shapes raise EnvelopeError instead of being guessed.
"""
from decimal import Decimal
from typing import Any, Dict, List

from ..envelope import (EnvelopeError, EventType, MarketEnvelope, Provenance, Source, ms_to_utc,
                        to_decimal)
from ..symbols import binance_to_common


def _env(src, futures, sym, et, event_ms, rx, raw, norm, seq=None, prov=Provenance.OBSERVED, flags=()):
    return MarketEnvelope(source=src, symbol=binance_to_common(sym, futures), exchange_symbol=sym,
                          event_type=et, event_time=None if event_ms is None else ms_to_utc(event_ms),
                          received_at=rx, raw_payload=raw, normalized_payload=norm, sequence_id=seq,
                          provenance=prov, quality_flags=flags)


def book_imbalance(bids, asks, depth: int = 5) -> Decimal:
    """(sum bid qty - sum ask qty) / (sum bid qty + sum ask qty) over the top `depth` levels, in
    [-1, 1]. DERIVED data. Raises on an empty book rather than returning 0."""
    b = sum((q for _, q in bids[:depth]), Decimal(0))
    a = sum((q for _, q in asks[:depth]), Decimal(0))
    if b + a == 0:
        raise EnvelopeError("cannot compute imbalance of an empty book")
    return (b - a) / (b + a)


class BinanceParser:
    def __init__(self, futures: bool):
        self.futures = futures
        self.src = Source.BINANCE_FUTURES if futures else Source.BINANCE_SPOT

    def _e(self, *a, **k):
        return _env(self.src, self.futures, *a, **k)

    # ---- websocket / REST message dispatch -------------------------------------------------
    def parse(self, raw: Any, received_at) -> List[MarketEnvelope]:
        if isinstance(raw, dict) and "stream" in raw and "data" in raw:   # combined-stream wrapper
            raw = raw["data"]
        if not isinstance(raw, dict):
            raise EnvelopeError("unsupported Binance message (not an object)")
        e = raw.get("e")
        if e in ("trade", "aggTrade"):
            return [self._trade(raw, received_at)]
        if e == "depthUpdate":
            return [self._depth_update(raw, received_at)]
        if e == "kline":
            return [self._kline(raw, received_at)]
        if e == "markPriceUpdate":
            if not self.futures:
                raise EnvelopeError("markPriceUpdate on spot parser")
            return self._mark(raw, received_at)
        if e == "forceOrder":
            if not self.futures:
                raise EnvelopeError("forceOrder on spot parser")
            return [self._liq(raw, received_at)]
        if "lastUpdateId" in raw and "bids" in raw:
            raise EnvelopeError("depth snapshot needs a symbol: use parse_depth_snapshot()")
        if "u" in raw and all(k in raw for k in ("s", "b", "B", "a", "A")):
            return [self._book_ticker(raw, received_at)]
        raise EnvelopeError(f"unrecognised Binance message keys: {sorted(raw)[:8]}")

    def _trade(self, r, rx):
        # trade stream has event time "E" and trade time "T"; we use E (message time).
        return self._e(r["s"], EventType.TRADE, r.get("E"), rx, r,
                       {"price": r["p"], "quantity": r["q"], "trade_time_ms": r.get("T"),
                        "buyer_is_maker": r.get("m")},
                       seq=str(r.get("t", r.get("a"))))

    def _book_ticker(self, r, rx):
        # Spot bookTicker has NO event time; Futures bookTicker has "E"/"T".
        ms = r.get("E") if self.futures else None
        return self._e(r["s"], EventType.BOOK_TICKER, ms, rx, r,
                       {"bid_price": r["b"], "bid_qty": r["B"], "ask_price": r["a"], "ask_qty": r["A"]},
                       seq=str(r["u"]))

    def _depth_update(self, r, rx):
        norm = {"bids": r["b"], "asks": r["a"], "first_update_id": r["U"], "final_update_id": r["u"]}
        if self.futures:
            norm["prev_final_update_id"] = r["pu"]
        return self._e(r["s"], EventType.ORDERBOOK_DELTA, r.get("E"), rx, r, norm, seq=str(r["u"]))

    def parse_depth_snapshot(self, symbol: str, raw: Dict[str, Any], received_at) -> MarketEnvelope:
        if "lastUpdateId" not in raw:
            raise EnvelopeError("depth snapshot without lastUpdateId")
        # REST snapshot carries no exchange timestamp (Futures adds E/T; spot does not).
        ms = raw.get("E") if self.futures else None
        return self._e(symbol, EventType.ORDERBOOK_SNAPSHOT, ms, received_at, raw,
                       {"bids": raw["bids"], "asks": raw["asks"], "last_update_id": raw["lastUpdateId"]},
                       seq=str(raw["lastUpdateId"]))

    def _kline(self, r, rx):
        k = r["k"]
        return self._e(r["s"], EventType.KLINE, r.get("E"), rx, r,
                       {"open": k["o"], "high": k["h"], "low": k["l"], "close": k["c"], "volume": k["v"],
                        "interval": k["i"], "open_time_ms": k["t"], "close_time_ms": k["T"],
                        "is_closed": bool(k["x"])}, seq=str(k["t"]))

    def parse_price(self, raw: Dict[str, Any], received_at) -> MarketEnvelope:
        """REST /ticker/price: {"symbol","price"} (futures adds "time"). No event time on spot."""
        ms = raw.get("time") if self.futures else None
        return self._e(raw["symbol"], EventType.TICKER, ms, received_at, raw, {"price": raw["price"]})

    def parse_server_time(self, raw: Dict[str, Any], received_at) -> MarketEnvelope:
        """REST /time: {"serverTime": ms}. Used for clock-offset estimation, not as a market event."""
        return MarketEnvelope(source=self.src, symbol="*", event_type=EventType.SERVER_TIME,
                              event_time=ms_to_utc(raw["serverTime"]), received_at=received_at,
                              raw_payload=raw, normalized_payload={"server_time_ms": raw["serverTime"]})

    # ---- futures-only ----------------------------------------------------------------------
    def _mark(self, r, rx):
        out = [self._e(r["s"], EventType.MARK_PRICE, r.get("E"), rx, r,
                       {"mark_price": r["p"], "index_price": r.get("i")}),
               self._e(r["s"], EventType.INDEX_PRICE, r.get("E"), rx, r, {"index_price": r["i"]})]
        if r.get("r") not in (None, ""):
            out.append(self._e(r["s"], EventType.FUNDING_RATE, r.get("E"), rx, r,
                               {"funding_rate": r["r"], "next_funding_time_ms": r.get("T")}))
        return out

    def parse_open_interest(self, raw, received_at) -> MarketEnvelope:
        return self._e(raw["symbol"], EventType.OPEN_INTEREST, raw.get("time"), received_at, raw,
                       {"open_interest": raw["openInterest"]})

    def parse_funding_history(self, rows, received_at) -> List[MarketEnvelope]:
        return [self._e(r["symbol"], EventType.FUNDING_RATE, r["fundingTime"], received_at, r,
                        {"funding_rate": r["fundingRate"], "mark_price": r.get("markPrice") or None})
                for r in rows]

    def parse_long_short_ratio(self, rows, received_at) -> List[MarketEnvelope]:
        return [self._e(r["symbol"], EventType.LONG_SHORT_RATIO, r["timestamp"], received_at, r,
                        {"ratio": r["longShortRatio"], "long_account": r["longAccount"],
                         "short_account": r["shortAccount"]}) for r in rows]

    def _liq(self, r, rx):
        o = r["o"]    # directly observed forced-liquidation order (public forceOrder stream)
        return self._e(o["s"], EventType.LIQUIDATION, r.get("E"), rx, r,
                       {"price": o["ap"] if to_decimal(o.get("ap", 0), "ap") > 0 else o["p"],
                        "quantity": o["z"] if to_decimal(o.get("z", 0), "z") > 0 else o["q"],
                        "side": o["S"]})

    def parse_exchange_info_symbol(self, s: Dict[str, Any], received_at) -> MarketEnvelope:
        """One entry of exchangeInfo['symbols']; extracts tick/step size filters only."""
        tick = step = None
        for f in s.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick = f.get("tickSize")
            elif f.get("filterType") == "LOT_SIZE":
                step = f.get("stepSize")
        norm = {"tick_size": tick, "step_size": step, "status": s.get("status")}
        if self.futures:
            norm["contract_size"] = 1 if s.get("contractType") == "PERPETUAL" else None
            norm["contract_type"] = s.get("contractType")
        return self._e(s["symbol"], EventType.CONTRACT_SPEC, None, received_at, s, norm)


def estimated_liquidation_map(*_a, **_k):
    """Liquidation heatmaps / estimated liquidation prices / leverage distributions are NOT
    provided by the exchange and are not implemented. Never present them as observed data."""
    raise NotImplementedError("ESTIMATED_DATA_UNAVAILABLE: no estimation model implemented (NOT_IMPLEMENTED)")


SPOT = BinanceParser(False)
FUTURES = BinanceParser(True)
