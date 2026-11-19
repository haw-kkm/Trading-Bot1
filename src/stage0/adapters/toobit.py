"""Toobit USDT-M futures parsers (DATA-03).  *** ALL SHAPES UNVERIFIED ***

No network access was allowed in this task and Toobit's official documentation was NOT consulted,
so NONE of the field layouts below is confirmed against the official docs or a real response.
The layouts are an ASSUMED contract (based on the task statement's symbol format and general
exchange conventions). Therefore:
  * every envelope produced here carries the quality flag UNVERIFIED_SCHEMA (reliability penalty),
  * ReadinessGate reports UNVERIFIED for this source until the flag is removed by a person who
    has checked each parser against official docs/real samples (see docs/plans/stage0-data.md),
  * fixtures under tests/fixtures/stage0/toobit_* are SYNTHETIC, not official samples.
Missing/odd fields raise EnvelopeError; nothing is guessed to fill a gap.

Assumed REST shapes (UNVERIFIED):
  ticker/price : [{"s": "BTC-SWAP-USDT", "p": "price"}]
  trades       : [{"t": ms, "p": "price", "q": "qty", "ibm": bool}]   (+ "s" supplied by caller)
  depth        : {"t": ms, "b": [[p,q],..], "a": [[p,q],..]}
  mark / index : {"symbol": ..., "price": ..., "time": ms}
  funding      : {"symbol": ..., "rate": ..., "nextFundingTime": ms, "time": ms}
  open interest: {"symbol": ..., "openInterest": ..., "time": ms}
  contract     : {"symbol": ..., "tickSize": ..., "stepSize": ..., "contractMultiplier": ...}
"""
from typing import Any, Dict, List

from ..envelope import EnvelopeError, EventType, MarketEnvelope, Source, ms_to_utc
from ..symbols import toobit_to_common

FLAG = "UNVERIFIED_SCHEMA"


def _env(sym, et, ms, rx, raw, norm, seq=None):
    return MarketEnvelope(source=Source.TOOBIT_FUTURES, symbol=toobit_to_common(sym), exchange_symbol=sym,
                          event_type=et, event_time=None if ms is None else ms_to_utc(ms),
                          received_at=rx, raw_payload=raw, normalized_payload=norm, sequence_id=seq,
                          quality_flags=(FLAG,))


def _need(d: Dict[str, Any], *keys):
    if not isinstance(d, dict):
        raise EnvelopeError("Toobit payload must be an object")
    miss = [k for k in keys if k not in d]
    if miss:
        raise EnvelopeError(f"Toobit payload missing {miss} (shape unverified; not guessing)")


def parse_ticker_price(rows: List[Dict[str, Any]], rx) -> List[MarketEnvelope]:
    out = []
    for r in rows:
        _need(r, "s", "p")
        out.append(_env(r["s"], EventType.TICKER, None, rx, r, {"price": r["p"]}))
    return out


def parse_trades(symbol: str, rows: List[Dict[str, Any]], rx) -> List[MarketEnvelope]:
    out = []
    for r in rows:
        _need(r, "t", "p", "q")
        out.append(_env(symbol, EventType.TRADE, r["t"], rx, r,
                        {"price": r["p"], "quantity": r["q"], "buyer_is_maker": r.get("ibm")},
                        seq=str(r["t"])))
    return out


def parse_depth(symbol: str, r: Dict[str, Any], rx) -> MarketEnvelope:
    """Assumed to be a FULL snapshot (no sequence ids known) -> no gap detection is possible."""
    _need(r, "t", "b", "a")
    return _env(symbol, EventType.ORDERBOOK_SNAPSHOT, r["t"], rx, r,
                {"bids": r["b"], "asks": r["a"], "no_sequence_ids": True})


def parse_mark_price(r, rx) -> MarketEnvelope:
    _need(r, "symbol", "price", "time")
    return _env(r["symbol"], EventType.MARK_PRICE, r["time"], rx, r, {"mark_price": r["price"]})


def parse_index_price(r, rx) -> MarketEnvelope:
    _need(r, "symbol", "price", "time")
    return _env(r["symbol"], EventType.INDEX_PRICE, r["time"], rx, r, {"index_price": r["price"]})


def parse_funding_rate(r, rx) -> MarketEnvelope:
    _need(r, "symbol", "rate", "time")
    return _env(r["symbol"], EventType.FUNDING_RATE, r["time"], rx, r,
                {"funding_rate": r["rate"], "next_funding_time_ms": r.get("nextFundingTime")})


def parse_open_interest(r, rx) -> MarketEnvelope:
    _need(r, "symbol", "openInterest", "time")
    return _env(r["symbol"], EventType.OPEN_INTEREST, r["time"], rx, r, {"open_interest": r["openInterest"]})


def parse_contract(r, rx) -> MarketEnvelope:
    """contract_size (multiplier) is REQUIRED: quantities are not comparable to Binance without it."""
    _need(r, "symbol", "tickSize", "stepSize", "contractMultiplier")
    return _env(r["symbol"], EventType.CONTRACT_SPEC, None, rx, r,
                {"tick_size": r["tickSize"], "step_size": r["stepSize"],
                 "contract_size": r["contractMultiplier"]})


def contracts_to_base_qty(contracts, contract_size):
    """Convert a Toobit contract count to base-asset quantity using the contract multiplier."""
    from ..envelope import to_decimal
    return to_decimal(contracts, "contracts") * to_decimal(contract_size, "contract_size")
