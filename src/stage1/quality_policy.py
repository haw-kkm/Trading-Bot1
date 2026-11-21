"""Stage 1 input-admission policy: who/what an agent may read.

Stage 0's DataQualityGate decides readiness per (source, symbol, event type). This module adds the Stage 1
rules on top (it never relaxes the gate):
  * source and symbol must be known and consistent (common form, exchange symbol maps back to it);
  * events carrying UNVERIFIED_* flags, BACKFILL or FUTURE_EVENT_TIME never reach an agent;
  * semantics the gate cannot see (what a number MEANS) are registered here as explicit UNVERIFIED.
"""
import re
from datetime import datetime
from typing import Optional, Tuple

from ..stage0.envelope import UNVERIFIED_FLAGS, EnvelopeError, MarketEnvelope, Source
from ..stage0.quality import Readiness
from ..stage0.symbols import binance_to_common, toobit_to_common
from .models import AgentStatus

FUTURES_SOURCES = frozenset({Source.BINANCE_FUTURES, Source.TOOBIT_FUTURES})
_SYM = re.compile(r"^[A-Z0-9]{2,20}/[A-Z]{3,5}(:[A-Z]{3,5})?$")

# Trade quantity confirmed as BASE-asset units (so price x quantity is a valid notional). Toobit swap trade
# quantity units are not confirmed in Stage 0 -> excluded until verified.
NOTIONAL_VERIFIED_SOURCES = frozenset({Source.BINANCE_SPOT, Source.BINANCE_FUTURES})

# Open interest: only sources whose aggregation basis is confirmed may feed the OI agent.
# Toobit: unit BASE_ASSET confirmed, contract coefficient already applied, aggregation basis UNVERIFIED.
OI_AGGREGATION_VERIFIED = {Source.BINANCE_FUTURES: True, Source.TOOBIT_FUTURES: False}
TOOBIT_OI_NOTE = ("Toobit OI: unit BASE_ASSET confirmed, contract coefficient already applied, "
                  "aggregation basis UNVERIFIED -> quarantined, never compared with or summed to other exchanges")

# Long/short ratio: Binance public ratio events carry no population / update-period metadata in the envelope.
LSR_REQUIRED_META = ("population", "period_seconds")

SEMANTIC_BLOCK_FLAGS = UNVERIFIED_FLAGS | {"BACKFILL", "FUTURE_EVENT_TIME"}


def parse_source(source) -> Source:
    try:
        return Source(source)
    except ValueError:
        raise EnvelopeError(f"unknown source {source!r}")


def validate_request(source, symbol) -> Tuple[Optional[Source], Optional[str]]:
    """(Source, None) if valid else (None, reason)."""
    try:
        src = parse_source(source)
    except EnvelopeError as e:
        return None, str(e)
    if not isinstance(symbol, str) or not _SYM.match(symbol):
        return None, f"symbol {symbol!r} is not in common form (BASE/QUOTE or BASE/QUOTE:QUOTE)"
    futures = src in FUTURES_SOURCES
    if futures != (":" in symbol):
        return None, f"symbol {symbol!r} does not match market type of {src.value}"
    base_quote = symbol.split(":")[0]
    if futures and symbol.partition(":")[2] != base_quote.partition("/")[2]:
        return None, f"symbol {symbol!r}: settle currency must equal quote for a linear perpetual"
    return src, None


def event_problem(env: MarketEnvelope, source: Source, symbol: str) -> Optional[str]:
    """None if the envelope may reach an agent, else the reason it is quarantined."""
    if env.source != source:
        return f"source {env.source.value} != requested {source.value}"
    if env.symbol != symbol:
        return f"symbol {env.symbol} != requested {symbol}"
    xs = env.exchange_symbol
    if not xs:
        return "exchange symbol missing (symbol origin unconfirmed)"
    try:
        back = (toobit_to_common(xs) if source == Source.TOOBIT_FUTURES
                else binance_to_common(xs, source in FUTURES_SOURCES))
    except EnvelopeError:
        return f"exchange symbol {xs!r} not parseable"
    if back != symbol:
        return f"exchange symbol {xs!r} maps to {back}, not {symbol}"
    bad = sorted(SEMANTIC_BLOCK_FLAGS & set(env.quality_flags))
    if bad:
        return f"flags {bad}"
    return None


def readiness_to_status(readiness: Readiness) -> AgentStatus:
    if readiness == Readiness.UNVERIFIED:
        return AgentStatus.UNVERIFIED
    if readiness == Readiness.INVALID:
        return AgentStatus.INVALID
    if readiness == Readiness.READY:
        return AgentStatus.READY
    return AgentStatus.DATA_UNAVAILABLE      # STALE / DISCONNECTED / SYNCING


def event_instant(env: MarketEnvelope) -> datetime:
    return env.event_time or env.received_at
