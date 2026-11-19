"""Common symbol <-> exchange symbol mapping.

Common form: spot "BASE/QUOTE", USDT-margined perpetual "BASE/QUOTE:QUOTE" (ccxt style).
The exchange's own symbol is ALWAYS kept separately on the envelope (exchange_symbol).

Binance:  BTCUSDT          (spot and USDT-M perp share the string; the SOURCE disambiguates)
Toobit:   BTC-SWAP-USDT    (USDT-M perpetual; format taken from the task statement, UNVERIFIED
                            against Toobit's official docs because no network access was allowed)
"""
from .envelope import EnvelopeError

_QUOTES = ("USDT", "USDC", "BUSD", "FDUSD")


def binance_to_common(sym: str, futures: bool) -> str:
    if not isinstance(sym, str) or not sym.isalnum() or sym != sym.upper():
        raise EnvelopeError(f"unrecognised Binance symbol {sym!r}")
    for q in _QUOTES:
        if sym.endswith(q) and len(sym) > len(q):
            base = sym[: -len(q)]
            return f"{base}/{q}:{q}" if futures else f"{base}/{q}"
    raise EnvelopeError(f"cannot split Binance symbol {sym!r}")


def common_to_binance(common: str) -> str:
    return common.split(":")[0].replace("/", "")


def toobit_to_common(sym: str) -> str:
    parts = sym.split("-") if isinstance(sym, str) else []
    if len(parts) != 3 or parts[1] != "SWAP" or not parts[0] or not parts[2]:
        raise EnvelopeError(f"unrecognised Toobit symbol {sym!r} (expected BASE-SWAP-QUOTE)")
    return f"{parts[0]}/{parts[2]}:{parts[2]}"


def common_to_toobit(common: str) -> str:
    pair, _, settle = common.partition(":")
    base, _, quote = pair.partition("/")
    if not (base and quote and settle == quote):
        raise EnvelopeError(f"not a linear perpetual common symbol: {common!r}")
    return f"{base}-SWAP-{quote}"
