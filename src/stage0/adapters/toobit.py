"""Toobit USDT-M futures public-data parsers (DATA-03).

SOURCE OF TRUTH: Toobit official docs, https://api-docs.toobit.com/ (pages usdt-m-market-data and
usdt-m-websocket-market-data), read on 2026-11-20. The old toobit-docs.github.io mirror says it is no
longer updated and was NOT used. Field NAMES below follow those pages; the pages were read through a
summarising fetch tool, so verbatim response samples were not available: fixtures reproduce the
documented STRUCTURE with synthetic values. Nothing has been compared with a real response yet.

REST and WebSocket shapes are DIFFERENT and parsed separately:
  REST  price     GET /quote/v1/contract/ticker/price   [{"s","p"}]
  REST  trades    GET /quote/v1/trades                  [{"p","q","t","ibm"}]            ibm = is buyer maker
  REST  depth     GET /quote/v1/depth                   {"t","b":[[p,q]],"a":[[p,q]]}   (no sequence ids)
  REST  bookTick. GET /quote/v1/contract/ticker/bookTicker [{"t","s","b","bq","a","aq"}]
  REST  mark      GET /quote/v1/markPrice               {"exchangeId","symbolId","price","time"} or list of them
  REST  index     GET /quote/v1/index                   {"index": {"BTCUSDT": "p"}, "edp": {"BTCUSDT": "p"}}
  REST  funding   GET /api/v1/futures/fundingRate       [{"symbol","rate","period","nextFundingTime",
                                                           "interest","fundingRateCap","fundingRateFloor"}]  (no "time")
  REST  OI        GET /quote/v1/openInterest            {"openInterestList": [{"size","symbol"}]}
  REST  contracts GET /api/v1/exchangeInfo              {"contracts": [{..."contractMultiplier","filters":[...]}]}
  WS    wss://stream.toobit.com  (path /quote/ws/v1)    {"topic","symbol","data":[...],"f","sendTime","params"}
        topics: trade {v,t,p,q,m(true=buy)}, markPrice {symbol,markPrice,time}, index {symbol,index,edp,formula,time},
                depth {s,t,v,b,a} (partial book), bookTicker {e,E,s,b,bq,a,aq,t}, kline_<i> {t,s,o,h,l,c,v}

VERIFICATION LEVELS (per data type, see VERIFICATION):
  DOC         structure matches the official docs; NOT yet compared with a live response.
              Envelope gets informational flag DOC_ONLY (no penalty); the gate may call it READY.
  UNVERIFIED  documentation is silent/ambiguous (units, symbol relation, sequence semantics).
              Envelope gets UNVERIFIED_SCHEMA; the gate reports UNVERIFIED, never READY.
Promotion to a higher level is a deliberate human edit after a live check (tools: src/stage0/live.py --check).
Missing/odd fields raise EnvelopeError; nothing is guessed.
"""
from typing import Any, Dict, List

from ..envelope import EnvelopeError, EventType, MarketEnvelope, Source, ms_to_utc, to_decimal
from ..symbols import binance_to_common, toobit_to_common

class SubscriptionError(EnvelopeError):
    """The server answered a subscribe with an error object ({"code","desc"}), e.g. -100010 "Invalid Symbols!"."""


DOC, UNVERIFIED = "DOC", "UNVERIFIED"
DOC_DATE = "2026-11-20"
DOC_URL = "https://api-docs.toobit.com/api/usdt-m-market-data.html"

VERIFICATION = {
    "rest_ticker_price": DOC, "rest_trades": DOC, "rest_depth": DOC, "rest_book_ticker": DOC,
    "rest_mark_price": DOC, "rest_funding_rate": DOC, "rest_contract": DOC,
    # docs silent on the symbol relation of the index map keys (BTCUSDT vs BTC-SWAP-USDT):
    "rest_index_price": UNVERIFIED,
    # docs give no unit for `size` (contracts? base asset?):
    "rest_open_interest": UNVERIFIED,
    "ws_trade": DOC, "ws_mark_price": DOC, "ws_depth": DOC, "ws_book_ticker": DOC, "ws_kline": DOC,
    "ws_index": UNVERIFIED,
    # diffDepth: docs say version numbers are not guaranteed unique -> cannot detect gaps:
    "ws_diff_depth": UNVERIFIED,
}


def _flags(key: str):
    return ("UNVERIFIED_SCHEMA",) if VERIFICATION[key] == UNVERIFIED else ("DOC_ONLY",)


def _env(key, sym, et, ms, rx, raw, norm, seq=None, common=None, extra_flags=()):
    return MarketEnvelope(source=Source.TOOBIT_FUTURES, symbol=common or toobit_to_common(sym),
                          exchange_symbol=sym, event_type=et, event_time=None if ms is None else ms_to_utc(ms),
                          received_at=rx, raw_payload=raw, normalized_payload=norm, sequence_id=seq,
                          quality_flags=_flags(key) + tuple(extra_flags))


def index_symbol(symbol: str) -> str:
    """Index endpoints are keyed by the spot-style name (BTCUSDT), NOT BTC-SWAP-USDT.
    OBSERVED 2026-11-19 (live): GET /quote/v1/index?symbol=BTC-SWAP-USDT -> {"index":{},"edp":{}} (empty);
    ?symbol=BTCUSDT -> {"index":{"BTCUSDT":...},...}. WS topic "index" with BTC-SWAP-USDT -> code -100010."""
    parts = symbol.split("-")
    if len(parts) != 3 or parts[1] != "SWAP":
        raise EnvelopeError(f"unrecognised Toobit symbol {symbol!r}")
    return parts[0] + parts[2]


def _need(d, *keys):
    if not isinstance(d, dict):
        raise EnvelopeError("Toobit payload must be an object")
    miss = [k for k in keys if k not in d]
    if miss:
        raise EnvelopeError(f"Toobit payload missing {miss} (not guessing)")


def _as_list(x, name):
    if isinstance(x, dict):
        return [x]
    if isinstance(x, list):
        return x
    raise EnvelopeError(f"{name}: expected object or array")


def _index_common(sym: str) -> str:
    """Index map keys look like BTCUSDT; accept BASE-SWAP-QUOTE too. Relation is UNVERIFIED."""
    return toobit_to_common(sym) if "-" in sym else binance_to_common(sym, True)


# ============================== REST ==============================
def parse_ticker_price(rows, rx) -> List[MarketEnvelope]:
    out = []
    for r in _as_list(rows, "ticker/price"):
        _need(r, "s", "p")
        out.append(_env("rest_ticker_price", r["s"], EventType.TICKER, None, rx, r, {"price": r["p"]}))
    return out


def parse_trades(symbol: str, rows, rx) -> List[MarketEnvelope]:
    """The REST trades response carries no symbol: the caller's request symbol is used."""
    out = []
    for r in _as_list(rows, "trades"):
        _need(r, "t", "p", "q")
        out.append(_env("rest_trades", symbol, EventType.TRADE, r["t"], rx, r,
                        {"price": r["p"], "quantity": r["q"], "buyer_is_maker": r.get("ibm")},
                        seq=None))     # no trade id on REST -> no duplicate detection possible
    return out


def parse_depth(symbol: str, r, rx) -> MarketEnvelope:
    """Full REST snapshot WITHOUT sequence ids: no gap detection is possible for it."""
    _need(r, "t", "b", "a")
    return _env("rest_depth", symbol, EventType.ORDERBOOK_SNAPSHOT, r["t"], rx, r,
                {"bids": r["b"], "asks": r["a"], "no_sequence_ids": True})


def parse_book_ticker(rows, rx) -> List[MarketEnvelope]:
    out = []
    for r in _as_list(rows, "bookTicker"):
        _need(r, "s", "b", "bq", "a", "aq")
        out.append(_env("rest_book_ticker", r["s"], EventType.BOOK_TICKER, r.get("t"), rx, r,
                        {"bid_price": r["b"], "bid_qty": r["bq"], "ask_price": r["a"], "ask_qty": r["aq"]}))
    return out


def parse_mark_price(rows, rx) -> List[MarketEnvelope]:
    out = []
    for r in _as_list(rows, "markPrice"):
        _need(r, "symbolId", "price", "time")
        out.append(_env("rest_mark_price", r["symbolId"], EventType.MARK_PRICE, r["time"], rx, r,
                        {"mark_price": r["price"]}))
    return out


def parse_index_price(r, rx) -> List[MarketEnvelope]:
    _need(r, "index")
    if not isinstance(r["index"], dict):
        raise EnvelopeError("index must be a {symbol: price} map")
    out = []                  # an EMPTY map (wrong symbol form requested) yields no events: callers must treat as no data
    for sym, price in r["index"].items():
        norm = {"index_price": price}
        edp = r.get("edp", {})
        if isinstance(edp, dict) and sym in edp:
            norm["edp_10m_avg"] = edp[sym]
        out.append(_env("rest_index_price", sym, EventType.INDEX_PRICE, None, rx, {"index": {sym: price}, "edp": edp},
                        norm, common=_index_common(sym)))
    return out


def parse_funding_rate(rows, rx) -> List[MarketEnvelope]:
    """Documented response has NO observation time: event_time stays None (flagged NO_EVENT_TIME)."""
    out = []
    for r in _as_list(rows, "fundingRate"):
        _need(r, "symbol", "rate")
        out.append(_env("rest_funding_rate", r["symbol"], EventType.FUNDING_RATE, None, rx, r,
                        {"funding_rate": r["rate"], "next_funding_time_ms": r.get("nextFundingTime"),
                         "period": r.get("period")}))
    return out


def parse_open_interest(r, rx) -> List[MarketEnvelope]:
    """`size` UNIT IS UNDOCUMENTED (contracts vs base asset) -> kept raw, flagged UNVERIFIED."""
    _need(r, "openInterestList")
    out = []
    for it in r["openInterestList"]:
        _need(it, "size", "symbol")
        out.append(_env("rest_open_interest", it["symbol"], EventType.OPEN_INTEREST, None, rx, it,
                        {"open_interest": it["size"], "unit": "UNDOCUMENTED"}))
    return out


def parse_contracts(r, rx) -> List[MarketEnvelope]:
    """exchangeInfo['contracts']; tick/step from `filters`; contractMultiplier is mandatory."""
    _need(r, "contracts")
    out = []
    for c in r["contracts"]:
        _need(c, "symbol", "contractMultiplier", "filters")
        tick = step = None
        for f in c["filters"]:
            if f.get("filterType") == "PRICE_FILTER":
                tick = f.get("tickSize")
            elif f.get("filterType") == "LOT_SIZE":
                step = f.get("stepSize")
        if tick is None or step is None:
            raise EnvelopeError(f"{c['symbol']}: PRICE_FILTER/LOT_SIZE missing in filters")
        try:
            common = toobit_to_common(c["symbol"])
        except EnvelopeError:
            continue                      # not a BASE-SWAP-QUOTE linear perpetual (e.g. inverse BTC-SWAP): skipped
        out.append(_env("rest_contract", c["symbol"], EventType.CONTRACT_SPEC, None, rx, c,
                        {"tick_size": tick, "step_size": step, "contract_size": c["contractMultiplier"],
                         "status": c.get("status"), "inverse": bool(c.get("inverse"))}, common=common))
    return out


# ============================== WebSocket ==============================
def parse_ws(msg: Any, rx) -> List[MarketEnvelope]:
    """Top-level wrapper {topic, symbol, data, f, sendTime, params}. ping/pong -> []."""
    if not isinstance(msg, dict):
        raise EnvelopeError("Toobit WS message must be an object")
    if "pong" in msg or "ping" in msg:
        return []
    if "code" in msg and "desc" in msg and "topic" not in msg:
        raise SubscriptionError(f"server rejected the request: code={msg['code']} desc={msg['desc']}")
    _need(msg, "topic", "data")
    topic, wsym = msg["topic"], msg.get("symbol")
    items = _as_list(msg["data"], "data")
    out: List[MarketEnvelope] = []
    for d in items:
        if topic == "trade":
            _need(d, "t", "p", "q")
            # OBSERVED live: the first push (f=true) replays the ~60 most recent trades (up to ~15 s old).
            # They are history, not live data: flagged BACKFILL (no lag check, never counts as fresh).
            backfill = ("BACKFILL",) if msg.get("f") is True else ()
            out.append(_env("ws_trade", wsym or d.get("s"), EventType.TRADE, d["t"], rx, d,
                            {"price": d["p"], "quantity": d["q"], "is_buy": d.get("m"), "first_push": msg.get("f")},
                            seq=None if d.get("v") is None else str(d["v"]), extra_flags=backfill))
        elif topic == "markPrice":
            # OBSERVED live (2026-11-19): after an empty ack push (data: []), pushes carry a dict
            # {"exchangeId","symbolId","price","time"} (REST-like) and no top-level "symbol". The docs
            # describe {"symbol","markPrice","time"}; both are accepted, the observed one first.
            sym = d.get("symbolId") or d.get("symbol") or wsym
            price = d.get("price") if d.get("price") is not None else d.get("markPrice")
            if sym is None or price is None or d.get("time") is None:
                raise EnvelopeError("markPrice push without (symbolId|symbol), (price|markPrice) and time")
            out.append(_env("ws_mark_price", sym, EventType.MARK_PRICE, d["time"], rx, d, {"mark_price": price}))
        elif topic == "index":
            _need(d, "symbol", "index")
            norm = {"index_price": d["index"]}
            if d.get("edp") is not None:
                norm["edp_10m_avg"] = d["edp"]
            out.append(_env("ws_index", d["symbol"], EventType.INDEX_PRICE, d.get("time"), rx, d, norm,
                            common=_index_common(d["symbol"])))
        elif topic == "depth":
            _need(d, "s", "t", "b", "a")
            out.append(_env("ws_depth", d["s"], EventType.ORDERBOOK_SNAPSHOT, d["t"], rx, d,
                            {"bids": d["b"], "asks": d["a"], "no_sequence_ids": True, "partial_depth": True,
                             "version": d.get("v")}))
        elif topic == "diffDepth":
            _need(d, "s", "t", "b", "a")
            out.append(_env("ws_diff_depth", d["s"], EventType.ORDERBOOK_DELTA, d["t"], rx, d,
                            {"bids": d["b"], "asks": d["a"], "version": d.get("v"), "no_sequence_ids": True}))
        elif topic in ("bookTicker",):
            _need(d, "s", "b", "bq", "a", "aq")
            out.append(_env("ws_book_ticker", d["s"], EventType.BOOK_TICKER, d.get("E") or d.get("t"), rx, d,
                            {"bid_price": d["b"], "bid_qty": d["bq"], "ask_price": d["a"], "ask_qty": d["aq"]}))
        elif topic == "kline":
            _need(d, "t", "s", "o", "h", "l", "c", "v")
            interval = (msg.get("params") or {}).get("klineType")
            if not interval:
                raise EnvelopeError("kline push without params.klineType")
            out.append(_env("ws_kline", d["s"], EventType.KLINE, d["t"], rx, d,
                            {"open": d["o"], "high": d["h"], "low": d["l"], "close": d["c"], "volume": d["v"],
                             "interval": interval}))
        else:
            raise EnvelopeError(f"unsupported/undocumented Toobit topic {topic!r}")
    return out


def ws_subscribe_message(topic: str, symbol: str, binary: bool = False) -> Dict[str, Any]:
    """Subscribe frame per docs: {"symbol","topic","event":"sub","params":{"binary":false}}."""
    if topic not in ("trade", "markPrice", "depth", "diffDepth", "bookTicker", "index") and not topic.startswith("kline_"):
        raise EnvelopeError(f"unsupported topic {topic!r}")
    if topic == "index":
        symbol = index_symbol(symbol)         # live: BTC-SWAP-USDT is rejected ("Invalid Symbols!") for this topic
    return {"symbol": symbol, "topic": topic, "event": "sub", "params": {"binary": binary}}


def contracts_to_base_qty(contracts, contract_size):
    """Contract count -> base-asset quantity using contractMultiplier (e.g. BTC-SWAP-USDT: 0.0001)."""
    return to_decimal(contracts, "contracts") * to_decimal(contract_size, "contract_size")
