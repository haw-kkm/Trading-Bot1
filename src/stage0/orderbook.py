"""Local order book following Binance's documented snapshot + diff-stream procedure.

Spot:    drop buffered events with u <= lastUpdateId; first applied event must satisfy
         U <= lastUpdateId+1 <= u; afterwards each event must have U == previous u + 1.
Futures: same start rule; afterwards each event must have pu == previous u.
Any violation -> state INVALID (book unusable until a fresh snapshot is applied via resync()).
A crossed book (best bid >= best ask) also -> INVALID.
"""
from decimal import Decimal
from enum import Enum
from typing import Dict, List, Optional

from .envelope import EnvelopeError, to_decimal


class BookState(str, Enum):
    EMPTY = "EMPTY"
    SYNCING = "SYNCING"
    VALID = "VALID"
    INVALID = "INVALID"


class LocalOrderBook:
    def __init__(self, futures: bool):
        self.futures = futures
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.state = BookState.EMPTY
        self.reason: Optional[str] = None
        self.last_update_id: Optional[int] = None
        self._buffer: List[dict] = []
        self._first_applied = False

    # -- helpers
    def _invalidate(self, reason: str):
        self.state, self.reason = BookState.INVALID, reason
        return self.state

    def best_bid(self):
        return max(self.bids) if self.bids else None

    def best_ask(self):
        return min(self.asks) if self.asks else None

    def _apply_levels(self, side: Dict[Decimal, Decimal], levels):
        for p, q in levels:
            p, q = to_decimal(p, "price"), to_decimal(q, "qty")
            if p <= 0 or q < 0:
                raise EnvelopeError(f"abnormal level {p} {q}")
            if q == 0:
                side.pop(p, None)
            else:
                side[p] = q

    def _check_cross(self):
        bb, ba = self.best_bid(), self.best_ask()
        if bb is not None and ba is not None and bb >= ba:
            return self._invalidate(f"crossed book: bid {bb} >= ask {ba}")
        return None

    # -- public
    def start_sync(self):
        """Begin (re)synchronisation: events are buffered until a snapshot arrives."""
        self.bids, self.asks, self._buffer = {}, {}, []
        self.state, self.reason, self.last_update_id, self._first_applied = BookState.SYNCING, None, None, False

    resync = start_sync

    def on_delta(self, U: int, u: int, bids, asks, pu: Optional[int] = None) -> BookState:
        ev = {"U": U, "u": u, "pu": pu, "b": bids, "a": asks}
        if self.state == BookState.SYNCING and self.last_update_id is None:
            self._buffer.append(ev)
            return self.state
        if self.state != BookState.VALID and not (self.state == BookState.SYNCING):
            return self.state            # EMPTY/INVALID: ignore until start_sync()
        return self._apply(ev)

    def apply_snapshot(self, last_update_id: int, bids, asks) -> BookState:
        if self.state != BookState.SYNCING:
            raise EnvelopeError("apply_snapshot requires start_sync() first")
        try:
            self._apply_levels(self.bids, bids)
            self._apply_levels(self.asks, asks)
        except EnvelopeError as e:
            return self._invalidate(str(e))
        self.last_update_id = int(last_update_id)
        pending, self._buffer = self._buffer, []
        for ev in pending:
            if ev["u"] <= self.last_update_id:
                continue                 # already contained in the snapshot
            if self._apply(ev) == BookState.INVALID:
                return self.state
        if self.state == BookState.SYNCING and self._first_applied:
            self.state = BookState.VALID
        elif self.state == BookState.SYNCING:
            self.state = BookState.VALID  # snapshot alone is a valid (if possibly aging) book
        return self._check_cross() or self.state

    def _apply(self, ev) -> BookState:
        if self.state == BookState.INVALID:
            return self.state
        if not self._first_applied:
            if ev["u"] <= self.last_update_id:
                return self.state        # stale, ignore
            if not (ev["U"] <= self.last_update_id + 1 <= ev["u"]):
                return self._invalidate(f"gap at sync: U={ev['U']} u={ev['u']} snapshot={self.last_update_id}")
        else:
            ok = (ev["pu"] == self.last_update_id) if self.futures else (ev["U"] == self.last_update_id + 1)
            if ev["u"] <= self.last_update_id:
                return self.state        # duplicate / out-of-date event: ignored, not applied twice
            if not ok:
                return self._invalidate(f"sequence gap: last={self.last_update_id} got U={ev['U']} pu={ev['pu']}")
        try:
            self._apply_levels(self.bids, ev["b"])
            self._apply_levels(self.asks, ev["a"])
        except EnvelopeError as e:
            return self._invalidate(str(e))
        self.last_update_id, self._first_applied = ev["u"], True
        return self._check_cross() or self.state

    def top(self, depth: int = 5):
        bids = sorted(self.bids.items(), reverse=True)[:depth]
        asks = sorted(self.asks.items())[:depth]
        return [list(x) for x in bids], [list(x) for x in asks]
