"""Stage 1 data access layer: the ONLY way an agent receives market data.

Replay model (deterministic, leak-free): for a request at `now`, a FRESH Stage 0 DataQualityGate is fed every
envelope of that (source, symbol) with received_at <= now, in (received_at, event_time, digest) order. Events
the gate accepted as current are kept as history; the readiness of every requested event type is then asked
from the gate itself (`status(types=[t])`). Events of types that are not READY are withheld (quarantine).
Nothing observed after `now` can influence the snapshot.

This layer replays recorded/fixture envelopes. It makes NO network call and holds no credentials.
"""
import json
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Iterable, List, Optional

from ..stage0.envelope import EnvelopeError, EventType, MarketEnvelope, utc
from ..stage0.quality import DataQualityGate, Readiness
from ..stage0.kline import KlineStore, MAX_CANDLES_PER_KEY
from . import quality_policy as policy
from .models import _GATE_TOKEN, AgentInput, Stage0Context, Stage1Error


class GatedDataAccess:
    def __init__(self, envelopes: Iterable[MarketEnvelope], stage0: Optional[Stage0Context] = None,
                 gate_kwargs: Optional[dict] = None, load_rejections: Optional[List[str]] = None):
        self._events: List[MarketEnvelope] = []
        for e in envelopes:
            if not isinstance(e, MarketEnvelope):
                raise Stage1Error("only Stage 0 MarketEnvelope objects are accepted")
            self._events.append(e)
        self._events.sort(key=lambda e: (e.received_at, e.event_time or e.received_at, e.digest))
        self.stage0 = stage0 or Stage0Context()
        self._gate_kwargs = dict(gate_kwargs or {})
        self.load_rejections = list(load_rejections or [])

    # ---- loading --------------------------------------------------------------------------------
    @classmethod
    def from_jsonl(cls, path, stage0: Optional[Stage0Context] = None, **kw) -> "GatedDataAccess":
        """Envelope JSON lines (Stage 0 evidence format). Every line is re-verified (digest recomputed);
        tampered / malformed lines are rejected and listed in `load_rejections`, never used."""
        envs, rej = [], []
        with open(path, "r", encoding="utf-8") as f:
            for n, line in enumerate(f, 1):
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                try:
                    envs.append(MarketEnvelope.from_dict(json.loads(line)))
                except (EnvelopeError, json.JSONDecodeError, KeyError, TypeError) as e:
                    rej.append(f"line {n}: {e}")
        return cls(envs, stage0=stage0, load_rejections=rej, **kw)

    def latest_received_at(self, source: str, symbol: str) -> Optional[datetime]:
        xs = [e.received_at for e in self._events if e.source.value == source and e.symbol == symbol]
        return max(xs) if xs else None

    # ---- snapshot -------------------------------------------------------------------------------
    def snapshot(self, source, symbol, now: datetime, event_types: Iterable,
                 lookback: timedelta = timedelta(hours=1), connected: bool = True) -> AgentInput:
        src, why = policy.validate_request(source, symbol)
        if src is None:
            raise Stage1Error(f"invalid request: {why}")
        now = utc(now, "now")
        types = []
        for t in event_types:
            try:
                types.append(EventType(getattr(t, "value", t)).value)
            except ValueError:
                raise Stage1Error(f"unknown event type {t!r}")
        types = sorted(set(types))
        kwargs = dict(self._gate_kwargs)
        kwargs.setdefault("kline_lookback", lookback)
        cap = kwargs.get("max_candles_per_key", MAX_CANDLES_PER_KEY)
        kline_supported = KlineStore.supported_lookback(lookback, cap)
        if not kline_supported:
            kwargs["kline_lookback"] = timedelta(hours=1)
        gate = DataQualityGate(**kwargs)
        if connected:
            gate.on_connect(src)
        else:
            gate.on_disconnect(src)
        history: Dict[str, List[MarketEnvelope]] = defaultdict(list)
        quarantined: List[str] = []
        start = now - lookback
        for env in self._events:
            if env.received_at > now:
                break                                     # sorted: nothing later may be seen
            if env.source != src or env.symbol != symbol:
                continue
            problem = policy.event_problem(env, src, symbol)
            issues = gate.observe(env)                    # the gate sees everything, incl. flagged events
            if env.event_type == EventType.KLINE and env.source.value.startswith("binance"):
                for issue in issues:
                    if issue in {"KLINE_INVALID", "KLINE_NOT_SUPPORTED", "KLINE_PREMATURE_CLOSE",
                                 "KLINE_CONFLICT", "KLINE_SUSPECT", "KLINE_NONMONOTONE_COUNT",
                                 "KLINE_E_TIE_AMBIGUOUS"}:
                        if "KLINE" in types:
                            quarantined.append(f"KLINE {env.digest[:12]}: {issue}")
            if problem is not None:
                if env.event_type.value in types:
                    quarantined.append(f"{env.event_type.value} {env.digest[:12]}: {problem}")
                continue
            t = env.event_type.value
            if t not in types or not gate.is_current(env):
                continue
            if t == "KLINE" and env.source.value.startswith("binance"):
                continue      # build after replay from FINAL per-bar states, never from transient revisions
            if policy.event_instant(env) > now or policy.event_instant(env) < start:
                continue                                  # future (leak) or outside the analysis window
            history[t].append(env)
        if "KLINE" in types and src.value.startswith("binance") and kline_supported:
            history["KLINE"] = gate.approved_klines(src.value, symbol, now, lookback)
        readiness: Dict[str, str] = {}
        diagnosis: Dict[str, str] = {}
        events: Dict[str, tuple] = {}
        for t in types:
            st = gate.status(src, symbol, now, types=[t])
            readiness[t] = st.value
            diagnosis[t] = gate.explain(src, symbol, now, types=[t]).get(t, "")
            if t == "KLINE" and src.value.startswith("binance"):
                if not kline_supported:
                    readiness[t] = Readiness.INVALID.value
                    diagnosis[t] = "KLINE_LOOKBACK_NOT_SUPPORTED: exceeds bounded 256-candle budget"
                    continue
                if readiness[t] == Readiness.STALE.value and not gate.kline_ready_intervals(src.value, symbol, now):
                    diagnosis[t] = "no current CLOSED and fresh per-interval KLINE observation"
            if readiness[t] == Readiness.READY.value:
                events[t] = tuple(history.get(t, ()))
        # an event type whose events were all quarantined is not READY even if the gate saw clean ones
        for t in types:
            if readiness[t] == "READY" and not events.get(t):
                readiness[t] = Readiness.STALE.value
                diagnosis[t] = "no admissible event in the analysis window"
                events.pop(t, None)
        max_age = {t: gate.max_age_by_type.get(t, gate.max_age) for t in types}
        return AgentInput(source=src.value, symbol=symbol, now=now, lookback=lookback, events=events,
                          readiness=readiness, diagnosis=diagnosis, max_age=max_age,
                          quarantined=tuple(quarantined), stage0=self.stage0, _token=_GATE_TOKEN)
