"""BaseAgent: the single template every Stage 1 agent follows, so all outputs share one schema.

Flow: validate request -> gate-filtered snapshot -> agent.analyze(inp) -> AgentResult.
Agents NEVER touch raw feeds, files, the network, accounts or the order engine.
"""
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ...stage0.envelope import EnvelopeError, utc
from ...stage0.quality import Readiness
from .. import quality_policy as policy
from ..models import (AgentInput, AgentResult, AgentStatus, Stage0Context, Stage1Error, evidence_ref,
                      make_confidence, provenance_summary)


class Analysis:
    """What analyze() returns; BaseAgent turns it into the common AgentResult."""

    def __init__(self, status: AgentStatus, metrics=None, missing=None, warnings=None, completeness=0.0,
                 basis="", reliability_floor: Optional[float] = None, used_types=None):
        self.status, self.metrics = status, metrics or {}
        self.missing, self.warnings = list(missing or []), list(warnings or [])
        self.completeness, self.basis = completeness, basis
        self.reliability_floor = reliability_floor
        self.used_types = used_types                       # event types actually used (None = all READY ones)


class BaseAgent:
    agent_id = "base"
    request_types: Tuple[str, ...] = ()                    # event types asked from the gate
    default_lookback = timedelta(hours=1)

    # ---- to implement ---------------------------------------------------------------------------
    def analyze(self, inp: AgentInput) -> Analysis:
        raise NotImplementedError

    # ---- helpers for subclasses -----------------------------------------------------------------
    @staticmethod
    def blocked_status(inp: AgentInput, types) -> Optional[AgentStatus]:
        """Worst blocked status among `types` (UNVERIFIED > INVALID > DATA_UNAVAILABLE); None if all READY."""
        order = [AgentStatus.UNVERIFIED, AgentStatus.INVALID, AgentStatus.DATA_UNAVAILABLE]
        found = {policy.readiness_to_status(Readiness(inp.readiness[t])) for t in types if t in inp.readiness
                 and inp.readiness[t] != "READY"}
        found |= {AgentStatus.DATA_UNAVAILABLE for t in types if t not in inp.readiness}
        return next((s for s in order if s in found), None)

    def unavailable(self, inp: AgentInput, types, reason: str, missing=None) -> Analysis:
        st = self.blocked_status(inp, types) or AgentStatus.DATA_UNAVAILABLE
        blocked = [f"{t}={inp.readiness.get(t, 'NOT_REQUESTED')}" for t in types if not inp.ready(t)]
        return Analysis(st, missing=(missing or []) + blocked, warnings=[reason])

    # ---- template -------------------------------------------------------------------------------
    def run(self, access, source, symbol, now: datetime, lookback: Optional[timedelta] = None,
            connected: bool = True) -> AgentResult:
        lookback = lookback or self.default_lookback
        src, why = policy.validate_request(source, symbol)
        if src is None:
            return self._invalid(source, symbol, now, lookback, why, getattr(access, "stage0", Stage0Context()))
        try:
            inp = access.snapshot(src, symbol, now, self.request_types, lookback, connected)
        except Stage1Error as e:
            return self._invalid(source, symbol, now, lookback, str(e), getattr(access, "stage0", Stage0Context()))
        return self.build_result(inp, self.analyze(inp))

    def build_result(self, inp: AgentInput, a: Analysis) -> AgentResult:
        if a.status in (AgentStatus.DATA_UNAVAILABLE, AgentStatus.UNVERIFIED, AgentStatus.INVALID):
            a.metrics = {}                                  # never leave numbers on an unusable result
        used = {t: inp.events[t] for t in (a.used_types if a.used_types is not None else inp.events)
                if t in inp.events} if a.status in (AgentStatus.READY, AgentStatus.DEGRADED) else {}
        prov = provenance_summary(used)
        warnings = list(a.warnings)
        if inp.quarantined:
            warnings.append(f"{len(inp.quarantined)} event(s) quarantined before analysis: {list(inp.quarantined[:3])}")
        floor = a.reliability_floor
        rel = prov["min_reliability_score"]
        if floor is not None:
            rel = floor if rel is None else min(rel, floor)
        fresh = {}
        for t in sorted(inp.readiness):
            evs = inp.events.get(t, ())
            last = max((e.received_at for e in evs), default=None)
            fresh[t] = {"readiness": inp.readiness[t], "last_received_at": last.isoformat() if last else None,
                        "age_seconds": (inp.now - last).total_seconds() if last else None,
                        "max_age_seconds": inp.max_age[t].total_seconds(), "diagnosis": inp.diagnosis.get(t, "")}
        return AgentResult(
            agent_id=self.agent_id, source=inp.source, symbol=inp.symbol,
            analysis_window={"start": (inp.now - inp.lookback).isoformat(), "end": inp.now.isoformat(),
                             "lookback_seconds": inp.lookback.total_seconds()},
            generated_at=inp.now, input_event_types=sorted(used), input_evidence=evidence_ref(used),
            input_provenance=prov, data_freshness=fresh,
            data_quality={"gate_readiness": dict(inp.readiness), "quarantined_events": len(inp.quarantined),
                          "stage0_overall_does_not_promote_agents": True},
            status=a.status, metrics=a.metrics,
            confidence=make_confidence(a.status, a.completeness, rel, a.basis or "n/a"),
            missing_data=a.missing, warnings=warnings, stage0_context=inp.stage0.to_dict())

    def _invalid(self, source, symbol, now, lookback, why, stage0) -> AgentResult:
        try:
            t = utc(now, "now")
        except EnvelopeError:
            t = None
        src = getattr(source, "value", source)
        return AgentResult(
            agent_id=self.agent_id, source=str(src), symbol=str(symbol),
            analysis_window={"start": None, "end": t.isoformat() if t else None,
                             "lookback_seconds": lookback.total_seconds()},
            generated_at=t, input_event_types=[], input_evidence=[], input_provenance={},
            data_freshness={}, data_quality={"gate_readiness": {}, "request_rejected": why},
            status=AgentStatus.INVALID, metrics={},
            confidence=make_confidence(AgentStatus.INVALID, 0.0, None, "request rejected"),
            missing_data=["valid source/symbol/time"], warnings=[f"request rejected: {why}"],
            stage0_context=stage0.to_dict())
