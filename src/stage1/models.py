"""Stage 1 common agent input / output models.

Every agent returns the SAME `AgentResult` shape. Status semantics:

  READY             every required input was READY per the Stage 0 gate and every metric was computed.
  DEGRADED          usable inputs exist but part of the analysis is missing / derived / partial (see missing_data).
  DATA_UNAVAILABLE  required inputs are absent, stale, disconnected, syncing or too short. No metrics.
  UNVERIFIED        the input exists but its semantics (unit / population / aggregation / mapping) are NOT
                    confirmed. The data is quarantined: no metric is computed from it.
  INVALID           the request or the input is malformed (bad symbol / source) or the gate says INVALID.

Only READY (and, with explicit caveats, DEGRADED) carry metrics. `confidence` is a DATA-SUFFICIENCY score,
never a forecast accuracy or a strategy return.
"""
import hashlib
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Mapping, Optional, Tuple

from ..stage0.envelope import MarketEnvelope


class AgentStatus(str, Enum):
    READY = "READY"
    DEGRADED = "DEGRADED"
    DATA_UNAVAILABLE = "DATA_UNAVAILABLE"
    UNVERIFIED = "UNVERIFIED"
    INVALID = "INVALID"


class Stage1Error(ValueError):
    pass


# Only data_access may mint an AgentInput: constructing one by hand (bypassing the quality gate) fails.
_GATE_TOKEN = object()

CONFIDENCE_KIND = "DATA_SUFFICIENCY"
CONFIDENCE_DISCLAIMER = ("Data-sufficiency score (completeness x input reliability). "
                         "NOT a prediction accuracy, NOT a strategy return.")


@dataclass(frozen=True)
class Stage0Context:
    """Stage 0 verification outcome the Stage 1 start is based on. It is recorded, never used to promote
    an agent: READY needs the agent's OWN event types to be READY, whatever the overall Stage 0 result."""
    core_collection_status: str = "UNKNOWN"
    extended_semantics_status: str = "UNKNOWN"
    overall: str = "UNKNOWN"
    exit_code: Optional[int] = None
    fully_verified: bool = False

    @classmethod
    def from_report(cls, d: Mapping[str, Any]) -> "Stage0Context":
        fv = d.get("fully_verified", False)
        return cls(str(d.get("core_collection_status", "UNKNOWN")),
                   str(d.get("extended_semantics_status", "UNKNOWN")),
                   str(d.get("overall", "UNKNOWN")), d.get("exit_code"), fv is True)

    def to_dict(self) -> Dict[str, Any]:
        return {"core_collection_status": self.core_collection_status,
                "extended_semantics_status": self.extended_semantics_status, "overall": self.overall,
                "exit_code": self.exit_code, "fully_verified": self.fully_verified}


# Recorded Stage 0 outcome that justified starting Stage 1 (user-run live verification, Windows PC).
STAGE0_RECORDED = Stage0Context("PASS", "INCONCLUSIVE", "INCONCLUSIVE", 2, False)


@dataclass(frozen=True)
class AgentInput:
    """Gate-filtered snapshot. `events` holds ONLY events of types the gate reported READY at `now`;
    events of blocked types are withheld entirely (quarantine)."""
    source: str
    symbol: str
    now: datetime
    lookback: timedelta
    events: Mapping[str, Tuple[MarketEnvelope, ...]]
    readiness: Mapping[str, str]            # event type -> Readiness value at `now`
    diagnosis: Mapping[str, str]            # event type -> gate.explain() text
    max_age: Mapping[str, timedelta]
    quarantined: Tuple[str, ...] = ()       # reasons events were dropped before reaching the agent
    stage0: Stage0Context = field(default_factory=Stage0Context)
    _token: Any = field(default=None, repr=False, compare=False)

    def __post_init__(self):
        if self._token is not _GATE_TOKEN:
            raise Stage1Error("AgentInput can only be created by GatedDataAccess (quality gate bypass blocked)")

    def ready(self, event_type: str) -> bool:
        return self.readiness.get(event_type) == "READY"

    def blocked(self) -> Dict[str, str]:
        return {t: r for t, r in self.readiness.items() if r != "READY"}


def _ts(v: Optional[datetime]) -> Optional[str]:
    return v.isoformat() if v else None


def _ser(v: Any) -> Any:
    if isinstance(v, Decimal):
        return format(v, "f")
    if isinstance(v, dict):
        return {k: _ser(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_ser(x) for x in v]
    if isinstance(v, Enum):
        return v.value
    return v


def evidence_ref(events: Mapping[str, Tuple[MarketEnvelope, ...]]) -> List[Dict[str, Any]]:
    """Reference to the exact envelopes an analysis used: count, first/last digest and a rollup digest."""
    out = []
    for t in sorted(events):
        evs = events[t]
        if not evs:
            continue
        roll = hashlib.sha256("".join(e.digest for e in evs).encode()).hexdigest()
        out.append({"event_type": t, "count": len(evs), "first_digest": evs[0].digest,
                    "last_digest": evs[-1].digest, "rollup_sha256": roll})
    return out


def provenance_summary(events: Mapping[str, Tuple[MarketEnvelope, ...]]) -> Dict[str, Any]:
    prov, flags, rel = set(), set(), 1.0
    n = 0
    for evs in events.values():
        for e in evs:
            n += 1
            prov.add(e.provenance.value)
            flags.update(e.quality_flags)
            rel = min(rel, e.reliability_score)
    return {"provenance": sorted(prov), "quality_flags": sorted(flags),
            "min_reliability_score": rel if n else None, "event_count": n}


@dataclass
class AgentResult:
    agent_id: str
    source: str
    symbol: str
    analysis_window: Dict[str, Any]
    generated_at: datetime
    input_event_types: List[str]
    input_evidence: List[Dict[str, Any]]
    input_provenance: Dict[str, Any]
    data_freshness: Dict[str, Any]
    data_quality: Dict[str, Any]
    status: AgentStatus
    metrics: Dict[str, Any]
    confidence: Dict[str, Any]
    missing_data: List[str]
    warnings: List[str]
    stage0_context: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return _ser({
            "agent_id": self.agent_id, "source": self.source, "symbol": self.symbol,
            "analysis_window": self.analysis_window, "generated_at": _ts(self.generated_at),
            "input_event_types": self.input_event_types, "input_evidence": self.input_evidence,
            "input_provenance": self.input_provenance, "data_freshness": self.data_freshness,
            "data_quality": self.data_quality, "status": self.status, "metrics": self.metrics,
            "confidence": self.confidence, "missing_data": self.missing_data, "warnings": self.warnings,
            "stage0_context": self.stage0_context,
        })


RESULT_FIELDS = ("agent_id", "source", "symbol", "analysis_window", "generated_at", "input_event_types",
                 "input_evidence", "input_provenance", "data_freshness", "data_quality", "status", "metrics",
                 "confidence", "missing_data", "warnings", "stage0_context")


def make_confidence(status: AgentStatus, completeness: float, min_reliability: Optional[float],
                    basis: str) -> Dict[str, Any]:
    """score = completeness x min input reliability, and 0.0 for anything that is not READY/DEGRADED."""
    if status not in (AgentStatus.READY, AgentStatus.DEGRADED) or min_reliability is None:
        score = 0.0
    else:
        score = max(0.0, min(1.0, completeness * min_reliability))
        if status == AgentStatus.DEGRADED:
            score = min(score, 0.7)
    return {"score": round(score, 4), "kind": CONFIDENCE_KIND, "basis": basis,
            "completeness": round(completeness, 4), "min_input_reliability": min_reliability,
            "disclaimer": CONFIDENCE_DISCLAIMER}
