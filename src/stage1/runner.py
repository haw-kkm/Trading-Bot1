"""Stage 1 fixture runner: runs all 8 agents on one (source, symbol, now) and states Stage 1 readiness.

Offline only. It never submits orders and never claims live trading is approved."""
import argparse
import json
import sys
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

from ..stage0.envelope import utc
from .agents import build_agents
from .data_access import GatedDataAccess
from .models import STAGE0_RECORDED, AgentStatus, Stage0Context


def run_all(access: GatedDataAccess, source, symbol, now: datetime, agents=None,
            lookback: Optional[timedelta] = None, connected: bool = True) -> Dict[str, Any]:
    agents = agents if agents is not None else build_agents()
    results = [a.run(access, source, symbol, now, lookback, connected) for a in agents]
    counts: Dict[str, int] = {}
    for r in results:
        counts[r.status.value] = counts.get(r.status.value, 0) + 1
    not_ready = {r.agent_id: r.status.value for r in results if r.status != AgentStatus.READY}
    return {
        "scope": "fixture_replay (offline); NOT a live verification",
        "stage0": {**access.stage0.to_dict(), "note": "recorded, never used to promote an agent"},
        "agents": [r.to_dict() for r in results],
        "status_counts": counts,
        "stage1_ready": not not_ready and len(results) == 8,
        "stage1_not_ready_agents": not_ready,
        "live_trading": "NOT_APPROVED",
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Stage 1 offline agent runner (fixture/evidence replay)")
    p.add_argument("events", help="Stage 0 envelope JSONL (e.g. stage0-live-evidence/binance_futures.events.jsonl)")
    p.add_argument("--source", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--now", help="ISO time with offset; default = latest received_at of that source/symbol")
    p.add_argument("--stage0-recorded", action="store_true",
                   help="record the known Stage 0 outcome (core PASS / extended INCONCLUSIVE)")
    a = p.parse_args(argv)
    access = GatedDataAccess.from_jsonl(a.events, stage0=STAGE0_RECORDED if a.stage0_recorded else Stage0Context())
    now = utc(a.now, "now") if a.now else access.latest_received_at(a.source, a.symbol)
    if now is None:
        print("no events for that source/symbol", file=sys.stderr)
        return 2
    rep = run_all(access, a.source, a.symbol, now)
    rep["load_rejections"] = access.load_rejections
    print(json.dumps(rep, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
