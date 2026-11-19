"""Long-run memory bounds: many events must not grow the Collector/Gate without limit, and eviction must not
weaken duplicate detection inside the window or lose evidence (the JSONL sink receives every envelope)."""
import json
from datetime import timedelta

from src.stage0 import live
from src.stage0.adapters import toobit
from src.stage0.collector import Collector
from src.stage0.quality import DataQualityGate
from tests.unit.test_stage0_live import T0, TSYM
from tests.unit.test_stage0_live import no_real_network  # noqa: F401


def trade_env(i, rx):
    ms = int(rx.timestamp() * 1000)
    msg = {"symbol": TSYM, "topic": "trade", "params": {}, "f": False,
           "data": [{"v": str(i), "t": ms, "p": "1", "q": "1", "m": True}]}
    return toobit.parse_ws(msg, rx)[0]


def test_collector_memory_is_bounded_but_sink_and_counts_are_exact(tmp_path):
    sink = live.JsonlSink(tmp_path / "e.jsonl")
    c = Collector(max_envelopes=50, max_rejected=5, sink=sink)
    n = 5000
    for i in range(n):
        c._ingest(trade_env(i, T0 + timedelta(milliseconds=i)))
        c.reject({"error": f"e{i}"})
    att = sink.close()
    assert len(c.envelopes) == 50 and len(c.rejected) == 5                  # memory bounded
    assert c.total_envelopes == n and c.total_rejected == n                 # totals exact
    assert att["lines"] == n and len((tmp_path / "e.jsonl").read_text().splitlines()) == n   # nothing lost
    assert json.loads((tmp_path / "e.jsonl").read_text().splitlines()[0])["event_type"] == "TRADE"


def test_gate_sequence_window_and_issue_log_are_bounded_but_duplicates_inside_window_detected():
    g = DataQualityGate(max_seq_history=100, max_issues=20)
    g.on_connect(live.Source.TOOBIT_FUTURES)
    for i in range(1000):
        g.observe(trade_env(i, T0 + timedelta(milliseconds=i)))
    [(_, w)] = list(g._seen_seq.items())
    assert len(w) == 100
    last = trade_env(999, T0 + timedelta(milliseconds=999))
    assert g.observe(last) == ["DUPLICATE_EVENT"]                          # recent id: still caught
    for _ in range(100):
        g.observe(last)
    assert len(g.issues) == 20 and g.issue_counts["DUPLICATE_EVENT"] == 101  # log bounded, totals exact


def test_id_older_than_the_window_is_accepted_again_documented_tradeoff():
    g = DataQualityGate(max_seq_history=10)
    g.on_connect(live.Source.TOOBIT_FUTURES)
    for i in range(50):
        g.observe(trade_env(i, T0 + timedelta(milliseconds=i)))
    # id 0 was evicted: replaying it is no longer recognised as a duplicate (it is merely out of order here)
    assert "DUPLICATE_EVENT" not in g.observe(trade_env(0, T0 + timedelta(milliseconds=0)))


def test_health_uses_exact_totals_not_the_evicted_list():
    c = Collector(max_envelopes=1, max_rejected=1)
    for i in range(10):
        c._ingest(trade_env(i, T0 + timedelta(milliseconds=i)))
    for _ in range(100):
        c.reject({"error": "x"})
    r = live.LiveRunner(c, None, None, clock=lambda: T0 + timedelta(seconds=1))
    code, why = r.health()
    assert code == live.EXIT_UNHEALTHY and any("100 of 110" in w for w in why)
