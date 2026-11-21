"""Stage 1 agents: input safety, quarantine, schema uniformity, determinism, no-order guarantees (offline)."""
import ast
import json
import pathlib
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0.envelope import EventType, MarketEnvelope
from src.stage1.agents import AGENT_CLASSES, build_agents
from src.stage1.agents import indicators as ind
from src.stage1.agents.dynamic_simulation import DynamicSimulationAgent
from src.stage1.agents.funding_technical import FundingTechnicalAgent
from src.stage1.agents.liquidation_map import LiquidationMapAgent
from src.stage1.agents.open_interest import OpenInterestAgent
from src.stage1.agents.retail_position_change import RetailPositionChangeAgent
from src.stage1.agents.trend import TrendAgent
from src.stage1.agents.whale_position import WhalePositionAgent
from src.stage1.data_access import GatedDataAccess
from src.stage1.models import (RESULT_FIELDS, STAGE0_RECORDED, AgentInput, AgentStatus, Stage0Context,
                               Stage1Error)
from src.stage1.runner import run_all

T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)
FUT, SYM = "binance-futures", "BTC/USDT:USDT"
SPOT, SPOT_SYM = "binance-spot", "BTC/USDT"
TOOBIT = "toobit-futures"


def env(et, norm, event_time, rx, source=FUT, symbol=SYM, xsym="BTCUSDT", flags=(), seq=None):
    return MarketEnvelope(source=source, symbol=symbol, exchange_symbol=xsym, event_type=et, event_time=event_time,
                          received_at=rx, raw_payload={"x": 1}, normalized_payload=norm, quality_flags=flags,
                          sequence_id=seq)


def kline(i, close, source=FUT, symbol=SYM, xsym="BTCUSDT", high=None, low=None, vol="10", closed=True):
    open_t = T0 + timedelta(minutes=i)
    close_t = open_t + timedelta(seconds=59)
    c = Decimal(str(close))
    return env("KLINE", {"open": c, "high": high or c + 1, "low": low or c - 1, "close": c, "volume": vol,
                         "interval": "1m", "open_time_ms": int(open_t.timestamp() * 1000), "is_closed": closed},
               close_t, close_t + timedelta(milliseconds=100), source, symbol, xsym,
               seq=str(int(open_t.timestamp() * 1000)))


def series(n=60, start=100, step=1, **kw):
    return [kline(i, Decimal(start) + Decimal(step) * i, **kw) for i in range(n)]


def end_of(envs):
    return max(e.received_at for e in envs)


def now_after(envs):
    return end_of(envs) + timedelta(seconds=1)


def full_set(n=60):
    ks = series(n)
    now = end_of(ks) + timedelta(seconds=1)
    rx = now - timedelta(seconds=1)
    extra = [
        env("TRADE", {"price": "160.5", "quantity": "1000", "buyer_is_maker": False}, rx - timedelta(milliseconds=500),
            rx, seq="t1"),
        env("TRADE", {"price": "160.6", "quantity": "2", "buyer_is_maker": True}, rx, rx, seq="t2"),
    ]
    extra += [env("FUNDING_RATE", {"funding_rate": f"0.0000{i + 1}"}, T0 + timedelta(minutes=i), rx) for i in range(3)]
    extra += [env("OPEN_INTEREST", {"open_interest": v}, rx - timedelta(seconds=d), rx - timedelta(seconds=d))
              for v, d in (("1000.123456789", 60), ("1100.5", 0))]
    return ks + extra, now


def access_for(envs, **kw):
    return GatedDataAccess(envs, **kw)


# ---- indicator math ------------------------------------------------------------------------------
def test_indicator_known_values():
    up = [Decimal(i) for i in range(1, 40)]
    assert ind.sma(up[:5], 5) == Decimal(3)
    assert ind.rsi(up, 14) == Decimal(100)
    assert ind.rsi([Decimal(5)] * 20, 14) == Decimal(50)
    assert ind.macd([Decimal(7)] * 40) == {"macd": 0, "signal": 0, "histogram": 0}
    bars = [ind.Bar(T0, Decimal(10), Decimal(12), Decimal(9), Decimal(10), Decimal(1))] * 20
    assert ind.atr(bars, 14) == Decimal(3)
    assert ind.atr(bars[:14], 14) is None and ind.rsi(up[:14], 14) is None and ind.macd(up[:33]) is None
    assert ind.efficiency_ratio([Decimal(i) for i in range(12)], 10) == 1


# ---- correct input -> metrics --------------------------------------------------------------------
def test_trend_ready_with_exchange_klines_decimal_precision():
    envs, now = full_set()
    envs[10] = kline(10, "110.123456789012345")
    r = TrendAgent().run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.READY and r.metrics["direction"] == "UP"
    assert r.metrics["price_source"] == "KLINE"
    assert isinstance(r.metrics["last_close"], Decimal) and r.metrics["last_close"] == Decimal(159)
    assert r.metrics["price_change_pct"] > 0
    assert r.confidence["score"] > 0 and r.confidence["kind"] == "DATA_SUFFICIENCY"
    assert "NOT a prediction accuracy" in r.confidence["disclaimer"]
    # precision survives a metric computed from the 18-digit price and the JSON form
    pk = [kline(i, "100.123456789012345678") for i in range(25)]
    s = TrendAgent().run(access_for(pk), FUT, SYM, now_after(pk))
    assert s.metrics["sma_20"] == Decimal("100.123456789012345678")
    assert json.loads(json.dumps(s.to_dict()))["metrics"]["last_close"] == "100.123456789012345678"


def test_funding_technical_ready_and_min_lengths():
    envs, now = full_set()
    r = FundingTechnicalAgent().run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.READY, r.missing_data
    for k in ("rsi_14", "macd", "atr_14", "sma_20", "volatility_20", "funding_rate_latest", "funding_rate_mean"):
        assert k in r.metrics
    s20 = series(20)
    short = FundingTechnicalAgent().run(access_for(s20), FUT, SYM, now_after(s20))
    assert short.status == AgentStatus.DEGRADED
    assert "macd" not in short.metrics and any("macd" in m for m in short.missing_data)
    assert "sma_20" in short.metrics and "rsi_14" in short.metrics
    s3 = series(3)
    tiny = FundingTechnicalAgent().run(access_for(s3), FUT, SYM, now_after(s3))
    assert tiny.status == AgentStatus.DATA_UNAVAILABLE and tiny.metrics == {}


def test_open_interest_binance_level_and_change():
    envs, now = full_set()
    r = OpenInterestAgent().run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.READY
    assert r.metrics["latest_open_interest"] == Decimal("1100.5") and r.metrics["unit"] == "BASE_ASSET"
    assert r.metrics["change_abs"] == Decimal("1100.5") - Decimal("1000.123456789")
    assert r.metrics["cross_exchange_comparison"] == "NOT_PERFORMED"


# ---- request validation --------------------------------------------------------------------------
@pytest.mark.parametrize("source,symbol", [(FUT, "BTCUSDT"), (FUT, "BTC/USDT"), (SPOT, SYM), (FUT, "eth/usdt:usdt"),
                                           ("kraken", SYM), (None, SYM), (FUT, None), (FUT, "BTC/USDT:BTC")])
def test_bad_symbol_or_source_is_invalid(source, symbol):
    envs, now = full_set()
    for agent in build_agents():
        r = agent.run(access_for(envs), source, symbol, now)
        assert r.status == AgentStatus.INVALID and r.metrics == {} and r.confidence["score"] == 0.0


def test_wrong_exchange_symbol_event_is_quarantined():
    envs, now = full_set()
    ks = [kline(i, 100 + i, xsym="ETHUSDT") for i in range(60)]        # symbol says BTC, exchange says ETH
    r = TrendAgent().run(access_for(ks + envs[60:]), FUT, SYM, now)
    assert r.status in (AgentStatus.DATA_UNAVAILABLE, AgentStatus.DEGRADED) and "KLINE" not in r.input_event_types
    assert any("quarantined" in w for w in r.warnings)


def test_other_symbol_and_other_source_events_never_leak():
    envs, now = full_set()
    other = [kline(i, 9999, symbol="ETH/USDT:USDT", xsym="ETHUSDT") for i in range(60)]
    other += [kline(i, 7777, source=SPOT, symbol=SPOT_SYM) for i in range(60)]
    a = TrendAgent().run(access_for(envs + other), FUT, SYM, now).to_dict()
    b = TrendAgent().run(access_for(envs), FUT, SYM, now).to_dict()
    assert a == b


# ---- freshness / gate bypass ---------------------------------------------------------------------
def test_stale_data_blocked():
    envs, now = full_set()
    r = TrendAgent().run(access_for(envs), FUT, SYM, now + timedelta(hours=1))
    assert r.status == AgentStatus.DATA_UNAVAILABLE and r.metrics == {}
    assert r.data_freshness["KLINE"]["readiness"] == "STALE"


def test_disconnected_blocks_everything():
    envs, now = full_set()
    for agent in (TrendAgent(), FundingTechnicalAgent(), OpenInterestAgent(), DynamicSimulationAgent()):
        r = agent.run(access_for(envs), FUT, SYM, now, connected=False)
        assert r.status == AgentStatus.DATA_UNAVAILABLE and r.metrics == {}


def test_agent_input_cannot_be_forged_to_bypass_gate():
    with pytest.raises(Stage1Error):
        AgentInput(source=FUT, symbol=SYM, now=T0, lookback=timedelta(hours=1), events={}, readiness={},
                   diagnosis={}, max_age={})
    with pytest.raises(Stage1Error):
        AgentInput(source=FUT, symbol=SYM, now=T0, lookback=timedelta(hours=1), events={}, readiness={},
                   diagnosis={}, max_age={}, _token=object())
    with pytest.raises(Stage1Error):
        access_for([]).snapshot(FUT, SYM, T0, ["NOT_A_TYPE"])
    with pytest.raises(Stage1Error):
        GatedDataAccess([{"event_type": "TRADE"}])                      # raw dicts are not accepted


@pytest.mark.parametrize("flag", ["UNVERIFIED_SCHEMA", "UNVERIFIED_UNIT", "UNVERIFIED_AGGREGATION",
                                  "UNVERIFIED_CONTRACT_MAPPING"])
def test_unverified_flag_is_isolated(flag):
    envs, now = full_set()
    envs = [e for e in envs if e.event_type != EventType.OPEN_INTEREST]
    rx = now - timedelta(seconds=1)
    envs.append(env("OPEN_INTEREST", {"open_interest": "5"}, rx, rx, flags=(flag,)))
    r = OpenInterestAgent().run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.UNVERIFIED and r.metrics == {} and r.input_evidence == []
    assert r.confidence["score"] == 0.0


def test_unverified_type_only_blocks_agents_that_need_it():
    envs, now = full_set()
    rx = now - timedelta(seconds=1)
    envs.append(env("OPEN_INTEREST", {"open_interest": "5"}, rx, rx, flags=("UNVERIFIED_AGGREGATION",)))
    assert TrendAgent().run(access_for(envs), FUT, SYM, now).status == AgentStatus.READY


def test_core_pass_does_not_make_agent_ready():
    """Core Collection PASS recorded, but the agent's own event type (LONG_SHORT_RATIO / LIQUIDATION) is missing."""
    envs, now = full_set()
    acc = access_for(envs, stage0=STAGE0_RECORDED)
    rep = run_all(acc, FUT, SYM, now)
    assert rep["stage0"]["core_collection_status"] == "PASS" and rep["stage0"]["fully_verified"] is False
    by = {a["agent_id"]: a["status"] for a in rep["agents"]}
    assert by["liquidation_map"] == "DATA_UNAVAILABLE" and by["retail_position_change"] == "DATA_UNAVAILABLE"
    assert rep["stage1_ready"] is False and rep["live_trading"] == "NOT_APPROVED"


def test_backfill_trades_are_not_live_input():
    rx = T0 + timedelta(seconds=30)
    envs = [env("TRADE", {"price": "100", "quantity": "1"}, rx - timedelta(seconds=1), rx, flags=("BACKFILL",),
                seq=str(i)) for i in range(3)]
    r = WhalePositionAgent(Decimal(1)).run(access_for(envs), FUT, SYM, rx + timedelta(seconds=1))
    assert r.status == AgentStatus.DATA_UNAVAILABLE


# ---- Toobit / unverified semantics ---------------------------------------------------------------
def tb(et, norm, rx, event_time=None, flags=()):
    return env(et, norm, event_time, rx, TOOBIT, SYM, "BTC-SWAP-USDT", flags)


def test_toobit_oi_stays_unverified_and_never_compared():
    rx = T0 + timedelta(seconds=5)
    toobit = access_for([tb("OPEN_INTEREST", {"open_interest": "38995.689735"}, rx,
                            flags=("UNVERIFIED_UNIT", "UNVERIFIED_AGGREGATION"))])
    r = OpenInterestAgent().run(toobit, TOOBIT, SYM, rx + timedelta(seconds=1))
    assert r.status == AgentStatus.UNVERIFIED and r.metrics == {}
    assert any("aggregation basis UNVERIFIED" in w for w in r.warnings)
    # even a flag-free Toobit OI event is refused: aggregation is registered as UNVERIFIED in the policy
    clean = access_for([tb("OPEN_INTEREST", {"open_interest": "1"}, rx)])
    r2 = OpenInterestAgent().run(clean, TOOBIT, SYM, rx + timedelta(seconds=1))
    assert r2.status == AgentStatus.UNVERIFIED and r2.metrics == {}


def test_toobit_whale_notional_unverified():
    rx = T0 + timedelta(seconds=5)
    a = access_for([tb("TRADE", {"price": "100000", "quantity": "5"}, rx, rx)])
    assert WhalePositionAgent().run(a, TOOBIT, SYM, rx + timedelta(seconds=1)).status == AgentStatus.UNVERIFIED


def test_retail_ratio_without_population_is_unverified_with_metadata_is_usable():
    rx = T0 + timedelta(minutes=1)
    bare = {"ratio": "3.4823", "long_account": "0.7769", "short_account": "0.2231"}
    r = RetailPositionChangeAgent().run(access_for([env("LONG_SHORT_RATIO", bare, rx, rx)]), FUT, SYM,
                                        rx + timedelta(seconds=1))
    assert r.status == AgentStatus.UNVERIFIED and r.metrics == {}
    rich = dict(bare, population="accounts", period_seconds=300)
    evs = [env("LONG_SHORT_RATIO", dict(rich, ratio=x), rx - timedelta(seconds=s), rx - timedelta(seconds=s))
           for x, s in (("3.0", 30), ("3.5", 0))]
    ok = RetailPositionChangeAgent().run(access_for(evs), FUT, SYM, rx + timedelta(seconds=1))
    assert ok.status == AgentStatus.READY and ok.metrics["ratio_change"] == Decimal("0.5")
    assert "not total retail positioning" in ok.metrics["scope"]


# ---- whale / liquidation / fear-greed ------------------------------------------------------------
def test_whale_reports_only_observed_prints():
    envs, now = full_set()
    r = WhalePositionAgent(Decimal(100000)).run(access_for(envs), FUT, SYM, now)
    o = r.metrics["observed_large_trades"]
    assert r.status == AgentStatus.DEGRADED and o["count"] == 1
    assert o["aggressive_buy_notional"] == Decimal("160500.0") and o["aggressive_sell_notional"] == 0
    assert r.metrics["actual_account_positions"] == "UNAVAILABLE"
    flat = json.dumps(r.to_dict()["metrics"]).lower()          # no fabricated whale entry / leverage / size
    assert "entry" not in flat and "leverage" not in flat and "position_size" not in flat
    none = WhalePositionAgent().run(access_for([]), FUT, SYM, now)
    assert none.status == AgentStatus.DATA_UNAVAILABLE


def test_liquidation_map_separates_observed_from_modeled():
    rx = T0 + timedelta(seconds=10)
    evs = [env("LIQUIDATION", {"price": "100", "quantity": "2", "side": "SELL"}, rx, rx, seq="a"),
           env("LIQUIDATION", {"price": "101", "quantity": "1", "side": "BUY"}, rx, rx, seq="b")]
    r = LiquidationMapAgent().run(access_for(evs), FUT, SYM, rx + timedelta(seconds=1))
    assert r.status == AgentStatus.DEGRADED
    assert r.metrics["observed"]["long_liquidations"]["notional"] == 200
    assert r.metrics["heatmap"].startswith("WITHHELD") and r.metrics["modeled"].startswith("NONE")
    assert LiquidationMapAgent().run(access_for([]), FUT, SYM, rx).status == AgentStatus.DATA_UNAVAILABLE


def test_fear_greed_always_unavailable_without_network():
    envs, now = full_set()
    r = [a for a in build_agents() if a.agent_id == "fear_greed"][0].run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.DATA_UNAVAILABLE and r.metrics == {}


# ---- missing / insufficient data -----------------------------------------------------------------
def test_missing_funding_degrades_not_ready():
    envs, now = full_set()
    envs = [e for e in envs if e.event_type != EventType.FUNDING_RATE]
    r = FundingTechnicalAgent().run(access_for(envs), FUT, SYM, now)
    assert r.status == AgentStatus.DEGRADED and any("funding" in m for m in r.missing_data)
    assert "funding_rate_latest" not in r.metrics


def test_partial_forming_bar_not_used_and_unclosed_kline_excluded():
    ks = series(30)
    ks.append(kline(30, 999, closed=False))
    r = TrendAgent().run(access_for(ks), FUT, SYM, end_of(ks) + timedelta(seconds=1))
    assert r.metrics["last_close"] == Decimal(129)


def test_trade_derived_bars_are_degraded_and_labelled():
    base = T0 + timedelta(seconds=3)
    trades = [env("TRADE", {"price": str(100 + i), "quantity": "1", "buyer_is_maker": False},
                  base + timedelta(seconds=i), base + timedelta(seconds=i) + timedelta(milliseconds=50), seq=str(i))
              for i in range(60 * 30)]
    now = trades[-1].received_at + timedelta(seconds=1)
    r = TrendAgent().run(access_for(trades), FUT, SYM, now)
    assert r.status == AgentStatus.DEGRADED and r.metrics["price_source"] == "TRADE_DERIVED"
    assert r.confidence["min_input_reliability"] <= 0.8
    assert any("trade-derived" in w for w in r.warnings)


# ---- leak prevention / determinism ---------------------------------------------------------------
def test_future_data_does_not_leak_into_a_past_snapshot():
    envs, now = full_set(60)
    mid = T0 + timedelta(minutes=40, seconds=1)
    full = [a.run(access_for(envs), FUT, SYM, mid).to_dict() for a in build_agents()]
    past_only = [a.run(access_for([e for e in envs if e.received_at <= mid]), FUT, SYM, mid).to_dict()
                 for a in build_agents()]
    assert full == past_only
    assert TrendAgent().run(access_for(envs), FUT, SYM, mid).metrics["last_close"] == Decimal(139)


def test_future_event_time_is_not_used():
    envs, now = full_set()
    rx = now - timedelta(milliseconds=200)
    bad = env("OPEN_INTEREST", {"open_interest": "999999"}, now + timedelta(minutes=5), rx)
    r = OpenInterestAgent().run(access_for(envs + [bad]), FUT, SYM, now)
    # the gate marks the type INVALID (bad timestamp); the future value is never used
    assert r.status == AgentStatus.INVALID and r.metrics == {}
    assert "999999" not in json.dumps(r.to_dict())


def test_deterministic_and_order_independent():
    envs, now = full_set()
    a = run_all(access_for(envs), FUT, SYM, now)
    b = run_all(access_for(envs), FUT, SYM, now)
    shuffled = list(envs)
    random.Random(7).shuffle(shuffled)
    c = run_all(access_for(shuffled), FUT, SYM, now)
    assert a == b == c


# ---- schema uniformity ---------------------------------------------------------------------------
@pytest.mark.parametrize("source,symbol", [(FUT, SYM), (SPOT, SPOT_SYM), (TOOBIT, SYM), (FUT, "BAD")])
def test_all_agents_share_one_output_schema(source, symbol):
    envs, now = full_set()
    rep = run_all(access_for(envs), source, symbol, now)
    assert len(rep["agents"]) == 8
    assert [a["agent_id"] for a in rep["agents"]] == ["trend", "whale_position", "retail_position_change",
                                                      "liquidation_map", "open_interest", "funding_technical",
                                                      "fear_greed", "dynamic_simulation"]
    for a in rep["agents"]:
        assert tuple(a) == tuple(sorted(RESULT_FIELDS)) or set(a) == set(RESULT_FIELDS)
        assert a["status"] in {s.value for s in AgentStatus}
        assert a["confidence"]["kind"] == "DATA_SUFFICIENCY" and 0.0 <= a["confidence"]["score"] <= 1.0
        if a["status"] in ("DATA_UNAVAILABLE", "UNVERIFIED", "INVALID"):
            assert a["metrics"] == {} and a["confidence"]["score"] == 0.0
        json.dumps(a)                                           # fully serialisable (Decimal -> str)
    assert len(AGENT_CLASSES) == 8


def test_ready_requires_all_inputs_stage1_not_declared_ready_with_8_files():
    envs, now = full_set()
    assert run_all(access_for(envs), FUT, SYM, now)["stage1_ready"] is False


# ---- simulation safety ---------------------------------------------------------------------------
def test_simulation_is_labelled_and_makes_no_orders():
    envs, now = full_set()
    r = DynamicSimulationAgent(Decimal(1000), 10).run(access_for(envs), FUT, SYM, now)
    sc = r.metrics["scenarios"]
    assert r.status == AgentStatus.READY and sc["simulated"] is True and sc["is_real_fill_return"] is False
    assert sc["real_orders_created"] == 0
    long_up = [s for s in sc["price_shocks"] if s["shock_pct"] == 5][0]
    assert long_up["hypothetical_long_pnl_quote"] == Decimal("1000") * Decimal("0.05")
    assert long_up["hypothetical_short_pnl_quote"] == -Decimal("1000") * Decimal("0.05")
    assert sc["toy_liquidation_model"]["model"].startswith("ESTIMATED")
    assert {d["outcome"] for d in sc["data_delay"]} <= {"WOULD_BE_STALE", "STILL_FRESH"}
    assert any(m["lost_indicators"] for m in sc["data_missing"])
    assert any("not an executed fill" in w for w in r.warnings)


def test_stage1_never_imports_order_or_account_code():
    root = pathlib.Path(__file__).resolve().parents[2] / "src" / "stage1"
    banned = ("src.core", "src.orders", "execution_engine", "order_guardian", "risk_engine", "safe_submitter",
              "account_equity", "psycopg", "urllib", "requests", "websockets", "socket", "http")
    for f in root.rglob("*.py"):
        for node in ast.walk(ast.parse(f.read_text(encoding="utf-8"))):
            names = []
            if isinstance(node, ast.Import):
                names = [a.name for a in node.names]
            elif isinstance(node, ast.ImportFrom):
                names = [("." * node.level) + (node.module or "")] + [a.name for a in node.names]
            for n in names:
                assert not any(b in n for b in banned), f"{f.name} imports {n}"
        src = f.read_text(encoding="utf-8").lower()
        for word in ("api_key", "apikey", "secret", "place_order", "submit_order"):
            assert word not in src, f"{f.name} mentions {word}"


# ---- evidence loading ----------------------------------------------------------------------------
def test_tampered_evidence_line_is_rejected(tmp_path):
    envs, _ = full_set(25)
    good = [e.to_dict() for e in envs[:25]]
    bad = dict(good[0])
    bad["normalized_payload"] = dict(bad["normalized_payload"], close="1")
    p = tmp_path / "ev.jsonl"
    p.write_text("\n".join(json.dumps(d) for d in good[1:] + [bad]) + "\nnot json\n", encoding="utf-8")
    acc = GatedDataAccess.from_jsonl(p)
    assert len(acc.load_rejections) == 2 and any("digest" in r for r in acc.load_rejections)


def test_real_toobit_evidence_keeps_oi_unverified():
    p = pathlib.Path(__file__).resolve().parents[2] / "stage0-live-evidence" / "toobit_futures.events.jsonl"
    if not p.exists():
        pytest.skip("live evidence not present")
    acc = GatedDataAccess.from_jsonl(p, stage0=STAGE0_RECORDED)
    now = acc.latest_received_at(TOOBIT, SYM)
    rep = run_all(acc, TOOBIT, SYM, now)
    by = {a["agent_id"]: a for a in rep["agents"]}
    assert by["open_interest"]["status"] == "UNVERIFIED" and by["open_interest"]["metrics"] == {}
    assert rep["stage1_ready"] is False and rep["stage0"]["overall"] == "INCONCLUSIVE"
    assert Stage0Context().fully_verified is False
