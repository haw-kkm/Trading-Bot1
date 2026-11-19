"""P0-2: RiskEngine uses real (adapter-reported) equity, a persisted high-water-mark drawdown,
fails closed for ENTRY on account-data problems, and never blocks risk reduction."""
import pytest

from src.core.account_equity import AccountDataUnavailable, EquityTracker, PgEquityStore
from src.core.execution_engine import ExecutionEngine, UnauthorizedExecutionError
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from tests.support.paper_exchange import PaperExchange


def _hyp(side="BUY", qty=0.01, price=50000.0):
    return TradeHypothesis(symbol="BTCUSDT", direction=side, quantity=qty, leverage=5.0,
                           price=price, stop_loss=49000.0, take_profit=52000.0)


def _engine(pg_dsn, ex):
    return RiskEngine(equity_tracker=EquityTracker(ex.get_account_equity, PgEquityStore(pg_dsn)))


@pytest.fixture
def ex(store):          # `store` truncates account_equity => each test starts with no baseline
    return PaperExchange()


def test_loss_limit_exceeded_blocks_new_entry(pg_dsn, ex):
    re = _engine(pg_dsn, ex)
    assert re.validate_trade(_hyp())["status"] in ("APPROVED", "CLAMPED")   # peak = 100000
    ex.equity = 80000.0                                                     # -20% > 15% limit
    r = re.validate_trade(_hyp())
    assert r["status"] == "REJECTED" and "drawdown circuit breaker" in r["reason"]
    assert "validated_order" not in r


def test_loss_within_limit_still_allows_entry(pg_dsn, ex):
    re = _engine(pg_dsn, ex)
    re.validate_trade(_hyp())
    ex.equity = 90000.0                                                     # -10% < 15%
    assert re.validate_trade(_hyp())["status"] in ("APPROVED", "CLAMPED")


def test_baseline_is_high_water_mark_and_survives_restart(pg_dsn, ex):
    _engine(pg_dsn, ex).validate_trade(_hyp())            # peak 100000 persisted
    ex.equity = 120000.0
    _engine(pg_dsn, ex).validate_trade(_hyp())            # peak 120000 (a "new process")
    ex.equity = 100000.0                                  # -16.7% from the PERSISTED peak
    r = _engine(pg_dsn, ex).validate_trade(_hyp())        # brand-new engine/tracker objects
    assert r["status"] == "REJECTED" and "drawdown" in r["reason"]


def test_peak_never_decreases_on_lower_observation(pg_dsn, ex):
    st = PgEquityStore(pg_dsn)
    assert st.observe(100000.0, 1.0) == 100000.0
    assert st.observe(90000.0, 2.0) == 100000.0


def test_equity_scales_the_position_limit(pg_dsn, ex):
    ex.equity = 1000.0                                    # max position = 10% = $100
    r = _engine(pg_dsn, ex).validate_trade(_hyp(qty=0.01, price=50000.0))   # $500 notional
    assert r["status"] == "CLAMPED"
    assert r["validated_order"].quantity == pytest.approx(100.0 / 50000.0)


@pytest.mark.parametrize("mode", ["raise", "stale", "malformed", "zero"])
def test_account_data_failure_blocks_entry_fail_closed(pg_dsn, ex, mode):
    ex.equity_mode = mode
    r = _engine(pg_dsn, ex).validate_trade(_hyp())
    assert r["status"] == "REJECTED" and "ACCOUNT_DATA_UNAVAILABLE" in r["reason"]


def test_no_account_source_configured_blocks_entry(store):
    saved, RiskEngine.default_equity_tracker = RiskEngine.default_equity_tracker, None
    try:
        r = RiskEngine().validate_trade(_hyp())
        assert r["status"] == "REJECTED" and "ACCOUNT_DATA_UNAVAILABLE" in r["reason"]
    finally:
        RiskEngine.default_equity_tracker = saved


def test_baseline_not_persistable_blocks_entry(ex):
    bad = PgEquityStore("postgresql://nobody@127.0.0.1:1/none?connect_timeout=1")
    re = RiskEngine(equity_tracker=EquityTracker(ex.get_account_equity, bad))
    assert re.validate_trade(_hyp())["status"] == "REJECTED"


@pytest.mark.parametrize("mode", ["raise", "stale", "malformed"])
def test_reduce_only_still_allowed_when_account_data_unavailable(pg_dsn, ex, mode):
    ex.equity_mode = mode
    r = _engine(pg_dsn, ex).validate_trade(_hyp(side="SELL"), reduce_only=True)
    assert r["status"] in ("APPROVED", "CLAMPED")
    assert r["validated_order"].reduce_only is True


def test_reduce_only_allowed_even_during_drawdown_breach(pg_dsn, ex):
    re = _engine(pg_dsn, ex)
    re.validate_trade(_hyp())
    ex.equity = 50000.0
    assert re.validate_trade(_hyp())["status"] == "REJECTED"
    assert re.validate_trade(_hyp(side="SELL"), reduce_only=True)["status"] in ("APPROVED", "CLAMPED")


def test_reduce_only_token_cannot_authorize_entry(store, pg_dsn, ex):
    ex.equity_mode = "raise"
    tok = _engine(pg_dsn, ex).validate_trade(_hyp(), reduce_only=True)["validated_order"]
    e = ExecutionEngine(order_store=store)
    e.toobit_client = PaperExchange()
    with pytest.raises(UnauthorizedExecutionError):
        e.execute_market_order("BTCUSDT", "BUY", tok.quantity, validated_order=tok)
    with pytest.raises(UnauthorizedExecutionError):
        e.execute_ioc_order("BTCUSDT", "BUY", tok.quantity, 50000.0, validated_order=tok)
    assert e.toobit_client.sent == []


def test_reduce_only_flag_is_signed_cannot_be_flipped(pg_dsn, ex):
    import dataclasses
    tok = _engine(pg_dsn, ex).validate_trade(_hyp(), reduce_only=True)["validated_order"]
    forged = dataclasses.replace(tok, reduce_only=False)
    assert RiskEngine.verify_token(tok) and not RiskEngine.verify_token(forged)


def test_exit_order_goes_through_when_account_data_is_down(store, pg_dsn, ex):
    ex.equity_mode = "raise"
    tok = _engine(pg_dsn, ex).validate_trade(_hyp(side="SELL"), reduce_only=True)["validated_order"]
    e = ExecutionEngine(order_store=store)
    paper = PaperExchange(position=0.01)
    e.toobit_client = paper
    out = e.execute_reduce_only_order("BTCUSDT", "SELL", tok.quantity, 50000.0, validated_order=tok)
    assert out["status"] == "SUCCESS" and len(paper.sent) == 1


def test_operator_rebase_resets_baseline(pg_dsn, ex):
    tr = EquityTracker(ex.get_account_equity, PgEquityStore(pg_dsn))
    tr.current()
    ex.equity = 50000.0
    assert tr.current().drawdown_pct == pytest.approx(50.0)
    tr.rebase()
    assert tr.current().drawdown_pct == 0.0


def test_unavailable_exception_type(ex):
    ex.equity_mode = "raise"
    with pytest.raises(AccountDataUnavailable):
        EquityTracker(ex.get_account_equity, PgEquityStore("x")).current()
