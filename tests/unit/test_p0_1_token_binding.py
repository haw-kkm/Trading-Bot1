"""P0-1: tokens bind symbol/side/price(/SL/TP); protection qty = actual cumulative fill."""
import pytest

from src.core.execution_engine import ExecutionEngine, UnauthorizedExecutionError
from src.core.models import TradeHypothesis
from src.core.risk_engine import RiskEngine
from tests.support.paper_exchange import PaperExchange


def _token(side="BUY", qty=0.1, price=50000.0, sl=49000.0, tp=52000.0, symbol="BTCUSDT"):
    r = RiskEngine().validate_trade(TradeHypothesis(
        symbol=symbol, direction=side, quantity=qty, leverage=5.0, price=price,
        stop_loss=sl, take_profit=tp))
    return r["validated_order"]


class Recorder(PaperExchange):
    def __init__(self):
        super().__init__()
        self.sl_tp_calls = []

    def register_sl_tp(self, symbol, side, quantity, price, stop_loss, take_profit):
        self.sl_tp_calls.append((symbol, side, quantity, price, stop_loss, take_profit))
        return {"status": "SUCCESS"}


@pytest.fixture
def eng(store):
    ex = Recorder()
    e = ExecutionEngine(order_store=store)
    e.toobit_client = ex
    return e, ex


@pytest.mark.parametrize("kw", [
    dict(symbol="ETHUSDT", side="BUY", price=50000.0),
    dict(symbol="BTCUSDT", side="SELL", price=50000.0),
    dict(symbol="BTCUSDT", side="BUY", price=51000.0),
])
def test_ioc_and_reduce_only_reject_params_differing_from_token(eng, kw):
    e, ex = eng
    tok = _token()
    with pytest.raises(UnauthorizedExecutionError):
        e.execute_ioc_order(kw["symbol"], kw["side"], tok.quantity, kw["price"], validated_order=tok)
    with pytest.raises(UnauthorizedExecutionError):
        e.execute_reduce_only_order(kw["symbol"], kw["side"], tok.quantity, kw["price"], validated_order=tok)
    assert ex.sent == []


@pytest.mark.parametrize("symbol,side", [("ETHUSDT", "BUY"), ("BTCUSDT", "SELL")])
def test_market_rejects_symbol_or_side_mismatch(eng, symbol, side):
    e, ex = eng
    tok = _token()
    with pytest.raises(UnauthorizedExecutionError):
        e.execute_market_order(symbol, side, tok.quantity, validated_order=tok)
    assert ex.sent == []


def test_matching_order_is_accepted(eng):
    e, ex = eng
    tok = _token()
    assert e.execute_ioc_order("BTCUSDT", "BUY", tok.quantity, 50000.0, validated_order=tok)["status"] == "SUCCESS"


@pytest.mark.parametrize("kw", [dict(symbol="ETHUSDT"), dict(side="SELL"),
                                dict(stop_loss=1.0), dict(take_profit=1.0)])
def test_register_sl_tp_rejects_mismatch(eng, kw):
    e, ex = eng
    tok = _token()
    args = dict(symbol="BTCUSDT", side="BUY", stop_loss=49000.0, take_profit=52000.0)
    args.update(kw)
    with pytest.raises(UnauthorizedExecutionError):
        e.register_sl_tp(args["symbol"], args["side"], 0.03, 50010.0, args["stop_loss"],
                         args["take_profit"], validated_order=tok)
    assert ex.sl_tp_calls == []


def test_partial_fill_protection_uses_cumulative_fill_not_approved_qty(eng):
    e, ex = eng
    tok = _token(qty=0.1)
    assert tok.quantity == pytest.approx(0.1)
    r = e.register_sl_tp("BTCUSDT", "BUY", 0.03, 50123.0, 49000.0, 52000.0, validated_order=tok)
    assert r["status"] == "SUCCESS"
    assert ex.sl_tp_calls[0][2] == pytest.approx(0.03)         # NOT 0.1
    assert ex.sl_tp_calls[0][3] == 50123.0                      # differing fill price is normal


@pytest.mark.parametrize("q", [0, -1, 0.0])
def test_non_positive_protection_quantity_refused(eng, q):
    e, ex = eng
    with pytest.raises(UnauthorizedExecutionError):
        e.register_sl_tp("BTCUSDT", "BUY", q, 50000.0, 49000.0, 52000.0, validated_order=_token())
    assert ex.sl_tp_calls == []


def test_overfill_is_still_protected_with_actual_quantity(eng):
    e, ex = eng
    tok = _token(qty=0.1)
    e.register_sl_tp("BTCUSDT", "BUY", 0.15, 50000.0, 49000.0, 52000.0, validated_order=tok)
    assert ex.sl_tp_calls[0][2] == pytest.approx(0.15)
