"""Toobit REST index price / open interest SEMANTICS (the two INCONCLUSIVE rows of the 2026-11-20 live run).

Evidence (official docs https://api-docs.toobit.com/api/usdt-m-market-data.html + live public GETs, 2026-11-20):
  * docs exchangeInfo example: contract "index": "BTCUSDT"; LIVE: BTC-SWAP-USDT has "index": "BTC" (bare coin),
    /quote/v1/index?symbol=BTCUSDT -> keyed "BTCUSDT", ?symbol=BTC and ?symbol=BTC-SWAP-USDT -> empty maps.
    => the contract -> index-name link is NOT proven: the row must stay INCONCLUSIVE, never PASS.
  * docs OI `size`: "Total open interest of the platform Specific coins, eg.: ETH in ETHUSDT" (no explicit unit,
    nothing about contractMultiplier); LIVE sizes are multiples of neither contractMultiplier nor LOT stepSize.
    => the unit stays UNVERIFIED (INCONCLUSIVE); value sanity is still enforced (FAIL on contradictions).
LIVE_* fixtures reproduce the observed shapes with values rounded from the live session (market data only)."""
from decimal import Decimal

import pytest

from src.stage0 import live
from src.stage0.adapters import toobit
from src.stage0.collector import HttpError
from src.stage0.envelope import EnvelopeError, Source
from tests.unit.test_stage0_hardening import GOOD_SPOT, GOOD_TB, check, make_rest, run
from tests.unit.test_stage0_live import APPROVED, MS, T0, TSYM

TB = Source.TOOBIT_FUTURES
IDX, OI, INFO = "/quote/v1/index", "/quote/v1/openInterest", "/api/v1/exchangeInfo"
FILTERS = [{"filterType": "PRICE_FILTER", "tickSize": "0.1"}, {"filterType": "LOT_SIZE", "stepSize": "0.0001"}]


def contract(symbol=TSYM, index="BTC", underlying="BTC", mult="0.001", **extra):
    c = {"symbol": symbol, "underlying": underlying, "contractMultiplier": mult, "inverse": False, "marginToken": "USDT",
         "status": "TRADING", "filters": FILTERS, **extra}
    if index is not None:
        c["index"] = index
    return c


def tb(index=None, oi=None, contracts=None, symbol=TSYM):
    """GOOD_TB with LIVE-observed index / OI / exchangeInfo shapes (contract index field = bare coin)."""
    r = dict(GOOD_TB)
    r[IDX] = index if index is not None else {"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}
    r[OI] = oi if oi is not None else {"openInterestList": [{"size": "39449.36077", "symbol": symbol}]}
    r[INFO] = {"contracts": contracts if contracts is not None else [contract(), contract("ETH-SWAP-USDT", "ETH", "ETH", "0.01")]}
    return r


def verdicts(rep):
    return {k: v["verdict"] for k, v in rep.items()}


# ============================================================ Index price
def test_live_observed_contract_index_field_differs_from_key_stays_inconclusive():
    rep = check(tb(), TB, TSYM)
    row = rep[IDX]
    assert row["verdict"] == "INCONCLUSIVE" and row["structure"] == "PASS"
    assert row["semantics"]["contract_index_field"] == "BTC" and row["semantics"]["queried_index_name"] == "BTCUSDT"
    assert row["semantics"]["response_index_keys"] == ["BTCUSDT"] and "not proven" in row["reason"]


def test_contract_index_equal_to_response_key_passes():
    rep = check(tb(contracts=[contract(index="BTCUSDT")]), TB, TSYM)          # documented relation, same-run metadata
    assert rep[IDX]["verdict"] == "PASS" and rep[IDX]["semantics"]["contract_index_field"] == "BTCUSDT"


def test_mapping_comes_from_same_run_metadata_not_a_hardcoded_name():
    sym = "ETH-SWAP-USDT"
    r = tb(index={"index": {"ETHUSDT": "1731.5"}, "edp": {"ETHUSDT": "1734.2"}}, symbol=sym,
           contracts=[contract(sym, "ETHUSDT", "ETH", "0.01"), contract(index="BTCUSDT")])
    r["/quote/v1/contract/ticker/price"] = [{"s": sym, "p": "1"}]
    r["/quote/v1/contract/ticker/bookTicker"] = [{"t": MS, "s": sym, "b": "1", "bq": "1", "a": "2", "aq": "1"}]
    r["/quote/v1/markPrice"] = {"exchangeId": 301, "symbolId": sym, "price": "1", "time": MS}
    r["/api/v1/futures/fundingRate"] = [{"symbol": sym, "rate": "0.0001"}]
    rep = check(r, TB, sym)
    assert rep[IDX]["verdict"] == "PASS" and rep[IDX]["semantics"]["queried_index_name"] == "ETHUSDT"
    # the BTC contract's (matching) index field must not vouch for ETH
    r[INFO] = {"contracts": [contract(sym, "ETH", "ETH", "0.01"), contract(index="BTCUSDT")]}
    assert check(r, TB, sym)[IDX]["verdict"] == "INCONCLUSIVE"


def test_contract_without_index_field_never_passes():
    rep = check(tb(contracts=[contract(index=None)]), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE" and "no 'index' field" in rep[IDX]["reason"]
    rep = check(tb(contracts=[contract(index="")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE"


def test_missing_contract_or_failed_exchange_info_never_passes():
    assert check(tb(contracts=[contract("ETH-SWAP-USDT", "ETHUSDT")]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"
    r = tb(contracts=[contract(index="BTCUSDT")])
    r[INFO] = HttpError(500)
    rep = check(r, TB, TSYM)
    assert rep[INFO]["verdict"] == "FAIL" and rep[IDX]["verdict"] == "INCONCLUSIVE"
    assert "unavailable" in rep[IDX]["reason"]


def test_duplicate_contract_metadata_is_fail():
    rep = check(tb(contracts=[contract(index="BTCUSDT"), contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL" and rep[OI]["verdict"] == "FAIL"


def test_empty_index_map_is_inconclusive():
    rep = check(tb(index={"index": {}, "edp": {}}, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE" and rep[IDX]["parsed_events"] == 0


@pytest.mark.parametrize("bad", ["0", "-1", "NaN", "Infinity", "-Infinity", "abc", "", None, True])
def test_invalid_index_price_is_fail(bad):
    rep = check(tb(index={"index": {"BTCUSDT": bad}, "edp": {"BTCUSDT": "1"}}, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"
    with pytest.raises(EnvelopeError):
        toobit.parse_index_price({"index": {"BTCUSDT": bad}}, T0)


def test_invalid_edp_is_fail():
    rep = check(tb(index={"index": {"BTCUSDT": "1"}, "edp": {"BTCUSDT": "NaN"}}, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"


def test_edp_missing_for_key_is_not_pass():
    rep = check(tb(index={"index": {"BTCUSDT": "1"}, "edp": {}}, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE" and "edp" in rep[IDX]["reason"]


def test_other_contracts_index_value_is_never_used():
    for resp in ({"index": {"ETHUSDT": "1731.5"}, "edp": {"ETHUSDT": "1734.2"}},                     # wrong instrument only
                 {"index": {"BTCUSDT": "61367.6", "ETHUSDT": "1731.5"}, "edp": {"BTCUSDT": "61491.6"}},   # extra instrument
                 {"index": {"BTCUSDT": "61367.6"}, "edp": {"BTCUSDT": "61491.6", "ETHUSDT": "1734.2"}}):  # foreign edp
        rep = check(tb(index=resp, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
        assert rep[IDX]["verdict"] == "FAIL", resp
        with pytest.raises(EnvelopeError):
            toobit.parse_index_price(resp, T0, expected_key="BTCUSDT")


def test_swap_symbol_and_index_name_are_not_confused():
    assert toobit.index_symbol(TSYM) == "BTCUSDT"
    with pytest.raises(EnvelopeError):
        toobit.index_symbol("BTCUSDT")                                     # an index name is not a contract symbol
    # response keyed by the contract symbol instead of the requested index name: contradicts the request
    rep = check(tb(index={"index": {TSYM: "1"}, "edp": {TSYM: "1"}}, contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"
    # contract claims its own symbol as index name: does not link to the BTCUSDT key
    assert check(tb(contracts=[contract(index=TSYM)]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"


def test_poll_reference_rejects_foreign_index_key():
    resp = {"/api/v1/futures/fundingRate": [{"symbol": TSYM, "rate": "0.0001"}],
            OI: {"openInterestList": [{"size": "1", "symbol": TSYM}]},
            "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS},
            IDX: {"index": {"ETHUSDT": "1731.5"}, "edp": {}}, INFO: {"contracts": [contract()]}}
    r, c, fr, clock = make_rest(resp, pol=APPROVED)
    run(r.poll_reference(TB, TSYM))
    assert r.rest_counts["INDEX_PRICE"] == 0
    assert any(i.kind == "REFERENCE_UNAVAILABLE" and "INDEX_PRICE" in i.detail for i in c.gate.issues)


# ============================================================ Open interest
def test_valid_open_interest_is_inconclusive_because_unit_unconfirmed():
    rep = check(tb(), TB, TSYM)
    row = rep[OI]
    assert row["verdict"] == "INCONCLUSIVE" and row["structure"] == "PASS" and row["parsed_events"] == 1
    sem = row["semantics"]
    assert sem["unit"] == "UNVERIFIED" and sem["conversion_applied"] is False and sem["response_symbols"] == [TSYM]
    assert sem["contract_underlying"] == "BTC" and sem["contract_multiplier"] == "0.001"
    assert "UNIT" in row["reason"] and "Specific coins" in sem["doc_quote"]


def test_open_interest_symbol_mismatch_is_fail():
    rep = check(tb(oi={"openInterestList": [{"size": "1", "symbol": "ETH-SWAP-USDT"}]}), TB, TSYM)
    assert rep[OI]["verdict"] == "FAIL"
    rep = check(tb(oi={"openInterestList": [{"size": "1", "symbol": "BTCUSDT"}]}), TB, TSYM)     # index-style name
    assert rep[OI]["verdict"] == "FAIL"
    with pytest.raises(EnvelopeError):
        toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": "ETH-SWAP-USDT"}]}, T0, expected_symbol=TSYM)


def test_duplicate_open_interest_items_are_fail():
    two = {"openInterestList": [{"size": "1", "symbol": TSYM}, {"size": "2", "symbol": TSYM}]}
    assert check(tb(oi=two), TB, TSYM)[OI]["verdict"] == "FAIL"


@pytest.mark.parametrize("bad", ["-1", "-0.0001", "NaN", "Infinity", "-Infinity", "sNaN", "abc", "", None, True])
def test_invalid_open_interest_size_is_rejected(bad):
    with pytest.raises(EnvelopeError):
        toobit.parse_open_interest({"openInterestList": [{"size": bad, "symbol": TSYM}]}, T0)
    assert check(tb(oi={"openInterestList": [{"size": bad, "symbol": TSYM}]}), TB, TSYM)[OI]["verdict"] == "FAIL"


def test_zero_open_interest_is_valid_value():
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "0", "symbol": TSYM}]}, T0)
    assert e.normalized_payload["open_interest"] == Decimal("0")


def test_empty_or_malformed_open_interest_list():
    assert check(tb(oi={"openInterestList": []}), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    assert check(tb(oi={"openInterestList": {"size": "1", "symbol": TSYM}}), TB, TSYM)[OI]["verdict"] == "FAIL"
    assert check(tb(oi={"openInterest": "1"}), TB, TSYM)[OI]["verdict"] == "FAIL"


@pytest.mark.parametrize("mult", ["0.0001", "0.001", "1", "10", "100"])
def test_size_is_never_rescaled_by_contract_multiplier(mult):
    raw = {"openInterestList": [{"size": "4204694", "symbol": TSYM}]}
    [e] = toobit.parse_open_interest(raw, T0, expected_symbol=TSYM)
    assert e.normalized_payload["open_interest"] == Decimal("4204694") and e.raw_payload["size"] == "4204694"
    assert e.normalized_payload["unit"] == "UNVERIFIED" and "UNVERIFIED_SCHEMA" in e.quality_flags
    rep = check(tb(oi=raw, contracts=[contract(mult=mult)]), TB, TSYM)
    assert rep[OI]["verdict"] == "INCONCLUSIVE" and rep[OI]["semantics"]["conversion_applied"] is False


def test_open_interest_contract_metadata_missing_never_passes(monkeypatch):
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)       # even if the unit were promoted
    assert check(tb(contracts=[contract(mult="0")]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    assert check(tb(contracts=[contract(underlying=None)]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    assert check(tb(contracts=[]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"


def test_unit_is_only_attached_after_explicit_promotion(monkeypatch):
    assert toobit.VERIFICATION["rest_open_interest"] == toobit.UNVERIFIED          # not auto-approved
    assert check(tb(), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)       # deliberate human edit
    rep = check(tb(), TB, TSYM)
    assert rep[OI]["verdict"] == "PASS" and rep[OI]["semantics"]["unit"] == "BASE_ASSET"
    # promotion never excuses a contradiction
    assert check(tb(oi={"openInterestList": [{"size": "-1", "symbol": TSYM}]}), TB, TSYM)[OI]["verdict"] == "FAIL"


# ============================================================ whole verifier
def _judge(rep):
    from tools.stage0_live_verify import judge_rest
    rows = list(rep.values())
    code = 1 if any(r["verdict"] == "FAIL" for r in rows) else 0
    return judge_rest({"exit_code": code, "timed_out": False}, rows, live.required_rest_endpoints(TB))


def test_live_observed_shapes_give_8_pass_2_inconclusive_and_overall_inconclusive():
    rep = check(tb(), TB, TSYM)
    v = verdicts(rep)
    assert v[IDX] == v[OI] == "INCONCLUSIVE"
    assert sum(x == "PASS" for x in v.values()) == 8 and len(v) == 10
    assert _judge(rep)[0] == "INCONCLUSIVE"


def test_only_proven_rows_make_rest_pass(monkeypatch):
    rep = check(tb(contracts=[contract(index="BTCUSDT")]), TB, TSYM)            # index proven, OI unit not
    assert verdicts(rep)[IDX] == "PASS" and _judge(rep)[0] == "INCONCLUSIVE"
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)
    rep = check(tb(contracts=[contract(index="BTCUSDT")]), TB, TSYM)
    assert all(x == "PASS" for x in verdicts(rep).values()) and _judge(rep)[0] == "PASS"


def test_semantic_contradiction_fails_rest_and_check_exit_code(monkeypatch, capsys):
    resp = tb(oi={"openInterestList": [{"size": "1", "symbol": "ETH-SWAP-USDT"}]})
    assert _judge(check(resp, TB, TSYM))[0] == "FAIL"
    from tests.unit.test_stage0_hardening import _resp
    from src.stage0.collector import PublicRestClient
    monkeypatch.setattr(live, "PublicRestClient", lambda pol: PublicRestClient(pol, transport=lambda u, p: _resp(resp, u)))
    rc = live.main(["--check", "--source", "toobit-futures", "--symbol", TSYM, "--allow-toobit-network",
                    "--i-approve-toobit-public-readonly"])
    assert rc == 1


def test_semantic_check_returning_unknown_verdict_fails_closed(monkeypatch):
    monkeypatch.setitem(live.SEMANTIC_CHECKS, "rest_index_price", lambda *a: {"verdict": "OK", "reason": "", "evidence": {}})
    assert check(tb(), TB, TSYM)[IDX]["verdict"] == "FAIL"


def test_semantic_rows_never_report_response_values():
    rep = check(tb(), TB, TSYM)
    text = repr(rep[IDX]) + repr(rep[OI])
    for value in ("61367.66", "61491.64", "39449.36077"):
        assert value not in text


def test_binance_check_unaffected():
    rep = check(GOOD_SPOT)
    assert all(v["verdict"] == "PASS" and "semantics" not in v for v in rep.values())
    assert not any(k in live.SEMANTIC_CHECKS for k in ("binance:price", "binance:oi"))
