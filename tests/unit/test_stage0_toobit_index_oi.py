"""Toobit REST index price / open interest SEMANTICS.

Evidence layers (kept apart on purpose):
  * OFFICIAL SUPPORT ANSWER (Toobit Support, ticket #20261120000097, reply 2026-11-20 15:56:04): the index of a contract
    is exchangeInfo `indexToken` (BTC -> BTCUSDT), used as the `symbol` of /quote/v1/index; openInterest `size` is a
    base-token quantity with the contract coefficient already applied. Support could NOT confirm the aggregation basis.
  * LIVE OBSERVATION (public GET, 2026-11-21): every exchangeInfo contract carries a string `contracts[].indexToken`
    (795/795); BTC-SWAP-USDT: index "BTC", indexToken "BTCUSDT"; 13 contracts differ from index+quote
    (e.g. 1000PEPE-SWAP-USDT -> "PEPEUSDT"); /quote/v1/index?symbol=BTCUSDT is keyed "BTCUSDT"; ?symbol=BTC is empty.
  * The official answer is the RULE; a row passes only if the same run's response matches it.
Fixtures reproduce the observed shapes with values rounded from the live session (market data only)."""
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
_UNSET = object()


def contract(symbol=TSYM, index="BTC", underlying="BTC", mult="0.001", token="BTCUSDT", **extra):
    """exchangeInfo contract as observed live: legacy `index` = bare coin, `indexToken` = the /quote/v1/index key."""
    c = {"symbol": symbol, "underlying": underlying, "contractMultiplier": mult, "inverse": False, "marginToken": "USDT",
         "status": "TRADING", "filters": FILTERS, **extra}
    if index is not None:
        c["index"] = index
    if token is not _UNSET and token is not None:
        c["indexToken"] = token
    return c


def tb(index=None, oi=None, contracts=None, symbol=TSYM):
    """GOOD_TB with LIVE-observed index / OI / exchangeInfo shapes (contract index = BTC, indexToken = BTCUSDT)."""
    r = dict(GOOD_TB)
    r[IDX] = index if index is not None else {"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}
    r[OI] = oi if oi is not None else {"openInterestList": [{"size": "39449.36077", "symbol": symbol}]}
    r[INFO] = {"contracts": contracts if contracts is not None else
               [contract(), contract("ETH-SWAP-USDT", "ETH", "ETH", "0.01", token="ETHUSDT")]}
    return r


def official(q, value):
    """A well-formed (synthetic, test-only) official-answer record for the semantic registry."""
    return {"value": value, "source": "test: Toobit API support", "date": "2026-11-20", "reference": "TEST-1",
            "quote": "synthetic answer used only by this test"}


def promote_oi(monkeypatch, unit="BASE_ASSET", aggregation="ONE_SIDE"):
    """Simulates the deliberate human edit after the OPEN answer (aggregation) arrives: VERIFICATION + its record.
    unit / coefficient are already on record in the shipped registry; they are re-set here to pin the values."""
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_size_unit", official("oi_size_unit", unit))
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_contract_multiplier", official("oi_contract_multiplier", "ALREADY_APPLIED"))
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_aggregation", official("oi_aggregation", aggregation))


def verdicts(rep):
    return {k: v["verdict"] for k, v in rep.items()}


# ============================================================ Index price
def test_official_index_rule_is_on_record_with_its_source():
    rec = toobit.SEMANTIC_EVIDENCE["index_contract_mapping"]
    assert toobit.semantic_answer("index_contract_mapping") == "INDEX_TOKEN_FIELD"
    assert "20261120000097" in rec["reference"] and rec["source"] == "Toobit Support"


def test_contract_index_token_equal_to_response_key_passes_and_legacy_index_is_ignored():
    rep = check(tb(), TB, TSYM)                 # LIVE shape: index "BTC", indexToken "BTCUSDT", response keyed "BTCUSDT"
    row = rep[IDX]
    assert row["verdict"] == "PASS" and row["structure"] == "PASS" and row["index_token"] == "BTCUSDT"
    ev = row["semantics"]
    assert ev["contract_index_token"] == "BTCUSDT" and ev["legacy_index_field"] == "BTC"
    assert ev["response_index_keys"] == ["BTCUSDT"] and ev["mapping_rule"] == "INDEX_TOKEN_FIELD"


def test_index_is_requested_with_the_contract_index_token_after_exchange_info():
    from tests.unit.test_stage0_live_findings import _toobit_check
    rep, seen = _toobit_check(tb())
    paths = [p for p, _ in seen]
    assert ("/quote/v1/index", {"symbol": "BTCUSDT"}) in seen and paths.index(INFO) < paths.index(IDX)
    assert rep[IDX]["verdict"] == "PASS"


@pytest.mark.parametrize("sym,token,index,underlying,mult", [("ETH-SWAP-USDT", "ETHUSDT", "ETH", "ETH", "0.01"),
                                                              ("1000PEPE-SWAP-USDT", "PEPEUSDT", "1000PEPE", "PEPE", "1")])
def test_mapping_comes_from_same_run_metadata_not_a_name_rule(sym, token, index, underlying, mult):
    from tests.unit.test_stage0_live_findings import _toobit_check
    r = tb(index={"index": {token: "1731.5"}, "edp": {token: "1734.2"}}, symbol=sym,
           contracts=[contract(sym, index, underlying, mult, token=token), contract()])
    r["/quote/v1/contract/ticker/price"] = [{"s": sym, "p": "1"}]
    r["/quote/v1/contract/ticker/bookTicker"] = [{"t": MS, "s": sym, "b": "1", "bq": "1", "a": "2", "aq": "1"}]
    r["/quote/v1/markPrice"] = {"exchangeId": 301, "symbolId": sym, "price": "1", "time": MS}
    r["/api/v1/futures/fundingRate"] = [{"symbol": sym, "rate": "0.0001"}]
    rep = check(r, TB, sym)
    assert rep[IDX]["verdict"] == "PASS" and rep[IDX]["semantics"]["queried_index_name"] == token
    # 1000PEPE: the legacy base+quote name (1000PEPEUSDT) is never requested
    from src.stage0.collector import NetworkPolicy, PublicRestClient
    seen = []

    def transport(url, params):
        seen.append(("/" + url.split("/", 3)[3], dict(params)))
        return r["/" + url.split("/", 3)[3]]
    run(live.check_rest(PublicRestClient(NetworkPolicy(allow_toobit=True, toobit_user_approved=True), transport=transport),
                        TB, sym, now=lambda: T0))
    assert (IDX, {"symbol": token}) in seen
    if index + "USDT" != token:
        assert not any(p.get("symbol") == index + "USDT" for _, p in seen if p)
    # the BTC contract's (matching) indexToken must not vouch for another contract
    r[INFO] = {"contracts": [contract(sym, index, underlying, mult, token=None), contract()]}
    row = check(r, TB, sym)[IDX]
    assert row["verdict"] == "INCONCLUSIVE" and row["request_ok"] is False


@pytest.mark.parametrize("tok", [None, "", " BTCUSDT", "BTC-SWAP-USDT", "BTC USDT", 123, ["BTCUSDT"]])
def test_missing_or_malformed_index_token_never_passes_and_is_not_requested(tok):
    c = contract(token=None)
    if tok is not None:
        c["indexToken"] = tok
    rep = check(tb(contracts=[c]), TB, TSYM)
    row = rep[IDX]
    assert row["verdict"] == "INCONCLUSIVE" and row["request_ok"] is False and row["http_status"] is None
    assert "indexToken" in row["reason"] and "not requested" in row["reason"]


def test_legacy_index_field_is_never_the_mapping():
    # `index` names the key but there is no indexToken: still not proven (and no guess is requested)
    assert check(tb(contracts=[contract(index="BTCUSDT", token=None)]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"
    # no `index` field at all: the indexToken alone decides
    assert check(tb(contracts=[contract(index=None)]), TB, TSYM)[IDX]["verdict"] == "PASS"
    # `index` + quote asset would be BTCUSDT, but the token says otherwise: the response for BTCUSDT is rejected
    rep = check(tb(contracts=[contract(index="BTC", token="ETHUSDT")]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"           # response keyed BTCUSDT contradicts the requested ETHUSDT


def test_response_key_mismatching_the_index_token_never_passes():
    for resp in ({"index": {"ETHUSDT": "1731.5"}, "edp": {"ETHUSDT": "1734.2"}},
                 {"index": {"BTC": "61367.6"}, "edp": {"BTC": "61491.6"}}):
        assert check(tb(index=resp), TB, TSYM)[IDX]["verdict"] == "FAIL", resp


def test_missing_contract_or_failed_exchange_info_never_passes():
    assert check(tb(contracts=[contract("ETH-SWAP-USDT", "ETH", token="ETHUSDT")]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"
    r = tb()
    r[INFO] = HttpError(500)
    rep = check(r, TB, TSYM)
    assert rep[INFO]["verdict"] == "FAIL" and rep[IDX]["verdict"] == "INCONCLUSIVE" and rep[IDX]["request_ok"] is False
    assert "unavailable" in rep[IDX]["reason"]
    r[INFO] = {"contracts": "not-a-list"}                       # response structure change: fail closed
    assert check(r, TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"


def test_duplicate_contract_metadata_is_fail():
    rep = check(tb(contracts=[contract(), contract()]), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL" and rep[OI]["verdict"] == "FAIL"
    # contradicting tokens for one symbol: same story
    assert check(tb(contracts=[contract(), contract(token="ETHUSDT")]), TB, TSYM)[IDX]["verdict"] == "FAIL"


def test_empty_index_map_is_inconclusive():
    rep = check(tb(index={"index": {}, "edp": {}}), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE" and rep[IDX]["parsed_events"] == 0


@pytest.mark.parametrize("bad", ["0", "-1", "NaN", "Infinity", "-Infinity", "abc", "", None, True])
def test_invalid_index_price_is_fail(bad):
    rep = check(tb(index={"index": {"BTCUSDT": bad}, "edp": {"BTCUSDT": "1"}}), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"
    with pytest.raises(EnvelopeError):
        toobit.parse_index_price({"index": {"BTCUSDT": bad}}, T0)


def test_invalid_edp_is_fail():
    rep = check(tb(index={"index": {"BTCUSDT": "1"}, "edp": {"BTCUSDT": "NaN"}}), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"


def test_edp_missing_for_key_is_not_pass():
    rep = check(tb(index={"index": {"BTCUSDT": "1"}, "edp": {}}), TB, TSYM)
    assert rep[IDX]["verdict"] == "INCONCLUSIVE" and "edp" in rep[IDX]["reason"]


@pytest.mark.parametrize("resp", [{"edp": {"BTCUSDT": "1"}}, {"index": ["BTCUSDT"], "edp": {}}, {"index": "61367"},
                                  [], "oops"])
def test_changed_index_response_structure_fails_closed(resp):
    row = check(tb(index=resp), TB, TSYM)[IDX]
    assert row["verdict"] in ("FAIL", "INCONCLUSIVE") and row["verdict"] != "PASS"


def test_other_contracts_index_value_is_never_used():
    for resp in ({"index": {"ETHUSDT": "1731.5"}, "edp": {"ETHUSDT": "1734.2"}},                     # wrong instrument only
                 {"index": {"BTCUSDT": "61367.6", "ETHUSDT": "1731.5"}, "edp": {"BTCUSDT": "61491.6"}},   # extra instrument
                 {"index": {"BTCUSDT": "61367.6"}, "edp": {"BTCUSDT": "61491.6", "ETHUSDT": "1734.2"}}):  # foreign edp
        rep = check(tb(index=resp), TB, TSYM)
        assert rep[IDX]["verdict"] == "FAIL", resp
        with pytest.raises(EnvelopeError):
            toobit.parse_index_price(resp, T0, expected_key="BTCUSDT")


def test_swap_symbol_and_index_name_are_not_confused():
    assert toobit.index_symbol(TSYM) == "BTCUSDT"                          # legacy guess helper, no longer used for requests
    with pytest.raises(EnvelopeError):
        toobit.index_symbol("BTCUSDT")                                     # an index name is not a contract symbol
    # response keyed by the contract symbol instead of the requested index name: contradicts the request
    rep = check(tb(index={"index": {TSYM: "1"}, "edp": {TSYM: "1"}}), TB, TSYM)
    assert rep[IDX]["verdict"] == "FAIL"
    # contract claims its own symbol as index token: not a plain index name -> not requested, not proven
    assert check(tb(contracts=[contract(token=TSYM)]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"


def test_rule_not_on_record_or_malformed_never_passes(monkeypatch):
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "index_contract_mapping", None)
    row = check(tb(), TB, TSYM)[IDX]
    assert row["verdict"] == "INCONCLUSIVE" and "not on record" in row["reason"]
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "index_contract_mapping", {"value": "INDEX_TOKEN_FIELD"})
    assert check(tb(), TB, TSYM)[IDX]["verdict"] == "FAIL"                 # malformed record: fail closed
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "index_contract_mapping", official("index_contract_mapping", "INDEX_FIELD"))
    assert check(tb(), TB, TSYM)[IDX]["verdict"] == "FAIL"                 # unsupported legacy rule value


def test_index_envelope_mapping_is_verified_only_with_a_matching_contract_token():
    resp = {"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}
    [ok] = toobit.parse_index_price(resp, T0, expected_key="BTCUSDT", index_token="BTCUSDT", contract_symbol=TSYM)
    assert ok.normalized_payload["contract_mapping"] == "VERIFIED" and ok.normalized_payload["contract_symbol"] == TSYM
    assert "UNVERIFIED_CONTRACT_MAPPING" not in ok.quality_flags and "UNVERIFIED_SCHEMA" not in ok.quality_flags
    for kw in ({}, {"index_token": "BTCUSDT"}, {"contract_symbol": TSYM},
               {"index_token": "ETHUSDT", "contract_symbol": TSYM}):      # no / partial / contradicting contract proof
        [e] = toobit.parse_index_price(resp, T0, **kw)
        assert e.normalized_payload["contract_mapping"] == "UNVERIFIED" and "UNVERIFIED_CONTRACT_MAPPING" in e.quality_flags, kw


def test_poll_reference_requests_exchange_info_first_and_uses_the_index_token():
    resp = {"/api/v1/futures/fundingRate": [{"symbol": TSYM, "rate": "0.0001"}],
            OI: {"openInterestList": [{"size": "1", "symbol": TSYM}]},
            "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS},
            IDX: {"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}, INFO: {"contracts": [contract()]}}
    r, c, fr, clock = make_rest(resp, pol=APPROVED)
    run(r.poll_reference(TB, TSYM))
    paths = [p for p, _ in fr.calls]
    assert (IDX, {"symbol": "BTCUSDT"}) in fr.calls and paths.index(INFO) < paths.index(IDX)
    assert r.rest_counts["INDEX_PRICE"] == 1 and r.index_token == "BTCUSDT"
    idx_env = [e for e in c.envelopes if e.event_type.value == "INDEX_PRICE"]
    assert idx_env and all(e.normalized_payload["contract_mapping"] == "VERIFIED" for e in idx_env)


def test_poll_reference_without_index_token_does_not_request_a_guessed_index():
    resp = {"/api/v1/futures/fundingRate": [{"symbol": TSYM, "rate": "0.0001"}],
            OI: {"openInterestList": [{"size": "1", "symbol": TSYM}]},
            "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS},
            IDX: {"index": {"BTCUSDT": "61367.66"}, "edp": {}}, INFO: {"contracts": [contract(token=None)]}}
    r, c, fr, clock = make_rest(resp, pol=APPROVED)
    run(r.poll_reference(TB, TSYM))
    assert not any(p == IDX for p, _ in fr.calls) and r.rest_counts["INDEX_PRICE"] == 0 and r.index_token is None
    assert any(i.kind == "REFERENCE_UNAVAILABLE" and "INDEX_PRICE" in i.detail for i in c.gate.issues)


def test_poll_reference_rejects_foreign_index_key():
    resp = {"/api/v1/futures/fundingRate": [{"symbol": TSYM, "rate": "0.0001"}],
            OI: {"openInterestList": [{"size": "1", "symbol": TSYM}]},
            "/quote/v1/markPrice": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS},
            IDX: {"index": {"ETHUSDT": "1731.5"}, "edp": {}}, INFO: {"contracts": [contract()]}}
    r, c, fr, clock = make_rest(resp, pol=APPROVED)
    run(r.poll_reference(TB, TSYM))
    assert r.rest_counts["INDEX_PRICE"] == 0
    assert any(i.kind == "REFERENCE_UNAVAILABLE" and "INDEX_PRICE" in i.detail for i in c.gate.issues)


def test_ws_index_uses_the_same_token_and_stays_isolated():
    assert toobit.ws_subscribe_message("index", "BTCUSDT")["symbol"] == "BTCUSDT"        # the REST key, as given
    with pytest.raises(EnvelopeError):
        toobit.ws_subscribe_message("index", TSYM)                          # a contract symbol is never turned into a name
    assert toobit.ws_subscribe_message("trade", TSYM)["symbol"] == TSYM
    assert toobit.semantic_answer("ws_index_symbol") is None                # the official answer covers REST only
    [e] = toobit.parse_ws({"topic": "index", "symbol": "BTCUSDT", "data": [{"symbol": "BTCUSDT", "index": "61367.66",
                                                                          "edp": "61491.64", "time": MS}]}, T0)
    assert e.normalized_payload["contract_mapping"] == "UNVERIFIED" and "UNVERIFIED_CONTRACT_MAPPING" in e.quality_flags
    assert toobit.level("ws_index") == toobit.UNVERIFIED


def test_extended_components_check_uses_only_a_resolved_index_token():
    from src.stage0.collector import NetworkPolicy, PublicRestClient
    seen = []
    rest = PublicRestClient(NetworkPolicy(allow_toobit=True, toobit_user_approved=True),
                            transport=lambda u, p: seen.append(dict(p)) or {})
    [row] = run(live.check_extended(rest, TB, TSYM, index_token=None))
    assert row["verdict"] == "INCONCLUSIVE" and seen == []                 # nothing requested for a guessed name
    run(live.check_extended(rest, TB, TSYM, index_token="BTCUSDT"))
    assert seen == [{"symbol": "BTCUSDT"}]


# ============================================================ Open interest
def test_official_oi_unit_and_coefficient_are_on_record_but_aggregation_is_not():
    st = toobit.semantic_status()
    assert st["oi_size_unit"]["status"] == "VERIFIED" and st["oi_size_unit"]["value"] == "BASE_ASSET"
    assert st["oi_contract_multiplier"]["status"] == "VERIFIED" and st["oi_contract_multiplier"]["value"] == "ALREADY_APPLIED"
    assert st["oi_aggregation"]["status"] == "UNVERIFIED" and st["oi_aggregation"]["value"] is None
    assert "20261120000097" in st["oi_size_unit"]["reference"]
    assert toobit.semantics_verified("rest_open_interest") is False       # unit != aggregation: never one combined verdict


def test_valid_open_interest_is_inconclusive_because_aggregation_unconfirmed():
    rep = check(tb(), TB, TSYM)
    row = rep[OI]
    assert row["verdict"] == "INCONCLUSIVE" and row["structure"] == "PASS" and row["parsed_events"] == 1
    sem = row["semantics"]
    assert sem["unit"] == "BASE_ASSET" and sem["contract_coefficient_already_applied"] == "VERIFIED"
    assert sem["aggregation"] == "UNVERIFIED" and sem["conversion_applied"] is False and sem["response_symbols"] == [TSYM]
    assert sem["open_questions"] == ["oi_aggregation"] and "oi_aggregation" in row["reason"]
    assert sem["contract_underlying"] == "BTC" and sem["contract_multiplier"] == "0.001" and "Specific coins" in sem["doc_quote"]


def test_oi_envelope_records_each_meaning_separately_and_stays_isolated():
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "39449.36077", "symbol": TSYM}]}, T0, expected_symbol=TSYM)
    n = e.normalized_payload
    assert n["unit"] == "BASE_ASSET" and n["contract_coefficient_already_applied"] == "VERIFIED"
    assert n["aggregation"] == "UNVERIFIED" and n["contract_multiplier_applied"] is False
    assert n["comparable_across_exchanges"] is False
    assert "UNVERIFIED_AGGREGATION" in e.quality_flags and "UNVERIFIED_UNIT" not in e.quality_flags
    assert "UNVERIFIED_SCHEMA" in e.quality_flags                         # static level stays UNVERIFIED


def test_gate_never_ready_for_oi_while_aggregation_is_open():
    from src.stage0.quality import DataQualityGate
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": TSYM}]}, T0, expected_symbol=TSYM)
    g = DataQualityGate()
    g.observe(e)
    assert (e.source.value, e.symbol, "OPEN_INTEREST") in g._unverified


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
    assert e.normalized_payload["open_interest"] == Decimal("4204694") and e.raw_payload["size"] == "4204694"   # as sent
    assert e.normalized_payload["unit"] == "BASE_ASSET" and e.normalized_payload["contract_multiplier_applied"] is False
    assert "UNVERIFIED_AGGREGATION" in e.quality_flags
    rep = check(tb(oi=raw, contracts=[contract(mult=mult)]), TB, TSYM)
    assert rep[OI]["verdict"] == "INCONCLUSIVE" and rep[OI]["semantics"]["conversion_applied"] is False


def test_size_string_and_decimal_precision_are_preserved():
    for size in ("39449.36077", "123456789.123456789012345678", "0.000000000000000001", "1E+3"):
        [e] = toobit.parse_open_interest({"openInterestList": [{"size": size, "symbol": TSYM}]}, T0, expected_symbol=TSYM)
        assert e.raw_payload["size"] == size                              # original string untouched
        assert e.normalized_payload["open_interest"] == Decimal(size)     # exact Decimal, no float, no rescale
        assert isinstance(e.normalized_payload["open_interest"], Decimal)


def test_open_interest_contract_metadata_missing_never_passes(monkeypatch):
    promote_oi(monkeypatch)                                                          # even if the unit were confirmed
    assert check(tb(contracts=[contract(mult="0")]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    assert check(tb(contracts=[contract(underlying=None)]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    assert check(tb(contracts=[]), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"


def test_confirmed_unit_never_promotes_an_unconfirmed_aggregation(monkeypatch):
    assert toobit.VERIFICATION["rest_open_interest"] == toobit.UNVERIFIED          # not auto-approved
    assert check(tb(), TB, TSYM)[OI]["verdict"] == "INCONCLUSIVE"
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)       # VERIFICATION edit ALONE...
    rep = check(tb(), TB, TSYM)
    assert rep[OI]["verdict"] == "INCONCLUSIVE" and rep[OI]["semantics"]["aggregation"] == "UNVERIFIED"   # ...never promotes
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.UNVERIFIED)
    promote_oi(monkeypatch, unit="CONTRACTS", aggregation="LONG_PLUS_SHORT")         # every answer on record
    rep = check(tb(), TB, TSYM)
    assert rep[OI]["verdict"] == "PASS" and rep[OI]["semantics"]["unit"] == "CONTRACTS"
    assert rep[OI]["semantics"]["aggregation"] == "LONG_PLUS_SHORT"
    # promotion never excuses a contradiction
    assert check(tb(oi={"openInterestList": [{"size": "-1", "symbol": TSYM}]}), TB, TSYM)[OI]["verdict"] == "FAIL"


def test_unit_unanswered_or_malformed_is_isolated_separately_from_aggregation(monkeypatch):
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_size_unit", None)
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": TSYM}]}, T0)
    assert e.normalized_payload["unit"] == "UNVERIFIED" and "UNVERIFIED_UNIT" in e.quality_flags
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_size_unit", {"value": "BASE_ASSET"})   # malformed record
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": "1", "symbol": TSYM}]}, T0)
    assert e.normalized_payload["unit"] == "UNVERIFIED" and "UNVERIFIED_UNIT" in e.quality_flags
    assert check(tb(), TB, TSYM)[OI]["verdict"] == "FAIL"                  # malformed evidence: fail closed


def test_aggregation_only_answer_without_unit_still_not_pass(monkeypatch):
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_aggregation", official("oi_aggregation", "ONE_SIDE"))
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_contract_multiplier", None)
    rep = check(tb(), TB, TSYM)
    assert rep[OI]["verdict"] == "INCONCLUSIVE" and rep[OI]["semantics"]["open_questions"] == ["oi_contract_multiplier"]


# ============================================================ whole verifier
def _judge(rep):
    from tools.stage0_live_verify import judge_rest
    rows = list(rep.values())
    code = 1 if any(r["verdict"] == "FAIL" for r in rows) else 0
    return judge_rest({"exit_code": code, "timed_out": False}, rows, live.required_rest_endpoints(TB))


def test_live_observed_shapes_give_9_pass_1_inconclusive_and_overall_inconclusive():
    rep = check(tb(), TB, TSYM)
    v = verdicts(rep)
    assert v[IDX] == "PASS" and v[OI] == "INCONCLUSIVE"                    # index proven by indexToken, OI aggregation open
    assert sum(x == "PASS" for x in v.values()) == 9 and len(v) == 10
    assert _judge(rep)[0] == "INCONCLUSIVE"


def test_only_proven_rows_make_rest_pass(monkeypatch):
    rep = check(tb(contracts=[contract(token=None)]), TB, TSYM)                  # index NOT proven (no indexToken)
    assert verdicts(rep)[IDX] == "INCONCLUSIVE" and _judge(rep)[0] == "INCONCLUSIVE"
    rep = check(tb(), TB, TSYM)                                                  # index proven, OI aggregation not
    assert verdicts(rep)[IDX] == "PASS" and _judge(rep)[0] == "INCONCLUSIVE"
    promote_oi(monkeypatch)
    rep = check(tb(), TB, TSYM)
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
