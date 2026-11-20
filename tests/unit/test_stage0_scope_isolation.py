"""Stage 0 scope split (core_collection vs extended_semantics) and isolation of unverified Toobit data.

Facts these tests encode (official docs https://api-docs.toobit.com/api/usdt-m-market-data.html, live public GETs and
Toobit Support ticket #20261120000097, reply 2026-11-20 15:56:04): the contract->index rule is exchangeInfo indexToken,
the open-interest size is a BASE_ASSET quantity with the contract coefficient already applied; the open-interest
AGGREGATION basis (and the WebSocket index symbol) are NOT officially confirmed. So:
  * an INDEX_PRICE envelope is isolated unless the caller proves the contract's indexToken; OPEN_INTEREST envelopes
    stay isolated (never READY / never compared across exchanges) while the aggregation basis is open;
  * open interest is never rescaled by contractMultiplier and never comparable across exchanges;
  * only a well-formed OFFICIAL evidence record (plus the VERIFICATION edit) can lift that - never VERIFICATION alone;
  * the full `overall` / exit code are unchanged (Toobit stays INCONCLUSIVE, exit 2); core_collection may PASS only
    because core never uses those two types, and the exclusion is reported with its status and reason."""
import contextlib
import hashlib
import io
import json
import time
from decimal import Decimal

import pytest

from src.stage0 import live
from src.stage0.adapters import binance, toobit
from src.stage0.collector import HttpError, PublicRestClient
from src.stage0.envelope import UNVERIFIED_FLAGS, EventType, MarketEnvelope, Source
from src.stage0.quality import DataQualityGate, Readiness
from tests.unit.test_stage0_hardening import _resp, check
from tests.unit.test_stage0_live import MS, T0, TSYM
from tests.unit.test_stage0_live_findings import _tool
from tests.unit.test_stage0_toobit_index_oi import IDX, OI, contract, official, promote_oi, tb

TB = Source.TOOBIT_FUTURES
COMMON = "BTC/USDT:USDT"
COMP = "/quote/v1/indexPriceComponents"
LIVE_COMPONENTS = {"index": "61747.31", "edp": "61584.90", "time": MS,
                   "components": [{"exchange": x, "spotPair": "BTCUSDT", "weight": "1.0"}
                                  for x in ("OKEX", "BITGET", "KUCOIN", "BYBIT", "COINBASE", "BINANCE")]}


def oi_env(size="39449.36077"):
    [e] = toobit.parse_open_interest({"openInterestList": [{"size": size, "symbol": TSYM}]}, T0, expected_symbol=TSYM)
    return e


def idx_env():
    """Index envelope WITHOUT contract proof (no indexToken supplied): stays isolated."""
    [e] = toobit.parse_index_price({"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}, T0, expected_key="BTCUSDT")
    return e


def idx_env_proven():
    [e] = toobit.parse_index_price({"index": {"BTCUSDT": "61367.66"}, "edp": {"BTCUSDT": "61491.64"}}, T0,
                                   expected_key="BTCUSDT", index_token="BTCUSDT", contract_symbol=TSYM)
    return e


# ================================================================ isolation of unverified values
def test_index_envelope_is_not_a_verified_contract_index():
    e = idx_env()
    assert "UNVERIFIED_CONTRACT_MAPPING" in e.quality_flags
    assert e.normalized_payload["contract_mapping"] == "UNVERIFIED" and e.normalized_payload["index_name"] == "BTCUSDT"
    assert e.exchange_symbol == "BTCUSDT" and e.raw_payload["index"] == {"BTCUSDT": "61367.66"}    # source kept
    [w] = toobit.parse_ws({"topic": "index", "data": [{"symbol": "BTCUSDT", "index": "1", "time": MS}]}, T0)
    assert "UNVERIFIED_CONTRACT_MAPPING" in w.quality_flags and w.normalized_payload["contract_mapping"] == "UNVERIFIED"


def test_open_interest_unit_is_base_asset_but_aggregation_open_and_not_rescaled_or_comparable():
    e = oi_env()
    n = e.normalized_payload
    assert n["unit"] == "BASE_ASSET" and n["contract_coefficient_already_applied"] == "VERIFIED"   # support ticket
    assert n["aggregation"] == "UNVERIFIED"                                                      # support could not confirm
    assert n["contract_multiplier_applied"] is False and n["comparable_across_exchanges"] is False
    assert n["open_interest"] == Decimal("39449.36077") and e.raw_payload == {"size": "39449.36077", "symbol": TSYM}
    assert {"UNVERIFIED_SCHEMA", "UNVERIFIED_AGGREGATION"} <= set(e.quality_flags) and "UNVERIFIED_UNIT" not in e.quality_flags


def test_verification_edit_alone_never_lifts_isolation(monkeypatch):
    monkeypatch.setitem(toobit.VERIFICATION, "rest_open_interest", toobit.DOC)
    monkeypatch.setitem(toobit.VERIFICATION, "ws_index", toobit.DOC)
    assert toobit.level("rest_open_interest") == toobit.level("ws_index") == toobit.UNVERIFIED   # open answers remain
    assert "UNVERIFIED_SCHEMA" in oi_env().quality_flags and "UNVERIFIED_AGGREGATION" in oi_env().quality_flags
    assert oi_env().normalized_payload["aggregation"] == "UNVERIFIED"
    assert "UNVERIFIED_CONTRACT_MAPPING" in idx_env().quality_flags                  # no contract proof either
    [w] = toobit.parse_ws({"topic": "index", "data": [{"symbol": "BTCUSDT", "index": "1", "time": MS}]}, T0)
    assert "UNVERIFIED_CONTRACT_MAPPING" in w.quality_flags


def test_registry_holds_only_the_officially_answered_questions():
    assert set(toobit.SEMANTIC_QUESTIONS) == {"index_contract_mapping", "ws_index_symbol", "oi_size_unit",
                                              "oi_contract_multiplier", "oi_aggregation"}
    st = toobit.semantic_status()
    answered = {q for q, v in st.items() if v["status"] == "VERIFIED"}
    assert answered == {"index_contract_mapping", "oi_size_unit", "oi_contract_multiplier"}     # support ticket only
    assert all("20261120000097" in st[q]["reference"] for q in answered)
    assert {q for q, v in st.items() if v["status"] == "UNVERIFIED"} == {"ws_index_symbol", "oi_aggregation"}
    assert toobit.SEMANTIC_EVIDENCE["oi_aggregation"] is None and toobit.SEMANTIC_EVIDENCE["ws_index_symbol"] is None
    assert toobit.VERIFICATION["rest_open_interest"] == toobit.VERIFICATION["ws_index"] == toobit.UNVERIFIED
    assert toobit.VERIFICATION["rest_index_price"] == toobit.DOC


@pytest.mark.parametrize("bad", [
    {"value": "USD_NOTIONAL", "source": "s", "date": "d", "reference": "r", "quote": "q"},      # not an allowed unit
    {"value": "BASE_ASSET", "source": "s", "date": "d", "reference": "r"},                      # no verbatim quote
    {"value": "BASE_ASSET", "source": " ", "date": "d", "reference": "r", "quote": "q"},       # blank source
    "BASE_ASSET",                                                                               # not a record
])
def test_malformed_evidence_fails_closed(monkeypatch, bad):
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "oi_size_unit", bad)
    with pytest.raises(toobit.SemanticEvidenceError):
        toobit.semantic_answer("oi_size_unit")
    assert toobit.semantic_status()["oi_size_unit"]["status"] == "INVALID_EVIDENCE"
    assert not toobit.semantics_verified("rest_open_interest")
    assert oi_env().normalized_payload["unit"] == "UNVERIFIED"
    assert check(tb(), TB, TSYM)[OI]["verdict"] == "FAIL"


def test_malformed_mapping_evidence_fails_index_row(monkeypatch):
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "index_contract_mapping", {"value": "GUESS"})
    assert check(tb(), TB, TSYM)[IDX]["verdict"] == "FAIL"


def test_official_mapping_answer_is_applied_to_same_run_metadata(monkeypatch):
    # live-shaped contract: index "BTC", quoteAsset "USDT", indexToken "BTCUSDT"
    info = [contract(quoteAsset="USDT")]
    row = check(tb(contracts=info), TB, TSYM)[IDX]
    assert row["verdict"] == "PASS" and row["semantics"]["contract_index_token"] == "BTCUSDT"
    assert row["semantics"]["legacy_index_field"] == "BTC"
    assert check(tb(contracts=[contract(token=None, quoteAsset="USDT")]), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"
    p = idx_env_proven()
    assert "UNVERIFIED_CONTRACT_MAPPING" not in p.quality_flags and "UNVERIFIED_SCHEMA" not in p.quality_flags
    assert p.normalized_payload["contract_mapping"] == "VERIFIED" and p.symbol == COMMON
    assert "UNVERIFIED_CONTRACT_MAPPING" in idx_env().quality_flags
    # the record removed: even a matching token proves nothing
    monkeypatch.setitem(toobit.SEMANTIC_EVIDENCE, "index_contract_mapping", None)
    assert "UNVERIFIED_CONTRACT_MAPPING" in idx_env_proven().quality_flags
    assert check(tb(contracts=info), TB, TSYM)[IDX]["verdict"] == "INCONCLUSIVE"


def test_official_oi_answer_sets_unit_but_still_no_rescaling_or_cross_exchange_use(monkeypatch):
    promote_oi(monkeypatch, unit="CONTRACTS")
    n = oi_env("4204694").normalized_payload
    assert n["unit"] == "CONTRACTS" and n["aggregation"] == "ONE_SIDE"
    assert n["open_interest"] == Decimal("4204694") and n["contract_multiplier_applied"] is False
    assert n["comparable_across_exchanges"] is False


# ================================================================ quality gate
def test_gate_treats_semantic_flags_as_unverified():
    assert {"UNVERIFIED_SCHEMA", "UNVERIFIED_CONTRACT_MAPPING", "UNVERIFIED_UNIT", "UNVERIFIED_AGGREGATION"} == set(UNVERIFIED_FLAGS)
    for flag in ("UNVERIFIED_CONTRACT_MAPPING", "UNVERIFIED_UNIT", "UNVERIFIED_AGGREGATION"):
        g = DataQualityGate()
        g.on_connect(TB)
        g.observe(MarketEnvelope(source=TB, symbol=COMMON, exchange_symbol=TSYM, event_type=EventType.OPEN_INTEREST,
                                 event_time=None, received_at=T0, raw_payload={"size": "1"},
                                 normalized_payload={"open_interest": "1"}, quality_flags=("DOC_ONLY", flag)))
        assert g.status(TB, COMMON, T0, types=["OPEN_INTEREST"]) == Readiness.UNVERIFIED
        assert not g.usable(TB, COMMON, T0, types=["OPEN_INTEREST"])
        assert g.explain(TB, COMMON, T0, types=["OPEN_INTEREST"]) == {"OPEN_INTEREST": "UNVERIFIED"}


def test_unverified_types_do_not_block_core_types_but_block_their_consumers():
    g = DataQualityGate()
    g.on_connect(TB, [COMMON])
    g.mark_synced(TB, COMMON)
    g.observe(toobit.parse_book_ticker([{"t": MS, "s": TSYM, "b": "99", "bq": "1", "a": "100", "aq": "1"}], T0)[0])
    g.observe(idx_env())
    g.observe(oi_env())
    assert g.status(TB, COMMON, T0, types=["BOOK_TICKER"]) == Readiness.READY
    assert g.status(TB, COMMON, T0, types=["BOOK_TICKER", "INDEX_PRICE"]) == Readiness.UNVERIFIED
    assert g.status(TB, COMMON, T0) == Readiness.UNVERIFIED            # a consumer naming no types gets everything


def test_toobit_index_is_never_compared_with_another_exchange(monkeypatch):
    g = DataQualityGate()
    g.on_connect(TB, [COMMON])
    g.on_connect(Source.BINANCE_FUTURES, [COMMON])
    g.observe(idx_env())
    for e in binance.FUTURES.parse({"e": "markPriceUpdate", "E": MS, "s": "BTCUSDT", "p": "61400", "i": "61367"}, T0):
        g.observe(e)
    d = g.divergence(TB, Source.BINANCE_FUTURES, COMMON, "index", T0)
    assert isinstance(d, str) and d.startswith("NOT_COMPARABLE")
    monkeypatch.setattr(g, "status", lambda *a, **k: Readiness.READY)  # even if readiness were (wrongly) READY
    d = g.divergence(TB, Source.BINANCE_FUTURES, COMMON, "index", T0)
    assert isinstance(d, str) and "UNVERIFIED_CONTRACT_MAPPING" in d
    assert "PRICE_DIVERGENCE" not in g.kinds()


# ================================================================ extended evidence: indexPriceComponents
def test_index_components_valid_structure_never_proves_mapping():
    r = toobit.check_index_components("BTCUSDT", LIVE_COMPONENTS)
    assert r["verdict"] == "PASS" and r["evidence"]["proves_contract_mapping"] is False
    assert r["evidence"]["component_count"] == 6 and "61747" not in repr(r)
    assert toobit.check_index_components("BTC", {})["verdict"] == "INCONCLUSIVE"     # live: ?symbol=BTC -> {}


@pytest.mark.parametrize("mut", [
    lambda d: d.pop("components"), lambda d: d.update(components=[]), lambda d: d.update(index="NaN"),
    lambda d: d.update(edp="0"), lambda d: d.update(time=True), lambda d: d.update(time="1"),
    lambda d: d["components"][0].update(weight="0"), lambda d: d["components"][0].update(exchange=""),
    lambda d: d["components"][0].pop("spotPair"), lambda d: d.update(components={"a": 1}),
])
def test_index_components_invalid_is_fail(mut):
    d = json.loads(json.dumps(LIVE_COMPONENTS))
    mut(d)
    assert toobit.check_index_components("BTCUSDT", d)["verdict"] == "FAIL"


def _check_main(resp, monkeypatch):
    resp = dict(resp, **{"/api/v1/time": {"serverTime": int(time.time() * 1000)}})   # main() uses the real clock
    monkeypatch.setattr(live, "PublicRestClient", lambda pol: PublicRestClient(pol, transport=lambda u, p: _resp(resp, u)))
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = live.main(["--check", "--source", "toobit-futures", "--symbol", TSYM, "--allow-toobit-network",
                        "--i-approve-toobit-public-readonly"])
    return rc, json.loads(buf.getvalue())


def test_check_output_carries_extended_evidence_without_changing_exit_code(monkeypatch):
    resp = dict(tb(), **{COMP: LIVE_COMPONENTS})
    rc, out = _check_main(resp, monkeypatch)
    assert rc == 0 and out["summary"] == {"PASS": 9, "INCONCLUSIVE": 1}
    assert COMP not in [r["endpoint"] for r in out["endpoints"]]                      # not part of the REST contract
    assert COMP not in live.required_rest_endpoints(TB)
    [ev] = out["extended_evidence"]
    assert ev["verdict"] == "PASS" and ev["params"] == {"symbol": "BTCUSDT"} and ev["proves_contract_mapping"] is False
    assert set(out["semantic_questions"]) == set(toobit.SEMANTIC_QUESTIONS)
    for broken in (HttpError(500), KeyError("boom"), {"index": "1"}):
        rc, out = _check_main(dict(tb(), **{COMP: broken}), monkeypatch)
        assert rc == 0 and out["extended_evidence"][0]["verdict"] == "FAIL"          # extended never breaks the check


# ================================================================ verifier scope split (end to end, no network)
def _envs():
    rx = T0
    ws = (toobit.parse_ws({"topic": "trade", "symbol": TSYM, "f": False, "data": [{"v": "1", "t": MS, "p": "1", "q": "1", "m": True}]}, rx)
          + toobit.parse_ws({"topic": "bookTicker", "data": {"s": TSYM, "b": "1", "bq": "1", "a": "2", "aq": "1", "t": MS}}, rx)
          + toobit.parse_ws({"topic": "markPrice", "data": {"exchangeId": 301, "symbolId": TSYM, "price": "1", "time": MS}}, rx)
          + toobit.parse_ws({"topic": "depth", "data": {"s": TSYM, "t": MS, "b": [["1", "1"]], "a": [["2", "1"]]}}, rx))
    rest = [idx_env(), oi_env()]
    return ws, rest


def _fake_runs(tmp_path, monkeypatch, check_out, per_type=None, collector_code=0, required_unverified=False):
    t = _tool()
    ws, rest = _envs()
    lines = [e.to_json() + "\n" for e in ws + rest]
    count = lambda envs: {k: sum(1 for e in envs if e.event_type.value == k) for k in {e.event_type.value for e in envs}}  # noqa: E731
    fresh = {k: {"live_count": 1, "first_at": T0.isoformat(), "last_at": T0.isoformat(), "max_gap_s": 1.0, "end_age_s": 1.0}
             for k in ("TRADE", "BOOK_TICKER", "MARK_PRICE", "ORDERBOOK_SNAPSHOT")}
    pt = per_type or {"TRADE": "OK", "BOOK_TICKER": "OK", "MARK_PRICE": "OK", "ORDERBOOK_SNAPSHOT": "OK",
                      "INDEX_PRICE": "UNVERIFIED", "OPEN_INTEREST": "UNVERIFIED"}
    summary = {"exit_code": collector_code, "error": None, "unhealthy_reasons": [] if collector_code == 0 else ["x"],
               "envelopes": len(lines), "rejected": 0, "max_gap_s": 1.0,
               "ws_events_by_type": count(ws), "rest_events_by_type": count(rest),
               "stream_freshness": {"window_start": T0.isoformat(), "window_end": T0.isoformat(), "window_s": 30.0, "by_type": fresh},
               "end_of_collection": {"at": T0.isoformat(), "core_types": ["TRADE", "BOOK_TICKER", "MARK_PRICE"],
                                     "readiness": "READY", "per_type": pt},
               "events_file_written": {"lines": len(lines), "sha256": hashlib.sha256("".join(lines).encode()).hexdigest()}}

    def run_command(name, argv, logdir, timeout_s):
        out = logdir / f"{name}.stdout.json"
        if name.endswith("rest_check"):
            out.write_text(json.dumps(check_out[1]), encoding="utf-8")
            return {"command": "x", "exit_code": check_out[0], "timed_out": False, "stdout_file": out.name, "stderr_file": "e"}
        with open(argv[argv.index("--out") + 1], "w", encoding="utf-8", newline="\n") as fh:
            fh.write("".join(lines))
        out.write_text(json.dumps(summary), encoding="utf-8")
        return {"command": "x", "exit_code": collector_code, "timed_out": False, "stdout_file": out.name, "stderr_file": "e"}
    monkeypatch.setattr(t, "run_command", run_command)
    return t


def _run_tool(t, tmp_path):
    code = t.main(["--output-dir", str(tmp_path), "--sources", "toobit-futures", "--approve-toobit-public-readonly"])
    return code, json.loads((tmp_path / "verification-report.json").read_text(encoding="utf-8"))


def test_live_like_toobit_run_core_pass_extended_inconclusive_overall_unchanged(tmp_path, monkeypatch):
    check_out = _check_main(dict(tb(), **{COMP: LIVE_COMPONENTS}), monkeypatch)
    t = _fake_runs(tmp_path, monkeypatch, check_out)
    code, rep = _run_tool(t, tmp_path)
    assert code == 2 and rep["overall"] == "INCONCLUSIVE" and rep["fully_verified"] is False   # full result unchanged
    assert rep["core_collection_status"] == "PASS" and rep["extended_semantics_status"] == "INCONCLUSIVE"
    r = rep["results"][0]
    assert r["status"] == "INCONCLUSIVE" and r["stages"]["rest"]["status"] == "INCONCLUSIVE"
    exc = {e["data_type"]: e for e in rep["excluded_from_core"]}
    assert set(exc) == {"INDEX_PRICE", "OPEN_INTEREST"}                               # listed, not hidden
    assert exc["INDEX_PRICE"]["status"] == "PASS" and exc["OPEN_INTEREST"]["status"] == "INCONCLUSIVE"   # aggregation open
    assert all(e["used_by_core"] is False and e["reason"] for e in exc.values())
    assert "oi_aggregation" in exc["OPEN_INTEREST"]["reason"]
    assert rep["downstream_restrictions"]["toobit-futures"]
    assert set(r["extended_semantics"]["open_official_questions"]) == {"ws_index_symbol", "oi_aggregation"}
    md = (tmp_path / "verification-summary.md").read_text(encoding="utf-8")
    assert "core_collection: **PASS**" in md and "overall (full, unchanged): **INCONCLUSIVE**" in md
    assert "downstream restriction" in md


def test_core_fails_when_a_core_rest_row_fails_even_if_extended_is_fine(tmp_path, monkeypatch):
    resp = dict(tb(), **{COMP: LIVE_COMPONENTS})
    resp["/quote/v1/markPrice"] = {"symbol": TSYM, "price": "1", "time": MS}          # core REST row broken
    t = _fake_runs(tmp_path, monkeypatch, _check_main(resp, monkeypatch))
    code, rep = _run_tool(t, tmp_path)
    assert code == 1 and rep["core_collection_status"] == "FAIL"


def test_extended_fail_does_not_hide_but_does_not_break_core(tmp_path, monkeypatch):
    resp = dict(tb(oi={"openInterestList": [{"size": "-1", "symbol": TSYM}]}), **{COMP: LIVE_COMPONENTS})
    t = _fake_runs(tmp_path, monkeypatch, _check_main(resp, monkeypatch))
    code, rep = _run_tool(t, tmp_path)
    assert code == 1 and rep["overall"] == "FAIL"                                      # full verdict still FAIL
    assert rep["extended_semantics_status"] == "FAIL" and rep["core_collection_status"] == "PASS"
    assert {e["data_type"]: e["status"] for e in rep["excluded_from_core"]}["OPEN_INTEREST"] == "FAIL"


def test_core_cannot_pass_if_a_core_type_is_unverified(tmp_path, monkeypatch):
    pt = {"TRADE": "OK", "BOOK_TICKER": "OK", "MARK_PRICE": "OK", "ORDERBOOK_SNAPSHOT": "UNVERIFIED"}
    t = _fake_runs(tmp_path, monkeypatch, _check_main(dict(tb(), **{COMP: LIVE_COMPONENTS}), monkeypatch), per_type=pt)
    code, rep = _run_tool(t, tmp_path)
    assert rep["core_collection_status"] == "INCONCLUSIVE" and code == 2


def test_core_cannot_exclude_a_type_the_core_uses(monkeypatch):
    t = _tool()
    monkeypatch.setitem(t.CORE_TYPES, TB, ("TRADE", "BOOK_TICKER", "MARK_PRICE", "OPEN_INTEREST"))
    stages = {k: {"status": "PASS"} for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready", "evidence_integrity")}
    stages["rest"] = {"endpoints": []}
    summ = {"exit_code": 0, "end_of_collection": {"per_type": {"TRADE": "OK", "BOOK_TICKER": "OK", "MARK_PRICE": "OK",
                                                               "OPEN_INTEREST": "UNVERIFIED", "ORDERBOOK_SNAPSHOT": "OK"}}}
    core = t.judge_core("toobit-futures", stages, ("PASS", "ok"), summ, ["TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "MARK_PRICE"])
    assert core["status"] != "PASS" and "OPEN_INTEREST" in core["excluded_types_used_by_core"]
    assert "OPEN_INTEREST" not in [e["data_type"] for e in core["excluded"]]


def test_core_requires_clean_collector_exit_and_evidence(tmp_path, monkeypatch):
    t = _fake_runs(tmp_path, monkeypatch, _check_main(dict(tb(), **{COMP: LIVE_COMPONENTS}), monkeypatch), collector_code=6)
    code, rep = _run_tool(t, tmp_path)
    assert rep["core_collection_status"] == "FAIL" and code == 1
    t = _tool()
    stages = {k: {"status": "PASS"} for k in ("ws_received", "ws_parsed", "ws_fresh", "gate_ready")}
    stages.update(evidence_integrity={"status": "INCONCLUSIVE", "reason": "attestation missing"}, rest={"endpoints": []})
    pt = {"TRADE": "OK", "BOOK_TICKER": "OK", "MARK_PRICE": "OK", "ORDERBOOK_SNAPSHOT": "OK"}
    core = t.judge_core("toobit-futures", stages, ("PASS", "ok"), {"exit_code": 0, "end_of_collection": {"per_type": pt}},
                        ["TRADE", "BOOK_TICKER", "ORDERBOOK_SNAPSHOT", "MARK_PRICE"])
    assert core["status"] == "INCONCLUSIVE"


def test_rest_core_keeps_full_integrity_preconditions():
    t = _tool()
    rows = [{"endpoint": "/api/v1/time", "verdict": "PASS"}, {"endpoint": IDX, "verdict": "FAIL"}]
    req = ["/api/v1/time", IDX]
    assert t.judge_rest_core({"exit_code": 1, "timed_out": False}, rows, req, [IDX])[0] == "PASS"
    assert t.judge_rest_core({"exit_code": 0, "timed_out": False}, rows, req, [IDX])[0] == "FAIL"     # contradiction
    assert t.judge_rest_core({"exit_code": 1, "timed_out": True}, rows, req, [IDX])[0] == "FAIL"
    assert t.judge_rest_core({"exit_code": 0, "timed_out": False}, rows[:1], req, [IDX])[0] == "PASS"  # excluded row absent
    assert t.judge_rest_core({"exit_code": 0, "timed_out": False}, [rows[0], rows[0]], req, [IDX])[0] == "FAIL"


def test_extended_never_pass_with_open_questions_or_missing_registry():
    t = _tool()
    rows = [{"endpoint": IDX, "verdict": "PASS"}, {"endpoint": OI, "verdict": "PASS"}]
    qs = {q: {"status": "UNVERIFIED"} for q in toobit.SEMANTIC_QUESTIONS}
    assert t.judge_extended("toobit-futures", rows, [], qs)["status"] == "INCONCLUSIVE"
    assert t.judge_extended("toobit-futures", rows, [], None)["status"] == "INCONCLUSIVE"
    assert t.judge_extended("toobit-futures", rows[:1], [], qs)["status"] == "FAIL"               # missing row
    bad = dict(qs, oi_size_unit={"status": "INVALID_EVIDENCE"})
    assert t.judge_extended("toobit-futures", rows, [], bad)["status"] == "FAIL"
    ok = {q: {"status": "VERIFIED"} for q in toobit.SEMANTIC_QUESTIONS}
    assert t.judge_extended("toobit-futures", rows, [{"endpoint": COMP, "verdict": "PASS"}], ok)["status"] == "PASS"
    assert t.judge_extended("binance-futures", rows, [], None)["status"] == "NOT_APPLICABLE"


def test_binance_scope_is_not_applicable_and_overall_rule_unchanged():
    t = _tool()
    assert t.scope_status(["NOT_APPLICABLE", "NOT_APPLICABLE"]) == "NOT_APPLICABLE"
    assert t.scope_status(["NOT_APPLICABLE", "INCONCLUSIVE"]) == "INCONCLUSIVE"
    assert t.exit_code([{"status": "INCONCLUSIVE"}], "INCONCLUSIVE") == 2
    assert t.exit_code([{"status": "PASS"}], "PASS") == 0
    assert "binance-futures" not in t.DOWNSTREAM_RESTRICTIONS and not live.EXTENDED_SEMANTIC_ENDPOINTS.get(Source.BINANCE_FUTURES)
