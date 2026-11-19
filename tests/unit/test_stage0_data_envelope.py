"""Stage 0 MarketEnvelope (DATA-04)."""
import copy
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0.envelope import EnvelopeError, MarketEnvelope

RX = datetime(2026, 3, 31, 12, 0, 1, tzinfo=timezone.utc)


def mk(**o):
    d = dict(source="binance-spot", symbol="BTC/USDT", exchange_symbol="BTCUSDT", event_type="TRADE",
             event_time=RX - timedelta(milliseconds=100), received_at=RX, raw_payload={"p": "1"},
             normalized_payload={"price": "50000.5", "quantity": "0.01"}, sequence_id="9")
    d.update(o)
    return MarketEnvelope(**d)


def test_roundtrip_dict_and_json():
    e = mk()
    for e2 in (MarketEnvelope.from_dict(e.to_dict()), MarketEnvelope.from_json(e.to_json())):
        assert e2.to_dict() == e.to_dict() and e2.digest == e.digest
        assert e2.normalized_payload["price"] == Decimal("50000.5")


def test_book_and_negative_funding_roundtrip():
    e = mk(event_type="ORDERBOOK_SNAPSHOT", normalized_payload={"bids": [["1", "2"]], "asks": [["3", "4"]]})
    assert MarketEnvelope.from_json(e.to_json()).normalized_payload["bids"] == [[Decimal(1), Decimal(2)]]
    f = mk(event_type="FUNDING_RATE", normalized_payload={"funding_rate": "-0.0001"})
    assert MarketEnvelope.from_json(f.to_json()).normalized_payload["funding_rate"] == Decimal("-0.0001")


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf"), True, -1, "0", "abc", None, [1], 0])
def test_bad_price_rejected(bad):
    with pytest.raises(EnvelopeError):
        mk(normalized_payload={"price": bad, "quantity": "1"})


@pytest.mark.parametrize("bad", [float("nan"), True, -0.1, "x"])
def test_bad_quantity_rejected(bad):
    with pytest.raises(EnvelopeError):
        mk(normalized_payload={"price": "1", "quantity": bad})


def test_zero_quantity_ok_but_missing_field_not():
    mk(normalized_payload={"price": "1", "quantity": "0"})
    with pytest.raises(EnvelopeError):
        mk(normalized_payload={"price": "1"})


def test_naive_and_invalid_times_rejected():
    with pytest.raises(EnvelopeError):
        mk(received_at=datetime(2026, 3, 31, 12, 0, 0))
    with pytest.raises(EnvelopeError):
        mk(event_time="2026-13-45T00:00:00Z")
    with pytest.raises(EnvelopeError):
        mk(event_time=12345)


def test_timezone_normalised_to_utc():
    e = mk(event_time="2026-03-31T21:00:00+09:00", received_at="2026-03-31T12:00:01Z")
    assert e.event_time.utcoffset() == timedelta(0) and e.event_time.hour == 12


def test_unknown_source_and_type_and_provenance_rejected():
    for k, v in (("source", "kraken"), ("event_type", "WAT"), ("provenance", "GUESSED")):
        with pytest.raises(EnvelopeError):
            mk(**{k: v})


def test_schema_version_mismatch_rejected():
    d = mk().to_dict()
    d["schema_version"] = "0.9"
    with pytest.raises(EnvelopeError, match="schema_version"):
        MarketEnvelope.from_dict(d)


@pytest.mark.parametrize("path,val", [(("normalized_payload", "price"), "99999"), (("symbol",), "ETH/USDT"),
                                      (("event_time",), "2026-03-31T11:00:00+00:00"),
                                      (("raw_payload", "p"), "2")])
def test_tampering_detected(path, val):
    d = mk().to_dict()
    t = d
    for k in path[:-1]:
        t = t[k]
    t[path[-1]] = val
    with pytest.raises(EnvelopeError, match="digest"):
        MarketEnvelope.from_dict(d)


def test_tampered_reliability_score_rejected():
    d = mk().to_dict()
    d["reliability_score"] = 1.0
    d["quality_flags"] = []
    d["digest"] = mk().digest
    mk().to_dict()      # sanity
    d2 = mk(event_time=None).to_dict()
    d2["reliability_score"] = 1.0
    with pytest.raises(EnvelopeError):
        MarketEnvelope.from_dict(d2)


def test_missing_fields_rejected():
    d = mk().to_dict()
    del d["digest"]
    with pytest.raises(EnvelopeError, match="missing"):
        MarketEnvelope.from_dict(d)
    with pytest.raises(EnvelopeError):
        MarketEnvelope.from_json("{not json")


def test_external_mutation_does_not_change_envelope():
    raw, norm = {"p": ["1"]}, {"price": "5", "quantity": "1"}
    e = mk(raw_payload=raw, normalized_payload=norm)
    before = e.digest
    raw["p"].append("x")
    norm["price"] = "7"
    e.raw_payload["p"].append("y")
    e.normalized_payload["price"] = Decimal(9)
    assert e.digest == before and e._compute_digest() == before and e.normalized_payload["price"] == Decimal(5)


def test_event_time_vs_received_at_are_separate_and_future_flagged():
    e = mk(event_time=RX + timedelta(minutes=5))
    assert "FUTURE_EVENT_TIME" in e.quality_flags and e.event_time != e.received_at
    assert e.reliability_score == 0.75


def test_missing_event_time_allowed_but_flagged():
    e = mk(event_time=None)
    assert e.event_time is None and "NO_EVENT_TIME" in e.quality_flags


@pytest.mark.parametrize("prov,flags,expected", [("OBSERVED", (), 1.0), ("DERIVED", (), 0.8),
                                                 ("ESTIMATED", (), 0.5), ("ESTIMATED", ("CROSSED_BOOK", "UNVERIFIED_SCHEMA", "NO_EVENT_TIME"), 0.0),
                                                 ("OBSERVED", ("INFO_ONLY",), 1.0)])
def test_reliability_in_range_and_documented(prov, flags, expected):
    e = mk(provenance=prov, quality_flags=flags)
    assert 0.0 <= e.reliability_score <= 1.0
    assert e.reliability_score == pytest.approx(expected, abs=0.01)


def test_non_price_events_do_not_require_price_or_quantity():
    mk(event_type="FUNDING_RATE", normalized_payload={"funding_rate": "0.0001"})
    mk(event_type="OPEN_INTEREST", normalized_payload={"open_interest": "10"})
    mk(event_type="MARK_PRICE", normalized_payload={"mark_price": "1"})
    mk(event_type="CONTRACT_SPEC", normalized_payload={"tick_size": "0.1"})


def test_hypothesis_id_optional_and_roundtrips():
    assert mk().hypothesis_id is None
    assert MarketEnvelope.from_json(mk(hypothesis_id="h1").to_json()).hypothesis_id == "h1"


def test_provenance_roundtrips():
    e = mk(provenance="ESTIMATED")
    assert MarketEnvelope.from_json(e.to_json()).provenance.value == "ESTIMATED"
