"""Binance KLINE v3.5 regression/evidence tests. All input is synthetic, no live claims."""
import copy
import random
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from src.stage0.adapters.binance import FUTURES, SPOT
from src.stage0.envelope import EnvelopeError, EventType, Source
from src.stage0.kline import (KlineStore, INTERVAL_MS, MAX_CANDLES_PER_KEY, millis,
                              validate as validate_kline)
from src.stage0.quality import DataQualityGate, Readiness
from src.stage1.data_access import GatedDataAccess
from src.stage1.agents.indicators import bars_from_klines
from src.stage1.agents.trend import TrendAgent

START = 1795219200000  # 2026-11-21 00:00:00 UTC, divisible by 1m and 5m
SRC = Source.BINANCE_FUTURES
SYM = "BTC/USDT:USDT"


def dt(ms):
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def kline(bar=0, iv="1m", revision=0, closed=False, event_ms=None,
          rx_ms=None, volume=None, high="110", low="90", close="101", source=SRC):
    start = START + bar * INTERVAL_MS[iv]
    end = start + INTERVAL_MS[iv]
    event_ms = event_ms if event_ms is not None else (end if closed else start + 1000 + 1000 * revision)
    rx_ms = rx_ms if rx_ms is not None else event_ms + 100
    raw = {"e": "kline", "E": event_ms, "s": "BTCUSDT",
           "k": {"t": start, "T": end - 1, "s": "BTCUSDT", "i": iv,
                 "x": closed, "o": "100", "h": str(high), "l": str(low),
                 "c": str(close), "v": str(volume if volume is not None else revision + 1),
                 "n": revision + 1, "f": 1, "L": revision + 1}}
    parser = SPOT if source == Source.BINANCE_SPOT else FUTURES
    return parser.parse(raw, dt(rx_ms))[0]


def test_r1_100_updates_25_finalized_candles():
    gate = DataQualityGate()
    gate.on_connect(SRC)
    accepted = []
    for i in range(25):
        for revision in range(3):
            e = kline(i, revision=revision)
            assert not gate.observe(e)
            accepted.append(e)
        e = kline(i, closed=True, revision=3)
        assert not gate.observe(e)
        accepted.append(e)
    now = accepted[-1].received_at + timedelta(seconds=1)
    assert len(accepted) == 100
    assert gate.issue_counts["DUPLICATE_EVENT"] == 0
    assert len(gate._klines.approved(SRC.value, SYM, now, timedelta(hours=1))) == 25
    assert gate.status(SRC, SYM, now, types=["KLINE"]) == Readiness.READY
    snapshot = GatedDataAccess(accepted).snapshot(SRC.value, SYM, now, ["KLINE"])
    assert snapshot.readiness["KLINE"] == "READY"
    assert len(snapshot.events["KLINE"]) == 25
    assert TrendAgent().run(GatedDataAccess(accepted), SRC.value, SYM, now).metrics["bar_count"] == 25


def test_duplicate_raw_different_local_receive_does_not_refresh():
    g = DataQualityGate()
    g.on_connect(SRC)
    a = kline(0, closed=True)
    b = kline(0, closed=True, rx_ms=millis(a.received_at) + 2000)
    assert not g.observe(a)
    before = g._type_rx[(SRC.value, SYM, "KLINE")]
    assert "DUPLICATE_EVENT" in g.observe(b)
    assert g._type_rx[(SRC.value, SYM, "KLINE")] == before


def test_tied_open_payloads_rollback_and_recovery_both_arrival_orders():
    a = kline(revision=0, close="101")
    b = kline(revision=0, close="102")
    assert a.event_time == b.event_time
    for first, second in ((a, b), (b, a)):
        g = DataQualityGate()
        g.on_connect(SRC)
        assert not g.observe(first)
        assert "KLINE_E_TIE_AMBIGUOUS" in g.observe(second)
        c = next(iter(g._klines.rows.values()))
        assert c.open_env is None and c.ambiguous_e == millis(a.event_time)
        assert "KLINE_E_TIE_AMBIGUOUS" in g.observe(kline(revision=0, close="103"))
        final = kline(closed=True, revision=3)
        assert not g.observe(final)
        assert c.state == "CLOSED" and c.closed_env is final


def test_tie_undoes_only_last_revision_not_prior_trusted():
    g = DataQualityGate()
    first = kline(revision=0)
    tied_a = kline(revision=1, close="101")
    tied_b = kline(revision=1, close="102")
    for e in (first, tied_a, tied_b):
        g.observe(e)
    state = next(iter(g._klines.rows.values()))
    assert state.open_env is first
    assert state.ambiguous_e == millis(tied_a.event_time)
    newer = kline(revision=2)
    assert not g.observe(newer)
    assert state.open_env is newer and state.ambiguous_e is None


def test_same_e_open_to_closed_and_nonmonotone_suspect_counter():
    end = START + 60000
    g = DataQualityGate()
    forming = kline(event_ms=end - 1, high="110", volume=4, close="103")
    final = kline(closed=True, event_ms=end - 1, high="110", volume=4, close="103")
    assert not g.observe(forming)
    assert not g.observe(final)
    assert next(iter(g._klines.rows.values())).state == "CLOSED"
    g2 = DataQualityGate()
    g2.observe(kline(high="110", volume=4))
    wrong = kline(closed=True, high="109", volume=3)
    assert "KLINE_NONMONOTONE_COUNT" in g2.observe(wrong)
    assert g2.issue_counts["KLINE_NONMONOTONE_COUNT"] == 1
    assert next(iter(g2._klines.rows.values())).state == "SUSPECT"


def test_conflicting_closes_quarantine_historical_candle():
    ev = [kline(i, closed=True, revision=3) for i in range(25)]
    now = ev[-1].received_at + timedelta(seconds=1)
    first = GatedDataAccess(ev).snapshot(SRC.value, SYM, now, ["KLINE"])
    assert len(first.events["KLINE"]) == 25
    changed = kline(5, closed=True, revision=3, close="102", rx_ms=millis(ev[-1].received_at)+100)
    second = GatedDataAccess(ev + [changed]).snapshot(SRC.value, SYM, now + timedelta(seconds=2), ["KLINE"])
    assert len(second.events["KLINE"]) == 24
    assert 5 not in [(e.normalized_payload["open_time_ms"] - START)//60000 for e in second.events["KLINE"]]
    old_again = GatedDataAccess(ev + [changed]).snapshot(SRC.value, SYM, now, ["KLINE"])
    assert len(old_again.events["KLINE"]) == 25


def test_1m_and_5m_intervals_isolated():
    g = DataQualityGate()
    a = kline(0, closed=True)
    b = kline(0, "5m", closed=True)
    assert not g.observe(a)
    assert not g.observe(b)
    assert len(g._klines.rows) == 2
    assert sorted(k[3] for k in g._klines.rows) == ["1m", "5m"]
    assert len(g._klines.approved(SRC.value, SYM, b.received_at, timedelta(hours=1))) == 2


def test_delayed_previous_closed_after_next_open():
    a = kline(1, revision=0)
    old = kline(0, closed=True, rx_ms=START+60000+1900)
    g = DataQualityGate()
    g.on_connect(SRC)
    assert not g.observe(a)
    assert not g.observe(old)
    assert len(g._klines.approved(SRC.value, SYM, old.received_at, timedelta(hours=1))) == 1


@pytest.mark.parametrize("lag,accepted", [(5000, True), (5001, False)])
def test_receive_lag_boundary(lag, accepted):
    e = kline(closed=True, rx_ms=START+60000+lag)
    g = DataQualityGate()
    issues = g.observe(e)
    assert ("RECEIVE_LAG" not in issues) is accepted


@pytest.mark.parametrize("advance,accepted", [(1000, True), (1001, False)])
def test_premature_close_boundary(advance, accepted):
    end = START+60000
    e_ms = end-advance
    raw = {"e": "kline", "E": e_ms, "s": "BTCUSDT",
           "k": {"t": START, "T": end-1, "s": "BTCUSDT", "i": "1m",
                 "x": True, "o": "100", "h": "110", "l": "90",
                 "c": "101", "v": "1"}}
    if accepted:
        e = FUTURES.parse(raw, dt(e_ms+100))[0]
        assert ("CLOSE_EARLY_BY_MS", str(advance)) in validate_kline(e)
    else:
        with pytest.raises(EnvelopeError, match="KLINE_PREMATURE_CLOSE"):
            FUTURES.parse(raw, dt(e_ms+100))


def test_late_finalization_accepted_and_observed():
    e = kline(closed=True, event_ms=START+60000+6000)
    g = DataQualityGate()
    assert "KLINE_LATE_FINALIZATION" in g.observe(e)
    assert g.issue_counts["KLINE_LATE_FINALIZATION"] == 1
    assert next(iter(g._klines.rows.values())).state == "CLOSED"


def test_c_latest_only_and_currency_plus_minus_one_ms():
    s = KlineStore()
    for i in range(21):
        assert s.observe(kline(i, closed=True))[0]
    newest_start = START + 20*60000
    at_end = dt(newest_start + 2*60000 + 5000)
    assert s.current_closed(SRC.value, SYM, at_end)
    assert not s.current_closed(SRC.value, SYM, at_end + timedelta(milliseconds=1))
    assert len(s.approved(SRC.value, SYM, at_end, timedelta(hours=1))) == 21


def test_close_arriving_one_ms_beyond_currency_is_rejected():
    end = START+60000
    accepted = kline(closed=True, event_ms=START+125000, rx_ms=START+125000)
    rejected = kline(closed=True, event_ms=START+125001, rx_ms=START+125001)
    assert KlineStore().observe(accepted)[0]
    assert "KLINE_CURRENCY_EXPIRED" == KlineStore().observe(rejected)[1][0][0]


@pytest.mark.parametrize("field,value", [("x", "false"), ("x", 1), ("T", START+59998),
                                         ("s", "ETHUSDT"), ("v", "-1"), ("h", "95")])
def test_parser_rejects_invalid_raw_fields(field, value):
    e = kline()
    raw = copy.deepcopy(e.raw_payload)
    raw["k"][field] = value
    with pytest.raises((EnvelopeError, KeyError)):
        FUTURES.parse(raw, e.received_at)


def test_parser_rejects_mismatched_top_level_symbol():
    e = kline()
    raw = copy.deepcopy(e.raw_payload)
    raw["s"] = "ETHUSDT"
    with pytest.raises(EnvelopeError):
        FUTURES.parse(raw, e.received_at)


def test_direct_bars_closed_immune_to_late_open_and_conflict():
    a = kline(closed=True)
    late = kline()
    s1 = bars_from_klines([a, late])
    s2 = bars_from_klines([late, a])
    assert s1 and s2 and s1.bars == s2.bars
    conflict = kline(closed=True, close="104")
    assert bars_from_klines([a, late, conflict]) is None


def test_no_kline_ready_from_only_open_or_after_disconnect():
    g = DataQualityGate()
    g.on_connect(SRC)
    g.observe(kline())
    assert g.status(SRC, SYM, dt(START+6000), ["KLINE"]) == Readiness.STALE
    g.observe(kline(closed=True))
    now = dt(START+60100)
    assert g.status(SRC, SYM, now, ["KLINE"]) == Readiness.READY
    g.on_disconnect(SRC)
    g.on_connect(SRC)
    assert g.status(SRC, SYM, now, ["KLINE"]) == Readiness.STALE


def test_1s_and_excess_lookback_not_supported():
    assert not KlineStore.supported_lookback(timedelta(hours=5))
    envs = [kline(i, closed=True) for i in range(25)]
    now = envs[-1].received_at + timedelta(seconds=1)
    s = GatedDataAccess(envs).snapshot(SRC.value, SYM, now, ["KLINE"], lookback=timedelta(hours=5))
    assert s.readiness["KLINE"] == "INVALID"
    assert "NOT_SUPPORTED" in s.diagnosis["KLINE"]
    raw = copy.deepcopy(kline().raw_payload)
    raw["k"]["i"] = "1s"
    with pytest.raises(EnvelopeError, match="NOT_SUPPORTED"):
        FUTURES.parse(raw, dt(raw["E"]+100))


def test_deterministic_replay_shuffled_input():
    envs = [kline(i, closed=True) for i in range(25)]
    now = envs[-1].received_at + timedelta(seconds=1)
    old = GatedDataAccess(envs).snapshot(SRC.value, SYM, now, ["KLINE"])
    random.Random(17).shuffle(envs)
    new = GatedDataAccess(envs).snapshot(SRC.value, SYM, now, ["KLINE"])
    assert [x.digest for x in old.events["KLINE"]] == [x.digest for x in new.events["KLINE"]]
    assert MAX_CANDLES_PER_KEY == 256
