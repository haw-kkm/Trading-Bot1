"""Isolated synthetic KLINE mutation audit (M1..M11). Never modifies the checkout.

Each weakened branch is written to a temporary copy of src/ and tested independently.
Exit 0 only if the baseline PASSES and every injected fault is KILLED (nonzero pytest).
An injection/runner error is NOT a kill.
"""
import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TEST = "tests/unit/test_stage0_kline_v35.py"
PARSER = "src/stage0/adapters/binance.py"
GATE = "src/stage0/quality.py"
STATE = "src/stage0/kline.py"
BARS = "src/stage1/agents/indicators.py"


def edits():
    return {
        "M1_open_time_as_sequence": [
            (PARSER, 'seq=None)    # k.t', 'seq=str(k["t"]))    # k.t'),
            (GATE, "if env.event_type == EventType.KLINE and env.source in",
             "if False and env.source in"),
        ],
        "M2a_gate_overwrite_closed": [
            (STATE, 'if c.state == "CLOSED":', 'if False and c.state == "CLOSED":'),
        ],
        "M2b_bars_overwrite_closed": [
            (BARS, "if previous is None:", "if True:"),
        ],
        "M3_interval_dropped_from_key": [
            (STATE, 'n["interval"], n["open_time_ms"])', '"1m", n["open_time_ms"])'),
        ],
        "M4_tie_permanent_conflict": [
            (STATE, 'c.open_env, c.ambiguous_e = c.previous_open, stamp',
             'c.state, c.ambiguous_e = "CONFLICT", stamp'),
        ],
        "M5_rejected_refreshes_stream": [
            (GATE, 'if not accepted:\n                return found',
             'if not accepted:\n                self._type_rx[key] = env.received_at\n                return found'),
        ],
        "M6_conflicting_close_left_approved": [
            (STATE, 'c.state = "CONFLICT"\n            c.closed_env = None',
             'c.state = "CLOSED"\n            c.closed_env = c.closed_env'),
        ],
        "M7_premature_close_accepted": [
            (STATE, 'PREMATURE_TOLERANCE_MS = 1000', 'PREMATURE_TOLERANCE_MS = 100000'),
        ],
        "M8_currency_applied_to_every_bar": [
            (STATE, 'if env and env.received_at <= now and start <= env.event_time <= now:',
             'if env and env.received_at <= now and start <= env.event_time <= now '
             'and millis(now) <= key[-1] + 2 * INTERVAL_MS[key[3]] + self.grace_ms:'),
        ],
        "M9_late_finalization_hard_rejected": [
            (STATE, 'if closed and event_ms - end > late_warn_ms:',
             'if closed and event_ms - end > late_warn_ms:\n'
             '        raise EnvelopeError("KLINE_LATE_FINALIZATION")\n'
             '    if closed and event_ms - end > late_warn_ms:'),
        ],
        "M10_first_tied_open_kept": [
            (STATE, 'c.open_env, c.ambiguous_e = c.previous_open, stamp',
             'c.ambiguous_e = stamp'),
        ],
        "M11_premature_off_by_one": [
            (STATE, 'if closed and event_ms < end - premature_ms:',
             'if closed and event_ms <= end - premature_ms:'),
        ],
    }


def prepare(dest):
    shutil.copytree(ROOT / "src", dest / "src")
    target = dest / TEST
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(ROOT / TEST, target)


def execute(dest):
    env = dict(os.environ, PYTHONPATH=str(dest), PYTHONDONTWRITEBYTECODE="1")
    proc = subprocess.run([sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", TEST],
                          cwd=dest, env=env, capture_output=True, text=True, timeout=120)
    return proc.returncode, (proc.stdout + "\n" + proc.stderr)[-10000:]


def main():
    arg = argparse.ArgumentParser()
    arg.add_argument("--out", default="kline-mutation-evidence")
    args = arg.parse_args()
    output = ROOT / args.out
    output.mkdir(parents=True, exist_ok=True)
    report = {"suite": TEST, "baseline": None, "mutants": {}}
    with tempfile.TemporaryDirectory(prefix="kline-mutations-") as location:
        base = Path(location) / "baseline"
        prepare(base)
        code, log = execute(base)
        report["baseline"] = {"result": "PASS" if code == 0 else "FAIL", "exit_code": code}
        (output / "baseline.log").write_text(log, encoding="utf-8")
        if code != 0:
            print("KLINE mutation baseline FAILED; no mutation outcome can be trusted")
        else:
            for name, rules in edits().items():
                target = Path(location) / name
                prepare(target)
                error = None
                for path, old, new in rules:
                    f = target / path
                    body = f.read_text(encoding="utf-8")
                    if old not in body:
                        error = f"INJECTION_ERROR: anchor missing in {path}: {old}"
                        break
                    f.write_text(body.replace(old, new), encoding="utf-8")
                if error:
                    report["mutants"][name] = {"result": "ERROR", "detail": error}
                    continue
                try:
                    code, log = execute(target)
                except subprocess.TimeoutExpired:
                    report["mutants"][name] = {"result": "ERROR", "detail": "TEST_TIMEOUT"}
                    continue
                killed = code == 1
                report["mutants"][name] = {"result": "KILLED" if killed else "SURVIVED_OR_ERROR",
                                           "exit_code": code}
                (output / f"{name}.log").write_text(log, encoding="utf-8")
                print(name, report["mutants"][name]["result"])
    (output / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    expected = len(edits())
    killed = sum(x["result"] == "KILLED" for x in report["mutants"].values())
    print(f"KLINE mutation: {killed}/{expected} KILLED, baseline={report['baseline']['result']}")
    return 0 if report["baseline"]["result"] == "PASS" and killed == expected else 1


if __name__ == "__main__":
    sys.exit(main())
