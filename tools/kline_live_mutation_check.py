"""Isolated mutation audit for the Binance KLINE LIVE WIRING (L1..L6). Never modifies the checkout.

Each weakened branch is written to a temporary copy of src/ and tested independently by
tests/unit/test_stage0_kline_live_wiring.py. Exit 0 only if the baseline PASSES and every injected fault is
KILLED (pytest exit code 1). An injection/runner error is NOT a kill.
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
TEST = "tests/unit/test_stage0_kline_live_wiring.py"
LIVE = "src/stage0/live.py"
COLLECTOR = "src/stage0/collector.py"


def edits():
    return {
        "L1_one_kline_subscription_dropped": [
            (LIVE, 'for iv in self.kline_intervals)', 'for iv in self.kline_intervals[:1])'),
        ],
        "L2_futures_kline_on_public_path": [
            (LIVE, '@depth@100ms", True),', '@depth@100ms{kl}", True),'),
            (LIVE, '@markPrice@1s{kl}", False)]', '@markPrice@1s", False)]'),
        ],
        "L3_reconnect_loses_kline_subscription": [
            (LIVE, 'conn = await self.ws.open(src, url)\n                self._open_stream_window()\n                self.c.gate.on_connect(src, [common] if with_book else [])',
             'conn = await self.ws.open(src, url if not self.reconnects else url.split("@kline_")[0].rsplit("/", 1)[0])\n'
             '                self._open_stream_window()\n                self.c.gate.on_connect(src, [common] if with_book else [])'),
        ],
        "L4_normal_sink_never_flushes": [
            (LIVE, '    def __call__(self, env):\n        self._emit(env.to_json() + "\\n")',
             '    def __call__(self, env):\n        line = env.to_json() + "\\n"\n        self._fh.write(line)\n'
             '        self._h.update(line.encode("utf-8"))\n        self.lines += 1'),
            (LIVE, '    def flush_if_due(self):\n        if self._pending', '    def flush_if_due(self):\n        if False and self._pending'),
        ],
        "L6_idle_flush_timer_disabled": [
            (LIVE, "            await asyncio.sleep(self.flush_poll_s)
            self._flush_sinks_if_due()",
             "            await asyncio.sleep(self.flush_poll_s)
            pass"),
        ],
        "L5_rejected_kline_sidecar_not_written": [
            (COLLECTOR, 'if self.rejected_sink is not None and "raw_message" in entry:',
             'if False and self.rejected_sink is not None and "raw_message" in entry:'),
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
                          cwd=dest, env=env, capture_output=True, text=True, timeout=300)
    return proc.returncode, (proc.stdout + "\n" + proc.stderr)[-10000:]


def main():
    arg = argparse.ArgumentParser()
    arg.add_argument("--out", default="kline-live-mutation-evidence")
    args = arg.parse_args()
    output = ROOT / args.out
    output.mkdir(parents=True, exist_ok=True)
    report = {"suite": TEST, "baseline": None, "mutants": {}}
    with tempfile.TemporaryDirectory(prefix="kline-live-mutations-") as location:
        base = Path(location) / "baseline"
        prepare(base)
        code, log = execute(base)
        report["baseline"] = {"result": "PASS" if code == 0 else "FAIL", "exit_code": code}
        (output / "baseline.log").write_text(log, encoding="utf-8")
        if code != 0:
            print("KLINE live mutation baseline FAILED; no mutation outcome can be trusted")
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
                    print(name, error)
                    continue
                try:
                    code, log = execute(target)
                except subprocess.TimeoutExpired:
                    report["mutants"][name] = {"result": "ERROR", "detail": "TEST_TIMEOUT"}
                    continue
                report["mutants"][name] = {"result": "KILLED" if code == 1 else "SURVIVED_OR_ERROR", "exit_code": code}
                (output / f"{name}.log").write_text(log, encoding="utf-8")
                print(name, report["mutants"][name]["result"])
    (output / "summary.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    expected = len(edits())
    killed = sum(x["result"] == "KILLED" for x in report["mutants"].values())
    print(f"KLINE live mutation: {killed}/{expected} KILLED, baseline={report['baseline']['result']}")
    return 0 if report["baseline"]["result"] == "PASS" and killed == expected else 1


if __name__ == "__main__":
    sys.exit(main())
