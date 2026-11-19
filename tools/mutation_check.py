"""Lock/guard MUTATION CHECK.

A race/guard test only proves something if it FAILS when the protection is removed. For each
mutation below this script
  1. copies src/orders/migrations to a temp dir and weakens ONE protection in the SQL,
  2. runs the targeted tests against a database built from the weakened SQL,
  3. requires pytest to FAIL (a surviving mutant means the test does not test the protection),
and it first requires the UNMUTATED baseline to PASS (otherwise a failure proves nothing).

Evidence (JSON + per-run logs + commit SHA) is written to --out for upload as a CI artifact.

Database: with TRACK_A_TEST_PG_DSN set (CI service container) a fresh database is created per
mutation; without it the tests use their embedded PostgreSQL (fresh per pytest process).
Exit code 0 only if baseline passes AND every mutant is killed.
"""
import argparse
import datetime
import json
import os
import pathlib
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
MIG = ROOT / "src" / "orders" / "migrations"

# (name, migration file, regex, replacement, tests that must catch it)
MUTATIONS = [
    ("advisory_lock_removed", "002_entry_gate.sql",
     r"SELECT pg_advisory_xact_lock\(hashtext\('track_a_entry_gate'\)\);", "SELECT 1;",
     ["tests/unit/test_trackA_entry_gate_concurrency.py"]),
    ("entry_gate_trigger_disabled", "002_entry_gate.sql",
     r"IF \(SELECT frozen FROM entry_control WHERE id = 1\) THEN", "IF FALSE THEN",
     ["tests/unit/test_trackA_entry_gate_concurrency.py", "tests/unit/test_trackA_pg_safety.py"]),
    ("release_guard_disabled", "002_entry_gate.sql",
     r"IF EXISTS \(SELECT 1 FROM order_intents\n\s+WHERE state IN \('INTENT_PERSISTED','SUBMIT_PENDING','SUBMISSION_UNKNOWN'\)\) THEN",
     "IF FALSE THEN",
     ["tests/unit/test_p1_2_reset_risk_mitigation.py"]),
    ("fill_decrease_guard_disabled", "004_order_lifecycle_fills.sql",
     r"IF NEW\.filled_qty < OLD\.filled_qty THEN", "IF FALSE THEN",
     ["tests/unit/test_p1_1_lifecycle_fills.py"]),
    ("fill_uniqueness_removed", "004_order_lifecycle_fills.sql",
     r"CONSTRAINT uq_fill_once UNIQUE \(intent_id, exchange_fill_id\)", "CONSTRAINT uq_fill_once CHECK (true)",
     ["tests/unit/test_p1_1_lifecycle_fills.py"]),
]


def run_pytest(tests, env, log_path):
    cmd = [sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider", "-x", *tests]
    with open(log_path, "w", encoding="utf-8") as fh:
        p = subprocess.run(cmd, cwd=ROOT, env=env, stdout=fh, stderr=subprocess.STDOUT)
    text = pathlib.Path(log_path).read_text(encoding="utf-8", errors="replace")
    failed = re.findall(r"^FAILED (\S+)", text, re.M)
    return p.returncode, failed, text.strip().splitlines()[-1] if text.strip() else ""


def fresh_db_env(base_env, name):
    dsn = base_env.get("TRACK_A_TEST_PG_DSN")
    if not dsn:
        return base_env, None
    import psycopg
    from psycopg.conninfo import conninfo_to_dict, make_conninfo
    dbname = f"mut_{name}"
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute(f'DROP DATABASE IF EXISTS "{dbname}"')
        c.execute(f'CREATE DATABASE "{dbname}"')
    params = conninfo_to_dict(dsn)
    params["dbname"] = dbname
    env = dict(base_env)
    env["TRACK_A_TEST_PG_DSN"] = make_conninfo(**params)
    return env, dbname


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="mutation-evidence")
    args = ap.parse_args()
    out = pathlib.Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    sha = os.environ.get("GITHUB_SHA") or subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True).stdout.strip()
    report = {"commit": sha, "at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "baseline": None, "mutations": []}
    ok = True

    all_tests = sorted({t for m in MUTATIONS for t in m[4]})
    env, _ = fresh_db_env(dict(os.environ), "baseline")
    rc, failed, last = run_pytest(all_tests, env, out / "baseline.log")
    report["baseline"] = {"returncode": rc, "summary": last, "tests": all_tests}
    if rc != 0:
        ok = False
        report["error"] = "baseline (unmutated) run FAILED: mutation results would be meaningless"
    else:
        for name, fname, pattern, repl, tests in MUTATIONS:
            tmp = pathlib.Path(tempfile.mkdtemp(prefix="mig_"))
            try:
                shutil.copytree(MIG, tmp / "migrations")
                f = tmp / "migrations" / fname
                src = f.read_text(encoding="utf-8")
                mutated, n = re.subn(pattern, repl, src, count=1)
                if n != 1:
                    ok = False
                    report["mutations"].append({"name": name, "status": "PATTERN_NOT_FOUND"})
                    continue
                f.write_text(mutated, encoding="utf-8")
                (out / f"{name}.diff.txt").write_text(
                    f"--- {fname}\nPATTERN: {pattern}\nREPLACED WITH: {repl}\n", encoding="utf-8")
                env, dbname = fresh_db_env(dict(os.environ), name)
                env["TRACK_A_MIGRATIONS_DIR"] = str(tmp / "migrations")
                rc, failed, last = run_pytest(tests, env, out / f"{name}.log")
                killed = rc != 0
                ok &= killed
                report["mutations"].append({
                    "name": name, "migration": fname, "tests": tests,
                    "status": "KILLED" if killed else "SURVIVED", "returncode": rc,
                    "failing_tests": failed[:20], "summary": last, "database": dbname})
            finally:
                shutil.rmtree(tmp, ignore_errors=True)
    report["all_mutants_killed"] = ok
    (out / "mutation-report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (out / "commit-sha.txt").write_text(sha + "\n", encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("commit", "baseline", "all_mutants_killed")}, indent=2))
    for m in report["mutations"]:
        print(f"  {m['name']}: {m['status']}  {m.get('summary', '')}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
