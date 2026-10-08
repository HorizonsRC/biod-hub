"""
Run every BioD Hub dashboard update script in one go.

Built to be launched by a Windows scheduled task with the ArcGIS Pro Python
environment, so the dashboards stay current without running each script by hand.

Each script runs in its own subprocess, so one failure does not stop the rest.
Every script's full output goes to its own file under logs/runner/<timestamp>/,
and the runner log there lists the outcome of each one. The runner exits with
code 1 if anything failed, so the scheduled task shows the run as failed.

Usage (from the ArcGIS Pro env python):
    python Run_All_Updates.py              # run everything enabled in STEPS
    python Run_All_Updates.py --dry-run    # list what would run, run nothing
    python Run_All_Updates.py --only hub   # run one step by name
"""

import argparse
import logging
import os
import subprocess
import sys
from datetime import datetime as dt
from pathlib import Path

HERE = Path(__file__).parent
TOTARA = Path("html") / "totara-reserve"

# One hour per script. A run that hangs longer than this is recorded as failed.
STEP_TIMEOUT_S = 60 * 60

# ============================================================
# STEPS — run in this order
# ============================================================
# name         : short label, used by --only and in the log
# script, args : what to run
# enabled      : False leaves the step out of a normal run (--only still runs it)
# needs        : name of an earlier step that must succeed first
# push         : files the runner commits and pushes after the step succeeds.
#                Empty for scripts that commit and push for themselves.

STEPS = [
    # Rebuilds from the manually exported SharePoint CSV, so this only changes
    # anything after a new export. It also pushes to the AGOL layer each run.
    dict(name="pm-join", script="Pressure_Management_Data_Join.py", args=[]),
    dict(name="pm-export", script="PM_Dashboard_Export.py", args=[], needs="pm-join"),

    dict(name="icon-sites", script="Icon_Sites_Data_Export.py", args=[]),

    # Off for now: the KKT data only changes once a year.
    dict(name="kkt", script="KKT_Dashboard_Export.py", args=["--push"], enabled=False),

    dict(name="hub", script="Hub_Stats_Export.py", args=["--push"]),

    # Does not push, so the runner pushes its two pages.
    dict(name="targeted-rates", script="Targeted_Rates_Data_Export.py", args=[],
         push=[Path("html") / "targeted-rates" / "WBP.html",
               Path("html") / "targeted-rates" / "REG.html"]),

    # Tōtara Reserve runs section by section. "river" is left out: the Hilltop
    # feed returns malformed XML and a full run blanks river-management.html.
    # The Tōtara script does not push, so the runner pushes its files.
    dict(name="totara-pco", script="Totara_Reserve_Data_Export.py", args=["--only", "pco"],
         push=[TOTARA / "pest-animal-header.html", TOTARA / "predator-control.html"]),
    dict(name="totara-plants", script="Totara_Reserve_Data_Export.py", args=["--only", "plants"],
         push=[TOTARA / "pest-plant-header.html", TOTARA / "pest-plant-control.html",
               TOTARA / "pest-plant-legend.html"]),
    dict(name="totara-bio", script="Totara_Reserve_Data_Export.py", args=["--only", "bio"],
         push=[TOTARA / "biodiversity.html"]),
    dict(name="totara-camp", script="Totara_Reserve_Data_Export.py", args=["--only", "camp"],
         push=[TOTARA / "campground.html"]),
]

# ============================================================
# LOGGING
# ============================================================

RUN_STAMP = dt.now().strftime('%Y-%m-%d_%H-%M-%S')
RUN_DIR = HERE / "logs" / "runner" / RUN_STAMP
RUN_DIR.mkdir(parents=True, exist_ok=True)
log_file = RUN_DIR / "runner.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


# ============================================================
# HELPERS
# ============================================================

def git(*args) -> subprocess.CompletedProcess:
    return subprocess.run(['git', '-C', str(HERE), *args],
                          capture_output=True, text=True, encoding='utf-8', errors='replace')


def first_error_line(output: str) -> str:
    """The line most likely to say what went wrong.

    For a Python traceback that is the last line (the exception message);
    otherwise the first line logged at ERROR level.
    """
    lines = [ln.strip() for ln in output.splitlines() if ln.strip()]
    if any(ln.startswith("Traceback") for ln in lines):
        return lines[-1]
    for ln in lines:
        if " - ERROR - " in ln or " - CRITICAL - " in ln:
            return ln
    return lines[-1] if lines else "(no output)"


def run_step(step: dict) -> dict:
    """Run one script in its own subprocess and record what happened."""
    cmd = [sys.executable, step["script"], *step["args"]]
    out_path = RUN_DIR / f"{step['name']}.log"
    log.info(f"[{step['name']}] {' '.join(cmd[1:])}")

    # The scripts log '—' and macrons; without this the console encoding chokes.
    env = dict(os.environ, PYTHONIOENCODING="utf-8")
    start = dt.now()
    try:
        proc = subprocess.run(cmd, cwd=HERE, env=env, capture_output=True, text=True,
                              encoding='utf-8', errors='replace', timeout=STEP_TIMEOUT_S)
        output = (proc.stdout or "") + (proc.stderr or "")
        ok = proc.returncode == 0
        error = "" if ok else first_error_line(output)
    except subprocess.TimeoutExpired as e:
        output = (e.stdout or "") if isinstance(e.stdout, str) else ""
        ok, error = False, f"Timed out after {STEP_TIMEOUT_S // 60} min"

    out_path.write_text(output, encoding='utf-8')
    warnings = sum(1 for ln in output.splitlines() if " - WARNING - " in ln)
    mins = (dt.now() - start).total_seconds() / 60

    if ok:
        log.info(f"[{step['name']}] OK in {mins:.1f} min, {warnings} warning(s)")
    else:
        log.error(f"[{step['name']}] FAILED in {mins:.1f} min: {error}")
    return dict(name=step["name"], ok=ok, error=error, warnings=warnings, output=out_path)


def push_files(name: str, paths: list) -> tuple[bool, str]:
    """Commit and push the given files, if any of them changed."""
    rel = [str(p).replace("\\", "/") for p in paths]
    add = git('add', *rel)
    if add.returncode != 0:
        return False, add.stderr.strip()
    if git('diff', '--cached', '--quiet', '--', *rel).returncode == 0:
        log.info(f"[{name}] nothing to push, files unchanged")
        return True, ""
    commit = git('commit', '-m', f'Auto-update {name} dashboard data ({dt.now():%d %B %Y %H:%M})',
                 '--', *rel)
    if commit.returncode != 0:
        return False, commit.stderr.strip() or commit.stdout.strip()
    push = git('push', 'origin', 'main')
    if push.returncode != 0:
        return False, push.stderr.strip()
    log.info(f"[{name}] pushed {len(rel)} file(s)")
    return True, ""


# ============================================================
# MAIN
# ============================================================

def main():
    ap = argparse.ArgumentParser(description="Run all BioD Hub dashboard update scripts.")
    ap.add_argument("--dry-run", action="store_true", help="List the steps, run nothing.")
    ap.add_argument("--only", choices=[s["name"] for s in STEPS], help="Run one step only.")
    args = ap.parse_args()

    if args.only:
        steps = [s for s in STEPS if s["name"] == args.only]
    else:
        steps = [s for s in STEPS if s.get("enabled", True)]

    log.info("=" * 70)
    log.info("BIOD HUB — RUN ALL UPDATES")
    log.info(f"Python : {sys.executable}")
    log.info(f"Steps  : {', '.join(s['name'] for s in steps)}")
    log.info("=" * 70)

    if args.dry_run:
        for s in steps:
            log.info(f"  would run: {s['script']} {' '.join(s['args'])}")
        return 0

    # Pick up anything edited on github.com first, or every push below is rejected.
    pull = git('pull', '--rebase', '--autostash', 'origin', 'main')
    if pull.returncode != 0:
        log.error(f"git pull failed, pushes will likely fail too: {pull.stderr.strip()}")

    results = {}
    for step in steps:
        needs = step.get("needs")
        if needs and needs in results and not results[needs]["ok"]:
            log.warning(f"[{step['name']}] skipped, {needs} failed")
            results[step["name"]] = dict(name=step["name"], ok=False,
                                         error=f"Skipped, {needs} failed", warnings=0,
                                         output=None)
            continue

        result = run_step(step)
        if result["ok"] and step.get("push"):
            pushed, err = push_files(step["name"], step["push"])
            if not pushed:
                log.error(f"[{step['name']}] git push failed: {err}")
                result.update(ok=False, error=f"git push failed: {err}")
        results[step["name"]] = result

    failed = [r for r in results.values() if not r["ok"]]

    log.info("=" * 70)
    log.info("SUMMARY")
    for r in results.values():
        status = "OK    " if r["ok"] else "FAILED"
        note = f"{r['warnings']} warning(s)" if r["ok"] else r["error"]
        log.info(f"  {status} {r['name']:<14} {note}")
    log.info(f"Logs: {RUN_DIR}")
    log.info("=" * 70)

    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
