from __future__ import annotations

import json
import os
import shlex
import subprocess
import time
from pathlib import Path

ROOT = Path("/Users/filmonghezehey/trading-agent/worktrees/databento")
COMPARE_ROOT = ROOT / "training_data" / "compare_runs_20260407_meta_ab"
VARIANT_DIR = COMPARE_ROOT / "with_timesfm_and_sequence_proxy"
MANIFEST_SH = VARIANT_DIR / "training_manifest.sh"
LOG_DIR = VARIANT_DIR / "logs"
STATUS_JSON = VARIANT_DIR / "completion_watcher_status.json"
WATCH_LOG = VARIANT_DIR / "completion_watcher.log"
RECONCILE_SCRIPT = ROOT / "runtime" / "reconcile_compare_variant_manifest_20260407.py"
REPORT_SCRIPT = ROOT / "runtime" / "generate_compare_report_20260407.py"
POLL_SECONDS = 60
QUIET_POLLS_BEFORE_FAIL = 5


def load_commands() -> list[tuple[str, str]]:
    lines = [
        line.strip()
        for line in MANIFEST_SH.read_text(encoding="utf-8").splitlines()
        if line.strip() and not line.startswith("#!") and not line.startswith("set -euo")
    ]
    commands: list[tuple[str, str]] = []
    for line in lines:
        parts = shlex.split(line)
        job_name = ""
        for idx, part in enumerate(parts[:-1]):
            if part == "--job-name":
                job_name = parts[idx + 1]
                break
        if not job_name:
            raise ValueError(f"Could not parse --job-name from command: {line}")
        commands.append((job_name, line))
    return commands


def log_text(path: Path) -> str:
    if not path.exists():
        return ""
    return path.read_text(encoding="utf-8", errors="replace")


def is_job_complete(job_name: str) -> bool:
    text = log_text(LOG_DIR / f"{job_name}.log").lower()
    return ">>> pipeline complete." in text


def active_variant_processes() -> list[str]:
    out = subprocess.check_output(["ps", "-axo", "pid=,command="], text=True)
    hits: list[str] = []
    needle = str(VARIANT_DIR)
    for line in out.splitlines():
        if needle not in line:
            continue
        if "prepare_databento_training.py" in line or "train_30s_models.py" in line:
            hits.append(line.strip())
    return hits


def write_status(payload: dict) -> None:
    STATUS_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with WATCH_LOG.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(payload) + "\n")


def run_command(command: str) -> int:
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env["MODEL_EXPORTS_ROOT"] = str(VARIANT_DIR / "model_exports")
    env["UPDATE_CANONICAL_MODEL_ALIASES"] = "0"
    with WATCH_LOG.open("a", encoding="utf-8") as fh:
        fh.write(f"RUN {command}\n")
        fh.flush()
        return subprocess.call(command, cwd=str(ROOT), env=env, shell=True, stdout=fh, stderr=subprocess.STDOUT)


def finalize_reports() -> None:
    subprocess.check_call([
        "python3",
        str(RECONCILE_SCRIPT),
        "--variant-dir",
        str(VARIANT_DIR),
        "--write",
    ], cwd=str(ROOT))
    subprocess.check_call(["python3", str(REPORT_SCRIPT)], cwd=str(ROOT))



def main() -> None:
    commands = load_commands()
    quiet_polls = 0
    last_completed = -1

    while True:
        completed = [job_name for job_name, _ in commands if is_job_complete(job_name)]
        pending = [(job_name, cmd) for job_name, cmd in commands if job_name not in completed]
        active = active_variant_processes()

        payload = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "expected_jobs": len(commands),
            "completed_jobs": len(completed),
            "pending_jobs": [job for job, _ in pending],
            "active_processes": active,
        }
        write_status(payload)

        if len(completed) == len(commands):
            finalize_reports()
            payload["finalized"] = True
            write_status(payload)
            return

        if len(completed) == last_completed:
            quiet_polls += 1
        else:
            quiet_polls = 0
            last_completed = len(completed)

        if active:
            time.sleep(POLL_SECONDS)
            continue

        next_job, next_cmd = pending[0]
        payload["action"] = f"resume:{next_job}"
        write_status(payload)
        rc = run_command(next_cmd)
        payload["resume_rc"] = rc
        write_status(payload)
        if rc != 0 and quiet_polls >= QUIET_POLLS_BEFORE_FAIL:
            raise SystemExit(f"Resume failed for {next_job} with rc={rc}")
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()

