"""Per-job driver, executed on the Spark as the Ray job entrypoint:

    python -m spark_orchestrator.driver <base64 job-spec JSON>

Lifecycle (spec §2): worktree add at pinned SHA -> symlink assets -> ledger
start row -> exec cmd (logs to job.log) -> ledger end row -> GC worktree on
success. Ray schedules this whole process under the job's mem_gb resource,
so capacity is held for the full lifecycle and freed when it exits.

Cancellation: `ray job stop` sends SIGTERM with a short grace before
SIGKILL, so the handler writes the cancelled ledger row immediately, then
kills the job's process group and exits. Worktree cleanup after cancel is
left to `sparkctl gc`.
"""

from __future__ import annotations

import base64
import json
import os
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

from . import config, ledger, worktree

_state = {"proc": None, "ended": False, "spec": None, "t0": 0.0, "cfg": None,
          "sidecar": None, "run_dir": None}


def _write_sidecar() -> None:
    """Atomically (re)write the run dir's dashboard.json (the job-metadata
    contract the training dashboard joins with Ray state by run_id)."""
    doc, run_dir = _state["sidecar"], _state["run_dir"]
    if doc is None or run_dir is None:
        return
    try:
        tmp = run_dir / "dashboard.json.tmp"
        tmp.write_text(json.dumps(doc, indent=2, sort_keys=True))
        tmp.replace(run_dir / "dashboard.json")
    except Exception as e:
        print(f"[driver] sidecar write failed: {e}", file=sys.stderr)


def _finalize_sidecar(status: str, exit_code: int | None) -> None:
    doc = _state["sidecar"]
    if doc is None:
        return
    doc["final"] = {
        "status": status,
        "exit_code": exit_code,
        "duration_s": round(time.monotonic() - _state["t0"], 1),
        "ended_ts": ledger.utc_ts(),
    }
    _write_sidecar()


def _end_row(status: str, exit_code: int | None) -> dict:
    spec, cfg = _state["spec"], _state["cfg"]
    return {
        "ts": ledger.utc_ts(),
        "run_id": spec["run_id"],
        "name": spec["name"],
        "sha": spec["sha"],
        "cmd": spec["cmd"],
        "mem_gb": spec["mem_gb"],
        "status": status,
        "exit_code": exit_code,
        "duration_s": round(time.monotonic() - _state["t0"], 1),
        "artifacts_dir": spec["artifacts_dir"],
        "log_path": spec["log_path"],
    }


def _on_sigterm(signum, frame):
    if _state["ended"]:
        os._exit(143)
    _state["ended"] = True
    ledger.append(config.ledger_path(_state["cfg"]), _end_row("cancelled", None))
    _finalize_sidecar("cancelled", None)
    proc = _state["proc"]
    if proc is not None and proc.poll() is None:
        try:
            os.killpg(proc.pid, signal.SIGTERM)
            for _ in range(20):
                if proc.poll() is not None:
                    break
                time.sleep(0.1)
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    os._exit(143)


def _tee(pipe, log_file) -> None:
    for chunk in iter(lambda: pipe.read(8192), b""):
        log_file.write(chunk)
        log_file.flush()
        sys.stdout.buffer.write(chunk)
        sys.stdout.buffer.flush()


def main() -> int:
    spec = json.loads(base64.b64decode(sys.argv[1]))
    cfg = config.load_capacity()
    _state["spec"], _state["cfg"] = spec, cfg
    _state["t0"] = time.monotonic()

    run_dir = config.runs_root(cfg) / spec["run_id"]
    run_dir.mkdir(parents=True, exist_ok=True)
    log_path = run_dir / "job.log"
    spec["log_path"] = str(log_path)
    if not spec.get("artifacts_dir"):
        spec["artifacts_dir"] = str(run_dir / "artifacts")
    Path(spec["artifacts_dir"]).mkdir(parents=True, exist_ok=True)
    (run_dir / "spec.json").write_text(json.dumps(spec, indent=2, sort_keys=True))

    repo = Path(spec["repo_path"]).expanduser()
    tree = config.trees_root(cfg) / spec["run_id"]

    _state["run_dir"] = run_dir
    _state["sidecar"] = {
        "run_id": spec["run_id"],
        "name": spec["name"],
        "sha": spec["sha"],
        "cmd": spec["cmd"],
        "mem_gb": spec["mem_gb"],
        "artifacts_dir": spec["artifacts_dir"],
        "log_path": str(log_path),
        "worktree": str(tree),
        "repo_path": str(repo),
        "registered": True,
        **(spec.get("dashboard") or {}),
        "started_ts": ledger.utc_ts(),
        "pid": None,
        "final": None,
    }
    mp = _state["sidecar"].get("metrics_path")
    if mp and not os.path.isabs(mp):
        _state["sidecar"]["metrics_path"] = str(Path(spec["artifacts_dir"]) / mp)
    _write_sidecar()

    ledger.append(
        config.ledger_path(cfg),
        {
            "ts": ledger.utc_ts(),
            "run_id": spec["run_id"],
            "name": spec["name"],
            "sha": spec["sha"],
            "cmd": spec["cmd"],
            "mem_gb": spec["mem_gb"],
            "status": "started",
            "repo_path": str(repo),
            "artifacts_dir": spec["artifacts_dir"],
            "log_path": str(log_path),
        },
    )
    signal.signal(signal.SIGTERM, _on_sigterm)

    try:
        worktree.add(repo, spec["sha"], tree)
        linked = worktree.symlink_assets(
            repo, tree, config.symlink_assets_for(cfg, str(repo))
        )
        print(f"[driver] worktree {tree} @ {spec['sha'][:12]}, "
              f"linked {len(linked)} assets", file=sys.stderr)
    except Exception as e:
        print(f"[driver] setup failed: {e}", file=sys.stderr)
        _state["ended"] = True
        ledger.append(config.ledger_path(cfg), _end_row("failed", None))
        _finalize_sidecar("failed", None)
        return 1

    env = dict(os.environ)
    env.update(spec.get("env") or {})
    env["SPARK_RUN_ID"] = spec["run_id"]
    env["SPARK_SHA"] = spec["sha"]
    env["SPARK_ARTIFACTS_DIR"] = spec["artifacts_dir"]

    timeout_s = spec.get("timeout_s")
    status, exit_code = "succeeded", 0
    with open(log_path, "wb") as log_file:
        proc = subprocess.Popen(
            ["bash", "-c", spec["cmd"]],
            cwd=tree,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        _state["proc"] = proc
        _state["sidecar"]["pid"] = proc.pid
        _write_sidecar()
        reader = threading.Thread(target=_tee, args=(proc.stdout, log_file), daemon=True)
        reader.start()
        try:
            exit_code = proc.wait(timeout=timeout_s)
            status = "succeeded" if exit_code == 0 else "failed"
        except subprocess.TimeoutExpired:
            status, exit_code = "timeout", None
            try:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
            except ProcessLookupError:
                pass
        reader.join(timeout=5)

    if _state["ended"]:  # cancelled via SIGTERM; handler wrote the row
        return 143
    _state["ended"] = True
    ledger.append(config.ledger_path(cfg), _end_row(status, exit_code))
    _finalize_sidecar(status, exit_code)
    print(f"[driver] {status} exit_code={exit_code}", file=sys.stderr)

    keep = spec.get("keep_tree_on_failure", True)
    if status == "succeeded" or not keep:
        try:
            worktree.remove(repo, tree)
        except Exception as e:
            print(f"[driver] worktree remove failed: {e}", file=sys.stderr)
    return 0 if status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
