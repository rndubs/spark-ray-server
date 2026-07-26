"""Server-side durable admission queue.

Why this exists
---------------
Before this module, `sparkctl submit` POSTed straight to the Ray Jobs API and
let Ray queue the job. Ray queues a job by trying to place its `JobSupervisor`
actor; while the actor cannot be placed the job sits PENDING, and Ray's job
manager fails any job that stays PENDING longer than
`RAY_JOB_START_TIMEOUT_SECONDS` (default 900 s, ray 2.56.1
`ray/dashboard/consts.py`). That failure happens *before* our driver ever runs,
so there is no ledger row, no run dir and no log: the job does not look failed,
it looks like it was never submitted. On 2026-07-25 that silently destroyed 78
of 96 submitted shards.

The fix is to stop queueing inside Ray. Submissions land here as durable files;
`spark_orchestrator.admit` hands one to Ray only when the box actually has room,
so a job is PENDING in Ray for seconds, never minutes, and the start timeout can
never fire. The queue is on disk (not in the daemon's memory) so a daemon
restart, a reboot, or a crashed client loses nothing.

The other half of the fix is the ledger: a `queued` row is written at enqueue
time, so from the instant a client's submit returns, the job is visible to
`sparkctl list` / `sparkctl status`. Combined with the reconciler in
`admit.py`, every submitted run_id is guaranteed to have a ledger row, and
every ledger row is guaranteed to reach a terminal status.

Layout under <runs_root>/queue:

    pending/<seq>-<run_id>.json    waiting for a slot; FIFO by <seq>
    admitted/<seq>-<run_id>.json   handed to Ray; kept until the ledger says
                                   the run reached a terminal status, so the
                                   reconciler knows what it is responsible for
    admit.heartbeat               last successful admit-daemon loop (unix ts)

<seq> is the server-side nanosecond enqueue clock, zero-padded to 20 digits so
lexicographic order is FIFO order.
"""

from __future__ import annotations

import argparse
import base64
import json
import sys
import time
from pathlib import Path

from . import config, ledger

# Ledger statuses that mean "this run is over"; nothing further will be
# written for it. Kept in sync with gc.TERMINAL.
TERMINAL = {"succeeded", "failed", "cancelled", "timeout", "lost"}

# A daemon loop is ~POLL_S; anything older than this means the admitter is
# not running and queued work is not moving.
HEARTBEAT_STALE_S = 60.0


def queue_root(cfg: dict) -> Path:
    return config.runs_root(cfg) / "queue"


def pending_dir(cfg: dict) -> Path:
    return queue_root(cfg) / "pending"


def admitted_dir(cfg: dict) -> Path:
    return queue_root(cfg) / "admitted"


def heartbeat_path(cfg: dict) -> Path:
    return queue_root(cfg) / "admit.heartbeat"


def _read_dir(d: Path) -> list[tuple[Path, dict]]:
    """Queue entries in FIFO order. Unreadable/partial files are skipped
    rather than raised: one bad file must not stall the whole queue."""
    if not d.exists():
        return []
    out = []
    for p in sorted(d.iterdir()):
        if p.suffix != ".json":
            continue
        try:
            out.append((p, json.loads(p.read_text())))
        except (OSError, ValueError):
            continue
    return out


def pending(cfg: dict) -> list[tuple[Path, dict]]:
    return _read_dir(pending_dir(cfg))


def admitted(cfg: dict) -> list[tuple[Path, dict]]:
    return _read_dir(admitted_dir(cfg))


def _write_atomic(path: Path, entry: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(entry, indent=2, sort_keys=True))
    tmp.replace(path)


def ledger_row(entry: dict, status: str, **extra) -> dict:
    row = {
        "ts": ledger.utc_ts(),
        "run_id": entry["run_id"],
        "name": entry["name"],
        "sha": entry["sha"],
        "cmd": entry["cmd"],
        "mem_gb": entry["mem_gb"],
        "status": status,
    }
    row.update(extra)
    return row


# ---------------------------------------------------------------- enqueue

def enqueue(cfg: dict, entry: dict) -> Path:
    """Persist a submission and write its `queued` ledger row.

    Ledger first, then the queue file. If we crash between the two the job is
    visible-but-stuck (a `queued` row that never progresses) rather than
    invisible — the failure mode we are trying to eliminate. `sparkctl status`
    and the reconciler both surface that state.
    """
    entry = dict(entry)
    entry["seq"] = f"{time.time_ns():020d}"
    entry["queued_ts"] = ledger.utc_ts()
    ledger.append(config.ledger_path(cfg), ledger_row(entry, "queued"))
    path = pending_dir(cfg) / f"{entry['seq']}-{entry['run_id']}.json"
    _write_atomic(path, entry)
    return path


def cancel_pending(cfg: dict, run_id: str) -> bool:
    """Drop a still-queued job. Returns False if it is not (or no longer) in
    the pending queue, in which case the caller should fall back to Ray."""
    for path, entry in pending(cfg):
        if entry["run_id"] == run_id:
            try:
                path.unlink()
            except FileNotFoundError:
                return False  # admitted out from under us; let Ray handle it
            ledger.append(config.ledger_path(cfg),
                          ledger_row(entry, "cancelled", exit_code=None,
                                      duration_s=0.0, reason="cancelled while queued"))
            return True
    return False


def heartbeat_age_s(cfg: dict) -> float | None:
    """Seconds since the admit daemon last completed a loop, or None if it has
    never run."""
    try:
        return max(0.0, time.time() - heartbeat_path(cfg).stat().st_mtime)
    except OSError:
        return None


def touch_heartbeat(cfg: dict) -> None:
    p = heartbeat_path(cfg)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(f"{time.time():.0f}\n")


def state(cfg: dict) -> dict:
    """Everything `sparkctl status` needs about the queue, in one blob so the
    client makes one ssh round trip instead of three."""
    age = heartbeat_age_s(cfg)
    return {
        "schedulable_mem_gb": cfg["schedulable_mem_gb"],
        "total_mem_gb": cfg["total_mem_gb"],
        "os_reserve_gb": cfg["os_reserve_gb"],
        "vllm_reserve_gb": cfg["vllm_reserve_gb"],
        "capacity_path": cfg["_path"],
        "heartbeat_age_s": age,
        "admitter_ok": age is not None and age <= HEARTBEAT_STALE_S,
        "pending": [
            {"run_id": e["run_id"], "name": e["name"], "mem_gb": e["mem_gb"],
             "sha": e["sha"], "queued_ts": e.get("queued_ts", "")}
            for _, e in pending(cfg)
        ],
        "admitted": [e["run_id"] for _, e in admitted(cfg)],
    }


# ---------------------------------------------------------------- cli

def main() -> None:
    ap = argparse.ArgumentParser(prog="python -m spark_orchestrator.queue")
    sub = ap.add_subparsers(dest="command", required=True)
    s = sub.add_parser("enqueue", help="enqueue a base64 JSON submission")
    s.add_argument("b64")
    sub.add_parser("state", help="print queue + capacity state as JSON")
    s = sub.add_parser("cancel", help="drop a still-queued job")
    s.add_argument("run_id")
    args = ap.parse_args()

    cfg = config.load_capacity()
    if args.command == "enqueue":
        entry = json.loads(base64.b64decode(args.b64))
        enqueue(cfg, entry)
        age = heartbeat_age_s(cfg)
        if age is None or age > HEARTBEAT_STALE_S:
            # Not fatal: the job is on disk with a `queued` ledger row and will
            # run when the admitter comes back. But it will not start now, and
            # the client must not be left thinking it will.
            print(
                f"[queue] WARNING: spark-admit heartbeat is "
                f"{'missing' if age is None else f'{age:.0f}s stale'} — this job "
                f"is queued but nothing is admitting. Check: "
                f"systemctl --user status spark-admit.service",
                file=sys.stderr,
            )
    elif args.command == "state":
        print(json.dumps(state(cfg)))
    elif args.command == "cancel":
        print("cancelled" if cancel_pending(cfg, args.run_id) else "not-queued")


if __name__ == "__main__":
    main()
