"""spark-admit: the orchestrator's own admission controller.

Run by the spark-admit.service systemd user unit, on the Spark, next to the
Ray head. Two jobs, once every POLL_S:

  admit()      hand queued jobs to Ray, in FIFO order, only while the box has
               room for them. Because a job is only submitted when its
               memory is already free, it goes PENDING -> RUNNING in seconds
               and Ray's 900 s job-supervisor start timeout can never fire.
               That timeout killing queued jobs — with no ledger row, no run
               dir and no log — is the bug this whole module exists to fix.

  reconcile()  make "silently lost" impossible for any *other* reason too.
               Every admitted job is watched until its ledger reaches a
               terminal status. If Ray finishes with a job and the driver
               never wrote a terminal row (killed before it started, OOM,
               head restart, GCS forgot the job), we write a `lost` row
               carrying Ray's own message. A run may fail; it may not vanish.

Admission is strict FIFO with head-of-line blocking: if the oldest queued job
does not fit, nothing behind it is admitted either, even if a smaller job
would fit right now. That trades a little utilisation for a guarantee that a
large job cannot be starved indefinitely by a stream of small ones, and it
makes queue position mean what a client expects it to mean.

The reservation we schedule against is read back from Ray each loop rather
than tracked in memory, so jobs submitted by anything other than this daemon
(including everything submitted before this daemon existed) are still counted
and we never over-commit the box.
"""

from __future__ import annotations

import argparse
import sys
import time

from . import config, ledger, queue
from .client import RayJobs

POLL_S = 3.0

# Ray states that still hold their entrypoint resources.
RAY_ACTIVE = {"PENDING", "RUNNING"}
# Ray is done with the job; the driver should have written a terminal row.
RAY_DONE = {"SUCCEEDED", "FAILED", "STOPPED"}

# How long after Ray reports a job finished (or forgets it entirely) we wait
# for the driver's terminal ledger row before declaring the run lost. The
# driver writes that row before exiting, so this only needs to cover write
# latency and our own poll period.
RECONCILE_GRACE_S = 30.0


def reserved_gb(jobs: list[dict]) -> float:
    """GB held by jobs Ray is currently running or trying to place.

    Counts PENDING as well as RUNNING: a PENDING job's resources are what it
    is waiting to acquire, and admitting against RUNNING alone would let us
    double-book the box during the seconds between submit and start. (The old
    `sparkctl status` capacity line counted RUNNING only, which is why the
    client-side pacing workaround could still over-admit.)
    """
    total = 0.0
    for j in jobs:
        if j.get("status") in RAY_ACTIVE:
            try:
                total += float((j.get("metadata") or {}).get("mem_gb", 0) or 0)
            except (TypeError, ValueError):
                pass
    return total


class Admitter:
    def __init__(self, cfg: dict, jobs: RayJobs):
        self.cfg = cfg
        self.jobs = jobs
        # run_id -> monotonic time we first saw Ray done-with/unaware-of it.
        # In memory only: losing it on restart re-arms the grace period, which
        # delays a `lost` row but can never drop one, because the admitted/
        # queue entry that drives reconciliation is on disk.
        self._done_since: dict[str, float] = {}

    # ------------------------------------------------------------ admission

    def admit(self, ray_jobs: list[dict]) -> list[str]:
        cfg = self.cfg
        sched = cfg["schedulable_mem_gb"]
        free = sched - reserved_gb(ray_jobs)
        admitted = []

        for path, entry in queue.pending(cfg):
            mem = float(entry["mem_gb"])

            if mem > sched:
                # Capacity shrank under a job that was accepted at submit
                # time. It can never run; failing it now keeps it from
                # blocking the whole queue forever behind an impossible head.
                self._fail_pending(
                    path, entry,
                    f"declares mem_gb={mem:g} but schedulable capacity is "
                    f"{sched:g} GB — this job can never run",
                )
                continue

            if mem > free:
                break  # strict FIFO: do not skip ahead of the head of line

            if self._submit_to_ray(path, entry):
                admitted.append(entry["run_id"])
                free -= mem

        return admitted

    def _submit_to_ray(self, path, entry) -> bool:
        """POST to Ray, then move the queue entry to admitted/.

        Deliberately in that order. A crash between the two re-submits on the
        next loop, and Ray rejects the duplicate submission_id — which we
        treat as success, since the job is running. The reverse order would
        lose a job on the same crash.
        """
        run_id = entry["run_id"]
        try:
            self.jobs.submit(
                entrypoint=entry["entrypoint"],
                submission_id=run_id,
                entrypoint_resources={"mem_gb": float(entry["mem_gb"])},
                metadata=entry["metadata"],
            )
        except Exception as e:
            if _is_duplicate(e):
                print(f"[admit] {run_id} already known to Ray; adopting", flush=True)
            else:
                self._fail_pending(path, entry, f"Ray rejected the submission: {e}")
                return False
        try:
            path.rename(queue.admitted_dir(self.cfg) / path.name)
        except OSError:
            queue.admitted_dir(self.cfg).mkdir(parents=True, exist_ok=True)
            path.rename(queue.admitted_dir(self.cfg) / path.name)
        print(f"[admit] {run_id} mem_gb={entry['mem_gb']:g}", flush=True)
        return True

    def _fail_pending(self, path, entry, reason: str) -> None:
        print(f"[admit] FAILED {entry['run_id']}: {reason}", file=sys.stderr, flush=True)
        ledger.append(
            config.ledger_path(self.cfg),
            queue.ledger_row(entry, "failed", exit_code=None, duration_s=0.0,
                              reason=reason),
        )
        try:
            path.unlink()
        except FileNotFoundError:
            pass

    # --------------------------------------------------------- reconciliation

    def reconcile(self, ray_jobs: list[dict]) -> list[str]:
        """Close out admitted jobs, and write a `lost` row for any run Ray has
        finished with that never got a terminal row of its own."""
        cfg = self.cfg
        by_id = {j.get("submission_id"): j for j in ray_jobs}
        latest = ledger.latest_by_run(config.ledger_path(cfg))
        now = time.monotonic()
        lost = []

        for path, entry in queue.admitted(cfg):
            run_id = entry["run_id"]
            row = latest.get(run_id)
            if row is not None and row["status"] in queue.TERMINAL:
                self._done_since.pop(run_id, None)
                path.unlink(missing_ok=True)  # run is closed; stop watching it
                continue

            j = by_id.get(run_id)
            ray_status = j.get("status") if j else None
            if ray_status in RAY_ACTIVE:
                self._done_since.pop(run_id, None)
                continue

            # Ray is finished with this job (or has forgotten it) but the
            # ledger still says queued/started. Give the driver a moment to
            # write its own row before we speak for it.
            first = self._done_since.setdefault(run_id, now)
            if now - first < RECONCILE_GRACE_S:
                continue

            if ray_status is None:
                reason = ("Ray has no record of this job; it was submitted but "
                          "never reached a terminal state")
            else:
                reason = (j.get("message")
                          or f"Ray reported {ray_status} before the job produced a "
                             f"terminal ledger row")
            ledger.append(
                config.ledger_path(cfg),
                queue.ledger_row(entry, "lost", exit_code=None, duration_s=None,
                                  ray_status=ray_status, reason=reason),
            )
            print(f"[admit] LOST {run_id}: {reason}", file=sys.stderr, flush=True)
            lost.append(run_id)
            self._done_since.pop(run_id, None)
            path.unlink(missing_ok=True)

        return lost

    # ------------------------------------------------------------- main loop

    def tick(self) -> dict:
        """One admit+reconcile pass. Returns a summary (used by the tests)."""
        ray_jobs = self.jobs.list()
        admitted = self.admit(ray_jobs)
        lost = self.reconcile(ray_jobs)
        queue.touch_heartbeat(self.cfg)
        return {"admitted": admitted, "lost": lost,
                "reserved_gb": reserved_gb(ray_jobs)}


def _is_duplicate(exc: Exception) -> bool:
    text = str(exc).lower()
    return "already exists" in text or "already registered" in text


def main() -> None:
    ap = argparse.ArgumentParser(prog="spark-admit")
    ap.add_argument("--ray-port", type=int, default=8265)
    ap.add_argument("--poll-s", type=float, default=POLL_S)
    ap.add_argument("--once", action="store_true", help="one pass, then exit")
    args = ap.parse_args()

    cfg = config.load_capacity()
    queue.pending_dir(cfg).mkdir(parents=True, exist_ok=True)
    queue.admitted_dir(cfg).mkdir(parents=True, exist_ok=True)
    adm = Admitter(cfg, RayJobs(args.ray_port))
    print(f"[admit] schedulable_mem_gb={cfg['schedulable_mem_gb']:g} "
          f"({cfg['_path']}), ray port {args.ray_port}", flush=True)

    while True:
        try:
            adm.tick()
        except Exception as e:
            # Ray unreachable, GCS restarting, disk blip. Admit nothing this
            # cycle and do not touch the heartbeat, so `sparkctl status` and
            # `doctor` report the queue as stalled rather than healthy-but-idle.
            print(f"[admit] loop error (nothing admitted): {e}",
                  file=sys.stderr, flush=True)
        if args.once:
            return
        time.sleep(args.poll_s)


if __name__ == "__main__":
    main()
