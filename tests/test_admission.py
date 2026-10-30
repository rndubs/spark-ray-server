"""Offline tests for the admission queue (spark_orchestrator.queue + .admit).

Runs entirely on a temp runs_root against a fake Ray Jobs API — no Spark, no
Ray, no network — so it can be run on the Mac while a campaign is in flight.

    python3 -m unittest discover -s tests -v

The regression these exist for: on 2026-07-25, 78 of 96 submitted jobs were
destroyed by Ray's 900 s job-supervisor start timeout while queued, leaving no
ledger row and no output. So the invariants under test are (a) we never hand
Ray more than the box can run, hence nothing queues inside Ray long enough to
time out, and (b) every submitted run_id has a ledger row from the moment of
submit and eventually reaches a terminal status, whatever goes wrong.
"""

from __future__ import annotations

import io
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from spark_orchestrator import admit, config, ledger, queue  # noqa: E402


class FakeRay:
    """Stand-in for the Ray Jobs REST API, with the bits admission cares
    about: a submission_id namespace, per-job status, and duplicate rejection."""

    def __init__(self):
        self.jobs: dict[str, dict] = {}
        self.reject: Exception | None = None

    def submit(self, *, entrypoint, submission_id, entrypoint_resources, metadata):
        if self.reject is not None:
            raise self.reject
        if submission_id in self.jobs:
            raise RuntimeError(f"POST /api/jobs/ -> 400: Job {submission_id} already exists")
        self.jobs[submission_id] = {
            "submission_id": submission_id, "status": "PENDING",
            "metadata": metadata, "entrypoint": entrypoint,
            "entrypoint_resources": entrypoint_resources, "message": "",
        }
        return {"submission_id": submission_id}

    def list(self):
        return list(self.jobs.values())

    def set(self, run_id, status, message=""):
        self.jobs[run_id]["status"] = status
        self.jobs[run_id]["message"] = message

    def forget(self, run_id):
        self.jobs.pop(run_id, None)


class Base(unittest.TestCase):
    SCHED = 112.0

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # The daemon narrates every admission; unittest reports on stderr, so
        # swallowing stdout keeps the test output readable.
        _stdout, sys.stdout = sys.stdout, io.StringIO()
        self.addCleanup(setattr, sys, "stdout", _stdout)
        self.cfg = {
            "total_mem_gb": 128.0, "os_reserve_gb": 16.0, "vllm_reserve_gb": 0.0,
            "schedulable_mem_gb": self.SCHED, "_path": "<test>",
            "paths": {"runs_root": self.tmp.name},
            "budgets": {"default": 8}, "gc": {"failed_tree_ttl_days": 7},
        }
        self.ray = FakeRay()
        self.adm = admit.Admitter(self.cfg, self.ray)

    # -- helpers ---------------------------------------------------------

    def enqueue(self, run_id, mem_gb=8.0, cpus=None):
        md = {"name": run_id, "sha": "a" * 40, "mem_gb": str(mem_gb)}
        entry = {
            "run_id": run_id, "name": run_id.split("-")[0], "sha": "a" * 40,
            "cmd": "true", "mem_gb": mem_gb,
            "entrypoint": f"python -m spark_orchestrator.driver {run_id}",
            "metadata": md,
        }
        if cpus is not None:  # None = an entry from a pre-CPU-slot client
            entry["cpus"] = cpus
            md["cpus"] = str(cpus)
        return queue.enqueue(self.cfg, entry)

    def rows(self, run_id=None):
        rs = ledger.read_rows(config.ledger_path(self.cfg))
        return [r for r in rs if run_id is None or r["run_id"] == run_id]

    def latest(self, run_id):
        rs = self.rows(run_id)
        return rs[-1] if rs else None

    def pending_ids(self):
        return [e["run_id"] for _, e in queue.pending(self.cfg)]

    def run_all(self, run_id):
        """Drive a job the way the real driver does: started row, then a
        terminal row, and Ray moving PENDING -> RUNNING -> SUCCEEDED."""
        self.ray.set(run_id, "RUNNING")
        ledger.append(config.ledger_path(self.cfg),
                      {"ts": ledger.utc_ts(), "run_id": run_id, "name": run_id,
                       "sha": "a" * 40, "cmd": "true", "mem_gb": 8.0,
                       "status": "started"})
        ledger.append(config.ledger_path(self.cfg),
                      {"ts": ledger.utc_ts(), "run_id": run_id, "name": run_id,
                       "sha": "a" * 40, "cmd": "true", "mem_gb": 8.0,
                       "status": "succeeded", "exit_code": 0, "duration_s": 1.0})
        self.ray.set(run_id, "SUCCEEDED")


class TestNoSilentLoss(Base):
    """The actual bug: a job that cannot start yet must never disappear."""

    def test_queued_job_has_a_ledger_row_before_ray_ever_sees_it(self):
        self.enqueue("job-1")
        self.assertEqual(self.latest("job-1")["status"], "queued")
        self.assertEqual(self.ray.list(), [], "must not reach Ray until admitted")

    def test_the_96_job_campaign_loses_nothing(self):
        """The 2026-07-25 scenario: 96 jobs at 8 GB against 112 GB of capacity.
        Previously 78 died in Ray's queue with no trace. Now every one of them
        is either running or visibly queued at all times, and all 96 finish."""
        ids = [f"bonanza-{i:04d}" for i in range(96)]
        for run_id in ids:
            self.enqueue(run_id)

        self.assertEqual(len(self.rows()), 96, "every submit is in the ledger")

        finished = []
        for _ in range(200):
            self.adm.tick()
            # INVARIANT: never hand Ray more than the box can run. This is what
            # keeps Ray-side queue time near zero and the start timeout inert.
            self.assertLessEqual(admit.reserved_gb(self.ray.list()), self.SCHED)
            self.assertLessEqual(
                sum(1 for j in self.ray.list() if j["status"] == "PENDING"), 14)

            for j in [j for j in self.ray.list() if j["status"] == "PENDING"]:
                self.run_all(j["submission_id"])
                finished.append(j["submission_id"])
            if len(finished) == len(ids):
                break

        self.adm.tick()
        self.assertEqual(sorted(finished), sorted(ids), "all 96 ran")
        for run_id in ids:
            self.assertEqual(self.latest(run_id)["status"], "succeeded")
        self.assertEqual(self.pending_ids(), [])

    def test_ray_dropping_an_admitted_job_produces_a_lost_row(self):
        """Whatever kills a job after admission — OOM, head restart, GCS
        amnesia — it gets a terminal ledger row instead of vanishing."""
        self.enqueue("job-1")
        self.adm.tick()
        self.ray.set("job-1", "FAILED", "Job supervisor actor failed to start "
                                        "within 900.0 seconds.")

        admit.RECONCILE_GRACE_S, grace = 0.0, admit.RECONCILE_GRACE_S
        self.addCleanup(setattr, admit, "RECONCILE_GRACE_S", grace)

        self.assertEqual(self.adm.tick()["lost"], ["job-1"])
        row = self.latest("job-1")
        self.assertEqual(row["status"], "lost")
        self.assertIn("900.0 seconds", row["reason"])
        self.assertEqual(row["ray_status"], "FAILED")

    def test_ray_forgetting_a_job_entirely_produces_a_lost_row(self):
        self.enqueue("job-1")
        self.adm.tick()
        self.ray.forget("job-1")
        admit.RECONCILE_GRACE_S, grace = 0.0, admit.RECONCILE_GRACE_S
        self.addCleanup(setattr, admit, "RECONCILE_GRACE_S", grace)

        self.assertEqual(self.adm.tick()["lost"], ["job-1"])
        self.assertEqual(self.latest("job-1")["status"], "lost")

    def test_grace_period_lets_the_driver_write_its_own_row_first(self):
        """A job that succeeds must not be mislabelled `lost` just because the
        reconciler saw Ray finish before the driver's row landed."""
        self.enqueue("job-1")
        self.adm.tick()
        self.ray.set("job-1", "SUCCEEDED")
        self.assertEqual(self.adm.tick()["lost"], [], "still inside the grace window")
        self.run_all("job-1")
        self.assertEqual(self.adm.tick()["lost"], [])
        self.assertEqual(self.latest("job-1")["status"], "succeeded")

    def test_finished_runs_stop_being_watched(self):
        self.enqueue("job-1")
        self.adm.tick()
        self.run_all("job-1")
        self.adm.tick()
        self.assertEqual(queue.admitted(self.cfg), [], "watch list is bounded")


class TestAdmissionControl(Base):
    def test_admits_only_what_fits(self):
        for i in range(20):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        self.assertEqual(len(self.ray.list()), 14)          # 14 * 8 = 112
        self.assertEqual(len(self.pending_ids()), 6)

    def test_admits_in_fifo_order(self):
        for i in range(20):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        self.assertEqual(sorted(j["submission_id"] for j in self.ray.list()),
                         [f"job-{i:02d}" for i in range(14)])

    def test_a_big_job_is_not_starved_by_later_small_ones(self):
        """Strict FIFO: the head of line blocks. A 100 GB job queued first must
        not sit behind an endless stream of 8 GB jobs queued after it."""
        self.enqueue("running", 96.0)
        self.adm.tick()
        self.enqueue("big", 100.0)
        self.enqueue("small", 8.0)
        self.adm.tick()
        self.assertEqual(self.pending_ids(), ["big", "small"],
                         "small must not jump the queue")

        self.run_all("running")
        self.adm.tick()
        self.assertIn("big", [j["submission_id"] for j in self.ray.list()])

    def test_capacity_freed_by_a_finished_job_is_reused(self):
        for i in range(16):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        self.assertEqual(len(self.pending_ids()), 2)
        self.run_all("job-00")
        self.run_all("job-01")
        self.adm.tick()
        self.assertEqual(self.pending_ids(), [])

    def test_pending_jobs_count_against_capacity(self):
        """A job Ray is still placing already owns its memory. Counting only
        RUNNING (as sparkctl status used to) reports room that is not there."""
        self.enqueue("job-1", 8.0)
        self.adm.tick()
        self.assertEqual([j["status"] for j in self.ray.list()], ["PENDING"])
        self.assertEqual(admit.reserved_gb(self.ray.list()), 8.0)

    def test_jobs_submitted_outside_the_queue_are_still_counted(self):
        """Anything already in Ray — including everything submitted before this
        daemon existed — reserves capacity, so deploying mid-campaign cannot
        over-commit the box."""
        self.ray.submit(entrypoint="x", submission_id="legacy",
                        entrypoint_resources={"mem_gb": 104.0},
                        metadata={"mem_gb": "104.0"})
        self.enqueue("new-1", 8.0)
        self.enqueue("new-2", 8.0)
        self.adm.tick()
        self.assertEqual(self.pending_ids(), ["new-2"])

    def test_a_job_larger_than_capacity_fails_instead_of_blocking_forever(self):
        self.enqueue("impossible", 999.0)
        self.enqueue("fine", 8.0)
        self.adm.tick()
        row = self.latest("impossible")
        self.assertEqual(row["status"], "failed")
        self.assertIn("can never run", row["reason"])
        self.assertIn("fine", [j["submission_id"] for j in self.ray.list()])

    def test_a_ray_rejection_fails_the_job_visibly(self):
        self.enqueue("job-1")
        self.ray.reject = RuntimeError("POST /api/jobs/ -> 500: boom")
        self.adm.tick()
        self.assertEqual(self.latest("job-1")["status"], "failed")
        self.assertEqual(self.pending_ids(), [])

    def test_nothing_is_admitted_when_ray_is_unreachable(self):
        self.enqueue("job-1")

        class Down(FakeRay):
            def list(self_):
                raise RuntimeError("connection refused")

        adm2 = admit.Admitter(self.cfg, Down())
        with self.assertRaises(RuntimeError):
            adm2.tick()
        self.assertEqual(self.pending_ids(), ["job-1"], "job survives the outage")
        self.assertIsNone(queue.heartbeat_age_s(self.cfg),
                          "a failed loop must not look healthy")


class TestCpuSlots(Base):
    """CPU-slot admission (2026-10-30). Memory-only admission let CPU-bound
    jobs pile onto the 20-core GB10 at load ~52 with 12 of 121 GB in use."""

    def setUp(self):
        super().setUp()
        self.cfg["schedulable_cpus"] = 20
        self.cfg["default_cpus"] = 6

    def ids_in_ray(self):
        return sorted(j["submission_id"] for j in self.ray.list())

    def test_cpus_limit_admission_when_memory_would_fit(self):
        # 10 x 8 GB = 80 GB fits in 112, but 10 x 4 cpus = 40 > 20.
        for i in range(10):
            self.enqueue(f"job-{i:02d}", 8.0, cpus=4)
        self.adm.tick()
        self.assertEqual(self.ids_in_ray(), [f"job-{i:02d}" for i in range(5)])
        self.assertEqual(admit.reserved_cpus(self.ray.list(), 6), 20)
        self.assertEqual(self.adm.tick()["reserved_cpus"], 20)
        self.assertEqual(len(self.pending_ids()), 5)

    def test_memory_still_limits_when_cpus_would_fit(self):
        for i in range(3):
            self.enqueue(f"job-{i}", 50.0, cpus=1)
        self.adm.tick()
        self.assertEqual(self.ids_in_ray(), ["job-0", "job-1"])

    def test_both_must_fit_and_fifo_still_blocks_on_cpus(self):
        """Head-of-line blocking applies to CPU too: a 16-cpu head that does
        not fit must not be overtaken by a 1-cpu job behind it."""
        self.enqueue("running", 8.0, cpus=10)
        self.adm.tick()
        self.enqueue("wide", 8.0, cpus=16)
        self.enqueue("narrow", 8.0, cpus=1)
        self.adm.tick()
        self.assertEqual(self.pending_ids(), ["wide", "narrow"])
        self.run_all("running")
        self.adm.tick()
        self.assertEqual(self.ids_in_ray(), ["narrow", "running", "wide"])

    def test_freed_cpus_are_reused(self):
        for i in range(6):
            self.enqueue(f"job-{i}", 1.0, cpus=5)
        self.adm.tick()
        self.assertEqual(len(self.pending_ids()), 2)
        self.run_all("job-0")
        self.adm.tick()
        self.assertEqual(self.pending_ids(), ["job-5"])

    def test_legacy_running_job_without_cpus_counts_as_default(self):
        """A job already in Ray from before this change has no `cpus` in its
        metadata; it must reserve default_cpus, not zero."""
        for i in range(3):
            self.ray.submit(entrypoint="x", submission_id=f"legacy-{i}",
                            entrypoint_resources={"mem_gb": 8.0},
                            metadata={"mem_gb": "8.0"})
        self.ray.set("legacy-0", "RUNNING")
        self.assertEqual(admit.reserved_cpus(self.ray.list(), 6), 18)
        self.enqueue("new-1", 1.0, cpus=2)
        self.enqueue("new-2", 1.0, cpus=1)
        self.adm.tick()
        self.assertEqual(self.pending_ids(), ["new-2"])  # 18 + 2 = 20, full

    def test_legacy_queue_entry_without_cpus_is_admitted_at_default(self):
        """An entry from an older client carries no cpus: it is admitted
        against default_cpus, and its Ray metadata says so, so the next loop
        reads the same reservation back."""
        for i in range(4):
            self.enqueue(f"old-{i}", 1.0)
        self.adm.tick()
        self.assertEqual(self.ids_in_ray(), ["old-0", "old-1", "old-2"])  # 3 x 6 = 18
        for j in self.ray.list():
            self.assertEqual(j["metadata"]["cpus"], "6")
        self.assertEqual(admit.reserved_cpus(self.ray.list(), 6), 18)

    def test_terminal_jobs_hold_no_cpus(self):
        self.enqueue("job-1", 1.0, cpus=20)
        self.adm.tick()
        self.run_all("job-1")
        self.assertEqual(admit.reserved_cpus(self.ray.list(), 6), 0)

    def test_impossible_cpus_fail_instead_of_blocking_forever(self):
        self.enqueue("impossible", 1.0, cpus=64)
        self.enqueue("fine", 1.0, cpus=2)
        self.adm.tick()
        row = self.latest("impossible")
        self.assertEqual(row["status"], "failed")
        self.assertIn("cpus=64", row["reason"])
        self.assertIn("can never run", row["reason"])
        self.assertEqual(self.ids_in_ray(), ["fine"])

    def test_queue_state_reports_cpus(self):
        self.enqueue("a", 1.0, cpus=3)
        self.enqueue("b", 1.0)
        st = queue.state(self.cfg)
        self.assertEqual(st["schedulable_cpus"], 20)
        self.assertEqual(st["default_cpus"], 6)
        self.assertEqual([p["cpus"] for p in st["pending"]], [3, 6])

    def test_queued_ledger_row_carries_cpus(self):
        self.enqueue("a", 1.0, cpus=3)
        self.assertEqual(self.latest("a")["cpus"], 3)


class TestCpuConfig(unittest.TestCase):
    BASE = ("total_mem_gb = 121\nos_reserve_gb = 9\nvllm_reserve_gb = 0\n")

    def test_explicit_values_are_read(self):
        cfg = config.load_capacity(self.BASE + "schedulable_cpus = 20\ndefault_cpus = 6\n")
        self.assertEqual((cfg["schedulable_cpus"], cfg["default_cpus"]), (20, 6))

    def test_client_parse_never_substitutes_its_own_core_count(self):
        """`text` = a config fetched over ssh by the Mac client; defaulting to
        the Mac's cores would check against the wrong machine."""
        cfg = config.load_capacity(self.BASE)
        self.assertIsNone(cfg["schedulable_cpus"])
        self.assertEqual(cfg["default_cpus"], config.DEFAULT_CPUS)

    def test_server_defaults_schedulable_cpus_to_host_cores(self):
        import os
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "capacity.toml"
            p.write_text(self.BASE)
            old = config.CAPACITY_PATHS
            config.CAPACITY_PATHS = [p]
            try:
                cfg = config.load_capacity()
            finally:
                config.CAPACITY_PATHS = old
        self.assertEqual(cfg["schedulable_cpus"], os.cpu_count())

    def test_cpus_of(self):
        cfg = {"default_cpus": 6}
        self.assertEqual(config.cpus_of(cfg, None), 6)
        self.assertEqual(config.cpus_of(cfg, ""), 6)
        self.assertEqual(config.cpus_of(cfg, "4"), 4)
        self.assertEqual(config.cpus_of(cfg, 2), 2)


class TestDriverEnv(unittest.TestCase):
    """The declaration and the job's actual parallelism must agree: job
    scripts read `${WORKERS:-8}`, so the driver sets WORKERS to the declared
    cpus unless the job set WORKERS itself."""

    CFG = {"default_cpus": 6}

    def spec(self, **kw):
        s = {"run_id": "r", "sha": "a" * 40, "artifacts_dir": "/art"}
        s.update(kw)
        return s

    def env(self, spec, base=None):
        from spark_orchestrator import driver
        return driver.job_env(spec, self.CFG, base or {"PATH": "/bin"})

    def test_declared_cpus_set_spark_cpus_and_workers(self):
        e = self.env(self.spec(cpus=4))
        self.assertEqual((e["SPARK_CPUS"], e["WORKERS"]), ("4", "4"))
        self.assertEqual(e["PATH"], "/bin")
        self.assertEqual(e["SPARK_RUN_ID"], "r")

    def test_spec_env_workers_wins(self):
        e = self.env(self.spec(cpus=4, env={"WORKERS": "2"}))
        self.assertEqual((e["SPARK_CPUS"], e["WORKERS"]), ("4", "2"))

    def test_no_declaration_uses_default(self):
        e = self.env(self.spec())
        self.assertEqual((e["SPARK_CPUS"], e["WORKERS"]), ("6", "6"))

    def test_inherited_workers_is_overridden(self):
        """Only the job's OWN env may override WORKERS; a stray WORKERS in the
        Ray worker's environment must not decouple it from the declaration."""
        e = self.env(self.spec(cpus=3), base={"WORKERS": "16"})
        self.assertEqual(e["WORKERS"], "3")


class TestCrashSafety(Base):
    def test_resubmitting_after_a_crash_adopts_the_existing_ray_job(self):
        """We POST to Ray before moving the queue entry, so a crash in between
        re-POSTs. Ray rejects the duplicate; that must count as success, not as
        a failure that kills a running job."""
        self.enqueue("job-1")
        path, entry = queue.pending(self.cfg)[0]
        self.ray.submit(entrypoint=entry["entrypoint"], submission_id="job-1",
                        entrypoint_resources={"mem_gb": 8.0},
                        metadata=entry["metadata"])  # the pre-crash POST

        self.adm.tick()
        self.assertEqual(self.pending_ids(), [])
        self.assertEqual([e["run_id"] for _, e in queue.admitted(self.cfg)], ["job-1"])
        self.assertNotEqual(self.latest("job-1")["status"], "failed")

    def test_the_queue_survives_a_daemon_restart(self):
        for i in range(20):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        fresh = admit.Admitter(self.cfg, self.ray)   # daemon restarted
        self.assertEqual(len(fresh.admit(self.ray.list())), 0, "box is full")
        for i in range(14):
            self.run_all(f"job-{i:02d}")
        fresh.tick()
        self.assertEqual(self.pending_ids(), [])

    def test_a_corrupt_queue_file_does_not_stall_the_queue(self):
        self.enqueue("job-1")
        (queue.pending_dir(self.cfg) / "00000000000000000000-junk.json").write_text("{")
        self.adm.tick()
        self.assertIn("job-1", [j["submission_id"] for j in self.ray.list()])


class TestCancel(Base):
    def test_cancelling_a_queued_job_removes_it_and_records_it(self):
        for i in range(20):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        self.assertTrue(queue.cancel_pending(self.cfg, "job-19"))
        self.assertNotIn("job-19", self.pending_ids())
        self.assertEqual(self.latest("job-19")["status"], "cancelled")

    def test_cancelling_an_already_admitted_job_defers_to_ray(self):
        self.enqueue("job-1")
        self.adm.tick()
        self.assertFalse(queue.cancel_pending(self.cfg, "job-1"),
                         "caller must fall through to `ray job stop`")

    def test_a_cancelled_job_is_never_admitted(self):
        for i in range(20):
            self.enqueue(f"job-{i:02d}")
        self.adm.tick()
        queue.cancel_pending(self.cfg, "job-15")
        for i in range(14):
            self.run_all(f"job-{i:02d}")
        self.adm.tick()
        self.assertNotIn("job-15", [j["submission_id"] for j in self.ray.list()])


class TestClientRendering(unittest.TestCase):
    """sparkctl's own output, with the Spark stubbed out. Guards the paths a
    client uses to answer "where did my job go?" — the question that had no
    answer on 2026-07-25."""

    CCFG = {"host": "spark-test", "local_port": 8265, "runs_root": "/runs",
            "orchestrator_root": "/orc", "default_repo": "/repo"}

    def setUp(self):
        from spark_orchestrator import cli
        self.cli = cli
        self.out = io.StringIO()
        _stdout, sys.stdout = sys.stdout, self.out
        self.addCleanup(setattr, sys, "stdout", _stdout)

        self.qs = {"schedulable_mem_gb": 112.0, "vllm_reserve_gb": 0.0,
                   "capacity_path": "<test>", "heartbeat_age_s": 2.0,
                   "admitter_ok": True, "pending": [], "admitted": []}
        self.ray_jobs: list[dict] = []
        self.ledger_lines = ""
        self.cancel_reply = "not-queued"
        self.stopped: list[str] = []

        outer = self

        class Jobs:
            def list(self_):
                return outer.ray_jobs

            def get(self_, run_id):
                for j in outer.ray_jobs:
                    if j["submission_id"] == run_id:
                        return j
                raise RuntimeError(f"GET /api/jobs/{run_id} -> 404: not found")

            def stop(self_, run_id):
                outer.stopped.append(run_id)
                return {"stopped": True}

        class Res:
            def __init__(self, stdout=""):
                self.stdout, self.stderr, self.returncode = stdout, "", 0

        self._patch(cli, "_jobs", lambda ccfg: Jobs())
        self._patch(cli, "_queue_state", lambda ccfg: self.qs)
        self._patch(cli, "_server",
                    lambda ccfg, argv, check=True: Res(self.cancel_reply))
        self._patch(cli.tunnel, "ssh_run",
                    lambda *a, **k: Res(self.ledger_lines))

    def _patch(self, obj, name, value):
        old = getattr(obj, name)
        setattr(obj, name, value)
        self.addCleanup(setattr, obj, name, old)

    def status(self, run_id=None):
        self.cli.cmd_status(type("A", (), {"run_id": run_id})(), self.CCFG)
        return self.out.getvalue()

    def test_status_shows_queued_jobs_and_counts_pending_against_capacity(self):
        self.ray_jobs = [
            {"submission_id": "r-1", "status": "RUNNING",
             "metadata": {"mem_gb": "96.0", "sha": "b" * 40}},
            {"submission_id": "r-2", "status": "PENDING",
             "metadata": {"mem_gb": "8.0", "sha": "b" * 40}},
        ]
        self.qs["pending"] = [{"run_id": "q-1", "name": "q", "mem_gb": 8.0,
                               "sha": "c" * 40, "queued_ts": ""}]
        out = self.status()
        self.assertIn("104/112 GB,", out)  # PENDING counted
        self.assertIn("(8 GB", out)
        self.assertIn("queued:   1 job(s) waiting for a slot (8 GB, 6 cpus)", out)
        self.assertIn("q-1", out)
        self.assertIn("QUEUED", out)

    def test_status_shows_cpu_reservation_beside_memory(self):
        self.qs.update(schedulable_cpus=20, default_cpus=6)
        self.ray_jobs = [
            {"submission_id": "r-1", "status": "RUNNING",
             "metadata": {"mem_gb": "96.0", "cpus": "12", "sha": "b" * 40}},
            {"submission_id": "legacy", "status": "RUNNING",
             "metadata": {"mem_gb": "8.0", "sha": "b" * 40}},
        ]
        self.qs["pending"] = [{"run_id": "q-1", "name": "q", "mem_gb": 8.0,
                               "cpus": 4, "sha": "c" * 40, "queued_ts": ""}]
        out = self.status()
        self.assertIn("capacity: 104/112 GB, 18/20 cpus reserved", out)
        self.assertIn("2 cpus free", out)
        self.assertIn("cpus=12", out)
        self.assertIn("cpus= 6*", out)   # legacy job, counted at the default
        self.assertIn("counted at default_cpus=6", out)
        self.assertIn("cpus= 4", out)    # queued job
        self.assertIn("(8 GB, 4 cpus)", out)

    def test_status_warns_loudly_when_the_admitter_is_down(self):
        self.qs.update(admitter_ok=False, heartbeat_age_s=None)
        self.assertIn("WARNING:  spark-admit is not running (never started)",
                      self.status())

    def test_status_of_a_queued_run_reports_its_queue_position(self):
        self.qs["pending"] = [
            {"run_id": f"q-{i}", "name": "q", "mem_gb": 8.0, "sha": "c" * 40,
             "queued_ts": ""} for i in range(3)
        ]
        self.ledger_lines = ('{"ts":"T","run_id":"q-2","status":"queued",'
                             '"mem_gb":8.0,"sha":"cccc"}\n')
        out = self.status("q-2")
        self.assertIn("ray:     QUEUED", out)  # acceptance.py parses this shape
        self.assertIn("held in the orchestrator queue", out)
        self.assertIn("queue:   position 3 of 3", out)
        self.assertIn("ledger:  T queued", out)

    def test_status_of_a_lost_run_shows_why(self):
        self.ledger_lines = (
            '{"ts":"T1","run_id":"x","status":"queued","mem_gb":8.0,"sha":"c"}\n'
            '{"ts":"T2","run_id":"x","status":"lost","mem_gb":8.0,"sha":"c",'
            '"reason":"Job supervisor actor failed to start"}\n')
        out = self.status("x")
        self.assertIn("ledger:  T2 lost", out)
        self.assertIn("Job supervisor actor failed to start", out)

    def test_list_renders_queued_and_lost_rows(self):
        self.ledger_lines = (
            '{"ts":"T1","run_id":"a","status":"queued","mem_gb":8.0,"sha":"'
            + "a" * 40 + '"}\n'
            '{"ts":"T2","run_id":"b","status":"lost","mem_gb":8.0,"sha":"'
            + "b" * 40 + '","reason":"gone"}\n')
        self.cli.cmd_list(type("A", (), {"n": 20})(), self.CCFG)
        out = self.out.getvalue()
        self.assertIn("queued", out)
        self.assertIn("lost", out)

    def test_cancel_of_a_queued_job_never_reaches_ray(self):
        self.cancel_reply = "cancelled"
        self.cli.cmd_cancel(type("A", (), {"run_id": "q-1"})(), self.CCFG)
        self.assertIn("cancelled while queued", self.out.getvalue())
        self.assertEqual(self.stopped, [])

    def test_cancel_of_an_admitted_job_falls_through_to_ray(self):
        self.cli.cmd_cancel(type("A", (), {"run_id": "r-1"})(), self.CCFG)
        self.assertEqual(self.stopped, ["r-1"])


class TestDashboard(Base):
    """The dashboard is the surface people actually watch. A fleet waiting in
    the orchestrator queue must show up there, not merely be absent."""

    def views(self):
        from spark_orchestrator.dashboard.collect import Collector
        from spark_orchestrator.dashboard.views import Views
        cfg = dict(self.cfg)
        cfg["dashboard"] = {"data_root": self.tmp.name}
        c = Collector(cfg)
        c._poll_queue()
        return c, Views(c)

    def test_queued_jobs_appear_with_a_QUEUED_status(self):
        self.enqueue("job-1", 8.0)
        self.enqueue("job-2", 8.0)
        _, v = self.views()
        rows = v.jobs()["jobs"]
        self.assertEqual([r["run_id"] for r in rows], ["job-1", "job-2"])
        self.assertEqual({r["status"] for r in rows}, {"QUEUED"})
        self.assertEqual(rows[0]["mem_gb"], 8.0)

    def test_admitted_jobs_leave_the_queued_view(self):
        self.enqueue("job-1")
        self.adm.tick()
        _, v = self.views()
        self.assertEqual(v.jobs()["jobs"], [])   # now Ray's to report

    def test_host_reports_the_backlog_and_a_dead_admitter(self):
        for i in range(3):
            self.enqueue(f"job-{i}", 8.0)
        c, _ = self.views()
        self.assertEqual(c.host["admit"]["pending"], 3)
        self.assertEqual(c.host["admit"]["pending_mem_gb"], 24.0)
        self.assertFalse(c.host["admit"]["up"])
        self.adm.tick()
        c2, _ = self.views()
        self.assertTrue(c2.host["admit"]["up"])


class TestDashboardSettledSearch(Base):
    """The 5 s metrics poll used to rglob every run's artifacts tree (~4,800
    runs on the Spark), holding ~0.8 of a core forever, and /api/jobs rglob'd
    config.json for every run per request (>100 s). Finished runs are now
    searched once; active runs every poll."""

    def setUp(self):
        super().setUp()
        import json as _json
        from spark_orchestrator.dashboard.collect import Collector
        from spark_orchestrator.dashboard.views import Views
        cfg = dict(self.cfg)
        cfg["dashboard"] = {"data_root": self.tmp.name}
        self.c = Collector(cfg)
        self.v = Views(self.c)
        self.runs = {"done-1": "SUCCEEDED", "done-2": "FAILED",
                     "live-1": "RUNNING", "wait-1": "PENDING"}
        for rid in list(self.runs) + ["old-sidecar-only"]:
            run_dir = Path(self.tmp.name) / rid
            art = run_dir / "artifacts" / "deep"
            art.mkdir(parents=True)
            (art / "metrics.jsonl").write_text('{"step": 1}\n')
            (art / "config.json").write_text(_json.dumps({"steps": 10}))
            (run_dir / "dashboard.json").write_text(_json.dumps({
                "run_id": rid, "artifacts_dir": str(run_dir / "artifacts"),
                "started_ts": "T"}))
        self.c._poll_sidecars()
        self.c.ray_jobs = {rid: {"submission_id": rid, "status": st,
                                 "metadata": {}}
                           for rid, st in self.runs.items()}
        self.searched = []
        orig = self.c.search_artifacts

        def counting(root, pattern):
            self.searched.append((Path(root).parent.name
                                  if Path(root).name == "artifacts"
                                  else Path(root).name, pattern))
            return orig(root, pattern)
        self.c.search_artifacts = counting

    def metric_searches(self):
        return sorted(r for r, p in self.searched if p == "metrics.jsonl")

    def test_finished_runs_searched_once_active_every_poll(self):
        self.c._poll_metrics()
        self.assertEqual(self.metric_searches(),
                         ["done-1", "done-2", "live-1", "old-sidecar-only", "wait-1"])
        self.searched.clear()
        self.c._poll_metrics()
        self.assertEqual(self.metric_searches(), ["live-1", "wait-1"])
        # finished runs' rows were still read on their one pass
        self.assertEqual(self.c.metrics["done-1"]["rows"], [{"step": 1}])

    def test_a_run_that_becomes_active_again_is_followed_again(self):
        self.c._poll_metrics()
        self.c.ray_jobs["done-1"]["status"] = "RUNNING"
        self.searched.clear()
        self.c._poll_metrics()
        self.assertIn("done-1", self.metric_searches())
        self.assertNotIn("done-1", self.c.metrics_settled)

    def test_a_run_finishing_gets_one_final_read_then_settles(self):
        self.c._poll_metrics()
        self.c.ray_jobs["live-1"]["status"] = "SUCCEEDED"
        self.searched.clear()
        self.c._poll_metrics()
        self.assertEqual(self.metric_searches(), ["live-1", "wait-1"])
        self.searched.clear()
        self.c._poll_metrics()
        self.assertEqual(self.metric_searches(), ["wait-1"])

    def test_api_jobs_searches_config_only_for_active_runs_after_first_call(self):
        self.v.jobs()
        first = sorted({r for r, p in self.searched if p == "config.json"})
        self.assertIn("done-1", first)
        self.searched.clear()
        rows = {r["run_id"]: r for r in self.v.jobs()["jobs"]}
        again = sorted({r for r, p in self.searched if p == "config.json"})
        self.assertEqual(again, ["live-1", "wait-1"])
        self.assertEqual(rows["done-1"]["progress"]["total"], 10)  # from cache


class TestState(Base):
    def test_state_reports_the_queue_and_a_dead_admitter(self):
        self.enqueue("job-1", 8.0)
        st = queue.state(self.cfg)
        self.assertEqual([p["run_id"] for p in st["pending"]], ["job-1"])
        self.assertFalse(st["admitter_ok"], "no heartbeat yet")
        self.adm.tick()
        self.assertTrue(queue.state(self.cfg)["admitter_ok"])

    def test_every_ledger_status_is_known_to_gc(self):
        from spark_orchestrator import gc
        self.assertEqual(queue.TERMINAL, gc.TERMINAL)


if __name__ == "__main__":
    unittest.main()
