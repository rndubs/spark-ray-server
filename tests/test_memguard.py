"""Offline tests for per-job memory enforcement (spark_orchestrator.memguard).

The regression these exist for: on 2026-07-26 a job that declared 8 GB
allocated ~119 GB, the kernel's host-level OOM killer chose the Ray head, and
every other running job died with it — five times in one day. `mem_gb` was
admission bookkeeping that nothing enforced.

So the invariants under test are (a) the wrapped command really carries the
declared cap AND a zero swap cap (without which the box thrashes instead of
killing the job), and (b) an OOM kill is never inferred from the exit code —
it arrives as SIGKILL, exactly like a timeout kill, and consumers already
read -9 as "timed out". The live case at the bottom runs a real 256 MB-capped
scope on Linux and is skipped elsewhere.
"""

from __future__ import annotations

import shutil
import subprocess
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from spark_orchestrator import memguard  # noqa: E402

GIB = 1024 ** 3


class Options(unittest.TestCase):
    def test_defaults_are_on(self):
        opts = memguard.options({}, env={})
        self.assertTrue(opts["enabled"])
        self.assertEqual(opts["factor"], 1.0)

    def test_config_overrides(self):
        cfg = {"enforcement": {"mem_enforce": False, "mem_limit_factor": 1.5,
                               "mem_limit_min_gb": 2}}
        opts = memguard.options(cfg, env={})
        self.assertFalse(opts["enabled"])
        self.assertEqual(opts["factor"], 1.5)
        self.assertEqual(opts["min_gb"], 2.0)

    def test_env_opt_out_and_back_in(self):
        self.assertFalse(memguard.options({}, env={"SPARK_MEM_ENFORCE": "0"})["enabled"])
        self.assertFalse(memguard.options({}, env={"SPARK_MEM_ENFORCE": "false"})["enabled"])
        # env wins over a config that disabled it, in both directions
        cfg = {"enforcement": {"mem_enforce": False}}
        self.assertTrue(memguard.options(cfg, env={"SPARK_MEM_ENFORCE": "1"})["enabled"])
        # empty/unset is not an opt-out
        self.assertTrue(memguard.options({}, env={"SPARK_MEM_ENFORCE": ""})["enabled"])

    def test_limit_is_the_declaration_by_default(self):
        opts = memguard.options({}, env={})
        self.assertEqual(memguard.limit_gb(8, opts), 8.0)
        self.assertEqual(memguard.limit_gb(0.25, opts), 1.0)  # floor
        opts["factor"] = 1.25
        self.assertEqual(memguard.limit_gb(8, opts), 10.0)


class Wrapping(unittest.TestCase):
    def guard(self, **kw):
        opts = memguard.options({}, env={})
        opts.update(kw.pop("opts", {}))
        return memguard.Guard("gate9-oom-20260726-1200-ab12", kw.pop("mem_gb", 8), opts)

    def test_unit_name_is_derived_and_sanitised(self):
        self.assertEqual(memguard.unit_name("a/b c"), "spark-job-a-b-c.scope")
        self.assertTrue(memguard.unit_name("x" * 500).endswith(".scope"))

    def test_wrapped_argv_carries_cap_and_no_swap(self):
        g = self.guard(mem_gb=8)
        argv = g.wrap(["bash", "-c", "echo hi"])
        self.assertEqual(argv[0], "systemd-run")
        self.assertIn("--user", argv)
        self.assertIn("--scope", argv)
        self.assertIn(f"MemoryMax={8 * GIB}", argv)
        # Without this the 16 GB swap turns the cap into thrashing, not a kill.
        self.assertIn("MemorySwapMax=0", argv)
        self.assertIn(f"--unit={g.unit}", argv)
        self.assertEqual(argv[-3:], ["bash", "-c", "echo hi"])
        # --collect would delete the scope on exit, taking `Result=oom-kill`
        # with it before we can read the post-mortem.
        self.assertNotIn("--collect", argv)

    def test_disabled_guard_is_a_passthrough_and_says_so(self):
        g = self.guard(opts={"enabled": False})
        self.assertFalse(g.active)
        self.assertEqual(g.wrap(["bash", "-c", "x"]), ["bash", "-c", "x"])
        v = g.verdict()
        self.assertFalse(v["mem_enforced"])
        self.assertIsNone(v["mem_limit_gb"])
        self.assertIn("mem_enforce_skipped", v)

    def test_unavailable_reason_is_recorded_not_swallowed(self):
        opts = memguard.options({}, env={})
        g = memguard.Guard("r1", 8, opts, reason="systemd-run not on PATH")
        self.assertFalse(g.active)
        self.assertEqual(g.verdict()["mem_enforce_skipped"],
                         "systemd-run not on PATH")

    def test_bus_env_filled_in_when_missing(self):
        env = memguard.bus_env({})
        self.assertTrue(env["DBUS_SESSION_BUS_ADDRESS"].startswith("unix:path=/run/user/"))
        self.assertEqual(memguard.bus_env({"XDG_RUNTIME_DIR": "/run/user/1000",
                                           "DBUS_SESSION_BUS_ADDRESS": "unix:path=x"}), {})


class Verdict(unittest.TestCase):
    """OOM must be read from the cgroup/systemd, never guessed from rc."""

    def guard(self):
        return memguard.Guard("r1", 8, memguard.options({}, env={}))

    def test_clean_exit_is_not_an_oom(self):
        g = self.guard()
        self.assertFalse(g.oom_killed)
        self.assertFalse(g.verdict()["oom_killed"])

    def test_cgroup_counter_is_enough(self):
        """The partial kill: the kernel took a child, the parent carried on
        and may even have exited 0. That is not a clean success."""
        g = self.guard()
        g.oom_kill = 1
        g.systemd_result = "success"
        self.assertTrue(g.oom_killed)
        self.assertEqual(g.verdict()["oom_kill_count"], 1)

    def test_systemd_result_is_enough(self):
        """Covers the fast kill: a job can die between two cgroup polls."""
        g = self.guard()
        g.systemd_result = "oom-kill"
        self.assertTrue(g.oom_killed)

    def test_other_systemd_results_are_not_oom(self):
        g = self.guard()
        self.assertEqual(g.oom_kill, 0)
        for r in ("success", "signal", "exit-code", "timeout", ""):
            g.systemd_result = r
            self.assertFalse(g.oom_killed, r)

    def test_disabled_guard_never_claims_oom(self):
        opts = memguard.options({}, env={})
        opts["enabled"] = False
        g = memguard.Guard("r1", 8, opts)
        g.oom_kill = 3          # cannot happen, but must not be reported
        self.assertFalse(g.oom_killed)

    def test_peak_is_reported_in_gb(self):
        g = self.guard()
        g.peak_bytes = 2 * GIB
        self.assertEqual(g.verdict()["mem_peak_gb"], 2.0)

    def test_reason_names_the_number_the_submitter_must_change(self):
        g = self.guard()
        g.peak_bytes = 8 * GIB
        self.assertIn("mem_gb=8", g.oom_reason())
        self.assertIn("--mem-gb", g.oom_reason())


class Vocabulary(unittest.TestCase):
    def test_oom_killed_is_terminal_everywhere(self):
        from spark_orchestrator import gc, queue
        self.assertIn("oom_killed", queue.TERMINAL)
        self.assertIn("oom_killed", gc.TERMINAL)
        # distinct from both of the statuses it could be confused with
        self.assertIn("timeout", queue.TERMINAL)
        self.assertIn("lost", queue.TERMINAL)


def _scopes_work() -> bool:
    return (sys.platform == "linux" and shutil.which("systemd-run") is not None
            and memguard.probe() is None)


@unittest.skipUnless(_scopes_work(), "needs Linux + a usable user systemd bus")
class Live(unittest.TestCase):
    """A real, 256 MB-bounded OOM. Cannot affect the host."""

    def test_over_allocating_job_is_killed_and_named(self):
        opts = memguard.options({}, env={})
        opts["min_gb"] = 0.25
        g = memguard.Guard("selftest-oom", 0.25, opts)
        self.assertTrue(g.active)
        cmd = "python3 -c 'a=bytearray(1024**3)'"
        proc = subprocess.Popen(g.wrap(["bash", "-c", cmd]),
                                env=g.env(dict(__import__("os").environ)),
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True)
        g.watch(proc)
        rc = proc.wait(timeout=120)
        v = g.finish()
        self.assertNotEqual(rc, 0)
        self.assertTrue(v["oom_killed"], v)
        # The peak is a sample and may be missed entirely on a fast kill; what
        # must hold is that it never exceeds the cap.
        self.assertLessEqual(v["mem_peak_gb"] or 0, 0.30, v)

    def test_job_under_its_declaration_is_untouched(self):
        opts = memguard.options({}, env={})
        opts["min_gb"] = 0.25
        g = memguard.Guard("selftest-ok", 0.25, opts)
        cmd = "python3 -c 'a=bytearray(64*1024*1024); print(len(a))'"
        proc = subprocess.Popen(g.wrap(["bash", "-c", cmd]),
                                env=g.env(dict(__import__("os").environ)),
                                stdout=subprocess.DEVNULL,
                                stderr=subprocess.DEVNULL,
                                start_new_session=True)
        g.watch(proc)
        rc = proc.wait(timeout=120)
        v = g.finish()
        self.assertEqual(rc, 0)
        self.assertFalse(v["oom_killed"], v)


if __name__ == "__main__":
    unittest.main()
