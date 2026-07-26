"""Make a job's declared `mem_gb` a real limit instead of bookkeeping.

Why this exists
---------------
Admission reserves `mem_gb` per job, but until 2026-07-26 nothing stopped a
job from using more: `systemctl --user show spark-ray.service` reported
`MemoryMax=infinity`. On 2026-07-26 one CAD part inside one 8 GB job
allocated ~119 GB on a 121 GB box. The kernel's host-level OOM killer then
picked the biggest thing on the machine — the Ray head — and killed it. That
happened five times in a day, and each time it took down every *other*
running job with it (~30 shards lost across four attempts). One job's
mis-declaration became a host-wide outage.

The fix is a per-job cgroup limit. Each job command runs inside its own
transient systemd scope with `MemoryMax` set from its declaration, so the
kernel kills *that job's* processes when *that job* goes over, while the Ray
head, the other jobs and the host are untouched. The driver itself stays
outside the scope, so it survives the kill and writes the terminal row.

Three details that are load-bearing:

- **`MemorySwapMax=0` is mandatory.** The box has 16 GB of swap active.
  With only `MemoryMax` set, a runaway job spills into swap and thrashes the
  whole machine for minutes instead of dying; capping swap at zero for the
  job's cgroup makes the limit a clean kill.
- **A cgroup kill is SIGKILL**, which surfaces as `rc -9` / exit 137 — the
  same shape a timeout kill has, and some consumers already read `-9` as
  "timed out". So the ledger must not infer OOM from the exit code. Two
  independent signals decide it instead, and they cover different cases:
  systemd's post-mortem `Result=oom-kill` on the scope unit is authoritative
  for the kill that ended the job (race-free — the unit is deliberately left
  loaded so it can be read after the processes are gone); the cgroup's
  `memory.events` `oom_kill` counter, polled while the job runs, catches the
  *partial* kill the job survived (the kernel takes one child, the parent
  carries on and may even exit 0). Either one means the job hit its cap, and
  a job that quietly lost a worker to the cap is exactly what must not be
  reported as a clean success.
- **The limit equals the declaration** (factor 1.0 by default). Anything
  larger means the sum of what jobs may use exceeds what the scheduler
  admitted, which leaves the host-OOM path open — the failure being fixed.
  A job that needs more memory declares more; that is a one-line change by
  the submitter, versus a host-wide outage.

Everything here degrades safely: if `systemd-run` is missing or the user bus
is unreachable, the job runs unenforced with a loud warning and the ledger
row says `mem_enforced: false` rather than silently claiming protection.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import threading
import time
from pathlib import Path

GIB = 1024 ** 3

# Config defaults, overridable in capacity.toml [enforcement] and by env.
DEFAULTS = {
    "enabled": True,      # SPARK_MEM_ENFORCE=0 opts a single driver out
    "factor": 1.0,        # hard limit = declared mem_gb * factor
    "min_gb": 1.0,        # floor, so a tiny declaration is still runnable
}

POLL_S = 0.25
_UNIT_SAFE = re.compile(r"[^A-Za-z0-9_.-]")


def options(cfg: dict | None = None, env: dict | None = None) -> dict:
    """Enforcement options from capacity.toml [enforcement] + environment."""
    env = os.environ if env is None else env
    opts = dict(DEFAULTS)
    section = (cfg or {}).get("enforcement") or {}
    if "mem_enforce" in section:
        opts["enabled"] = bool(section["mem_enforce"])
    if "mem_limit_factor" in section:
        opts["factor"] = float(section["mem_limit_factor"])
    if "mem_limit_min_gb" in section:
        opts["min_gb"] = float(section["mem_limit_min_gb"])
    raw = env.get("SPARK_MEM_ENFORCE")
    if raw is not None and raw.strip() != "":
        opts["enabled"] = raw.strip().lower() not in ("0", "false", "no", "off")
    return opts


def limit_gb(mem_gb: float, opts: dict) -> float:
    return max(float(mem_gb) * float(opts["factor"]), float(opts["min_gb"]))


def unit_name(run_id: str) -> str:
    """Transient scope unit for a run. systemd unit names allow a limited
    alphabet; run_ids are already `[A-Za-z0-9_-]` but never trust that."""
    safe = _UNIT_SAFE.sub("-", run_id)[:180]
    return f"spark-job-{safe}.scope"


def bus_env(env: dict | None = None) -> dict:
    """Env additions needed to reach the *user* systemd manager.

    A Ray worker inherits the head's environment, which normally carries
    XDG_RUNTIME_DIR/DBUS_SESSION_BUS_ADDRESS because the head is a user unit.
    When it does not (a stray `ray start` from a raw shell, say), point at
    the well-known per-uid bus rather than failing enforcement outright.
    """
    env = os.environ if env is None else env
    out: dict[str, str] = {}
    uid = os.getuid()
    runtime = env.get("XDG_RUNTIME_DIR") or f"/run/user/{uid}"
    if not env.get("XDG_RUNTIME_DIR"):
        out["XDG_RUNTIME_DIR"] = runtime
    if not env.get("DBUS_SESSION_BUS_ADDRESS"):
        out["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={runtime}/bus"
    return out


def scope_argv(unit: str, limit_bytes: int, argv: list[str]) -> list[str]:
    """`argv`, wrapped so it runs inside its own memory-capped scope.

    No `--collect`: a scope that OOMs is left loaded in the `failed` state so
    `systemctl --user show <unit> -p Result` can be read *after* the process
    is gone. `Guard.finish()` resets it.
    """
    return [
        "systemd-run", "--user", "--scope", "--quiet", f"--unit={unit}",
        "-p", "MemoryAccounting=yes",
        "-p", f"MemoryMax={int(limit_bytes)}",
        "-p", "MemorySwapMax=0",
        "--", *argv,
    ]


def probe(env: dict | None = None) -> str | None:
    """None if per-job scopes work here, else why not (one line, for the log)."""
    if shutil.which("systemd-run") is None:
        return "systemd-run not on PATH"
    child = dict(os.environ if env is None else env)
    child.update(bus_env(child))
    argv = [
        "systemd-run", "--user", "--scope", "--quiet", "--collect",
        "-p", "MemoryAccounting=yes", "-p", f"MemoryMax={64 * 1024 * 1024}",
        "-p", "MemorySwapMax=0", "--", "true",
    ]
    try:
        res = subprocess.run(argv, env=child, capture_output=True, text=True,
                             timeout=30)
    except (OSError, subprocess.SubprocessError) as e:
        return f"probe failed to run: {e}"
    if res.returncode != 0:
        return (res.stderr.strip().splitlines() or ["exit "
                f"{res.returncode}"])[-1]
    return None


def _cgroup_of(pid: int, unit: str) -> Path | None:
    """The job's own cgroup dir, or None until it has been moved into it.

    Matching on the unit name matters: for the first moments after fork the
    process is still in the caller's cgroup, which also has a `memory.events`
    — latching onto that one reports zeroes forever.
    """
    try:
        line = Path(f"/proc/{pid}/cgroup").read_text().strip()
    except OSError:
        return None
    rel = line.rsplit("::", 1)[-1]
    if not rel.endswith("/" + unit):
        return None
    return Path("/sys/fs/cgroup") / rel.lstrip("/")


def _read_events(cg: Path) -> int:
    try:
        for line in (cg / "memory.events").read_text().splitlines():
            key, _, val = line.partition(" ")
            if key == "oom_kill":
                return int(val)
    except (OSError, ValueError):
        pass
    return 0


def _read_peak(cg: Path) -> int:
    try:
        return int((cg / "memory.peak").read_text().strip())
    except (OSError, ValueError):
        return 0


class Guard:
    """Per-job memory enforcement for one run.

    Usage:
        g = Guard(run_id, mem_gb, opts)
        argv = g.wrap(["bash", "-c", cmd])
        proc = subprocess.Popen(argv, env=g.env(base_env), ...)
        g.watch(proc)
        ...
        v = g.finish()          # after proc has exited
        if v["oom_killed"]: ...
    """

    def __init__(self, run_id: str, mem_gb: float, opts: dict,
                 reason: str | None = None):
        self.run_id = run_id
        self.mem_gb = float(mem_gb)
        self.opts = opts
        self.unit = unit_name(run_id)
        self.limit_gb = limit_gb(mem_gb, opts)
        self.limit_bytes = int(self.limit_gb * GIB)
        self.disabled_reason = reason if reason else (
            None if opts.get("enabled", True) else "disabled by config/env")
        self.active = self.disabled_reason is None
        self.oom_kill = 0
        self.peak_bytes = 0
        self.systemd_result: str | None = None
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()

    # ------------------------------------------------------------- launching

    def wrap(self, argv: list[str]) -> list[str]:
        if not self.active:
            return list(argv)
        return scope_argv(self.unit, self.limit_bytes, argv)

    def env(self, base: dict) -> dict:
        out = dict(base)
        if self.active:
            out.update(bus_env(out))
        return out

    # ------------------------------------------------------------- watching

    def watch(self, proc) -> None:
        """Poll the job's cgroup while it runs.

        Belt to systemd's braces: `memory.events` gives us a live oom_kill
        count and a peak-usage number worth putting in the ledger, and it
        keeps working if the scope is torn down before `finish()` looks at it.
        """
        if not self.active:
            return
        def loop():
            cg = None
            while not self._stop.is_set() and proc.poll() is None:
                if cg is None:
                    cg = _cgroup_of(proc.pid, self.unit)
                if cg is not None:
                    self.oom_kill = max(self.oom_kill, _read_events(cg))
                    self.peak_bytes = max(self.peak_bytes, _read_peak(cg))
                time.sleep(POLL_S)
            if cg is not None:  # one last look; the cgroup may still be there
                self.oom_kill = max(self.oom_kill, _read_events(cg))
                self.peak_bytes = max(self.peak_bytes, _read_peak(cg))
        self._thread = threading.Thread(target=loop, daemon=True)
        self._thread.start()

    # -------------------------------------------------------------- stopping

    def stop_scope(self) -> None:
        """Kill everything left in the job's cgroup (cancel/timeout paths).

        `killpg` covers the normal case; this covers a job that escaped its
        process group. Best-effort and non-blocking-ish by design: it runs on
        the cancellation path, where the driver has a few seconds to live.
        """
        if not self.active:
            return
        try:
            subprocess.run(["systemctl", "--user", "stop", self.unit],
                           env=self.env(dict(os.environ)),
                           capture_output=True, timeout=15)
        except (OSError, subprocess.SubprocessError):
            pass

    def finish(self) -> dict:
        """Post-mortem, after the job process has exited. Idempotent."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=5)
        if self.active:
            self.systemd_result = self._show_result()
            self._reset_failed()
        return self.verdict()

    def _show_result(self) -> str | None:
        try:
            res = subprocess.run(
                ["systemctl", "--user", "show", self.unit, "-p", "Result"],
                env=self.env(dict(os.environ)), capture_output=True,
                text=True, timeout=15)
        except (OSError, subprocess.SubprocessError):
            return None
        for line in res.stdout.splitlines():
            if line.startswith("Result="):
                return line.split("=", 1)[1].strip() or None
        return None

    def _reset_failed(self) -> None:
        try:
            subprocess.run(["systemctl", "--user", "reset-failed", self.unit],
                           env=self.env(dict(os.environ)),
                           capture_output=True, timeout=15)
        except (OSError, subprocess.SubprocessError):
            pass

    # --------------------------------------------------------------- verdict

    @property
    def oom_killed(self) -> bool:
        return self.active and (self.oom_kill > 0
                                or self.systemd_result == "oom-kill")

    def verdict(self) -> dict:
        """Ledger/sidecar fields describing enforcement for this run.

        `mem_peak_gb` is a SAMPLED floor, not an exact high-water mark: it is
        polled every POLL_S, and systemd keeps no MemoryPeak for scopes to
        cross-check against. A job that dies in under a poll interval can
        report a peak below its limit — or none at all — while still being a
        real OOM. The kill verdict never depends on it.
        """
        out = {
            "mem_enforced": self.active,
            "mem_limit_gb": round(self.limit_gb, 3) if self.active else None,
            "mem_peak_gb": (round(self.peak_bytes / GIB, 3)
                            if self.peak_bytes else None),
            "oom_kill_count": self.oom_kill if self.active else None,
            "oom_killed": self.oom_killed,
        }
        if self.disabled_reason:
            out["mem_enforce_skipped"] = self.disabled_reason
        return out

    def oom_reason(self) -> str:
        peak = (f"{self.peak_bytes / GIB:.1f} GB" if self.peak_bytes
                else "its limit")
        return (f"exceeded its declared memory: the job's cgroup hit "
                f"{peak} against a {self.limit_gb:g} GB limit (declared "
                f"mem_gb={self.mem_gb:g}) and the kernel killed it. The host "
                f"and other jobs were unaffected. Re-submit with a larger "
                f"--mem-gb, or reduce the job's footprint.")
