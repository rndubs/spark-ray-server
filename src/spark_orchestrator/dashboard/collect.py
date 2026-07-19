"""Collectors + caches for the training dashboard.

One Collector holds every cache and a single background thread runs the
pollers at their configured cadences (Ray state 2s, host NVML/psutil 2s,
metrics.jsonl incremental tails 5s, /proc fd sampler 30s, sidecar scan 60s).
A single lock guards the shared snapshot; the FastAPI handlers only read it.

Design notes:
- We talk to Ray through its Jobs REST API on 127.0.0.1:8265 (same transport
  sparkctl uses) rather than importing ray — keeps the dashboard a thin,
  restartable observer with no ray runtime coupling.
- metrics.jsonl is tailed incrementally (seek to a stored offset, never
  re-read); rows are cached in memory per file.
- GPU-hours is the ledger currency: elapsed wall-clock x mean host GPU
  utilisation over the job's running window. On unified memory with no MIG a
  single GPU is shared, so this is HOST-attributed util — labelled as such.
  A completed job's final number is frozen into <run_dir>/gpu_hours.json so
  it survives dashboard AND ray restarts.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import threading
import time
import urllib.request
from pathlib import Path

from . import tiers

RAY_TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED"}
# ray status -> our status vocabulary
STATUS_MAP = {
    "PENDING": "PENDING", "RUNNING": "RUNNING",
    "SUCCEEDED": "SUCCEEDED", "FAILED": "FAILED", "STOPPED": "CANCELLED",
}


def _now() -> float:
    return time.time()


class Collector:
    def __init__(self, cfg: dict):
        d = cfg.get("dashboard", {})
        self.ray_port = int(d.get("ray_port", 8265))
        self.data_root = d.get("data_root", "/data/hexforge-data")
        self.runs_root = Path(
            cfg.get("paths", {}).get("runs_root", "~/spark-runs")
        ).expanduser()
        self.disk_watch = d.get("disk_watch", "/data")
        self.metrics_name = d.get("metrics_filename", "metrics.jsonl")
        self.stall_s = float(d.get("stall_threshold_s", 600))
        self.history_depth = int(d.get("history_depth", 50))
        self.intervals = {
            "ray": float(d.get("poll_ray_s", 2)),
            "host": float(d.get("poll_host_s", 2)),
            "metrics": float(d.get("poll_metrics_s", 5)),
            "fd": float(d.get("poll_fd_s", 30)),
            "sidecar": float(d.get("poll_sidecar_s", 60)),
        }

        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._last: dict[str, float] = {k: 0.0 for k in self.intervals}

        # shared snapshot pieces
        self.host: dict = {}
        self.ray_jobs: dict[str, dict] = {}      # run_id -> ray job dict
        self.sidecars: dict[str, dict] = {}       # run_id -> dashboard.json
        self.metrics: dict[str, dict] = {}        # run_id -> {path, rows, offset, inode, mtime}
        self.fd_reads: dict[str, dict] = {}       # run_id -> {observed:[{path,tier}], ts}
        self.gpu_hours: dict[str, dict] = {}      # run_id -> {seconds, util_sum, util_n, frozen}
        self.proc: dict[str, dict] = {}           # run_id -> {rss_mb, gpu_mem_mb}

    # ---------------------------------------------------------------- lifecycle
    def start(self) -> None:
        self._poll_sidecars()   # warm the durable view before serving
        self._poll_ray()
        self._poll_host()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def _loop(self) -> None:
        pollers = {
            "ray": self._poll_ray, "host": self._poll_host,
            "metrics": self._poll_metrics, "fd": self._poll_fd,
            "sidecar": self._poll_sidecars,
        }
        while not self._stop.is_set():
            now = _now()
            for key, fn in pollers.items():
                if now - self._last[key] >= self.intervals[key]:
                    try:
                        fn()
                    except Exception as e:  # a poller must never kill the loop
                        print(f"[dashboard] poller {key} failed: {e}", flush=True)
                    self._last[key] = now
            self._stop.wait(0.5)

    # ---------------------------------------------------------------- ray
    def _ray_get(self, path: str):
        with urllib.request.urlopen(
            f"http://127.0.0.1:{self.ray_port}{path}", timeout=5
        ) as r:
            return json.loads(r.read().decode())

    def _poll_ray(self) -> None:
        try:
            jobs = self._ray_get("/api/jobs/")
        except Exception:
            with self._lock:
                self.host.setdefault("ray", {})["up"] = False
            return
        by_id = {}
        for j in jobs:
            rid = j.get("submission_id") or j.get("job_id")
            if rid:
                by_id[rid] = j
        with self._lock:
            self.ray_jobs = by_id
            self.host.setdefault("ray", {})["up"] = True
            # Register a freshly-submitted job's sidecar without waiting for the
            # 60s full scan: read its dashboard.json directly the first time
            # Ray reports it. (The driver writes the sidecar at job start.)
            missing = [rid for rid in by_id if rid not in self.sidecars]
        for rid in missing:
            doc = self._read_sidecar(rid)
            if doc is not None:
                with self._lock:
                    self.sidecars[rid] = doc

    def _read_sidecar(self, rid: str) -> dict | None:
        sc = self.runs_root / rid / "dashboard.json"
        try:
            doc = json.loads(sc.read_text())
        except (OSError, json.JSONDecodeError):
            return None
        doc["_run_dir"] = str(sc.parent)
        return doc

    # ---------------------------------------------------------------- host
    def _nvidia_smi(self) -> dict | None:
        exe = shutil.which("nvidia-smi")
        if not exe:
            return None
        q = ("utilization.gpu,memory.used,memory.total,temperature.gpu,"
             "power.draw,power.limit")
        try:
            out = subprocess.run(
                [exe, f"--query-gpu={q}", "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5,
            ).stdout.strip().splitlines()
        except Exception:
            return None
        if not out:
            return None
        v = [x.strip() for x in out[0].split(",")]

        def num(i):
            try:
                return float(v[i])
            except (ValueError, IndexError):
                return None
        return {
            "util_pct": num(0), "mem_used_mb": num(1), "mem_total_mb": num(2),
            "temp_c": num(3), "power_w": num(4), "power_limit_w": num(5),
        }

    def _host_stats(self) -> dict:
        h: dict = {}
        try:
            h["loadavg"] = list(os.getloadavg())
        except OSError:
            h["loadavg"] = None
        try:
            import psutil  # optional
            vm = psutil.virtual_memory()
            h["mem_used_gb"] = round((vm.total - vm.available) / 1e9, 1)
            h["mem_total_gb"] = round(vm.total / 1e9, 1)
            h["mem_pct"] = vm.percent
            h["ncpu"] = psutil.cpu_count()
        except Exception:
            h.update(self._meminfo_fallback())
        try:
            du = shutil.disk_usage(self.disk_watch)
            h["disk_watch"] = self.disk_watch
            h["disk_free_gb"] = round(du.free / 1e9, 1)
            h["disk_total_gb"] = round(du.total / 1e9, 1)
        except OSError:
            pass
        return h

    def _meminfo_fallback(self) -> dict:
        try:
            info = {}
            for line in Path("/proc/meminfo").read_text().splitlines():
                k, _, rest = line.partition(":")
                info[k] = float(rest.split()[0]) * 1024  # kB -> B
            total, avail = info.get("MemTotal", 0), info.get("MemAvailable", 0)
            return {
                "mem_used_gb": round((total - avail) / 1e9, 1),
                "mem_total_gb": round(total / 1e9, 1),
                "mem_pct": round((total - avail) / total * 100, 1) if total else None,
            }
        except Exception:
            return {}

    def _rss_mb(self, pid) -> float | None:
        """Process RSS in MB (whole tree would be nicer, but the driver's
        child pid is what we track). /proc/<pid>/statm field 2 = resident
        pages."""
        try:
            fields = Path(f"/proc/{pid}/statm").read_text().split()
            pages = int(fields[1])
            return round(pages * (os.sysconf("SC_PAGE_SIZE") / 1e6), 1)
        except Exception:
            return None

    def _gpu_mem_by_pid(self) -> dict[int, float]:
        """pid -> GPU memory (MB) from nvidia-smi compute-apps. On GB10's
        unified memory this overlaps RSS; the UI labels both honestly."""
        exe = shutil.which("nvidia-smi")
        if not exe:
            return {}
        try:
            out = subprocess.run(
                [exe, "--query-compute-apps=pid,used_memory",
                 "--format=csv,noheader,nounits"],
                capture_output=True, text=True, timeout=5).stdout.strip()
        except Exception:
            return {}
        m = {}
        for line in out.splitlines():
            parts = [p.strip() for p in line.split(",")]
            try:
                m[int(parts[0])] = float(parts[1])
            except (ValueError, IndexError):
                continue
        return m

    def _poll_host(self) -> None:
        gpu = self._nvidia_smi()
        stats = self._host_stats()
        gpu_by_pid = self._gpu_mem_by_pid()
        with self._lock:
            for rid, j in self.ray_jobs.items():
                if STATUS_MAP.get(j.get("status")) != "RUNNING":
                    continue
                pid = self.sidecars.get(rid, {}).get("pid")
                if not pid:
                    continue
                self.proc[rid] = {"rss_mb": self._rss_mb(pid),
                                  "gpu_mem_mb": gpu_by_pid.get(int(pid))}
            self.host.update(stats)
            if gpu is not None:
                self.host["gpu"] = gpu
            self.host["ts"] = _now()
            self.host.setdefault("ray", {}).setdefault("up", None)
            self.host["ray"]["port"] = self.ray_port
            # attribute host GPU util to every currently-running job
            util = (gpu or {}).get("util_pct")
            dt = self.intervals["host"]
            for rid, j in self.ray_jobs.items():
                if STATUS_MAP.get(j.get("status")) == "RUNNING":
                    acc = self.gpu_hours.setdefault(
                        rid, {"seconds": 0.0, "util_sum": 0.0, "util_n": 0,
                              "frozen": False})
                    if acc["frozen"]:
                        continue
                    acc["seconds"] += dt
                    if util is not None:
                        acc["util_sum"] += util
                        acc["util_n"] += 1

    # ---------------------------------------------------------------- sidecars
    def _poll_sidecars(self) -> None:
        found = {}
        if self.runs_root.exists():
            for sc in self.runs_root.glob("*/dashboard.json"):
                try:
                    doc = json.loads(sc.read_text())
                    rid = doc.get("run_id") or sc.parent.name
                    doc["_run_dir"] = str(sc.parent)
                    found[rid] = doc
                except Exception:
                    continue
        with self._lock:
            self.sidecars = found
            # resume frozen gpu-hours persisted per run dir
            for rid, doc in found.items():
                gp = Path(doc["_run_dir"]) / "gpu_hours.json"
                if rid not in self.gpu_hours and gp.exists():
                    try:
                        self.gpu_hours[rid] = json.loads(gp.read_text())
                    except Exception:
                        pass

    # ---------------------------------------------------------------- metrics
    def _metrics_path_for(self, rid: str, sidecar: dict) -> str | None:
        """Explicit --metrics wins; else glob the newest metrics file under
        the job's artifacts dir (hexgen mints <variant>/<run_id>/ itself)."""
        mp = sidecar.get("metrics_path")
        if mp and os.path.exists(mp):
            return mp
        art = sidecar.get("artifacts_dir")
        if not art or not os.path.isdir(art):
            return None
        cands = list(Path(art).rglob(self.metrics_name))
        if not cands:
            return None
        return str(max(cands, key=lambda p: p.stat().st_mtime))

    def _poll_metrics(self) -> None:
        with self._lock:
            sidecars = dict(self.sidecars)
        for rid, sc in sidecars.items():
            path = self._metrics_path_for(rid, sc)
            if not path:
                continue
            try:
                st = os.stat(path)
            except OSError:
                continue
            with self._lock:
                cache = self.metrics.get(rid)
                if cache is None or cache.get("path") != path or \
                        cache.get("inode") != st.st_ino:
                    cache = {"path": path, "rows": [], "offset": 0,
                             "inode": st.st_ino, "mtime": st.st_mtime}
                    self.metrics[rid] = cache
                offset = cache["offset"]
            if st.st_size < offset:   # truncated/rotated -> re-read
                offset = 0
            new_rows = []
            try:
                with open(path, "rb") as f:
                    f.seek(offset)
                    data = f.read()
                    offset = f.tell()
            except OSError:
                continue
            text = data.decode("utf-8", "replace")
            # keep a trailing partial line for next poll
            keep = ""
            if text and not text.endswith("\n"):
                text, _, keep = text.rpartition("\n")
            for line in text.splitlines():
                line = line.strip()
                if line:
                    try:
                        new_rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            with self._lock:
                c = self.metrics.setdefault(rid, cache)
                c["rows"].extend(new_rows)
                c["offset"] = offset - len(keep.encode("utf-8"))
                c["mtime"] = st.st_mtime

    # ---------------------------------------------------------------- /proc fd
    def _poll_fd(self) -> None:
        if not os.path.isdir("/proc"):
            return
        with self._lock:
            running = {rid: j for rid, j in self.ray_jobs.items()
                       if STATUS_MAP.get(j.get("status")) == "RUNNING"}
            sidecars = dict(self.sidecars)
        for rid in running:
            sc = sidecars.get(rid, {})
            pid = sc.get("pid")
            observed = self._sample_fds(pid) if pid else []
            tagged = []
            seen = set()
            for real in observed:
                if real in seen:
                    continue
                seen.add(real)
                tier = tiers.tier_of(real, self.data_root)
                if tier == "outside-tree":
                    continue  # only surface reads inside the data tree
                declared = tiers.under_any(real, sc.get("declared_inputs") or [])
                tagged.append({"path": real, "tier": tier, "declared": declared,
                               "frozen": tiers.is_frozen(tier)})
            with self._lock:
                self.fd_reads[rid] = {"observed": tagged, "ts": _now()}

    def _sample_fds(self, pid) -> list[str]:
        fdir = f"/proc/{pid}/fd"
        out = []
        try:
            for name in os.listdir(fdir):
                try:
                    real = os.readlink(os.path.join(fdir, name))
                except OSError:
                    continue
                if real.startswith("/") and not real.startswith(("/proc", "/sys",
                        "/dev")):
                    out.append(real)
        except (OSError, ProcessLookupError):
            pass
        return out

    # ---------------------------------------------------------------- freeze
    def _freeze_gpu_hours(self, rid: str, run_dir: str | None) -> None:
        acc = self.gpu_hours.get(rid)
        if acc is None or acc.get("frozen"):
            return
        acc["frozen"] = True
        if run_dir:
            try:
                (Path(run_dir) / "gpu_hours.json").write_text(json.dumps(acc))
            except OSError:
                pass
