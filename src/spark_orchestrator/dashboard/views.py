"""Join the collector's caches into the API payloads.

One job row is the join, by run_id, of: Ray state (authoritative status),
the dashboard.json sidecar (provenance + declared inputs), the metrics tail
(progress + curves), the /proc fd sample (observed reads), and the GPU-hours
accumulator. Ray-only jobs (submitted outside sparkctl) render as
`registered: false` with whatever Ray knows — visible, never invisible.
"""

from __future__ import annotations

import json
import math
import os
import time
from pathlib import Path

from . import tiers
from .collect import Collector, STATUS_MAP

# hexgen adapter defaults (per-project; overridable via [dashboard] config)
DEFAULT_LOSS_KEYS = ["train_total", "holdout_total"]


def _num(x):
    return isinstance(x, (int, float)) and not isinstance(x, bool)


def _last_loss(rows: list[dict], keys: list[str]) -> tuple[float | None, bool]:
    """Last numeric value among `keys`, and whether it is NaN/inf."""
    for row in reversed(rows):
        for k in keys:
            v = row.get(k)
            if _num(v):
                return float(v), (math.isnan(v) or math.isinf(v))
    return None, False


def _total_steps(run_dir: str | None, artifacts_dir: str | None) -> int | None:
    """hexgen writes config.json with the planned step count; best-effort."""
    for base in (artifacts_dir, run_dir):
        if not base or not os.path.isdir(base):
            continue
        for cfg in Path(base).rglob("config.json"):
            try:
                doc = json.loads(cfg.read_text())
                if _num(doc.get("steps")):
                    return int(doc["steps"])
            except Exception:
                continue
    return None


class Views:
    def __init__(self, c: Collector):
        self.c = c
        self.loss_keys = c.__dict__.get("loss_keys", DEFAULT_LOSS_KEYS)

    # ------------------------------------------------------------------ join
    def _gpu_hours(self, rid: str) -> dict:
        acc = self.c.gpu_hours.get(rid)
        if not acc or not acc.get("util_n"):
            return {"hours": 0.0, "mean_util_pct": None, "frozen": False}
        mean_util = acc["util_sum"] / acc["util_n"]
        hours = acc["seconds"] / 3600.0 * mean_util / 100.0
        return {"hours": round(hours, 4), "mean_util_pct": round(mean_util, 1),
                "frozen": acc.get("frozen", False)}

    def _health(self, status: str, sidecar: dict, metrics: dict,
                nan: bool, pid_alive: bool | None) -> dict:
        badges = []
        eff = status
        if status == "RUNNING":
            last_activity = None
            if metrics:
                last_activity = metrics.get("mtime")
            log_path = sidecar.get("log_path")
            if log_path and os.path.exists(log_path):
                try:
                    lm = os.stat(log_path).st_mtime
                    last_activity = max(last_activity or 0, lm)
                except OSError:
                    pass
            if last_activity and (time.time() - last_activity) > self.c.stall_s:
                eff = "STALLED"
                badges.append({"kind": "stalled", "level": "amber",
                               "text": f"no output in >{int(self.c.stall_s/60)}m"})
            if pid_alive is False:
                badges.append({"kind": "state-mismatch", "level": "amber",
                               "text": "process gone but Ray RUNNING"})
        if nan:
            badges.append({"kind": "nan", "level": "red", "text": "NaN/inf loss"})
        return {"effective_status": eff, "badges": badges}

    def _reads(self, rid: str, sidecar: dict, running: bool) -> dict:
        declared = []
        for p in (sidecar.get("declared_inputs") or []):
            tier = tiers.tier_of(p, self.c.data_root)
            declared.append({"path": p, "tier": tier,
                             "frozen": tiers.is_frozen(tier)})
        observed = self.c.fd_reads.get(rid, {}).get("observed", [])
        contaminated = any(d["frozen"] for d in declared) or \
            any(o["frozen"] for o in observed)
        undeclared = [o for o in observed if not o["declared"]]
        return {
            "declared": declared,
            "observed": observed,
            "observed_ts": self.c.fd_reads.get(rid, {}).get("ts"),
            "contaminated": contaminated and running,
            "contaminated_ever": contaminated,
            "undeclared": [o["path"] for o in undeclared],
        }

    def _pid_alive(self, sidecar: dict) -> bool | None:
        pid = sidecar.get("pid")
        if not pid or not os.path.isdir("/proc"):
            return None
        return os.path.isdir(f"/proc/{pid}")

    def job_row(self, rid: str, detail: bool = False) -> dict:
        with self.c._lock:
            ray = dict(self.c.ray_jobs.get(rid, {}))
            sidecar = dict(self.c.sidecars.get(rid, {}))
            metrics = self.c.metrics.get(rid, {})
            rows = list(metrics.get("rows", [])) if metrics else []

        registered = bool(sidecar)
        ray_status = ray.get("status")
        # status: Ray is authoritative when the job is known to it; else the
        # sidecar's frozen final; else unknown.
        if ray_status:
            status = STATUS_MAP.get(ray_status, ray_status)
        elif sidecar.get("final"):
            status = sidecar["final"]["status"].upper().replace("TIMEOUT", "FAILED")
        else:
            status = "UNKNOWN"
        running = status in ("RUNNING", "PENDING")

        last_loss, nan = _last_loss(rows, self.loss_keys)
        pid_alive = self._pid_alive(sidecar) if running else None
        health = self._health(status, sidecar, metrics, nan, pid_alive)

        run_dir = sidecar.get("_run_dir")
        total = _total_steps(run_dir, sidecar.get("artifacts_dir"))
        cur_step = None
        for row in reversed(rows):
            if _num(row.get("step")):
                cur_step = int(row["step"])
                break

        # elapsed + rough steps/sec + ETA. Rate/ETA only make sense while
        # RUNNING — a finished job has no ETA. And metrics `step` is 0-indexed
        # (last row of an N-step run is step N-1), so display step+1 as the
        # count of completed steps; a SUCCEEDED job with a known total reads
        # as total/total (it ran them all).
        started = sidecar.get("started_ts")
        elapsed_s = self._elapsed_s(ray, sidecar)
        disp_step = None
        if cur_step is not None:
            disp_step = total if (status == "SUCCEEDED" and total) else cur_step + 1
        rate = eta_s = None
        if running and cur_step and elapsed_s:
            rate = cur_step / elapsed_s
            if total and total > cur_step:
                eta_s = (total - cur_step) / rate

        row = {
            "run_id": rid,
            "registered": registered,
            "status": status,
            "effective_status": health["effective_status"],
            "badges": health["badges"],
            "name": sidecar.get("name") or (ray.get("metadata") or {}).get("name"),
            "desc": sidecar.get("desc") or (ray.get("metadata") or {}).get("desc"),
            "sha": sidecar.get("sha") or (ray.get("metadata") or {}).get("sha"),
            "branch": sidecar.get("branch") or (ray.get("metadata") or {}).get("branch"),
            "dirty": sidecar.get("dirty",
                     (ray.get("metadata") or {}).get("dirty") == "1"),
            "variant": sidecar.get("variant") or (ray.get("metadata") or {}).get("variant"),
            "seeds": sidecar.get("seeds"),
            "mem_gb": sidecar.get("mem_gb") or (ray.get("metadata") or {}).get("mem_gb"),
            "progress": {"step": disp_step, "total": total,
                         "steps_per_s": round(rate, 3) if rate else None,
                         "eta_s": round(eta_s) if eta_s else None},
            "last_loss": last_loss,
            "resources": (self.c.proc.get(rid, {}) if running else {}),
            "elapsed_s": round(elapsed_s) if elapsed_s else None,
            "gpu_hours": self._gpu_hours(rid),
            "reads": self._reads(rid, sidecar, running),
            "started_ts": started,
            "ended_ts": (sidecar.get("final") or {}).get("ended_ts"),
        }
        if detail:
            row["sidecar"] = sidecar
            row["cmd"] = sidecar.get("cmd") or ray.get("entrypoint")
            row["log_path"] = sidecar.get("log_path")
            row["artifacts_dir"] = sidecar.get("artifacts_dir")
            row["run_dir"] = run_dir
            row["ray_message"] = ray.get("message")
            row["checkpoint"] = self._checkpoint(sidecar)
            row["config"] = self._variant_config(sidecar)
            row["metrics_path"] = metrics.get("path")
        return row

    def _elapsed_s(self, ray: dict, sidecar: dict) -> float | None:
        # prefer Ray's start/end (ms epoch); fall back to sidecar timestamps
        st = ray.get("start_time")
        et = ray.get("end_time")
        if st:
            end = (et / 1000.0) if et else time.time()
            return max(0.0, end - st / 1000.0)
        fin = sidecar.get("final")
        if fin and _num(fin.get("duration_s")):
            return float(fin["duration_s"])
        return None

    def _checkpoint(self, sidecar: dict) -> dict | None:
        art = sidecar.get("artifacts_dir")
        if not art or not os.path.isdir(art):
            return None
        cands = list(Path(art).rglob("checkpoint*.pt"))
        if not cands:
            return None
        latest = max(cands, key=lambda p: p.stat().st_mtime)
        st = latest.stat()
        return {"path": str(latest), "size_mb": round(st.st_size / 1e6, 1),
                "mtime": st.st_mtime}

    def _variant_config(self, sidecar: dict) -> dict | None:
        art = sidecar.get("artifacts_dir")
        if not art or not os.path.isdir(art):
            return None
        cands = list(Path(art).rglob("config.json"))
        if not cands:
            return None
        try:
            return json.loads(max(cands, key=lambda p: p.stat().st_mtime).read_text())
        except Exception:
            return None

    # ------------------------------------------------------------------ freeze
    def _maybe_freeze(self, rid: str) -> None:
        with self.c._lock:
            ray = self.c.ray_jobs.get(rid, {})
            sidecar = self.c.sidecars.get(rid, {})
            terminal = ray.get("status") in ("SUCCEEDED", "FAILED", "STOPPED") \
                or bool(sidecar.get("final"))
            if terminal:
                self.c._freeze_gpu_hours(rid, sidecar.get("_run_dir"))

    # ------------------------------------------------------------------ lists
    def _all_run_ids(self) -> set[str]:
        with self.c._lock:
            return set(self.c.ray_jobs) | set(self.c.sidecars)

    def jobs(self) -> dict:
        """Active + recently-terminal jobs (the main table)."""
        rows = []
        for rid in self._all_run_ids():
            self._maybe_freeze(rid)
            row = self.job_row(rid)
            rows.append(row)
        active = [r for r in rows if r["status"] in ("RUNNING", "PENDING", "UNKNOWN")]
        terminal = [r for r in rows if r["status"] not in
                    ("RUNNING", "PENDING", "UNKNOWN")]
        terminal.sort(key=lambda r: r.get("ended_ts") or "", reverse=True)
        active.sort(key=lambda r: r.get("started_ts") or "", reverse=True)
        return {"jobs": active + terminal[:10],
                "generated_ts": time.time()}

    def history(self) -> dict:
        """Completed jobs from durable sidecars, newest first (survives Ray
        restarts). Backed by the run-dir scan, capped at history_depth."""
        rows = []
        with self.c._lock:
            sidecars = dict(self.c.sidecars)
        for rid, sc in sidecars.items():
            if not sc.get("final"):
                continue
            self._maybe_freeze(rid)
            rows.append(self.job_row(rid))
        rows.sort(key=lambda r: r.get("ended_ts") or "", reverse=True)
        return {"jobs": rows[:self.c.history_depth]}

    def host(self) -> dict:
        with self.c._lock:
            return dict(self.c.host)
