"""FastAPI app for the training dashboard. Localhost-only (spec: bind
127.0.0.1, refuse 0.0.0.0 — the SSH tunnel is the auth boundary). JSON API +
one static page. Entry point: `spark-dashboard` (uvicorn on 127.0.0.1:8787).

    spark-dashboard            # reads capacity.toml [dashboard], serves
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles

from .. import config
from .collect import Collector
from .views import Views, _num

STATIC = Path(__file__).parent / "static"


def create_app(cfg: dict) -> FastAPI:
    collector = Collector(cfg)
    collector.start()
    views = Views(collector)
    # per-project loss column names (adapter section), else hexgen defaults
    lk = cfg.get("dashboard", {}).get("loss_keys")
    if lk:
        views.loss_keys = list(lk)

    app = FastAPI(title="Spark Training Dashboard", docs_url=None, redoc_url=None)

    @app.get("/api/host")
    def api_host():
        return views.host()

    @app.get("/api/jobs")
    def api_jobs():
        return views.jobs()

    @app.get("/api/history")
    def api_history():
        return views.history()

    @app.get("/api/jobs/{run_id}")
    def api_job(run_id: str):
        if run_id not in views._all_run_ids():
            raise HTTPException(404, f"unknown run_id {run_id}")
        return views.job_row(run_id, detail=True)

    @app.get("/api/jobs/{run_id}/series")
    def api_series(run_id: str):
        with collector._lock:
            cache = collector.metrics.get(run_id, {})
            rows = list(cache.get("rows", []))
        # discover numeric columns (flatten one level of nested dicts)
        cols: dict[str, list] = {}
        steps = []
        for row in rows:
            step = row.get("step")
            steps.append(step)
            for k, v in row.items():
                if _num(v):
                    cols.setdefault(k, [])
                elif isinstance(v, dict):
                    for sk, sv in v.items():
                        if _num(sv):
                            cols.setdefault(f"{k}.{sk}", [])
        for row in rows:
            flat = {}
            for k, v in row.items():
                if _num(v):
                    flat[k] = v
                elif isinstance(v, dict):
                    for sk, sv in v.items():
                        if _num(sv):
                            flat[f"{k}.{sk}"] = sv
            for c in cols:
                cols[c].append(flat.get(c))
        return {"steps": steps, "columns": cols,
                "default": [c for c in views.loss_keys if c in cols],
                "n": len(rows)}

    @app.get("/api/jobs/{run_id}/log", response_class=PlainTextResponse)
    def api_log(run_id: str, tail: int = Query(100, ge=1, le=5000)):
        with collector._lock:
            sc = dict(collector.sidecars.get(run_id, {}))
            ray = dict(collector.ray_jobs.get(run_id, {}))
        path = sc.get("log_path")
        if not path or not os.path.exists(path):
            # ray-only jobs: no orchestrator log; surface ray's message
            return ray.get("message") or "(no log file yet)"
        try:
            out = subprocess.run(["tail", "-n", str(tail), path],
                                 capture_output=True, text=True, timeout=10)
            return out.stdout
        except Exception as e:
            return f"(log read failed: {e})"

    @app.get("/api/jobs/{run_id}/git", response_class=PlainTextResponse)
    def api_git(run_id: str):
        with collector._lock:
            sc = dict(collector.sidecars.get(run_id, {}))
        repo, sha = sc.get("repo_path"), sc.get("sha")
        if not repo or not sha:
            raise HTTPException(404, "no repo/sha for this job")
        try:
            out = subprocess.run(
                ["git", "-C", repo, "show", "--stat", "--no-color", sha],
                capture_output=True, text=True, timeout=10)
            return out.stdout or out.stderr
        except Exception as e:
            return f"(git show failed: {e})"

    @app.get("/")
    def index():
        return FileResponse(STATIC / "index.html")

    if STATIC.is_dir():
        app.mount("/static", StaticFiles(directory=str(STATIC)), name="static")

    app.state.collector = collector
    return app


def main() -> None:
    import uvicorn

    cfg = config.load_capacity()
    d = cfg.get("dashboard", {})
    host = d.get("bind", "127.0.0.1")
    if host not in ("127.0.0.1", "localhost", "::1"):
        sys.exit(f"[dashboard] refusing to bind {host!r} — localhost only "
                 f"(the SSH tunnel is the auth boundary; edit [dashboard].bind)")
    port = int(d.get("port", 8787))
    print(f"[spark-dashboard] serving on http://{host}:{port} "
          f"(data_root={d.get('data_root', '/data/hexforge-data')}, "
          f"ray_port={d.get('ray_port', 8265)})", flush=True)
    uvicorn.run(create_app(cfg), host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
