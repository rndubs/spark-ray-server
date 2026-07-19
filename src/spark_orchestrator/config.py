"""Config loading: server capacity.toml + client client.toml."""

from __future__ import annotations

import tomllib
from pathlib import Path

CAPACITY_PATHS = [
    Path("~/.config/spark-orchestrator/capacity.toml").expanduser(),
    Path("/etc/spark-orchestrator/capacity.toml"),
]
CLIENT_PATH = Path("~/.config/spark-orchestrator/client.toml").expanduser()

CLIENT_DEFAULTS = {
    "host": "rwhit",
    "local_port": 8265,
    "remote_port": 8265,
    "dashboard_local_port": 8787,
    "dashboard_remote_port": 8787,
    "orchestrator_root": "/home/rwhit/spark-orchestrator",
    "runs_root": "/home/rwhit/spark-runs",
    "default_repo": "/home/rwhit/projection-meshing",
}


def load_capacity(text: str | None = None) -> dict:
    """Load capacity config. `text` lets the client parse a config fetched
    over ssh with the same validation the server uses."""
    if text is not None:
        cfg = tomllib.loads(text)
        cfg["_path"] = "<remote>"
    else:
        for p in CAPACITY_PATHS:
            if p.exists():
                cfg = tomllib.loads(p.read_text())
                cfg["_path"] = str(p)
                break
        else:
            raise FileNotFoundError(
                "no capacity.toml at " + " or ".join(str(p) for p in CAPACITY_PATHS)
            )
    for key in ("total_mem_gb", "os_reserve_gb", "vllm_reserve_gb"):
        if key not in cfg:
            raise ValueError(f"capacity.toml missing required key: {key}")
    cfg["schedulable_mem_gb"] = (
        cfg["total_mem_gb"] - cfg["os_reserve_gb"] - cfg["vllm_reserve_gb"]
    )
    cfg.setdefault("budgets", {}).setdefault("default", 8)
    cfg.setdefault("paths", {}).setdefault("runs_root", "~/spark-runs")
    cfg.setdefault("gc", {}).setdefault("failed_tree_ttl_days", 7)
    return cfg


def runs_root(cfg: dict) -> Path:
    return Path(cfg["paths"]["runs_root"]).expanduser()


def trees_root(cfg: dict) -> Path:
    return runs_root(cfg) / "trees"


def ledger_path(cfg: dict) -> Path:
    return runs_root(cfg) / "ledger.jsonl"


def budget_for(cfg: dict, job_class: str | None) -> float:
    budgets = cfg["budgets"]
    if job_class is None:
        return float(budgets["default"])
    if job_class not in budgets:
        raise KeyError(f"job_class {job_class!r} not in [budgets] of {cfg['_path']}")
    return float(budgets[job_class])


def symlink_assets_for(cfg: dict, repo_path: str) -> list[str]:
    want = str(Path(repo_path).expanduser())
    for repo in cfg.get("repos", []):
        if str(Path(repo["path"]).expanduser()) == want:
            return list(repo.get("symlink_assets", []))
    return []


def load_client() -> dict:
    cfg = dict(CLIENT_DEFAULTS)
    if CLIENT_PATH.exists():
        cfg.update(tomllib.loads(CLIENT_PATH.read_text()))
    return cfg
