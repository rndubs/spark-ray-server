"""spark-ray-head: exec the Ray head with capacity from capacity.toml.

Run by the spark-ray.service systemd user unit. Binds everything to
127.0.0.1 — Ray has no auth, LAN exposure is not acceptable (spec §2).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

from . import config


def main() -> None:
    cfg = config.load_capacity()
    sched = cfg["schedulable_mem_gb"]
    if sched <= 0:
        sys.exit(
            f"schedulable_mem_gb = {sched} (total {cfg['total_mem_gb']} - os "
            f"{cfg['os_reserve_gb']} - vllm {cfg['vllm_reserve_gb']}) — nothing "
            f"to schedule; fix {cfg['_path']}"
        )
    config.runs_root(cfg).mkdir(parents=True, exist_ok=True)
    ray_bin = Path(sys.executable).parent / "ray"
    os.environ.setdefault("RAY_USAGE_STATS_ENABLED", "0")
    args = [
        str(ray_bin), "start", "--head", "--block",
        "--node-ip-address=127.0.0.1",
        "--dashboard-host=127.0.0.1",
        "--dashboard-port=8265",
        "--port=6379",
        "--num-gpus=1",
        f"--resources={json.dumps({'mem_gb': sched})}",
    ]
    print(f"[spark-ray-head] schedulable_mem_gb={sched} ({cfg['_path']})", flush=True)
    os.execv(str(ray_bin), args)


if __name__ == "__main__":
    main()
