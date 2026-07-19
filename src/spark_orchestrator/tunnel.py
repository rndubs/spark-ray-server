"""Client-side ssh transport: -4 always (IPv6 mDNS addresses don't route),
control-master reuse so repeated sparkctl invocations don't re-dial, and an
auto-managed -L tunnel to the Ray jobs API."""

from __future__ import annotations

import subprocess
import urllib.request
from pathlib import Path

CONTROL_PATH = "~/.ssh/spark-orch-%r@%h-%p"


def ssh_cmd(host: str, *extra: str) -> list[str]:
    return [
        "ssh", "-4",
        "-o", "ControlMaster=auto",
        "-o", f"ControlPath={CONTROL_PATH}",
        "-o", "ControlPersist=600",
        "-o", "ConnectTimeout=10",
        *extra, host,
    ]


def ssh_run(host: str, remote_cmd: str, check: bool = True,
            timeout: float | None = 60) -> subprocess.CompletedProcess:
    res = subprocess.run(
        ssh_cmd(host) + [remote_cmd], capture_output=True, text=True, timeout=timeout
    )
    if check and res.returncode != 0:
        raise RuntimeError(
            f"ssh {host} {remote_cmd!r} failed ({res.returncode}): {res.stderr.strip()}"
        )
    return res


def api_alive(port: int, timeout: float = 1.5) -> bool:
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/api/version", timeout=timeout
        ) as r:
            return r.status == 200
    except Exception:
        return False


def ensure_tunnel(client_cfg: dict) -> None:
    port = client_cfg["local_port"]
    if api_alive(port):
        return
    Path("~/.ssh").expanduser().mkdir(mode=0o700, exist_ok=True)
    cmd = ssh_cmd(
        client_cfg["host"],
        "-f", "-N",
        "-o", "ExitOnForwardFailure=yes",
        "-L", f"{port}:127.0.0.1:{client_cfg['remote_port']}",
    )
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    if res.returncode != 0:
        raise RuntimeError(f"ssh tunnel failed: {res.stderr.strip()}")
    for _ in range(20):
        if api_alive(port):
            return
        import time
        time.sleep(0.25)
    raise RuntimeError(
        f"tunnel is up but Ray jobs API on 127.0.0.1:{port} is not responding "
        f"(is spark-ray.service running? try: sparkctl doctor)"
    )
