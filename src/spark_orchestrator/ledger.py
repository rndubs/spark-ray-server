"""Append-only JSONL run ledger. Two rows per job: start + end."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path


def utc_ts() -> str:
    now = time.time()
    ms = int((now % 1) * 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(now)) + f".{ms:03d}Z"


def append(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    line = (json.dumps(row, separators=(",", ":"), sort_keys=True) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(fd, line)
    finally:
        os.close(fd)


def read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text().splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def latest_by_run(path: Path) -> dict[str, dict]:
    """run_id -> latest row (end row wins over start row by file order)."""
    out: dict[str, dict] = {}
    for row in read_rows(path):
        out[row["run_id"]] = row
    return out
