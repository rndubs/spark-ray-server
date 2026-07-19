"""Stdlib HTTP client for the Ray Jobs REST API (through the ssh tunnel).
Keeping it REST avoids installing ray on the Mac."""

from __future__ import annotations

import json
import urllib.error
import urllib.request


class RayJobs:
    def __init__(self, port: int):
        self.base = f"http://127.0.0.1:{port}"

    def _req(self, method: str, path: str, body: dict | None = None):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.base + path, data=data, method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            raise RuntimeError(f"{method} {path} -> {e.code}: {e.read().decode()[:500]}")
        return json.loads(text) if text else {}

    def submit(self, *, entrypoint: str, submission_id: str,
               entrypoint_resources: dict, metadata: dict) -> dict:
        return self._req("POST", "/api/jobs/", {
            "entrypoint": entrypoint,
            "submission_id": submission_id,
            "entrypoint_resources": entrypoint_resources,
            "metadata": metadata,
        })

    def get(self, submission_id: str) -> dict:
        return self._req("GET", f"/api/jobs/{submission_id}")

    def list(self) -> list[dict]:
        return self._req("GET", "/api/jobs/")

    def stop(self, submission_id: str) -> dict:
        return self._req("POST", f"/api/jobs/{submission_id}/stop")
