# Acceptance record (spec §7) — DGX Spark GB10, 2026-07-18/19

All gates run against the real GB10 (`spark-3b57`, aarch64, 121 GB unified),
Ray 2.56.1, schedulable_mem_gb = 40 (total 121 − os 9 − vllm 72; operator
vLLM up at `--gpu-memory-utilization 0.60` throughout).

Gates 2–5 are scripted: `tools/acceptance.py [2 3 4 5]`.

| # | Gate | Result |
|---|------|--------|
| 1 | Ray aarch64: import, head start, mem_gb task | PASS — Ray 2.56.1 wheel imports on aarch64; head starts bound to 127.0.0.1; `mem_gb=1` task ran; dashboard/jobs API loopback-only (`ss` verified). GCS port 6379 binds wildcard (Ray offers no bind flag) — see README security note. |
| 2 | Blocking + zero-sleep race | PASS — second 24 GB job stayed PENDING until the first's end row (start 02:49:08.401Z ≥ end 02:49:07.876Z); 10×13 GB back-to-back: peak reserved 39/40 GB (ledger-timestamp interval analysis, starts-before-ends tie-break). |
| 3 | SHA pinning under mid-run push | PASS — mid-run commit to the consumer repo: running job's tree stayed at `7ddeb4a1`, job submitted after ran at the new SHA `a08667bc`; both SHAs correct in ledger start rows (test commit reverted afterwards). |
| 4 | Failure hygiene (keep/no-keep/gc/clean list) | PASS — `exit 3` job: ledger `failed`/`exit_code=3`, tree kept; `--no-keep-tree` job's tree removed; `gc --ttl-days 0` swept the kept tree; `git worktree list` shows no `spark-runs/trees` entries after. |
| 5 | Cancel + timeout ledger rows, capacity freed | PASS — cancel: ledger `cancelled` row via SIGTERM handler, a 30 GB job fit immediately after (capacity freed); `--timeout-s 8` on `sleep 300`: ledger `timeout` row at 8.7 s. |
| 6 | Reboot survival, doctor passes from Mac | READY, awaiting reboot — unit enabled, `Linger=yes`; reboot needs interactive auth (polkit `CanReboot=challenge`, sudo needs a password), which a non-interactive agent cannot supply. Run `ssh -4 -t rwhit "sudo reboot"`, wait ~2 min, then `sparkctl doctor` must pass with no manual steps. |
| 7 | Real-workload budgets measured into capacity.toml | PASS — real 27M baseline train (500 steps, 111 s, CUDA) via sparkctl: peak RSS 2.9 GB, system MemAvailable dip 7.1 GB → budget 10. Real eval (`sample_eval.py --n 32`, 20-graph cap, 35 s): RSS 1.6 GB, dip 3.1 GB → budget 6. Written to capacity.toml (live + example) with measured_on 2026-07-18. Note: eval reported `n_verify_crash: 20/20` — a consumer-repo issue at SHA `67063374`, outside the orchestrator contract (job exited 0). |

Gate 7 runs: `hexgen-train-27m-20260719-025615-9154`, `hexgen-eval-20260719-025842-3a88`
(artifacts under `~/spark-runs/<run_id>/artifacts`, incl. checkpoint + eval summary).

Environment notes from the acceptance session (2026-07-18 evening):
- The operator vLLM server shut down cleanly mid-session (not orchestrator-
  initiated; its log shows a graceful uvicorn shutdown). `vllm_reserve_gb`
  stays 72 in capacity.toml — set it to 0 (and restart spark-ray.service)
  if the server is meant to stay down.
- Ray GCS (port 6379) binds the wildcard interface; the dashboard/jobs API
  (8265) is loopback-only as required. Ray has no bind flag for GCS — if
  LAN exposure of 6379 matters, add a firewall rule (needs sudo):
  `sudo ufw deny in on <lan-if> to any port 6379 proto tcp`.
