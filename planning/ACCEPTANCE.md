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
| 6 | Reboot survival, doctor passes from Mac | pending |
| 7 | Real-workload budgets measured into capacity.toml | pending |
