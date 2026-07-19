# spark-orchestrator

Ray-based job scheduling for the DGX Spark (GB10, aarch64, ~121 GB unified
memory). Spec: `planning/SPARK_ORCHESTRATOR_SPEC.md`. Two jobs it exists to
do: honest declared-memory accounting (jobs declare `mem_gb`; the scheduler
refuses to co-schedule past capacity) and SHA-pinned worktrees (a push to the
consumer repo never changes code under a running job).

```
Mac: sparkctl ──ssh -4 tunnel──► Ray Jobs API (127.0.0.1:8265, Spark)
                                   └─ per-job driver: worktree @ SHA →
                                      ledger start → exec cmd → ledger end →
                                      GC tree on success
```

- Ray pinned at **2.56.1** (verified on the GB10 2026-07-18: imports, head
  starts, `mem_gb` task schedules).
- Server lives at `~/spark-orchestrator` on the Spark (this repo,
  push-to-deploy), venv `.venv` (uv-managed python 3.12).
- Ledger: `~/spark-runs/ledger.jsonl` (append-only JSONL, two rows per job).
- Logs: `~/spark-runs/<run_id>/job.log`; artifacts default to
  `~/spark-runs/<run_id>/artifacts` (survives worktree GC).
- Worktrees: `~/spark-runs/trees/<run_id>`, removed on success, kept on
  failure, swept by `sparkctl gc` after the TTL (default 7 days).

## Client (Mac)

```sh
uv venv && uv pip install -e .          # then use .venv/bin/sparkctl
sparkctl doctor                          # first triage step, always

sparkctl submit --name smoke --cmd 'python -c "print(42)"' --mem-gb 1 --wait
sparkctl submit --spec job.json          # or a spec file (flags override)
sparkctl status [run_id]                 # capacity + queue, or one job
sparkctl logs <run_id> -f
sparkctl cancel <run_id>
sparkctl list
sparkctl gc [--dry-run]
```

Job spec JSON (spec §2): `{name, repo_path, ref, cmd, env{}, mem_gb |
job_class, artifacts_dir, timeout_s, keep_tree_on_failure}`. `ref` resolves
to a SHA at submission; the SHA is what runs. Client config (optional):
`config/client.example.toml` → `~/.config/spark-orchestrator/client.toml`.

Contract with the consumer repo (spec §5): the job is an arbitrary shell
command run with `cwd` = the pinned worktree, caller env plus injected
`SPARK_RUN_ID`, `SPARK_SHA`, `SPARK_ARTIFACTS_DIR`; success == exit 0; put
anything durable in `$SPARK_ARTIFACTS_DIR` (the worktree is disposable).

## Server (Spark)

One-time setup is scripted in `tools/deploy.sh` (push + install + systemd
unit + start). Manual pieces it assumes already exist:

```sh
# on the Spark, once:
git init ~/spark-orchestrator && git -C ~/spark-orchestrator config receive.denyCurrentBranch updateInstead
~/.local/bin/uv venv --python 3.12 --python-preference only-managed ~/spark-orchestrator/.venv
loginctl enable-linger rwhit      # so the user unit survives logout/reboot
# on the Mac, once:
git remote add spark rwhit:spark-orchestrator
```

Then every deploy is `tools/deploy.sh` (add `--restart` to bounce the Ray
head — that kills running jobs, so by default it only starts it if down).

Capacity config: `~/.config/spark-orchestrator/capacity.toml` (see
`config/capacity.example.toml`). `schedulable_mem_gb = total - os_reserve -
vllm_reserve`. When you bring the operator vLLM server up or down, edit
`vllm_reserve_gb` and `systemctl --user restart spark-ray.service` (with no
jobs running). Budgets in `[budgets]` are measured, not guessed — remeasure
when a workload changes materially.

## Ops / triage

`sparkctl doctor` first. Service logs: `journalctl --user -u spark-ray
-f` on the Spark. Ray dashboard: with the tunnel up (any sparkctl command
opens it), http://127.0.0.1:8265.

Platform footguns (learned the hard way; do not re-litigate):

1. **uv-managed pythons only.** System python3.12 lacks dev headers; Triton/
   FlashInfer JIT builds fail mysteriously. Every venv here is created with
   `uv venv --python-preference only-managed`.
2. **`ssh -4` everywhere.** mDNS on this LAN publishes IPv6 addresses that
   don't route; bare `ssh rwhit` fails. sparkctl always passes `-4`; do the
   same for manual ssh/scp/rsync/git.
3. **JIT compiles eat tens of GB.** Triton/FlashInfer/nvcc builds fan out
   ~20 compilers; that plus a 0.92 vLLM reservation OOM-killed the box once
   (exit 137). Cap parallelism inside job commands that compile:
   `MAX_JOBS=4`. Declare the compile spike in the job's `mem_gb`.

Failure modes worth knowing: a driver SIGKILLed before writing its end row
leaves a dangling `started` ledger row — Ray's job status (`sparkctl status
<run_id>`) is the truth for liveness, and `sparkctl gc --force-started`
sweeps such trees once expired. Memory budgets are accounting, not
enforcement (no MIG/cgroups in v1): a job that lies about `mem_gb` can still
OOM the box; measure before you declare.

## Acceptance status (spec §7)

Run on the real GB10; results recorded in `planning/ACCEPTANCE.md`.
