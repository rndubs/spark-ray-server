# spark-orchestrator

Ray-based job scheduling for the DGX Spark (GB10, aarch64, ~121 GB unified
memory). Spec: `planning/SPARK_ORCHESTRATOR_SPEC.md`. Two jobs it exists to
do: enforced declared-memory budgets (jobs declare `mem_gb`; the scheduler
refuses to co-schedule past capacity **and** caps each job's cgroup at its
declaration, so an over-allocating job dies alone) and SHA-pinned worktrees
(a push to the consumer repo never changes code under a running job).

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
- Ledger: `~/spark-runs/ledger.jsonl` (append-only JSONL; a `queued` row at
  submit, a `started` row when the driver runs, one terminal row at the end).
- Queue: `~/spark-runs/queue/{pending,admitted}` — submissions wait here, not
  in Ray. See "Scheduling" below.
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

### Scheduling

Submit as many jobs as you like at once; **clients must not pace submissions.**
`submit` enqueues into `~/spark-runs/queue/pending` on the Spark and returns.
The `spark-admit` daemon hands a job to Ray strictly FIFO, and only once its
declared `mem_gb` is actually free — so a job is PENDING inside Ray for
seconds, not minutes.

That matters because Ray fails any job that stays PENDING past its
job-supervisor start timeout (`RAY_JOB_START_TIMEOUT_SECONDS`, default 900 s),
and it does so *before* the driver runs: no ledger row, no run dir, no log. On
2026-07-25 that silently destroyed 78 of 96 submitted shards. Two things now
prevent a recurrence:

1. Nothing queues inside Ray, so the timeout has nothing to fire on (and the
   systemd unit raises it to 24 h as a backstop).
2. A job is in the ledger from the instant `submit` returns (`queued`), and
   every run reaches a terminal status. If a run dies without writing its own
   terminal row, the admitter's reconciler writes a `lost` row with the reason.
   **A run may fail; it may not vanish.**

Admission is strict FIFO with head-of-line blocking — a small job never jumps
ahead of a large one, so a large job cannot be starved. `sparkctl status` shows
the queue and warns loudly if the admitter is not running; queued work is on
disk and survives a daemon restart or a reboot.

Job spec JSON (spec §2): `{name, repo_path, ref, cmd, env{}, mem_gb |
job_class, artifacts_dir, timeout_s, keep_tree_on_failure}`. `ref` resolves
to a SHA at submission; the SHA is what runs. Client config (optional):
`config/client.example.toml` → `~/.config/spark-orchestrator/client.toml`.

This layer is **not** hexgen-specific — any project on the Spark can use it.
To wire in a new consumer repo (a `[[repos]]` block, budgets, the job
contract, the dashboard metrics adapter), see `docs/CONSUMERS.md`. The
`skills/spark-jobs` skill is the project-agnostic scheduling surface for use
from other repos.

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
`spark-dashboard` and `spark-admit` are always bounced: neither touches
running jobs, and the admitter's queue is on disk, so restarting it mid-campaign
costs at most a few seconds of admission latency.

Three user units: `spark-ray.service` (the head), `spark-admit.service` (the
admission controller — see Scheduling above), `spark-dashboard.service`.

Capacity config: `~/.config/spark-orchestrator/capacity.toml` (see
`config/capacity.example.toml`). `schedulable_mem_gb = total - os_reserve -
vllm_reserve`. When you bring the operator vLLM server up or down, edit
`vllm_reserve_gb` and `systemctl --user restart spark-ray.service` (with no
jobs running). Budgets in `[budgets]` are measured, not guessed — remeasure
when a workload changes materially.

## Training dashboard

A read-only, localhost-only web overlay (`spark-dashboard.service`, port 8787)
answers per training job: what's running, on what code + data, how far along,
how healthy, and its GPU-hours. It reads the Ray Jobs API, the `dashboard.json`
sidecars `sparkctl submit` writes, and `metrics.jsonl` — never the Ray
internals the 8265 dashboard already shows. Design: `planning/DASHBOARD_SPEC.md`;
ops: `tools/dashboard/README.md`.

```sh
sparkctl dashboard                       # tunnel + print http://localhost:8787
sparkctl submit --name sft --class hexgen-train-27m \
  --desc "L3 P21 SFT baseline" --variant baseline --seed 0 \
  --input /data/hexforge-data/corpora/p21_sft \
  --cmd 'python hexgen/decoder/train.py --run-dir "$SPARK_ARTIFACTS_DIR" ...'
```

`--desc` is required (it's the row label + the copyable LEDGER line). `--input`
paths are tier-tagged against `docs/DATA.md`; a declared or observed read under
`eval/frozen/` raises the contamination badge.

## Ops / triage

`sparkctl doctor` first. Service logs: `journalctl --user -u spark-ray -f`,
`-u spark-admit -f`, `-u spark-dashboard -f` on the Spark. Ray dashboard: with
the tunnel up (any sparkctl command opens it), http://127.0.0.1:8265.

Offline tests (no Spark, no Ray — safe to run while a campaign is in flight):

```sh
python3 -m unittest discover -s tests -v
```

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
sweeps such trees once expired. `mem_gb` is enforced with a per-job cgroup
(`MemoryMax` = the declaration, `MemorySwapMax=0`), so an over-allocating job
is killed alone and gets an `oom_killed` ledger row carrying its measured
peak; GPU memory is still unenforced (no MIG), so VRAM is shared on trust.

## Acceptance status (spec §7)

Run on the real GB10; results recorded in `planning/ACCEPTANCE.md`.
