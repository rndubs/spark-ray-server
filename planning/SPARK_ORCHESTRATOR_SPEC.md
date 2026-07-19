# spark-orchestrator — build spec (v1, 2026-07-18)

Spec for a NEW standalone repo (suggested name: `spark-orchestrator`)
implementing the Ray-based job scheduling layer for the DGX Spark. Written to
be handed to a builder agent with no prior context. The consumer repo
(`projection-meshing`) interacts with this layer ONLY through the contracts in
§5 — the orchestrator must not import or depend on any projection-meshing
code.

## 1. Problem + context (read first)

Hardware: one NVIDIA DGX Spark — GB10 Grace-Blackwell, aarch64, ~121 GB
**unified** CPU/GPU memory, 20 cores, no MIG, single user (`rwhit`), Ubuntu
24.04 (DGX OS). CUDA 13.0 toolkit at `/usr/local/cuda`. Python via `uv`
(`~/.local/bin/uv`), uv-managed interpreters (system python3.12 lacks dev
headers — always use uv-managed pythons; this already bit us once via a
Triton JIT failure).

Workloads to schedule (from the `projection-meshing` repo, synced to
`~/projection-meshing` by git push-to-deploy):
- many parallel **hexgen trainings** (27 M-param decoder; minutes-to-hours,
  a few GB each) — the primary customer;
- **evals** of those trainings (some hit a served endpoint, some load their
  own weights);
- occasional misc GPU jobs.

NOT scheduled by this layer (hard non-goal): the always-on hexagent operator
vLLM server (`tools/serve_local_vllm.sh` in the consumer repo). It is a
long-lived service with its own explicit GPU-memory reservation; the
orchestrator's capacity is defined as what remains AFTER that reservation.

Why this exists — the two disasters it must prevent:
1. **Memory contention.** Unified memory + no MIG means NOTHING enforces
   per-job memory. Empirically: vLLM engine init at 0.92 reservation plus a
   ~20-way parallel `nvcc` JIT build → OOM-killer (exit 137). The scheduler
   must therefore do honest **declared-budget accounting**: jobs declare
   `mem_gb`, the scheduler refuses to co-schedule past capacity. Declarations
   are bookkeeping, not enforcement — the design accepts this and makes the
   budgets config-visible and measured, not guessed.
2. **Code drift under live runs.** One git checkout + push-to-deploy means a
   push can change code under a running job. Every job therefore runs from a
   **git worktree pinned at a SHA**, created at launch, never from the main
   checkout.

## 2. Architecture

```
Mac (client)                          Spark (server)
────────────                          ──────────────────────────────────────
sparkctl submit/status/logs/cancel ──► Ray Jobs API (127.0.0.1:8265, via SSH
  (thin CLI, talks HTTP through        tunnel `ssh -4 -L 8265:...`)
   an auto-managed ssh -4 tunnel)        │
                                       Ray head (single node, systemd unit)
                                         │ custom resource: mem_gb
                                       job driver (per job):
                                         1. worktree add at pinned SHA
                                         2. symlink gitignored assets
                                         3. append ledger row (start)
                                         4. exec job cmd, capture logs
                                         5. append ledger row (end)
                                         6. GC worktree on success
```

- **Ray**: latest stable 2.x, pinned after verifying the aarch64 wheel
  imports and schedules on the GB10 (do this FIRST — it gates everything).
  Single-node: `ray start --head`, dashboard + jobs API bound to
  **127.0.0.1 only** (Ray has no auth; LAN exposure is not acceptable).
  Custom resource at start: `--resources '{"mem_gb": <capacity>}'`.
- **Capacity config** (`/etc/spark-orchestrator/capacity.toml` or
  `~/.config/...`): `total_mem_gb` (measured), `os_reserve_gb`,
  `vllm_reserve_gb` (what the operator server is configured to hold; 0 when
  it is down), derived `schedulable_mem_gb`. Per-job-class default budgets:
  `[budgets] hexgen-train-27m = <measured>`, `hexgen-eval = <measured>`,
  `default = 8`. Budgets are MEASURED on the GB10 during acceptance (§7),
  not guessed.
- **Job spec** (JSON file or CLI flags):
  `{name, repo_path, ref, cmd, env{}, mem_gb | job_class, artifacts_dir,
  timeout_s, keep_tree_on_failure=true}`. `ref` resolves to a SHA at
  submission; the SHA (never the ref) is what gets pinned and recorded.
- **Run-tree manager**: `git -C <repo_path> worktree add <trees_root>/<run_id>
  <sha>` (detached); symlink a CONFIGURED list of gitignored assets from the
  main checkout into the tree (for projection-meshing today: `.venv`,
  `.venv-train`, `.venv-serve`, `data/brepgraph`'s gitignored emits,
  `data/hexagent_traces`, `target/release`); worktree removed on success,
  kept on failure with a TTL sweep (`sparkctl gc`, default 7 days).
- **Ledger**: append-only JSONL (one fixed path, e.g.
  `~/spark-runs/ledger.jsonl`). Two rows per job (start, end):
  `{ts, run_id, name, sha, cmd, mem_gb, status: started|succeeded|failed|
  cancelled|timeout, exit_code, duration_s, artifacts_dir, log_path}`.
  Never rewritten, never sorted, no other process writes it.
- **Logs**: per-job file `~/spark-runs/<run_id>/job.log` (stdout+stderr
  merged), path in the ledger row. `sparkctl logs <run_id> [-f]` streams it.

## 3. sparkctl (client CLI) commands

`submit` (job spec → run_id, non-blocking; `--wait` to block),
`status [run_id]` (one job, or table: running/queued + capacity used/free),
`logs <run_id> [-f]`, `cancel <run_id>`, `list` (recent ledger rows),
`gc`, `doctor` (checks: tunnel up, Ray up, capacity config sane, ledger
writable, worktree root writable, git repo reachable).
Client runs on macOS; the ONLY transport is an ssh tunnel it opens itself
(`ssh -4` — mDNS on this LAN publishes unroutable IPv6 addresses; IPv4 is
load-bearing) with control-master reuse so repeated commands don't re-dial.

## 4. Systemd + ops

- `spark-ray.service` (user unit, `loginctl enable-linger rwhit`): starts the
  Ray head with the capacity resources; restarts on failure; survives reboot.
- `sparkctl doctor` is the first triage step, and the README documents the
  three known platform footguns: (1) uv-managed pythons only (headers), (2)
  `ssh -4` everywhere, (3) JIT compiles (Triton/FlashInfer/nvcc) can eat tens
  of GB — cap their parallelism (`MAX_JOBS`) inside job commands that compile.

## 5. Contract with projection-meshing (the ONLY coupling)

1. Jobs are arbitrary shell commands executed with `cwd = <pinned worktree>`,
   the caller's `env` vars set, plus injected `SPARK_RUN_ID`, `SPARK_SHA`,
   `SPARK_ARTIFACTS_DIR`.
2. The orchestrator never parses job output; success == exit 0.
3. Artifacts belong in `artifacts_dir` (outside the worktree, survives GC).
4. The symlinked-assets list lives in orchestrator CONFIG, not code, so the
   consumer repo can evolve without orchestrator releases.
5. The vLLM operator server is invisible to the orchestrator except as
   `vllm_reserve_gb` in capacity config.

## 6. Non-goals (v1)

Multi-node; containers; memory *enforcement* (cgroups/MPS); GitHub access or
any credentials beyond local ssh; managing the vLLM server lifecycle;
scheduling policies beyond FIFO-with-resource-fit (Ray default); a web UI
(Ray dashboard through the tunnel suffices).

## 7. Acceptance gates (all on the real GB10, in order)

1. **Ray aarch64 gate**: pinned Ray imports, head starts, a `mem_gb=1` task
   runs. (If no working aarch64 wheel exists, STOP and report — the fallback
   architecture decision is not the builder's to make.)
2. **Blocking semantics**: with `schedulable_mem_gb = N`, submit two jobs of
   `mem_gb = 0.6N` each → the second QUEUES and starts only after the first
   finishes. Zero-sleep race check: submit 10 small jobs, capacity never
   oversubscribed (assert from ledger timestamps).
3. **Pinning**: push a new commit to the consumer repo mid-run → the running
   job's tree still shows its submission SHA; a job submitted after sees the
   new SHA. Both SHAs correct in the ledger.
4. **Failure hygiene**: a failing job keeps its worktree, `keep=false`
   removes it, `gc` sweeps expired ones, worktree list is clean afterwards
   (`git worktree list` shows no stale entries).
5. **Cancel + timeout**: both produce correct terminal ledger rows and free
   their reserved capacity immediately.
6. **Reboot survival**: after a Spark reboot, the service is up and `sparkctl
   doctor` passes from the Mac with no manual steps.
7. **Real-workload budget measurement**: run one real hexgen 27M training and
   one eval via `sparkctl`, record peak RSS/GPU memory, write the measured
   numbers into `capacity.toml` budgets with a `measured_on` date.

## 8. Suggested phasing

P0: Ray gate + head service + submit/status/logs + ledger (no worktrees —
runs from an explicit given directory). P1: worktree pinning + symlinks +
GC + cancel/timeout. P2: capacity config + budget measurement + doctor +
README ops guide. P3: control-master tunnel management polish, `list`
ergonomics, TTL sweeps.

Keep the whole thing small: this is ~1–2k lines of Python + a systemd unit +
docs, not a framework. Prefer boring code over abstractions; every feature
not needed by §7 is out of scope.
