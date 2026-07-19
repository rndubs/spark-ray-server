---
name: spark-jobs
description: >
  Schedule and monitor GPU jobs on the DGX Spark from ANY project via sparkctl
  (the Ray orchestrator) and the training dashboard — project-agnostic:
  declared-memory budgets, SHA-pinned worktrees, the job contract
  ($SPARK_ARTIFACTS_DIR / exit-0), and rendering runs on the localhost
  dashboard (provenance, live curves, GPU-hours, data-read contamination).
  Use whenever submitting, watching, cancelling, or dashboarding a GPU job on
  the Spark from a repo that is NOT projection-meshing. For hexproj-specific
  ops (the three venvs, serving the operator, hexgen training), use spark-ops
  instead — this skill is the generic scheduling surface it shares.
---

# Scheduling GPU jobs on the Spark (any project)

The Spark (GB10, aarch64, 121 GB unified memory, host alias `rwhit`, ssh from
the Mac with **`-4` always**) runs a Ray-based orchestrator + a read-only
training dashboard. Any project can use them — the coupling is config, not
code. Repo: `~/Workspace/spark-ray-server`; full onboarding:
`docs/CONSUMERS.md` there. Deployed on the Spark at `~/spark-orchestrator`.

**Never launch GPU work on the Spark by hand — submit it**, so it gets a
memory budget and a pinned worktree.

## Platform footguns (do not re-litigate)

1. **`ssh -4` everywhere.** mDNS on this LAN publishes IPv6 addresses that
   don't route; bare `ssh rwhit` fails. `sparkctl` already does this; match it
   for manual ssh/scp/rsync/git (`GIT_SSH_COMMAND="ssh -4"`).
2. **uv-managed pythons only** for GPU venvs (system python lacks headers →
   Triton/FlashInfer JIT builds fail).
3. **JIT compiles eat tens of GB.** Triton/FlashInfer/nvcc fan out ~20
   compilers; cap with `MAX_JOBS=4` **inside** the job command and declare the
   spike in `mem_gb`.
4. **Budgets are accounting, not enforcement** (no MIG/cgroups): a job that
   lies about `mem_gb` can still OOM the box. Measure, then declare.

## The job contract

Your `--cmd` runs with `cwd` = a git worktree of your repo pinned at the
submission SHA (a later push never disturbs a running job). Injected env:
`SPARK_RUN_ID`, `SPARK_SHA`, `SPARK_ARTIFACTS_DIR`. **Exit 0 = success**; the
orchestrator never parses output. **Write durable outputs to
`$SPARK_ARTIFACTS_DIR`** (`~/spark-runs/<run_id>/artifacts`) — the worktree is
disposable. Gitignored deps (venvs, data, binaries) are symlinked in per the
repo's `[[repos]].symlink_assets` in `capacity.toml`.

## Commands (from the Mac)

```sh
sparkctl doctor                 # ALWAYS the first triage step
sparkctl submit --name <n> --repo /home/rwhit/<proj> \
    --class <budget-class> | --mem-gb <N> \
    --desc "one line (REQUIRED — dashboard + LEDGER)" \
    [--variant V --seed S --input <declared-read-dir>] \
    [--metrics <path>] [--env K=V] [--ref <git-ref>] [--timeout-s N] [--wait] \
    --cmd '<shell cmd; write to "$SPARK_ARTIFACTS_DIR">'
sparkctl status [run_id]        # capacity used/free + queue, or one job
sparkctl logs <run_id> -f
sparkctl cancel <run_id>
sparkctl list                   # recent ledger rows
sparkctl gc [--dry-run]         # sweep expired failed-job worktrees
sparkctl dashboard [--open]     # tunnel to the training dashboard, print URL
```

`--desc` is required. `--class` pulls a measured budget from `capacity.toml`;
`--mem-gb` is ad-hoc. A `mem_gb` over `schedulable_mem_gb` is rejected at
submit (it could never run). Capacity = `total − os_reserve − vllm_reserve`
(the vLLM reserve is the always-on operator server; 0 when down).

## Onboarding a new repo (once)

On the Spark, edit `~/.config/spark-orchestrator/capacity.toml`:

- add a `[[repos]]` block: `path` + `symlink_assets` (gitignored deps to link
  into the worktree);
- add `[budgets]` entries for your job classes (measured GB + headroom).

The repo needs no orchestrator credentials — `sparkctl` reaches it over the
same `ssh -4` tunnel. Full walkthrough with examples: `docs/CONSUMERS.md`.

## Rendering on the dashboard

`sparkctl dashboard --open` → `localhost:8787` (localhost-only, SSH tunnel is
the auth boundary). To get live curves + progress, your trainer must **append
+ flush `metrics.jsonl` per eval row** (a trainer that writes the file once at
the end only shows curves at completion). The plotted columns and metrics
filename are the `[dashboard]` adapter knobs (`loss_keys`, `metrics_filename`)
in `capacity.toml`; `data_root` roots the tier map that flags reads under
`eval/frozen/`. Status, provenance, GPU-hours, and health work regardless of
metrics streaming.

## Triage

`sparkctl doctor` first (checks tunnel, Ray head, dashboard service, capacity
sanity, ledger/worktree writability, repo reachability). Service logs on the
Spark: `journalctl --user -u spark-ray -f` and `-u spark-dashboard -f`. A
driver SIGKILLed before writing its end row leaves a dangling `started` ledger
row — Ray job status (`sparkctl status <run_id>`) is the truth for liveness;
`sparkctl gc --force-started` sweeps such trees once expired.
