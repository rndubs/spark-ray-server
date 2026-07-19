# Using spark-orchestrator + the dashboard for a new project

spark-orchestrator is **not** hexgen-specific. The only thing tying it to
`projection-meshing` today is *config* — the code imports nothing from any
consumer repo (spec §5). This guide is how to point a **second** project at
the same Ray head + dashboard on the Spark, with zero orchestrator code
changes.

The whole coupling surface is four things: a `[[repos]]` block, `[budgets]`,
the job contract your command must honour, and (for the dashboard) the metrics
adapter. That's it.

## 1. What the orchestrator promises your job (the contract)

Every job is an arbitrary shell command. The orchestrator:

- runs it with **`cwd` = a git worktree of your repo pinned at the submission
  SHA** (a later push never changes code under a running job);
- symlinks your gitignored assets (venvs, data, build outputs) into that
  worktree — see `[[repos]].symlink_assets` below;
- injects `SPARK_RUN_ID`, `SPARK_SHA`, `SPARK_ARTIFACTS_DIR` into the env,
  plus any `--env K=V` you pass;
- treats **exit 0 as success**; it never parses your output.

Your side of the contract:

- **Write anything durable to `$SPARK_ARTIFACTS_DIR`** (`~/spark-runs/
  <run_id>/artifacts`). The worktree is disposable — it's GC'd on success and
  swept after a TTL on failure. Artifacts survive.
- Don't assume the main checkout's mutable state; you get a clean tree at a
  SHA plus the symlinked assets, nothing else.

## 2. Register your repo (capacity.toml)

On the Spark, edit `~/.config/spark-orchestrator/capacity.toml`
(see `config/capacity.example.toml`). Add a `[[repos]]` block:

```toml
[[repos]]
path = "/home/rwhit/my-project"
symlink_assets = [        # gitignored things your job needs in the worktree
  ".venv",                # a dir → symlinked whole
  "data/big_corpus",      # a tracked dir with gitignored extras → its missing
                          #   entries are linked one level down (not shadowed)
  "target/release",
]
```

`symlink_assets` is the escape hatch for everything git doesn't carry: venvs,
large data trees, compiled binaries. Paths that don't exist are skipped
silently, so an over-broad list is harmless.

Add budgets for your job classes (GB of unified memory — **declared
accounting, not enforced**; measure, don't guess):

```toml
[budgets]
default = 8
my-train = 12            # peak observed + headroom
my-eval  = 6
```

Then submit with `--class my-train` (uses the budget) or `--mem-gb N` (ad-hoc).
A job whose `mem_gb` exceeds `schedulable_mem_gb` is rejected at submit — it
could never run.

## 3. Submit from the Mac

`sparkctl` is repo-agnostic; point `--repo` at your project (or set
`default_repo` in `~/.config/spark-orchestrator/client.toml`):

```sh
sparkctl submit --name train-a \
  --repo /home/rwhit/my-project \
  --class my-train \
  --desc "what this run is (required — dashboard + LEDGER)" \
  --variant v1 --seed 0 \
  --input /data/my-project/corpora/train \
  --cmd 'python train.py --out "$SPARK_ARTIFACTS_DIR" --seed 0'

sparkctl status              # capacity + queue
sparkctl logs <run_id> -f
sparkctl dashboard --open    # the training dashboard for all of it
```

`--desc` is required. `--input` is directory-granularity and tier-checked
against the DATA.md tree (below); everything else is optional metadata that
enriches the dashboard row.

## 4. Make your runs render on the dashboard

The dashboard joins Ray state with the `dashboard.json` sidecar `sparkctl`
writes, then reads two project-shaped things from your run's artifacts. Both
are configured in the `[dashboard]` section of `capacity.toml`:

```toml
[dashboard]
data_root = "/data/hexforge-data"      # root of your DATA.md-style data tree
metrics_filename = "metrics.jsonl"     # the file your training appends to
loss_keys = ["train_total", "holdout_total"]   # columns summarised/plotted
```

- **Curves + progress** come from `metrics_filename` (default `metrics.jsonl`),
  tailed incrementally under `$SPARK_ARTIFACTS_DIR`. Write it as **JSON lines,
  one row per eval, appended and flushed as you go** (see the gotcha below).
  Any numeric column is plottable; `loss_keys` are the defaults shown. A
  `step` column drives the progress bar; a `config.json` with a `steps` field
  in the run dir supplies the total.
- **Data-read audit + contamination.** `data_root` roots the DATA.md tier map
  (`corpora/`, `banks/`, `eval/frozen/`, …). Declared `--input` paths and
  best-effort observed `/proc` reads are tagged by tier; anything under
  `eval/frozen/` raises the red contamination badge while the job runs. If
  your project has no such tree, set `data_root` to anything (reads just show
  `outside-tree`) — the dashboard still works.

### The one gotcha: stream your metrics

The dashboard tails `metrics.jsonl` **incrementally**, so live curves and live
progress only work if your trainer *appends and flushes each row as it happens*:

```python
mf = open(os.path.join(run_dir, "metrics.jsonl"), "a", buffering=1)  # line-buffered
for step in ...:
    mf.write(json.dumps(row) + "\n"); mf.flush()
```

A trainer that buffers rows in memory and writes the whole file once at the
end will only show curves at completion (this is the known hexgen gap — see
`planning/DASHBOARD_SPEC.md`). Everything else — status, provenance,
GPU-hours, health — works regardless.

## 5. What you get, for free

Per run: status/health (STALLED/NaN/state-mismatch), git sha+branch+dirty,
variant/seeds, live progress + curves, per-proc RSS + GPU mem, **GPU-hours**
(wall × mean host GPU util, frozen on completion), declared+observed data
reads with contamination flags, a completed-jobs table, and a one-click
copyable LEDGER line. All localhost-only behind the SSH tunnel.

## Limits worth knowing

- **Memory budgets are accounting, not enforcement** (no MIG/cgroups in v1):
  a job that lies about `mem_gb` can still OOM the box. Measure and declare
  honestly. Capacity = `total − os_reserve − vllm_reserve`; the vLLM reserve
  is whatever the always-on operator server holds (0 when it's down).
- **The `[dashboard]` metrics adapter is currently global** — one
  `metrics_filename`/`loss_keys` for the whole box. Two consumers with
  *different* metrics schemas running at once would need per-repo adapter
  sections; that's a small code change, not yet built (do it when a real
  second consumer with a different schema arrives).
- **GPU-hours is host-attributed.** On GB10's unified memory with no MIG a
  single GPU is shared; concurrent jobs each see whole-GPU util. The number is
  a ledger currency, not a per-job isolation measurement — it's labelled as
  such in the UI.

See also: `README.md` (ops), `planning/SPARK_ORCHESTRATOR_SPEC.md` (scheduler
design), `planning/DASHBOARD_SPEC.md` (dashboard design), `tools/dashboard/
README.md` (dashboard ops).
