# Spark Training Dashboard — objective + spec

Status: IMPLEMENTED (v1, 2026-07-18). Original spec authored in
`../projection-meshing`; preserved here as the design of record. Code lives in
`src/spark_orchestrator/dashboard/`; ops in `tools/dashboard/README.md`.
Target machine: DGX Spark (GB10, aarch64, 128 GB unified memory).
First customer: hexgen decoder training (L3 campaign, P21+).

## Objective

One glanceable, localhost-only web page that answers, for every training job
on the Spark: **what is running, on what code, on what data, how far along,
how healthy, and what it has cost so far (GPU-hours)** — without SSHing into
log files. Served like the DGX Dashboard: bound to 127.0.0.1 on the Spark,
reached via `ssh -L`. Read-only in v1: it observes jobs, it does not control
them.

Non-goals (v1): auth/multi-user, job submission or kill from the UI, cluster
federation, historical analytics beyond a simple completed-jobs table, mobile.

## Context

- Jobs run under **Ray** on the Spark (single node). Ray's own dashboard
  (port 8265) already covers cluster internals; do NOT reimplement it. This
  dashboard is the hexgen-aware overlay: per-job provenance, training curves,
  data-audit, GPU-hours. Pull job/actor state from Ray's state API
  (`ray.util.state` / the dashboard REST endpoints on 8265).
- hexgen training is plain single-process torch (`hexgen/decoder/train.py`,
  writes `metrics.jsonl` + `checkpoint.pt` into `runs/hexgen_decoder/
  <variant>/<run_id>/`). It is Ray-agnostic today and should stay that way —
  Ray is the *submission* layer, not a training dependency.
- Canonical data tree: `/data/hexforge-data` per `docs/DATA.md`. The
  campaign's contamination rule (no training read from `eval/frozen/`) is a
  frozen-protocol invariant the dashboard should surface.

## The job-metadata contract (the load-bearing piece)

The dashboard must not guess. Every job is submitted through a thin wrapper —
here, `sparkctl submit` (extended with `--desc/--variant/--seed/--input/
--metrics`), which:

1. Captures at submit time: git sha + branch + **dirty flag** (uncommitted
   changes = loud amber badge), the full command line, a required
   `--desc "one line"`, seeds, variant name, and the **declared input paths**
   (data the job intends to read, repo-external; directory granularity).
2. Writes them as a JSON sidecar `dashboard.json` into the job's run dir
   (written by the driver on the Spark), and registers a subset as Ray job
   metadata (so the dashboard can join Ray's view with the sidecar view by
   run_id).
3. Submits the real command to Ray unchanged.

Jobs not submitted through the wrapper still appear (from Ray state) but
render as "unregistered" with whatever Ray knows — visible, never invisible.

NB (Spark reality): the job runs from a clean worktree pinned at the SHA, so
a "dirty" badge means uncommitted work existed at submit that this run does
NOT include — not that the run itself is dirty.

## What the dashboard shows

Jobs table, job detail (curves + log tail + checkpoint + sidecar), host strip
(GPU/host/Ray), completed-jobs table with one-click LEDGER-line copy. Health
rules drive STALLED + NaN + state-mismatch badges. Data-reads column tags each
declared/observed path with its DATA.md tier and raises red (frozen read) /
amber (undeclared, or frozen declared) badges. See the source for the exact
fields; this file is the design intent.

### Health rules
- STALLED: RUNNING but no `metrics.jsonl` append (or log write) in 10 min
  (configurable). The log-write fallback matters because hexgen currently
  writes `metrics.jsonl` once at the end — see the known gap below.
- NaN/inf in the last loss value → red badge.
- Job process gone but Ray thinks RUNNING → amber "state mismatch".
- FAILED rows keep their last known everything.

## Architecture

- **Backend**: one FastAPI app (`src/spark_orchestrator/dashboard/`), uvicorn
  on `127.0.0.1:8787`. One background thread runs the pollers with small
  caches (Ray state 2s, NVML+psutil 2s, `metrics.jsonl` incremental tails 5s,
  `/proc` fd sampler 30s, run-dir sidecar scan 60s). Endpoints: `GET
  /api/jobs`, `/api/jobs/{id}`, `/api/jobs/{id}/series`, `/api/jobs/{id}/log`,
  `/api/jobs/{id}/git`, `/api/host`, `/api/history`. JSON only.
- **Frontend**: a single static `index.html` + `app.js` (vanilla) + uPlot
  vendored into `static/` (NO CDN — the Spark may be offline behind a bare
  SSH tunnel). Polls every 3s; no websockets in v1.
- **State**: none beyond the run-dir sidecars + a per-run `gpu_hours.json`
  freeze file. No database. Restarting the dashboard loses nothing.
- **Config**: `[dashboard]` in `capacity.toml` — data root, port, poll
  intervals, stall threshold, history depth. hexgen specifics
  (`metrics_filename`, `loss_keys`) are the per-project adapter knobs.
- **Serving**: `spark-dashboard.service` (systemd user unit) on the Spark;
  `tools/dashboard/README.md` documents the tunnel one-liner. `sparkctl
  dashboard` opens the tunnel and prints the URL.

## Security posture

Bind 127.0.0.1 ONLY (asserted at startup, refuses `0.0.0.0`). No auth in v1 —
the SSH tunnel is the auth boundary, same trust model as the DGX Dashboard.
The `/proc` sampler and log reader run as the same user as the jobs; no
privilege escalation.

## GPU-hours (the LEDGER currency)

`gpu_hours = elapsed_wall_hours × mean(host GPU utilisation)` over the job's
RUNNING window. On GB10's unified memory with no MIG, a single GPU is shared,
so this is **host-attributed** util (labelled as such in the UI); concurrent
jobs each see the whole-GPU util. A completed job's number is frozen into
`<run_dir>/gpu_hours.json`, surviving dashboard AND Ray restarts.

## Known gap handed to the consumer repo (not fixable here)

hexgen's `train.py` writes `metrics.jsonl` **once at the end** of training
(it buffers rows in memory, `train.py` writes the file after the loop). The
dashboard tails incrementally and will render live curves + live progress the
moment hexgen streams (append each eval row immediately). Until then, curves
appear at completion and STALLED is judged off the log-file mtime, not the
metrics file. Fixing this is a one-line change in the consumer repo
(open `metrics.jsonl` in append mode, flush per eval) and is out of scope for
the orchestrator, which must not depend on projection-meshing.

## Build order (shipped)

v0 (done): submit contract + jobs table from Ray state + sidecars; host
strip; GPU-hours; completed-jobs table + LEDGER copy. v1 (done): job detail
with curves + log tail; declared-path tier tagging + contamination/undeclared
badges (fd sampler); STALLED/NaN/mismatch health. v1.5 (deferred): pushed
alerts, Serve endpoint health, gated kill button.
