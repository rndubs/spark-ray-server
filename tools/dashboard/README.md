# Spark Training Dashboard (ops)

Read-only, localhost-only web page over the Ray orchestrator: per-job
provenance, live training curves, data-read audit, and GPU-hours. Design of
record: `planning/DASHBOARD_SPEC.md`. Code: `src/spark_orchestrator/dashboard/`.

## Reach it (from the Mac)

```sh
sparkctl dashboard          # opens the ssh -4 -L tunnel, prints the URL
sparkctl dashboard --open   # ...and opens it in your browser
# then browse http://localhost:8787
```

Equivalent by hand: `ssh -4 -L 8787:127.0.0.1:8787 rwhit` → `localhost:8787`.
The dashboard binds **127.0.0.1 only** on the Spark (it refuses to start on
anything else); the SSH tunnel is the auth boundary, same as the DGX Dashboard.

## What it reads

- **Ray Jobs API** (`127.0.0.1:8265`) — authoritative status, per-job metadata.
- **`~/spark-runs/<run_id>/dashboard.json`** — the sidecar `sparkctl submit`
  writes (desc, sha, branch, dirty, variant, seeds, declared inputs, pid).
  The durable, Ray-restart-surviving source; also backs the history table.
- **`metrics.jsonl`** under the job's artifacts dir — training curves +
  progress (tailed incrementally).
- **`nvidia-smi` + psutil + `/proc`** — host strip, per-proc RSS/GPU mem, and
  the best-effort fd sampler that flags reads under `eval/frozen/`.

## Registering a job for the dashboard

`--desc` is required; the rest is optional but makes the row richer:

```sh
sparkctl submit --name sft-baseline --class hexgen-train-27m \
  --desc "L3 P21 SFT baseline, lr 3e-4" \
  --variant baseline --seed 0 \
  --input /data/hexforge-data/corpora/p21_sft \
  --cmd 'python hexgen/decoder/train.py --run-dir "$SPARK_ARTIFACTS_DIR" \
         --variant baseline --seed 0 --steps 1000'
```

- `--input` is directory-granularity and contamination-checked against
  `eval/frozen/`. Declaring a frozen path (or the fd sampler *observing* a
  read under it while RUNNING) raises a red badge on the row.
- `--metrics PATH` pins the metrics file; omit it and the dashboard globs the
  newest `metrics.jsonl` under the artifacts dir (hexgen mints
  `<variant>/<run_id>/` itself, so globbing is the norm).

## Service

```sh
systemctl --user status  spark-dashboard.service
systemctl --user restart spark-dashboard.service   # safe: kills no jobs
journalctl --user -u spark-dashboard.service -f
```

`tools/deploy.sh` installs it (with the `fastapi`/`uvicorn`/`psutil` extras)
and always restarts it on deploy — it is read-only, so bouncing it never
touches running training jobs (unlike the Ray head).

## Config

`[dashboard]` in `~/.config/spark-orchestrator/capacity.toml` (see
`config/capacity.example.toml`): `port`, `data_root`, `disk_watch`,
`stall_threshold_s`, `history_depth`, poll cadences, and the per-project
adapter knobs `metrics_filename` + `loss_keys`.

## Known gap

hexgen's `train.py` writes `metrics.jsonl` once at the end, so live curves and
live step-progress only appear once training finishes; STALLED is judged off
the log mtime meanwhile. The fix is a one-line append-mode change in the
consumer repo — see `planning/DASHBOARD_SPEC.md`. The dashboard already tails
incrementally and will show live curves the moment hexgen streams.
