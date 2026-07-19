"""sparkctl: thin client CLI (runs on the Mac, talks to the Spark over an
auto-managed ssh -4 tunnel + ssh exec). Commands per spec §3."""

from __future__ import annotations

import argparse
import base64
import json
import secrets
import shlex
import subprocess
import sys
import time

from . import config, ledger, tunnel
from .client import RayJobs

RAY_TERMINAL = {"SUCCEEDED", "FAILED", "STOPPED"}


def _remote_capacity(ccfg: dict) -> dict:
    res = tunnel.ssh_run(
        ccfg["host"],
        "cat ~/.config/spark-orchestrator/capacity.toml 2>/dev/null"
        " || cat /etc/spark-orchestrator/capacity.toml",
    )
    return config.load_capacity(text=res.stdout)


def _jobs(ccfg: dict) -> RayJobs:
    tunnel.ensure_tunnel(ccfg)
    return RayJobs(ccfg["local_port"])


def _run_id(name: str) -> str:
    safe = "".join(c if c.isalnum() or c in "-_" else "-" for c in name)[:40]
    return f"{safe}-{time.strftime('%Y%m%d-%H%M%S', time.gmtime())}-{secrets.token_hex(2)}"


# ---------------------------------------------------------------- submit

def cmd_submit(args, ccfg) -> int:
    spec = {}
    if args.spec:
        with open(args.spec) as f:
            spec = json.load(f)
    if args.name: spec["name"] = args.name
    if args.repo: spec["repo_path"] = args.repo
    if args.ref: spec["ref"] = args.ref
    if args.cmd: spec["cmd"] = args.cmd
    if args.mem_gb is not None: spec["mem_gb"] = args.mem_gb
    if args.job_class: spec["job_class"] = args.job_class
    if args.artifacts_dir: spec["artifacts_dir"] = args.artifacts_dir
    if args.timeout_s is not None: spec["timeout_s"] = args.timeout_s
    if args.no_keep_tree: spec["keep_tree_on_failure"] = False
    for kv in args.env or []:
        k, _, v = kv.partition("=")
        spec.setdefault("env", {})[k] = v

    # dashboard contract (job-metadata contract in planning/DASHBOARD_SPEC.md)
    dash = spec.get("dashboard") or {}
    if args.desc: dash["desc"] = args.desc
    if args.variant: dash["variant"] = args.variant
    if args.seed: dash["seeds"] = args.seed
    if args.input: dash["declared_inputs"] = args.input
    if args.metrics: dash["metrics_path"] = args.metrics

    if not spec.get("name") or not spec.get("cmd"):
        sys.exit("submit needs at least --name and --cmd (or a --spec file with them)")
    if not dash.get("desc"):
        sys.exit('submit needs --desc "one line" (what is this job? shown on the '
                 "dashboard and copied into the LEDGER)")
    spec.setdefault("repo_path", ccfg["default_repo"])
    spec.setdefault("ref", "HEAD")
    spec.setdefault("keep_tree_on_failure", True)

    cap = _remote_capacity(ccfg)
    if spec.get("mem_gb") is None:
        spec["mem_gb"] = config.budget_for(cap, spec.get("job_class"))
    spec["mem_gb"] = float(spec["mem_gb"])
    if spec["mem_gb"] > cap["schedulable_mem_gb"]:
        sys.exit(
            f"mem_gb={spec['mem_gb']} exceeds schedulable capacity "
            f"{cap['schedulable_mem_gb']} GB — this job can never run"
        )

    # ref -> SHA at submission; the SHA is what gets pinned (spec §2)
    res = tunnel.ssh_run(
        ccfg["host"],
        f"git -C {shlex.quote(spec['repo_path'])} rev-parse --verify "
        f"{shlex.quote(spec['ref'])}^{{commit}}",
    )
    spec["sha"] = res.stdout.strip()
    spec["run_id"] = _run_id(spec["name"])

    # branch + dirty flag of the Spark checkout at submit time. NB: the job
    # runs from a clean worktree at the SHA, so a dirty checkout is NOT in
    # the run — the badge means "uncommitted work existed that this run does
    # not include".
    res = tunnel.ssh_run(
        ccfg["host"],
        f"git -C {shlex.quote(spec['repo_path'])} rev-parse --abbrev-ref "
        f"{shlex.quote(spec['ref'])}; "
        f"git -C {shlex.quote(spec['repo_path'])} status --porcelain -uno | head -1",
        check=False,
    )
    lines = res.stdout.splitlines()
    branch = lines[0].strip() if lines else ""
    dash["branch"] = spec["ref"] if branch in ("", "HEAD", spec["sha"]) else branch
    dash["dirty"] = len(lines) > 1 and bool(lines[1].strip())
    dash["submitted_ts"] = ledger.utc_ts()
    spec.pop("ref", None)
    spec["dashboard"] = dash

    driver_spec = {k: spec.get(k) for k in (
        "run_id", "name", "repo_path", "sha", "cmd", "env", "mem_gb",
        "artifacts_dir", "timeout_s", "keep_tree_on_failure", "dashboard")}
    b64 = base64.b64encode(json.dumps(driver_spec).encode()).decode()
    entrypoint = (
        f"{ccfg['orchestrator_root']}/.venv/bin/python -m spark_orchestrator.driver {b64}"
    )
    jobs = _jobs(ccfg)
    jobs.submit(
        entrypoint=entrypoint,
        submission_id=spec["run_id"],
        entrypoint_resources={"mem_gb": spec["mem_gb"]},
        metadata={"name": spec["name"], "sha": spec["sha"],
                  "mem_gb": str(spec["mem_gb"]),
                  "desc": dash["desc"][:200],
                  "branch": dash["branch"], "dirty": "1" if dash["dirty"] else "0",
                  **({"variant": dash["variant"]} if dash.get("variant") else {})},
    )
    print(spec["run_id"])
    if args.wait:
        return _wait(jobs, spec["run_id"])
    return 0


def _wait(jobs: RayJobs, run_id: str) -> int:
    last = None
    while True:
        st = jobs.get(run_id)["status"]
        if st != last:
            print(f"[{time.strftime('%H:%M:%S')}] {st}", file=sys.stderr)
            last = st
        if st in RAY_TERMINAL:
            return 0 if st == "SUCCEEDED" else 1
        time.sleep(2)


# ---------------------------------------------------------------- status

def cmd_status(args, ccfg) -> int:
    jobs = _jobs(ccfg)
    if args.run_id:
        j = jobs.get(args.run_id)
        print(f"run_id:  {args.run_id}")
        print(f"ray:     {j['status']}  ({j.get('message') or ''})".rstrip())
        for k in ("name", "sha", "mem_gb"):
            if k in (j.get("metadata") or {}):
                print(f"{k}:{' ' * (8 - len(k))}{j['metadata'][k]}")
        res = tunnel.ssh_run(
            ccfg["host"],
            f"grep -F '\"run_id\":\"{args.run_id}\"' {ccfg['runs_root']}/ledger.jsonl || true",
        )
        for line in res.stdout.splitlines():
            row = json.loads(line)
            extra = "" if row["status"] == "started" else (
                f" exit_code={row.get('exit_code')} duration_s={row.get('duration_s')}")
            print(f"ledger:  {row['ts']} {row['status']}{extra}")
        return 0

    cap = _remote_capacity(ccfg)
    active = [j for j in jobs.list() if j["status"] in ("PENDING", "RUNNING")]
    used = sum(float((j.get("metadata") or {}).get("mem_gb", 0))
               for j in active if j["status"] == "RUNNING")
    sched = cap["schedulable_mem_gb"]
    print(f"capacity: {used:g}/{sched:g} GB reserved ({sched - used:g} free; "
          f"vllm_reserve={cap['vllm_reserve_gb']:g}, config {cap['_path']})")
    if not active:
        print("no running or queued jobs")
    for j in sorted(active, key=lambda j: j.get("submission_id") or ""):
        md = j.get("metadata") or {}
        print(f"  {j.get('submission_id'):44s} {j['status']:8s} "
              f"mem_gb={md.get('mem_gb', '?'):>5s} sha={md.get('sha', '?')[:12]}")
    return 0


# ---------------------------------------------------------------- the rest

def cmd_logs(args, ccfg) -> int:
    follow = "-f " if args.follow else ""
    return subprocess.call(tunnel.ssh_cmd(ccfg["host"]) + [
        f"tail -n +1 {follow}{ccfg['runs_root']}/{shlex.quote(args.run_id)}/job.log"
    ])


def cmd_cancel(args, ccfg) -> int:
    jobs = _jobs(ccfg)
    out = jobs.stop(args.run_id)
    print(f"stop requested: {out}")
    return 0


def cmd_list(args, ccfg) -> int:
    res = tunnel.ssh_run(
        ccfg["host"], f"tail -n {args.n * 2} {ccfg['runs_root']}/ledger.jsonl || true"
    )
    latest: dict[str, dict] = {}
    for line in res.stdout.splitlines():
        if line.strip():
            row = json.loads(line)
            latest[row["run_id"]] = row
    rows = list(latest.values())[-args.n:]
    if not rows:
        print("ledger empty")
        return 0
    for row in rows:
        ec = row.get("exit_code")
        dur = row.get("duration_s")
        print(f"{row['ts']}  {row['run_id']:44s} {row['status']:9s} "
              f"mem_gb={row['mem_gb']:<5g} sha={row['sha'][:12]}"
              + (f" exit={ec}" if ec is not None else "")
              + (f" {dur}s" if dur is not None else ""))
    return 0


def cmd_gc(args, ccfg) -> int:
    flags = ""
    if args.ttl_days is not None:
        flags += f" --ttl-days {args.ttl_days}"
    if args.dry_run:
        flags += " --dry-run"
    if args.force_started:
        flags += " --force-started"
    res = tunnel.ssh_run(
        ccfg["host"],
        f"{ccfg['orchestrator_root']}/.venv/bin/python -m spark_orchestrator.gc{flags}",
        check=False, timeout=300,
    )
    sys.stdout.write(res.stdout)
    sys.stderr.write(res.stderr)
    return res.returncode


def cmd_dashboard(args, ccfg) -> int:
    port = ccfg.get("dashboard_local_port", 8787)
    tunnel.ensure_forward(
        ccfg["host"], port, ccfg.get("dashboard_remote_port", 8787), "/api/host"
    )
    url = f"http://127.0.0.1:{port}"
    print(url)
    if args.open:
        subprocess.call(["open", url])
    return 0


def cmd_doctor(args, ccfg) -> int:
    ok = True

    def check(label: str, fn):
        nonlocal ok
        try:
            detail = fn()
            print(f"  ok    {label}" + (f" — {detail}" if detail else ""))
        except Exception as e:
            ok = False
            print(f"  FAIL  {label} — {e}")

    print(f"doctor ({ccfg['host']}):")
    check("ssh reachable (ssh -4)", lambda: tunnel.ssh_run(ccfg["host"], "true") and "")
    def _tun():
        tunnel.ensure_tunnel(ccfg)
        return f"jobs API on 127.0.0.1:{ccfg['local_port']}"
    check("tunnel + Ray jobs API", _tun)
    check("spark-ray.service active", lambda: tunnel.ssh_run(
        ccfg["host"], "systemctl --user is-active spark-ray.service").stdout.strip())
    check("spark-dashboard.service active", lambda: tunnel.ssh_run(
        ccfg["host"], "systemctl --user is-active spark-dashboard.service",
        check=False).stdout.strip() or "inactive")
    def _cap():
        cap = _remote_capacity(ccfg)
        if cap["schedulable_mem_gb"] <= 0:
            raise RuntimeError(f"schedulable_mem_gb={cap['schedulable_mem_gb']}")
        return (f"schedulable={cap['schedulable_mem_gb']:g} GB "
                f"(total {cap['total_mem_gb']:g} - os {cap['os_reserve_gb']:g}"
                f" - vllm {cap['vllm_reserve_gb']:g})")
    check("capacity config sane", _cap)
    check("ledger writable", lambda: tunnel.ssh_run(
        ccfg["host"],
        f"mkdir -p {ccfg['runs_root']} && touch {ccfg['runs_root']}/ledger.jsonl"
        f" && test -w {ccfg['runs_root']}/ledger.jsonl && echo yes").stdout.strip())
    check("worktree root writable", lambda: tunnel.ssh_run(
        ccfg["host"],
        f"mkdir -p {ccfg['runs_root']}/trees && test -w {ccfg['runs_root']}/trees"
        f" && echo yes").stdout.strip())
    check("consumer repo reachable", lambda: tunnel.ssh_run(
        ccfg["host"],
        f"git -C {ccfg['default_repo']} rev-parse --short HEAD").stdout.strip())
    print("doctor: all checks passed" if ok else "doctor: FAILURES above")
    return 0 if ok else 1


# ---------------------------------------------------------------- main

def main() -> None:
    ap = argparse.ArgumentParser(prog="sparkctl",
                                 description="Job scheduling client for the DGX Spark")
    sub = ap.add_subparsers(dest="command", required=True)

    s = sub.add_parser("submit", help="submit a job (prints run_id)")
    s.add_argument("--spec", help="job spec JSON file (flags override it)")
    s.add_argument("--name")
    s.add_argument("--repo", help="repo path on the Spark")
    s.add_argument("--ref", help="git ref, resolved to a SHA at submission (default HEAD)")
    s.add_argument("--cmd", help="shell command, cwd = pinned worktree")
    s.add_argument("--env", action="append", metavar="K=V")
    s.add_argument("--mem-gb", type=float)
    s.add_argument("--class", dest="job_class", help="budget class from capacity.toml")
    s.add_argument("--artifacts-dir")
    s.add_argument("--timeout-s", type=float)
    s.add_argument("--no-keep-tree", action="store_true",
                   help="remove worktree even on failure")
    s.add_argument("--wait", action="store_true")
    s.add_argument("--desc", help="required one-line description (dashboard + LEDGER)")
    s.add_argument("--variant", help="model/config variant name (dashboard)")
    s.add_argument("--seed", action="append", metavar="N",
                   help="seed(s) used by the job (repeatable)")
    s.add_argument("--input", action="append", metavar="PATH", dest="input",
                   help="declared input path the job reads (dir granularity, "
                        "repeatable; contamination-checked against eval/frozen)")
    s.add_argument("--metrics", metavar="PATH",
                   help="where the job writes metrics.jsonl (absolute, or "
                        "relative to the artifacts dir)")
    s.set_defaults(fn=cmd_submit)

    s = sub.add_parser("status", help="one job, or running/queued + capacity")
    s.add_argument("run_id", nargs="?")
    s.set_defaults(fn=cmd_status)

    s = sub.add_parser("logs", help="stream a job's log")
    s.add_argument("run_id")
    s.add_argument("-f", "--follow", action="store_true")
    s.set_defaults(fn=cmd_logs)

    s = sub.add_parser("cancel", help="stop a job")
    s.add_argument("run_id")
    s.set_defaults(fn=cmd_cancel)

    s = sub.add_parser("list", help="recent ledger rows")
    s.add_argument("-n", type=int, default=20)
    s.set_defaults(fn=cmd_list)

    s = sub.add_parser("gc", help="sweep expired worktrees on the Spark")
    s.add_argument("--ttl-days", type=float)
    s.add_argument("--dry-run", action="store_true")
    s.add_argument("--force-started", action="store_true")
    s.set_defaults(fn=cmd_gc)

    s = sub.add_parser("dashboard", help="tunnel to the training dashboard + print URL")
    s.add_argument("--open", action="store_true", help="also open it in the browser")
    s.set_defaults(fn=cmd_dashboard)

    s = sub.add_parser("doctor", help="triage checks")
    s.set_defaults(fn=cmd_doctor)

    args = ap.parse_args()
    ccfg = config.load_client()
    sys.exit(args.fn(args, ccfg))


if __name__ == "__main__":
    main()
