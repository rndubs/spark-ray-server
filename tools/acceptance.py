#!/usr/bin/env python3
"""Acceptance gates 2-5 (spec §7), driven from the Mac via sparkctl.

Gate 1 (Ray aarch64) is checked at install; gate 6 (reboot survival) and
gate 7 (real-workload budgets) are run manually — results for all gates are
recorded in planning/ACCEPTANCE.md.

Assumes schedulable_mem_gb = 40 (current capacity.toml) and an otherwise
idle scheduler.
"""

import json
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPARKCTL = str(ROOT / ".venv" / "bin" / "sparkctl")
HOST = "rwhit"
RUNS = "/home/rwhit/spark-runs"
REPO = "/home/rwhit/projection-meshing"
CAPACITY = 40.0


def sh(*args, check=True, timeout=180):
    res = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise SystemExit(f"cmd failed ({res.returncode}): {args}\n{res.stdout}{res.stderr}")
    return res.stdout.strip()


def sparkctl(*args, **kw):
    return sh(SPARKCTL, *args, **kw)


def ssh(cmd, **kw):
    return sh("ssh", "-4", HOST, cmd, **kw)


def submit(name, cmd, mem_gb, *extra):
    return sparkctl("submit", "--name", name, "--cmd", cmd,
                    "--mem-gb", str(mem_gb), *extra).splitlines()[-1].strip()


def ray_status(run_id):
    for line in sparkctl("status", run_id).splitlines():
        if line.startswith("ray:"):
            return line.split()[1]
    raise SystemExit(f"no ray status for {run_id}")


def wait_status(run_id, statuses, timeout=300):
    t0 = time.time()
    while time.time() - t0 < timeout:
        st = ray_status(run_id)
        if st in statuses:
            return st
        time.sleep(1)
    raise SystemExit(f"timed out waiting for {run_id} -> {statuses} (now {st})")


def ledger_rows(run_id):
    out = ssh(f"grep -F '\"run_id\":\"{run_id}\"' {RUNS}/ledger.jsonl || true")
    return [json.loads(line) for line in out.splitlines() if line.strip()]


def end_row(run_id):
    rows = [r for r in ledger_rows(run_id) if r["status"] != "started"]
    assert len(rows) == 1, f"{run_id}: expected exactly one end row, got {rows}"
    return rows[0]


def start_row(run_id):
    return [r for r in ledger_rows(run_id) if r["status"] == "started"][0]


def gate2():
    # blocking: two 0.6N jobs -> second queues until first finishes
    a = submit("gate2-a", "sleep 15", 24)
    b = submit("gate2-b", "sleep 1", 24)
    wait_status(a, {"RUNNING"})
    time.sleep(2)
    st_b = ray_status(b)
    assert st_b == "PENDING", f"second 24 GB job should queue, got {st_b}"
    wait_status(a, {"SUCCEEDED"})
    wait_status(b, {"SUCCEEDED"})
    a_end, b_start = end_row(a)["ts"], start_row(b)["ts"]
    assert b_start >= a_end, f"b started {b_start} before a ended {a_end}"
    print(f"gate2 blocking: PASS (b started {b_start} >= a ended {a_end})")

    # zero-sleep race: 10 x 13 GB submitted back-to-back; never > capacity
    ids = [submit(f"gate2-race-{i}", "sleep 4", 13) for i in range(10)]
    for rid in ids:
        wait_status(rid, {"SUCCEEDED"}, timeout=600)
    events = []
    for rid in ids:
        events.append((start_row(rid)["ts"], 13.0))
        events.append((end_row(rid)["ts"], -13.0))
    events.sort(key=lambda e: (e[0], -e[1]))  # pessimistic: starts first on ties
    cur = peak = 0.0
    for _, delta in events:
        cur += delta
        peak = max(peak, cur)
    assert peak <= CAPACITY, f"oversubscribed: peak reserved {peak} > {CAPACITY}"
    print(f"gate2 race: PASS (peak reserved {peak:g}/{CAPACITY:g} GB across 10 jobs)")


def gate3():
    sha0 = ssh(f"git -C {REPO} rev-parse HEAD")
    a = submit("gate3-pin", "git rev-parse HEAD && sleep 25", 1)
    wait_status(a, {"RUNNING"})
    time.sleep(4)  # let the driver finish worktree add
    ssh(f"git -C {REPO} -c user.name=gate3 -c user.email=gate3@test "
        f"commit --allow-empty -q -m gate3-pin-test")
    sha1 = ssh(f"git -C {REPO} rev-parse HEAD")
    assert sha1 != sha0
    tree_sha = ssh(f"git -C {RUNS}/trees/{a} rev-parse HEAD")
    assert tree_sha == sha0, f"running tree drifted: {tree_sha} != {sha0}"
    b = submit("gate3-after", "git rev-parse HEAD", 1)
    wait_status(b, {"SUCCEEDED"})
    wait_status(a, {"SUCCEEDED"})
    assert start_row(a)["sha"] == sha0 and start_row(b)["sha"] == sha1
    log_b = sparkctl("logs", b)
    assert sha1 in log_b, f"job b ran at wrong sha: {log_b}"
    ssh(f"git -C {REPO} reset --hard -q {sha0}")  # drop the empty test commit
    print(f"gate3 pinning: PASS (mid-run tree {sha0[:12]}, later job {sha1[:12]})")


def gate4():
    a = submit("gate4-fail", "echo failing; exit 3", 1)
    wait_status(a, {"FAILED"})
    row = end_row(a)
    assert row["status"] == "failed" and row["exit_code"] == 3, row
    assert ssh(f"test -d {RUNS}/trees/{a} && echo yes") == "yes", "failed tree not kept"
    b = submit("gate4-nokeep", "exit 4", 1, "--no-keep-tree")
    wait_status(b, {"FAILED"})
    assert ssh(f"test -d {RUNS}/trees/{b} || echo gone") == "gone", "no-keep tree kept"
    sparkctl("gc", "--ttl-days", "0")
    assert ssh(f"test -d {RUNS}/trees/{a} || echo gone") == "gone", "gc missed tree"
    wt = ssh(f"git -C {REPO} worktree list")
    assert f"{RUNS}/trees/" not in wt, f"stale worktree entries:\n{wt}"
    print("gate4 hygiene: PASS (kept on fail, removed with keep=false, gc swept, list clean)")


def gate5():
    a = submit("gate5-cancel", "sleep 300", 30)
    wait_status(a, {"RUNNING"})
    time.sleep(2)
    sparkctl("cancel", a)
    wait_status(a, {"STOPPED"}, timeout=60)
    row = end_row(a)
    assert row["status"] == "cancelled", row
    b = submit("gate5-free", "true", 30)  # only fits if a's 30 GB was freed
    wait_status(b, {"SUCCEEDED"}, timeout=60)
    c = submit("gate5-timeout", "sleep 300", 1, "--timeout-s", "8")
    wait_status(c, {"FAILED"}, timeout=120)
    row = end_row(c)
    assert row["status"] == "timeout", row
    assert 7 <= row["duration_s"] <= 40, row
    print(f"gate5 cancel+timeout: PASS (cancelled row ok, 30 GB refit ran, "
          f"timeout after {row['duration_s']}s)")
    sparkctl("gc", "--ttl-days", "0", "--force-started")


if __name__ == "__main__":
    gates = sys.argv[1:] or ["2", "3", "4", "5"]
    for g in gates:
        {"2": gate2, "3": gate3, "4": gate4, "5": gate5}[g]()
    print("all requested gates passed")
