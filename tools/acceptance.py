#!/usr/bin/env python3
"""Acceptance gates 2-5, 8 and 9 (spec §7), driven from the Mac via sparkctl.

Gate 1 (Ray aarch64) is checked at install; gate 6 (reboot survival) and
gate 7 (real-workload budgets) are run manually — results for all gates are
recorded in planning/ACCEPTANCE.md.

Assumes an otherwise idle scheduler. Job sizes are derived from the live
`schedulable_mem_gb` rather than hardcoded, so retuning capacity.toml does not
silently turn "this job must queue" into "this job fits".
"""

import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SPARKCTL = str(ROOT / ".venv" / "bin" / "sparkctl")
if not Path(SPARKCTL).exists():
    SPARKCTL = shutil.which("sparkctl") or SPARKCTL
HOST = "rwhit"
RUNS = "/home/rwhit/spark-runs"
REPO = "/home/rwhit/projection-meshing"


def sh(*args, check=True, timeout=180):
    res = subprocess.run(list(args), capture_output=True, text=True, timeout=timeout)
    if check and res.returncode != 0:
        raise SystemExit(f"cmd failed ({res.returncode}): {args}\n{res.stdout}{res.stderr}")
    return res.stdout.strip()


def sparkctl(*args, **kw):
    return sh(SPARKCTL, *args, **kw)


def ssh(cmd, **kw):
    return sh("ssh", "-4", HOST, cmd, **kw)


def capacity() -> float:
    line = sparkctl("status").splitlines()[0]
    m = re.search(r"capacity:\s*([\d.]+)/([\d.]+) GB", line)
    if not m:
        raise SystemExit(f"cannot parse capacity from: {line}")
    if float(m.group(1)) != 0:
        raise SystemExit(f"scheduler is not idle: {line}")
    return float(m.group(2))


CAPACITY = capacity()


def submit(name, cmd, mem_gb, *extra):
    return sparkctl("submit", "--name", name, "--cmd", cmd,
                    "--mem-gb", str(mem_gb),
                    "--desc", f"acceptance {name} ({mem_gb} GB)",
                    *extra).splitlines()[-1].strip()


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


NON_END = {"queued", "started"}


def end_row(run_id):
    rows = [r for r in ledger_rows(run_id) if r["status"] not in NON_END]
    assert len(rows) == 1, f"{run_id}: expected exactly one end row, got {rows}"
    return rows[0]


def queued_row(run_id):
    rows = [r for r in ledger_rows(run_id) if r["status"] == "queued"]
    assert len(rows) == 1, f"{run_id}: expected a queued row, got {ledger_rows(run_id)}"
    return rows[0]


def start_row(run_id):
    return [r for r in ledger_rows(run_id) if r["status"] == "started"][0]


def gate2():
    # blocking: two 0.6N jobs -> second waits in the ORCHESTRATOR queue until
    # the first finishes. It must not be handed to Ray while it cannot run:
    # queueing inside Ray is what let the 900 s job-supervisor start timeout
    # kill 78 of 96 shards on 2026-07-25, with no ledger row and no output.
    big = round(CAPACITY * 0.6)          # two of these cannot co-run
    a = submit("gate2-a", "sleep 15", big)
    b = submit("gate2-b", "sleep 1", big)
    wait_status(a, {"RUNNING"})
    time.sleep(2)
    st_b = ray_status(b)
    assert st_b == "QUEUED", f"second {big} GB job should be held by us, got {st_b}"
    assert queued_row(b), "a queued job must be in the ledger from submit"
    wait_status(a, {"SUCCEEDED"})
    wait_status(b, {"SUCCEEDED"})
    a_end, b_start = end_row(a)["ts"], start_row(b)["ts"]
    assert b_start >= a_end, f"b started {b_start} before a ended {a_end}"
    print(f"gate2 blocking: PASS (b started {b_start} >= a ended {a_end})")

    # zero-sleep race: 10 jobs summing to ~1.25x capacity, submitted
    # back-to-back with no pacing; reserved memory must never exceed capacity
    small = max(1, round(CAPACITY / 8))
    ids = [submit(f"gate2-race-{i}", "sleep 4", small) for i in range(10)]
    for rid in ids:
        wait_status(rid, {"SUCCEEDED"}, timeout=600)
    events = []
    for rid in ids:
        events.append((start_row(rid)["ts"], float(small)))
        events.append((end_row(rid)["ts"], -float(small)))
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
    # Scope this to the runs this gate created: `worktree list` is global, and
    # a leftover tree from an unrelated incident is not a gate 4 failure.
    lines = ssh(f"git -C {REPO} worktree list").splitlines()
    mine = [ln for ln in lines if f"{RUNS}/trees/{a}" in ln or f"{RUNS}/trees/{b}" in ln]
    assert not mine, "stale worktree entries for this gate's runs:\n" + "\n".join(mine)
    others = [ln for ln in lines if f"{RUNS}/trees/" in ln]
    if others:
        print(f"gate4 note: {len(others)} unrelated run tree(s) still registered "
              f"(pre-existing, not from this gate):\n  " + "\n  ".join(others))
    print("gate4 hygiene: PASS (kept on fail, removed with keep=false, gc swept, list clean)")


def gate5():
    half = round(CAPACITY * 0.3)
    a = submit("gate5-cancel", "sleep 300", half)
    wait_status(a, {"RUNNING"})
    time.sleep(2)
    sparkctl("cancel", a)
    wait_status(a, {"STOPPED"}, timeout=60)
    row = end_row(a)
    assert row["status"] == "cancelled", row
    b = submit("gate5-free", "true", half)  # only fits if a's memory was freed
    wait_status(b, {"SUCCEEDED"}, timeout=60)
    c = submit("gate5-timeout", "sleep 300", 1, "--timeout-s", "8")
    wait_status(c, {"FAILED"}, timeout=120)
    row = end_row(c)
    assert row["status"] == "timeout", row
    assert 7 <= row["duration_s"] <= 40, row
    print(f"gate5 cancel+timeout: PASS (cancelled row ok, {half} GB refit ran, "
          f"timeout after {row['duration_s']}s)")
    sparkctl("gc", "--ttl-days", "0", "--force-started")


def gate8():
    """Regression gate for the 2026-07-25 silent-loss incident.

    Oversubscribe the box by 3x in one burst, with no client-side pacing, and
    require that every single job is accounted for: a `queued` ledger row from
    submit, then exactly one terminal row, and none of them `lost`. The old
    behaviour (submitting straight to Ray) lost 78 of 96 here.
    """
    n = int(CAPACITY // 4) * 3
    ids = [submit(f"gate8-{i:03d}", "sleep 5", 4) for i in range(n)]

    seen_queued = 0
    for rid in ids:                       # every job is visible immediately
        seen_queued += bool(ledger_rows(rid))
    assert seen_queued == n, f"only {seen_queued}/{n} jobs are in the ledger"

    for rid in ids:
        wait_status(rid, {"SUCCEEDED"}, timeout=1800)
    for rid in ids:
        row = end_row(rid)
        assert row["status"] == "succeeded", f"{rid}: {row}"
        queued_row(rid)
    print(f"gate8 no-silent-loss: PASS ({n} jobs submitted at once into "
          f"{CAPACITY:g} GB, {n} succeeded, 0 lost)")


def gate9():
    """Regression gate for the 2026-07-26 host-OOM incident.

    A job declared 8 GB and used ~119 GB on a 121 GB box; the kernel killed
    the biggest thing on the host — the Ray head — five separate times, each
    one taking every other running job with it. The declaration was admission
    bookkeeping that nothing enforced.

    So: a job that deliberately over-allocates must die on its own, at its own
    declared limit, with a terminal ledger row that says memory (not timeout,
    not lost) — and the box, the head and a concurrent job must be untouched.
    """
    limit = 4                       # bounded on purpose: 4 GB of a 112 GB box
    innocent = submit("gate9-innocent", "sleep 45", 2)
    wait_status(innocent, {"RUNNING"})

    # Bounded at 12 GB, not unbounded: if enforcement were broken this gate
    # must waste a little memory, not repeat the incident it tests for.
    hog = ("python3 -u -c '\n"
           "b = []\n"
           "for i in range(24):\n"
           "    b.append(bytearray(512 * 1024 * 1024))\n"
           "    print(len(b) // 2, \"GB\")\n"
           "'")
    a = submit("gate9-oom", hog, limit)
    wait_status(a, {"FAILED"}, timeout=300)
    row = end_row(a)
    assert row["status"] == "oom_killed", \
        f"memory kill must not be reported as {row['status']!r}: {row}"
    assert row["mem_enforced"] is True, row
    assert row["mem_limit_gb"] == float(limit), row
    # oom_kill_count / mem_peak_gb are polled samples: a job that dies inside
    # one poll interval legitimately reports 0 / None, and the status comes
    # from systemd's own Result. So assert they are CONSISTENT, not present.
    assert (row["mem_peak_gb"] or 0) <= limit + 0.5, \
        f"job exceeded its cap before dying: {row}"
    assert "exceeded its declared memory" in (row.get("reason") or ""), row

    # the whole point: nothing else noticed
    assert ray_status(innocent) in ("RUNNING", "SUCCEEDED"), \
        "concurrent job died with the over-allocating one"
    wait_status(innocent, {"SUCCEEDED"}, timeout=120)
    assert end_row(innocent)["status"] == "succeeded"
    assert ssh("systemctl --user is-active spark-ray.service") == "active", \
        "the Ray head did not survive an over-allocating job"

    # and a job that stays under its declaration is unaffected by the cap
    b = submit("gate9-under",
               "python3 -c \"a=bytearray(1024**3); print(len(a))\"", 4)
    wait_status(b, {"SUCCEEDED"}, timeout=300)
    brow = end_row(b)
    assert brow["status"] == "succeeded" and brow["exit_code"] == 0, brow
    assert brow["mem_enforced"] is True, brow

    print(f"gate9 memory enforcement: PASS (oom_killed at {row['mem_limit_gb']:g} GB, "
          f"sampled peak {row['mem_peak_gb']} GB, head + concurrent job "
          f"survived, under-budget 1 GB job succeeded)")


if __name__ == "__main__":
    gates = sys.argv[1:] or ["2", "3", "4", "5", "8", "9"]
    for g in gates:
        {"2": gate2, "3": gate3, "4": gate4, "5": gate5, "8": gate8,
         "9": gate9}[g]()
    print("all requested gates passed")
