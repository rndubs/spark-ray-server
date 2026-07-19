"""Worktree GC (server-side): python -m spark_orchestrator.gc [--ttl-days N] [--dry-run]

Sweeps trees under <runs_root>/trees:
- runs whose latest ledger row is terminal (succeeded/failed/cancelled/timeout)
  and whose tree is older than the TTL -> removed;
- orphan trees with no ledger row at all, older than the TTL -> removed;
- runs still marked `started` are left alone (they may be live; a dead
  driver's tree ages out once its row is superseded or the TTL passes with
  --force-started).
Then prunes worktree metadata in every affected repo.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

from . import config, ledger, worktree

TERMINAL = {"succeeded", "failed", "cancelled", "timeout"}


def sweep(cfg: dict, ttl_days: float, dry_run: bool, force_started: bool) -> dict:
    troot = config.trees_root(cfg)
    latest = ledger.latest_by_run(config.ledger_path(cfg))
    cutoff = time.time() - ttl_days * 86400
    removed, kept, repos = [], [], set()
    if not troot.exists():
        return {"removed": [], "kept": []}
    for tree in sorted(troot.iterdir()):
        if not tree.is_dir():
            continue
        row = latest.get(tree.name)
        status = row["status"] if row else None
        expired = tree.stat().st_mtime < cutoff
        if status == "started" and not force_started:
            kept.append((tree.name, "started"))
            continue
        if not expired:
            kept.append((tree.name, status or "orphan"))
            continue
        repo = worktree.parent_repo_of(tree)
        if dry_run:
            removed.append(tree.name)
            continue
        try:
            if repo is not None:
                worktree.remove(repo, tree)
                repos.add(repo)
            else:
                import shutil
                shutil.rmtree(tree)
            removed.append(tree.name)
        except Exception as e:
            print(f"[gc] failed to remove {tree}: {e}", file=sys.stderr)
    for repo in repos:
        try:
            worktree._git(Path(repo), "worktree", "prune")
        except Exception:
            pass
    return {"removed": removed, "kept": kept}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ttl-days", type=float, default=None)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force-started", action="store_true",
                    help="also sweep expired trees whose ledger still says 'started'")
    args = ap.parse_args()
    cfg = config.load_capacity()
    ttl = args.ttl_days if args.ttl_days is not None else cfg["gc"]["failed_tree_ttl_days"]
    result = sweep(cfg, ttl, args.dry_run, args.force_started)
    verb = "would remove" if args.dry_run else "removed"
    print(f"[gc] {verb} {len(result['removed'])} tree(s): {result['removed']}")
    for name, why in result["kept"]:
        print(f"[gc] kept {name} ({why})")


if __name__ == "__main__":
    main()
