"""Offline tests for the worktree GC sweep (spark_orchestrator.gc).

Runs real git against a temp repo — no Spark, no Ray. The regression these
exist for: on 2026-07-26 a host OOM killed the Ray head while drivers were
still doing `git worktree add`, leaving trees locked with reason
"initializing". `git worktree remove --force` refuses those, so `sparkctl gc`
could never clear them. A terminal run's tree is by definition not in use, so
the sweep must be able to unlock it — but a tree whose row still says
`started` must stay untouched without --force-started, because it may be live.

    python3 -m unittest discover -s tests -v
"""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from spark_orchestrator import config, gc, ledger  # noqa: E402


def git(repo: Path, *args: str) -> str:
    res = subprocess.run(["git", "-C", str(repo), *args],
                         capture_output=True, text=True, check=True)
    return res.stdout.strip()


class GcLockedTreeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        self.repo = root / "repo"
        self.repo.mkdir()
        git(self.repo, "init", "-q", "-b", "main")
        (self.repo / "f.txt").write_text("hi\n")
        git(self.repo, "add", "f.txt")
        git(self.repo, "-c", "user.name=t", "-c", "user.email=t@t",
            "commit", "-q", "-m", "init")
        self.runs = root / "runs"
        (self.runs / "trees").mkdir(parents=True)
        self.cfg = {"paths": {"runs_root": str(self.runs)},
                    "gc": {"failed_tree_ttl_days": 0}}

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def make_tree(self, run_id: str, status: str | None, lock: bool) -> Path:
        tree = config.trees_root(self.cfg) / run_id
        git(self.repo, "worktree", "add", "--detach", "-q", str(tree), "HEAD")
        if lock:
            git(self.repo, "worktree", "lock", "--reason", "initializing", str(tree))
        if status is not None:
            ledger.append(config.ledger_path(self.cfg),
                          {"ts": ledger.utc_ts(), "run_id": run_id,
                           "status": status, "repo_path": str(self.repo)})
        return tree

    def registered(self) -> str:
        return git(self.repo, "worktree", "list")

    def test_terminal_locked_tree_is_removed(self) -> None:
        tree = self.make_tree("r-terminal", "failed", lock=True)
        res = gc.sweep(self.cfg, ttl_days=0, dry_run=False, force_started=False)
        self.assertEqual(res["removed"], ["r-terminal"])
        self.assertFalse(tree.exists())
        self.assertNotIn(str(tree), self.registered())

    def test_started_locked_tree_is_kept_without_force(self) -> None:
        tree = self.make_tree("r-live", "started", lock=True)
        res = gc.sweep(self.cfg, ttl_days=0, dry_run=False, force_started=False)
        self.assertEqual(res["removed"], [])
        self.assertEqual(res["kept"], [("r-live", "started")])
        self.assertTrue(tree.exists())

    def test_started_locked_tree_is_removed_with_force(self) -> None:
        tree = self.make_tree("r-stuck", "started", lock=True)
        res = gc.sweep(self.cfg, ttl_days=0, dry_run=False, force_started=True)
        self.assertEqual(res["removed"], ["r-stuck"])
        self.assertFalse(tree.exists())

    def test_orphan_locked_tree_respects_the_lock(self) -> None:
        # No ledger row: we cannot prove it is dead, so the lock stands and the
        # sweep reports the failure instead of destroying someone's tree.
        tree = self.make_tree("r-orphan", None, lock=True)
        res = gc.sweep(self.cfg, ttl_days=0, dry_run=False, force_started=False)
        self.assertEqual(res["removed"], [])
        self.assertTrue(tree.exists())

    def test_unlocked_terminal_tree_still_removed(self) -> None:
        tree = self.make_tree("r-plain", "succeeded", lock=False)
        res = gc.sweep(self.cfg, ttl_days=0, dry_run=False, force_started=False)
        self.assertEqual(res["removed"], ["r-plain"])
        self.assertFalse(tree.exists())


if __name__ == "__main__":
    unittest.main()
