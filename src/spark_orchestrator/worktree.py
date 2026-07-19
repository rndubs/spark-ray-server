"""Git worktree pinning + gitignored-asset symlinks (spec §2 run-tree manager)."""

from __future__ import annotations

import subprocess
from pathlib import Path


def _git(repo: Path, *args: str) -> str:
    res = subprocess.run(
        ["git", "-C", str(repo), *args], capture_output=True, text=True
    )
    if res.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {res.stderr.strip()}")
    return res.stdout.strip()


def add(repo: Path, sha: str, tree: Path) -> None:
    tree.parent.mkdir(parents=True, exist_ok=True)
    _git(repo, "worktree", "add", "--detach", str(tree), sha)


def symlink_assets(repo: Path, tree: Path, assets: list[str]) -> list[str]:
    """Symlink configured gitignored assets from the main checkout into the
    worktree. If the asset path already exists in the tree (a tracked dir
    with gitignored extras, e.g. data/brepgraph), link the missing entries
    one level down instead of shadowing tracked files."""
    linked = []
    for rel in assets:
        src = repo / rel
        if not src.exists():
            continue
        dst = tree / rel
        if not dst.exists() and not dst.is_symlink():
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.symlink_to(src)
            linked.append(rel)
        elif dst.is_dir() and not dst.is_symlink() and src.is_dir():
            for child in src.iterdir():
                cdst = dst / child.name
                if not cdst.exists() and not cdst.is_symlink():
                    cdst.symlink_to(child)
                    linked.append(f"{rel}/{child.name}")
    return linked


def remove(repo: Path, tree: Path) -> None:
    _git(repo, "worktree", "remove", "--force", str(tree))
    _git(repo, "worktree", "prune")


def parent_repo_of(tree: Path) -> Path | None:
    """Recover the main repo path from a worktree's .git file (for GC of
    orphaned trees whose ledger rows are missing)."""
    gitfile = tree / ".git"
    if not gitfile.is_file():
        return None
    line = gitfile.read_text().strip()
    if not line.startswith("gitdir:"):
        return None
    gitdir = Path(line.split(":", 1)[1].strip())
    # <repo>/.git/worktrees/<name> -> <repo>
    if gitdir.parent.name == "worktrees" and gitdir.parent.parent.name == ".git":
        return gitdir.parent.parent.parent
    return None
