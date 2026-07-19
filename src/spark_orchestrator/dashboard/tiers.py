"""DATA.md tier tagging for observed/declared read paths.

The canonical data tree (docs/DATA.md) is rooted at `data_root` (default
`/data/hexforge-data`). The first path component under the root is the tier:
`raw/`, `eval/frozen/`, `corpora/`, `banks/`, `checkpoints/`, `derived/`,
`attic/`. Anything not under the root is `outside-tree`. The frozen-protocol
invariant the dashboard surfaces: a training job must not read from
`eval/frozen/` (the L3 contamination rule, ∅-intersection check)."""

from __future__ import annotations

import os
from pathlib import Path

# tier -> severity for the UI. "frozen" is the contamination tier.
FROZEN_TIER = "eval/frozen"


def tier_of(path: str, data_root: str) -> str:
    """Return the DATA.md tier of an absolute path, or 'outside-tree'.
    `eval/frozen` is returned as a two-level tier so callers can flag it."""
    try:
        p = Path(path).resolve()
        root = Path(data_root).resolve()
    except (OSError, RuntimeError):
        return "outside-tree"
    try:
        rel = p.relative_to(root)
    except ValueError:
        return "outside-tree"
    parts = rel.parts
    if not parts:
        return "root"
    if parts[0] == "eval" and len(parts) >= 2 and parts[1] == "frozen":
        return FROZEN_TIER
    return parts[0]


def is_frozen(tier: str) -> bool:
    return tier == FROZEN_TIER


def under_any(path: str, roots: list[str]) -> bool:
    """True if `path` is at or under any of `roots` (declared-input match,
    directory granularity)."""
    try:
        p = os.path.realpath(path)
    except OSError:
        return False
    for r in roots:
        try:
            rp = os.path.realpath(os.path.expanduser(r))
        except OSError:
            continue
        if p == rp or p.startswith(rp.rstrip("/") + "/"):
            return True
    return False
