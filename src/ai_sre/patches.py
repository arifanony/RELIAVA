"""FR-7: patch candidates as unified diffs on allowed paths.

Each candidate renders a real unified diff (stdlib difflib) and is
identified by SHA-256 over diff + base revision. At most 3 candidates per
diagnosis; paths outside the allowed roots are rejected.
"""

from __future__ import annotations

import difflib
import hashlib
from dataclasses import dataclass
from pathlib import Path

MAX_CANDIDATES = 3


@dataclass
class PatchCandidate:
    path: str
    old: str
    new: str
    base_revision: str

    @property
    def diff(self) -> str:
        return "".join(
            difflib.unified_diff(
                self.old.splitlines(keepends=True),
                self.new.splitlines(keepends=True),
                fromfile=f"a/{self.path}",
                tofile=f"b/{self.path}",
            )
        )

    @property
    def digest(self) -> str:
        return hashlib.sha256((self.diff + self.base_revision).encode()).hexdigest()


def _inside_roots(path: str, allowed_roots: tuple[str, ...]) -> bool:
    candidate = Path(path)
    if candidate.is_absolute() or ".." in candidate.parts:
        return False
    return any(
        candidate == Path(root) or root in candidate.parents
        for root in (Path(r) for r in allowed_roots)
    )


def propose_patches(
    candidates: list[PatchCandidate], allowed_roots: tuple[str, ...]
) -> list[PatchCandidate]:
    if len(candidates) > MAX_CANDIDATES:
        raise ValueError(f"at most {MAX_CANDIDATES} candidates per diagnosis")
    for candidate in candidates:
        if not _inside_roots(candidate.path, allowed_roots):
            raise ValueError(f"path not allowed: {candidate.path}")
        if not candidate.old or candidate.old == candidate.new:
            raise ValueError(f"empty or no-op patch: {candidate.path}")
    return candidates


def apply_patch(tree_dir: str, candidate: PatchCandidate) -> None:
    """Apply by exact single replacement of the old block."""
    target = Path(tree_dir) / candidate.path
    text = target.read_text()
    if text.count(candidate.old) != 1:
        raise ValueError(f"old block must occur exactly once in {candidate.path}")
    target.write_text(text.replace(candidate.old, candidate.new, 1))
