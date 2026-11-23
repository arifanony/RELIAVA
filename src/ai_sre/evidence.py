"""FR-3: scoped evidence collection, Drain3 clustering, AST traceback slicing.

Everything that reaches a model is packed into at most 8000 tokens where
every item carries source, window, and content hash. Drain3 runs with no
persistence handler, so clustering never writes files into the repository.
"""

from __future__ import annotations

import ast
import hashlib
import re
from dataclasses import dataclass, field

from drain3 import TemplateMiner

MAX_PACK_TOKENS = 8000


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // 4)  # heuristic, same basis as gateway caps


def content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


@dataclass
class EvidenceItem:
    id: str
    source: str
    window: str
    text: str
    hash: str = field(init=False)
    tokens: int = field(init=False)

    def __post_init__(self) -> None:
        self.hash = content_hash(self.text)
        self.tokens = estimate_tokens(self.text)


def collect(items: list[tuple[str, str, str]]) -> list[EvidenceItem]:
    """Build items from (source, window, text) triples, stable E1, E2, ... ids."""
    return [
        EvidenceItem(id=f"E{i + 1}", source=source, window=window, text=text)
        for i, (source, window, text) in enumerate(items)
    ]


def cluster_logs(lines: list[str]) -> list[tuple[str, int, list[str]]]:
    """Drain3-cluster raw log lines -> (template, size, sample lines)."""
    miner = TemplateMiner()
    members: dict[int, list[str]] = {}
    templates: dict[int, str] = {}
    order: list[int] = []
    for line in lines:
        result = miner.add_log_message(line)
        cluster_id: int = result["cluster_id"]
        members.setdefault(cluster_id, []).append(line)
        templates[cluster_id] = str(result["template_mined"])
        if cluster_id not in order:
            order.append(cluster_id)
    return [(templates[c], len(members[c]), members[c][:2]) for c in order]


FRAME_RE = re.compile(r'^  File "([^"]+)", line (\d+), in (\S+)\s*$')


def slice_traceback(traceback_text: str, sources: dict[str, str]) -> list[str]:
    """Slice each traceback frame to its enclosing def/class via AST.

    `sources` maps filenames in the traceback to file contents. Frames
    without matching source fall back to the raw frame line.
    """
    chunks: list[str] = []
    for line in traceback_text.splitlines():
        match = FRAME_RE.match(line)
        if not match:
            continue
        filename, lineno, func = match.group(1), int(match.group(2)), match.group(3)
        body = sources.get(filename)
        if body is None:
            chunks.append(f"{filename}:{lineno} in {func} (source unavailable)")
            continue
        try:
            tree = ast.parse(body, filename=filename)
        except SyntaxError:
            chunks.append(f"{filename}:{lineno} in {func} (unparseable)")
            continue
        node = enclosing(tree, lineno)
        if node is None:
            lines = body.splitlines()
            snippet = lines[max(0, lineno - 2) : lineno + 1]
            chunks.append(f"{filename}:{lineno} in {func}\n" + "\n".join(snippet))
        else:
            chunks.append(f"{filename}:{lineno} in {func}\n{ast.get_source_segment(body, node)}")
    return chunks


def enclosing(tree: ast.AST, lineno: int) -> ast.AST | None:
    """Deepest FunctionDef/AsyncFunctionDef/ClassDef spanning lineno."""
    best: ast.AST | None = None
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            start = node.lineno
            end = node.end_lineno or start
            if start <= lineno <= end and (best is None or _span(node) < _span(best)):
                best = node
    return best


def _span(node: ast.AST) -> int:
    start = int(getattr(node, "lineno", 0))
    end = int(getattr(node, "end_lineno", start) or start)
    return end - start


def pack(items: list[EvidenceItem], max_tokens: int = MAX_PACK_TOKENS) -> tuple[str, int, int]:
    """Render items with source/window/hash headers, capped at max_tokens.

    Returns (packed_text, tokens_used, dropped_count).
    """
    blocks: list[str] = []
    used = 0
    dropped = 0
    for item in items:
        block = (
            f"[{item.id}] source={item.source} window={item.window} "
            f"hash={item.hash}\n{item.text}"
        )
        cost = estimate_tokens(block)
        if used + cost > max_tokens:
            dropped += 1
            continue
        blocks.append(block)
        used += cost
    return "\n\n".join(blocks), used, dropped
