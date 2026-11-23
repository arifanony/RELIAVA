"""FR-4: revisioned runbook store in Postgres with BM25 retrieval.

BM25 is Okapi (k1=1.5, b=0.75) in stdlib over runbooks stored in the
unified stock PostgreSQL 16 container: no extension, no new infra, fully
deterministic for the frozen baseline. Each indexing run mints a new
revision; retrieval always serves the latest; old revisions are retained.
"""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from psycopg_pool import AsyncConnectionPool

K1 = 1.5
B = 0.75


def tokenize(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def bm25_scores(docs: list[list[str]], query: list[str]) -> list[float]:
    """Okapi BM25, deterministic. Empty corpus or query -> all zeros."""
    n = len(docs)
    if n == 0 or not query:
        return [0.0] * n
    avgdl = sum(len(d) for d in docs) / n
    doc_freq: dict[str, int] = {}
    for doc in docs:
        for term in set(doc):
            doc_freq[term] = doc_freq.get(term, 0) + 1
    scores = [0.0] * n
    for i, doc in enumerate(docs):
        tf: dict[str, int] = {}
        for term in doc:
            tf[term] = tf.get(term, 0) + 1
        for term in query:
            f = tf.get(term, 0)
            if f == 0:
                continue
            n_t = doc_freq.get(term, 0)
            idf = math.log(1 + (n - n_t + 0.5) / (n_t + 0.5))
            denom = f + K1 * (1 - B + B * (len(doc) / avgdl if avgdl else 0))
            scores[i] += idf * f * (K1 + 1) / denom
    return scores


@dataclass
class Runbook:
    id: int
    title: str
    body: str
    revision: int
    score: float = 0.0


SCHEMA = """
CREATE TABLE IF NOT EXISTS runbooks (
  id SERIAL PRIMARY KEY,
  title TEXT NOT NULL,
  body TEXT NOT NULL,
  revision INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS runbook_index_revisions (
  revision INT PRIMARY KEY,
  doc_count INT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_runbooks(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()


async def latest_revision(pool: AsyncConnectionPool) -> int:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute("SELECT COALESCE(MAX(revision), 0) FROM runbook_index_revisions")
        row = await cur.fetchone()
        assert row is not None
        return int(row[0])


async def index_runbooks(pool: AsyncConnectionPool, docs: list[tuple[str, str]]) -> int:
    """Store docs as a new revision; returns the revision number."""
    revision = await latest_revision(pool) + 1
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            for title, body in docs:
                await cur.execute(
                    "INSERT INTO runbooks (title, body, revision) VALUES (%s, %s, %s)",
                    (title, body, revision),
                )
            await cur.execute(
                "INSERT INTO runbook_index_revisions (revision, doc_count) VALUES (%s, %s)",
                (revision, len(docs)),
            )
        await conn.commit()
    return revision


async def search_runbooks(pool: AsyncConnectionPool, query: str, k: int = 3) -> list[Runbook]:
    revision = await latest_revision(pool)
    if revision == 0:
        return []
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, title, body FROM runbooks WHERE revision = %s ORDER BY id",
            (revision,),
        )
        rows = await cur.fetchall()
    docs = [tokenize(f"{title} {body}") for _, title, body in rows]
    scores = bm25_scores(docs, tokenize(query))
    ranked = sorted(
        (
            Runbook(id=r[0], title=r[1], body=r[2], revision=revision, score=s)
            for r, s in zip(rows, scores)
        ),
        key=lambda rb: rb.score,
        reverse=True,
    )
    return [rb for rb in ranked if rb.score > 0][:k]


def evaluate(
    results: dict[str, list[Runbook]], expected: dict[str, str], k: int = 3
) -> dict[str, Any]:
    """Recall@k and MRR over {query: ranked runbooks} vs {query: expected title}."""
    recalls: list[float] = []
    reciprocal_ranks: list[float] = []
    details: dict[str, Any] = {}
    for query, want in expected.items():
        got = [rb.title for rb in results.get(query, [])[:k]]
        recalls.append(1.0 if want in got else 0.0)
        try:
            reciprocal_ranks.append(1.0 / (got.index(want) + 1))
        except ValueError:
            reciprocal_ranks.append(0.0)
        details[query] = {"want": want, "got": got}
    return {
        "recall_at_k": sum(recalls) / len(recalls) if recalls else 0.0,
        "mrr": sum(reciprocal_ranks) / len(reciprocal_ranks) if reciprocal_ranks else 0.0,
        "k": k,
        "details": details,
    }


def baseline_path() -> Path:
    return Path(__file__).resolve().parent.parent.parent / "evals" / "runbook_baseline.json"


def load_baseline() -> dict[str, Any]:
    return json.loads(baseline_path().read_text())
