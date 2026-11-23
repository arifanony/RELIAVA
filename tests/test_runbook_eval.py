"""M2 eval: revisioned BM25 runbooks with a frozen baseline."""

from __future__ import annotations

import asyncio
import os
from typing import Any

from psycopg_pool import AsyncConnectionPool

from ai_sre.db import database_url, init_schema
from ai_sre.runbooks import (
    evaluate,
    index_runbooks,
    init_runbooks,
    latest_revision,
    load_baseline,
    search_runbooks,
)

RUNBOOKS: list[tuple[str, str]] = [
    (
        "checkout error-rate",
        (
            "When the Prometheus checkout error-rate alert fires with HTTP 500s, "
            "inspect the last deployment config value, rollback the flag, and verify "
            "the Alertmanager webhook payload matches the incident service."
        ),
    ),
    (
        "payment code bug",
        (
            "A ValueError coupon traceback in pricing means a code bug in "
            "apply_discount. Reproduce with the order fixture, patch the branch, "
            "and rerun the payment suite before approval."
        ),
    ),
    (
        "config regression deploy",
        (
            "A deployment that changed a config value and raised errors is a config "
            "regression. Revert the value, redeploy, and confirm the error budget "
            "recovers within the verification window."
        ),
    ),
    (
        "inconclusive evidence escalation",
        (
            "With partial logs and missing evidence, record INCONCLUSIVE, attach "
            "observed and missing evidence, propose no change, and escalate to the "
            "on-call engineer immediately."
        ),
    ),
    (
        "database failover",
        (
            "On PostgreSQL primary loss, promote the synchronous replica, replay "
            "WAL to the last committed LSN, and repoint the unified store DSN."
        ),
    ),
    (
        "queue lag",
        (
            "Temporal workflow backlog with idle workers means queue lag. Drain the "
            "task queue, scale workers, and check the worker poll metrics."
        ),
    ),
]


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def fresh_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False
    )
    await pool.open()
    await init_schema(pool)
    await init_runbooks(pool)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE runbooks, runbook_index_revisions")
        await conn.commit()
    return pool


def test_search_top1_matches_expected() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            await index_runbooks(pool, RUNBOOKS)
            baseline = load_baseline()
            for query, want in baseline["queries"].items():
                hits = await search_runbooks(pool, query, k=3)
                assert hits, f"no hits for {query!r}"
                assert hits[0].title == want, f"{query!r} -> {[h.title for h in hits]}"
        finally:
            await pool.close()

    run(scenario())


def test_frozen_baseline() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            await index_runbooks(pool, RUNBOOKS)
            baseline = load_baseline()
            k = int(baseline["k"])
            results = {}
            for query in baseline["queries"]:
                results[query] = await search_runbooks(pool, query, k=k)
            metrics = evaluate(results, baseline["queries"], k=k)
            assert metrics["recall_at_k"] >= float(baseline["min_recall_at_3"])
            assert metrics["mrr"] >= float(baseline["min_mrr"])
        finally:
            await pool.close()

    run(scenario())


def test_index_revisions() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            rev1 = await index_runbooks(pool, RUNBOOKS[:2])
            assert rev1 == 1
            rev2 = await index_runbooks(pool, RUNBOOKS)
            assert rev2 == 2
            assert await latest_revision(pool) == 2
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT count(*) FROM runbook_index_revisions"
                )
                row = await cur.fetchone()
                assert row is not None and row[0] == 2
            hits = await search_runbooks(pool, "postgres replica failover WAL", k=3)
            assert hits and hits[0].title == "database failover"
        finally:
            await pool.close()

    run(scenario())
