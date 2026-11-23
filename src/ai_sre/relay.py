"""Embedded outbox relay (FR-2): claim rows with SKIP LOCKED, request dispatch.

Polls every 2 s. Inserts carry pg_notify for a future LISTEN fast-path; the
poll is the durable fallback, not the optimization. Temporal execution comes
later: dispatch here records workflow incident-<id> as requested.
"""

from __future__ import annotations

import asyncio
from typing import Any

from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool


async def relay_once(pool: AsyncConnectionPool) -> int | None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                SELECT id, incident_id, type, payload FROM outbox
                WHERE dispatched_at IS NULL
                ORDER BY id LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row: tuple[Any, ...] | None = await cur.fetchone()
            if row is None:
                await conn.commit()
                return None
            outbox_id, incident_id, typ, _payload = row
            workflow_id = f"incident-{incident_id}"
            await cur.execute(
                """
                UPDATE outbox
                SET claimed_at = now(), dispatched_at = now(), workflow_id = %s
                WHERE id = %s
                """,
                (workflow_id, outbox_id),
            )
            await cur.execute(
                """
                INSERT INTO incident_events (incident_id, type, source, data)
                VALUES (%s, 'ai-sre.incident.dispatch_requested', 'relay', %s)
                """,
                (incident_id, Json({"workflow_id": workflow_id, "trigger": typ})),
            )
        await conn.commit()
    return int(outbox_id)


async def relay_forever(pool: AsyncConnectionPool, poll_interval: float = 2.0) -> None:
    while True:
        while await relay_once(pool) is not None:
            pass
        await asyncio.sleep(poll_interval)
