"""Embedded outbox relay (FR-2/FR-14): LISTEN fast-path, 2 s SKIP LOCKED sweep.

The listener wakes on pg_notify with the outbox row id and hands it to the
callback (production: start Temporal IncidentWorkflow incident-<id>). Every
wake window also runs a SKIP LOCKED sweep so a missed notify can only delay,
never lose, a row. Uses a dedicated autocommit connection: pooled connections
stay inside transactions where LISTEN delivery is unreliable.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from psycopg import AsyncConnection
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

OUTBOX_CHANNEL = "incident_outbox"


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


async def _mark_dispatched(
    cur: Any, outbox_id: int, incident_id: str, trigger: str
) -> None:
    workflow_id = f"incident-{incident_id}"
    await cur.execute(
        """
        UPDATE outbox
        SET claimed_at = now(), dispatched_at = now(), workflow_id = %s
        WHERE id = %s AND dispatched_at IS NULL
        """,
        (workflow_id, outbox_id),
    )
    if cur.rowcount != 1:  # already dispatched by the other relay path
        return
    await cur.execute(
        """
        INSERT INTO incident_events (incident_id, type, source, data)
        VALUES (%s, 'ai-sre.incident.dispatch_requested', 'relay', %s)
        """,
        (incident_id, Json({"workflow_id": workflow_id, "trigger": trigger})),
    )


async def relay_sweep(
    pool: AsyncConnectionPool,
    on_outbox: Callable[[int, str], Awaitable[None]],
) -> int:
    """Claim pending rows (SKIP LOCKED) and hand each to on_outbox.

    A row is marked dispatched only after the callback succeeds, so a
    failed start (for example Temporal down) leaves the row for the next
    sweep; a crash between start and mark is safe because the workflow id
    incident-<id> is idempotent. Returns the number of rows handed over.
    """
    handed = 0
    while True:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    SELECT id, incident_id, type FROM outbox
                    WHERE dispatched_at IS NULL
                    ORDER BY id LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """
                )
                row = await cur.fetchone()
                if row is None:
                    await conn.commit()
                    return handed
                outbox_id, incident_id, typ = int(row[0]), str(row[1]), str(row[2])
                try:
                    await on_outbox(outbox_id, incident_id)
                except Exception:  # noqa: BLE001 - leave the row for a later sweep
                    await conn.rollback()
                    return handed
                await _mark_dispatched(cur, outbox_id, incident_id, typ)
            await conn.commit()
        handed += 1


async def relay_listen(
    conninfo: str,
    on_outbox: Callable[[int, str], Awaitable[None]],
    sweep: Callable[[], Awaitable[None]] | None = None,
    stop_after: int | None = None,
    on_error: Callable[[int, Exception], Awaitable[None]] | None = None,
) -> int:
    """LISTEN for outbox notifies and hand each row to on_outbox(outbox_id,
    incident_id). Every 2 s window also runs the sweep (SKIP LOCKED fallback).
    Returns the number of notifies handled. stop_after bounds test runs.

    Uses two connections: the notifies() generator holds the connection lock
    while suspended, so lookups on the same connection would deadlock.
    """
    listen_conn = await AsyncConnection.connect(conninfo, autocommit=True)
    query_conn = await AsyncConnection.connect(conninfo, autocommit=True)
    handled = 0
    try:
        await listen_conn.execute(f"LISTEN {OUTBOX_CHANNEL}")
        while True:
            async for notify in listen_conn.notifies(timeout=2.0):
                try:
                    outbox_id = int(notify.payload)
                except (TypeError, ValueError):
                    continue
                async with query_conn.cursor() as cur:
                    await cur.execute(
                        "SELECT incident_id FROM outbox WHERE id = %s", (outbox_id,)
                    )
                    row = await cur.fetchone()
                if row is None:
                    continue
                try:
                    await on_outbox(outbox_id, str(row[0]))
                except Exception as exc:  # noqa: BLE001 - relay must survive a bad row
                    if on_error is not None:
                        await on_error(outbox_id, exc)
                    continue
                async with query_conn.cursor() as cur:
                    await _mark_dispatched(cur, outbox_id, str(row[0]), "notify")
                handled += 1
                if stop_after is not None and handled >= stop_after:
                    return handled
            if sweep is not None:
                await sweep()
    finally:
        await listen_conn.close()
        await query_conn.close()
    return handled
