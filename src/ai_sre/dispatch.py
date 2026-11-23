"""FR-10 + AC-4: idempotent trusted dispatch with UNKNOWN reconciliation.

Only actions with a valid (digest-bound, in-validity) approval run, and
only when the worker holds the staging write credential. A repeat dispatch
of the same action returns the existing record without re-executing: a
timeout becomes UNKNOWN and stays UNKNOWN until an explicit reconcile
probes the target ledger. Attempts never increment on their own.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool

from ai_sre.approvals import find_valid

CREDENTIAL_ENV = "STAGING_WRITE_CREDENTIALS"


class NotApproved(Exception):
    pass


class MissingCredential(Exception):
    pass


class UnknownDispatch(Exception):
    pass


@dataclass
class Dispatch:
    id: int
    action_digest: str
    target_env: str
    source_revision: str
    approval_id: int
    status: str
    attempts: int
    created_at: datetime
    updated_at: datetime


SCHEMA = """
CREATE TABLE IF NOT EXISTS dispatches (
  id SERIAL PRIMARY KEY,
  action_digest TEXT NOT NULL,
  target_env TEXT NOT NULL,
  source_revision TEXT NOT NULL,
  approval_id INT NOT NULL REFERENCES approvals(id),
  status TEXT NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (action_digest, target_env, source_revision)
);
CREATE TABLE IF NOT EXISTS dispatch_events (
  id SERIAL PRIMARY KEY,
  dispatch_id INT NOT NULL REFERENCES dispatches(id),
  event TEXT NOT NULL,
  data JSONB NOT NULL DEFAULT '{}',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_dispatch(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()


def _row_to_dispatch(row: tuple) -> Dispatch:  # type: ignore[type-arg]
    return Dispatch(
        id=row[0],
        action_digest=row[1],
        target_env=row[2],
        source_revision=row[3],
        approval_id=row[4],
        status=row[5],
        attempts=row[6],
        created_at=row[7],
        updated_at=row[8],
    )


async def log_event(
    pool: AsyncConnectionPool, dispatch_id: int, event: str, data: dict[str, Any]
) -> None:
    import json as _json

    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO dispatch_events (dispatch_id, event, data) VALUES (%s, %s, %s)",
                (dispatch_id, event, _json.dumps(data)),
            )
        await conn.commit()


async def get_dispatch(
    pool: AsyncConnectionPool,
    action_digest: str,
    target_env: str,
    source_revision: str,
) -> Dispatch | None:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                SELECT id, action_digest, target_env, source_revision,
                       approval_id, status, attempts, created_at, updated_at
                FROM dispatches
                WHERE action_digest = %s AND target_env = %s AND source_revision = %s
                """,
            (action_digest, target_env, source_revision),
        )
        row = await cur.fetchone()
    return _row_to_dispatch(row) if row else None


async def dispatch_action(
    pool: AsyncConnectionPool,
    action_digest: str,
    target_env: str,
    source_revision: str,
    action: dict[str, Any],
    executor: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    timeout_s: float = 120.0,
) -> Dispatch:
    """Run once if approved and credentialed; repeats return the record."""
    if not os.environ.get(CREDENTIAL_ENV):
        raise MissingCredential(f"{CREDENTIAL_ENV} is not set")
    approval = await find_valid(pool, action_digest, target_env, source_revision)
    if approval is None:
        raise NotApproved(action_digest)
    existing = await get_dispatch(pool, action_digest, target_env, source_revision)
    if existing is not None:
        return existing  # idempotent: never auto-retry, never re-execute
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO dispatches
                  (action_digest, target_env, source_revision, approval_id,
                   status, attempts)
                VALUES (%s, %s, %s, %s, 'dispatching', 1)
                RETURNING id, action_digest, target_env, source_revision,
                          approval_id, status, attempts, created_at, updated_at
                """,
                (action_digest, target_env, source_revision, approval.id),
            )
            row = await cur.fetchone()
            assert row is not None
        await conn.commit()
    dispatch = _row_to_dispatch(row)
    await log_event(pool, dispatch.id, "dispatched", {"approval_id": approval.id})
    try:
        result = await asyncio.wait_for(executor(action), timeout=timeout_s)
    except TimeoutError as exc:
        await _set_status(pool, dispatch.id, "unknown")
        await log_event(pool, dispatch.id, "unknown", {"reason": f"timeout: {exc}"})
        dispatch.status = "unknown"
        return dispatch
    except Exception as exc:  # noqa: BLE001 - executor failure is a verdict input
        await _set_status(pool, dispatch.id, "failed")
        await log_event(pool, dispatch.id, "failed", {"reason": f"{type(exc).__name__}: {exc}"})
        dispatch.status = "failed"
        return dispatch
    await _set_status(pool, dispatch.id, "applied")
    await log_event(pool, dispatch.id, "applied", {"result": result})
    dispatch.status = "applied"
    return dispatch


async def _set_status(pool: AsyncConnectionPool, dispatch_id: int, status: str) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "UPDATE dispatches SET status = %s, updated_at = now() WHERE id = %s",
                (status, dispatch_id),
            )
        await conn.commit()


async def reconcile(
    pool: AsyncConnectionPool,
    dispatch_id: int,
    probe: Callable[[], bool],
) -> Dispatch:
    """Resolve UNKNOWN from the target ledger. Never re-executes: attempts stay."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                SELECT id, action_digest, target_env, source_revision,
                       approval_id, status, attempts, created_at, updated_at
                FROM dispatches WHERE id = %s
                """,
            (dispatch_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise UnknownDispatch(f"dispatch {dispatch_id}")
        dispatch = _row_to_dispatch(row)
    if dispatch.status != "unknown":
        raise UnknownDispatch(f"dispatch {dispatch_id} is {dispatch.status}, not unknown")
    observed = probe()
    status = "applied" if observed else "failed"
    await _set_status(pool, dispatch_id, status)
    await log_event(
        pool, dispatch_id, "reconciled",
        {"observed_on_target": observed, "attempts": dispatch.attempts},
    )
    dispatch.status = status
    return dispatch
