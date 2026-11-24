"""FR-15: canonical incident/action state machines with optimistic versions.

Transitions are enforced by table; persistence applies them atomically with
a version check (UPDATE ... WHERE version = expected).
"""

from __future__ import annotations

from psycopg_pool import AsyncConnectionPool

INCIDENT_TRANSITIONS: dict[str, frozenset[str]] = {
    "received": frozenset({"investigating"}),
    "investigating": frozenset({"diagnosed", "inconclusive"}),
    "diagnosed": frozenset({"awaiting_approval", "inconclusive"}),
    "awaiting_approval": frozenset({"remediating", "escalated"}),
    "remediating": frozenset({"verifying", "escalated"}),
    "verifying": frozenset({"resolved", "escalated"}),
    "inconclusive": frozenset({"escalated"}),
    "resolved": frozenset(),
    "escalated": frozenset(),
}

ACTION_TRANSITIONS: dict[str, frozenset[str]] = {
    "proposed": frozenset({"testing"}),
    "testing": frozenset({"ready_for_review", "failed"}),
    "ready_for_review": frozenset({"approved", "failed"}),
    "approved": frozenset({"dispatching"}),
    "dispatching": frozenset({"succeeded", "failed", "unknown"}),
    "unknown": frozenset({"succeeded", "failed"}),
    "succeeded": frozenset(),
    "failed": frozenset(),
}

_TABLES = {"incidents": INCIDENT_TRANSITIONS, "actions": ACTION_TRANSITIONS}


class IllegalTransition(Exception):
    pass


class VersionConflict(Exception):
    pass


class UnknownRecord(Exception):
    pass


def allowed(table: str, old: str, new: str) -> bool:
    return new in _TABLES[table].get(old, frozenset())


def check(table: str, old: str, new: str) -> None:
    if table not in _TABLES:
        raise IllegalTransition(f"no state machine for {table}")
    if not allowed(table, old, new):
        raise IllegalTransition(f"{table}: {old} -> {new} is not allowed")


async def advance(
    pool: AsyncConnectionPool, table: str, record_id: int | str,
    new_status: str, expected_version: int,
) -> int:
    """Atomically move status when the version matches; returns new version."""
    if table not in _TABLES:
        raise IllegalTransition(f"no state machine for {table}")
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"SELECT status, version FROM {table} WHERE id = %s",
            (record_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise UnknownRecord(f"{table} {record_id}")
        old_status, version = str(row[0]), int(row[1])
        if version != expected_version:
            raise VersionConflict(
                f"{table} {record_id}: expected version {expected_version},"
                f" found {version}"
            )
        check(table, old_status, new_status)
        await cur.execute(
            f"UPDATE {table} SET status = %s, version = version + 1,"
            " updated_at = now() WHERE id = %s AND version = %s"
            " RETURNING version",
            (new_status, record_id, expected_version),
        )
        updated = await cur.fetchone()
        assert updated is not None
        await conn.commit()
    return int(updated[0])
