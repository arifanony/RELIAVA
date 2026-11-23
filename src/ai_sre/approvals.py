"""FR-9 + AC-9: digest-bound approvals with 24 h request expiry, 2 h validity.

An approval binds action digest, target environment, source revision,
policy version, and approver. A grant more than 24 h after the request is
refused; a grant is valid for 2 h. Every operator demo action must carry
such a record (AC-9). Rows are insert-only; decisions update the row in
place while the request itself is never rewritten.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from psycopg_pool import AsyncConnectionPool

REQUEST_TTL = timedelta(hours=24)
GRANT_VALIDITY = timedelta(hours=2)


class ApprovalExpired(Exception):
    pass


class ApprovalNotFound(Exception):
    pass


@dataclass
class Approval:
    id: int
    action_digest: str
    target_env: str
    source_revision: str
    policy_version: str
    approver: str
    decision: str
    requested_at: datetime
    expires_at: datetime
    valid_until: datetime | None


SCHEMA = """
CREATE TABLE IF NOT EXISTS approvals (
  id SERIAL PRIMARY KEY,
  action_digest TEXT NOT NULL,
  target_env TEXT NOT NULL,
  source_revision TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  approver TEXT NOT NULL,
  decision TEXT NOT NULL DEFAULT 'pending',
  requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  valid_until TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_approvals(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()


def _row_to_approval(row: tuple) -> Approval:  # type: ignore[type-arg]
    return Approval(
        id=row[0],
        action_digest=row[1],
        target_env=row[2],
        source_revision=row[3],
        policy_version=row[4],
        approver=row[5],
        decision=row[6],
        requested_at=row[7],
        expires_at=row[8],
        valid_until=row[9],
    )


async def request_approval(
    pool: AsyncConnectionPool,
    action_digest: str,
    target_env: str,
    source_revision: str,
    policy_version: str,
    approver: str,
) -> Approval:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                """
                INSERT INTO approvals
                  (action_digest, target_env, source_revision, policy_version,
                   approver, expires_at)
                VALUES (%s, %s, %s, %s, %s, now() + (%s || ' seconds')::interval)
                RETURNING id, action_digest, target_env, source_revision,
                          policy_version, approver, decision, requested_at,
                          expires_at, valid_until
                """,
                (action_digest, target_env, source_revision, policy_version,
                 approver, int(REQUEST_TTL.total_seconds())),
            )
            row = await cur.fetchone()
            assert row is not None
        await conn.commit()
    return _row_to_approval(row)


async def decide(pool: AsyncConnectionPool, approval_id: int, grant: bool) -> Approval:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT now() > expires_at FROM approvals WHERE id = %s",
                (approval_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise ApprovalNotFound(f"approval {approval_id}")
            if row[0]:
                raise ApprovalExpired(f"approval {approval_id} expired")
            await cur.execute(
                """
                UPDATE approvals
                SET decision = %s,
                    valid_until = CASE WHEN %s
                        THEN now() + (%s || ' seconds')::interval END
                WHERE id = %s
                RETURNING id, action_digest, target_env, source_revision,
                          policy_version, approver, decision, requested_at,
                          expires_at, valid_until
                """,
                ("granted" if grant else "rejected", grant,
                 int(GRANT_VALIDITY.total_seconds()), approval_id),
            )
            updated = await cur.fetchone()
            assert updated is not None
        await conn.commit()
    return _row_to_approval(updated)


async def find_valid(
    pool: AsyncConnectionPool,
    action_digest: str,
    target_env: str,
    source_revision: str,
) -> Approval | None:
    """The granted, still-valid approval for this exact action, if any."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                SELECT id, action_digest, target_env, source_revision,
                       policy_version, approver, decision, requested_at,
                       expires_at, valid_until
                FROM approvals
                WHERE action_digest = %s AND target_env = %s
                  AND source_revision = %s AND decision = 'granted'
                  AND valid_until > now()
                ORDER BY valid_until DESC LIMIT 1
                """,
            (action_digest, target_env, source_revision),
        )
        row = await cur.fetchone()
    return _row_to_approval(row) if row else None
