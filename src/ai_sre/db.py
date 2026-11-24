"""Postgres 16 store: incidents, transactional outbox, append-only timeline."""

from __future__ import annotations

import os

from psycopg_pool import AsyncConnectionPool

SCHEMA = """
CREATE TABLE IF NOT EXISTS incidents (
  id UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  service TEXT NOT NULL,
  environment TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS outbox (
  id BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  type TEXT NOT NULL,
  payload JSONB NOT NULL,
  claimed_at TIMESTAMPTZ,
  dispatched_at TIMESTAMPTZ,
  workflow_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS incident_events (
  id BIGSERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  type TEXT NOT NULL,
  source TEXT NOT NULL,
  data JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/aisre"
    )


def get_pool() -> AsyncConnectionPool:
    # connect_timeout bounds loopback stalls: without it a wedged SYN hangs a
    # pool worker forever, and the retry path swallows teardown cancellation.
    return AsyncConnectionPool(
        database_url(), open=False, kwargs={"connect_timeout": 10}
    )


async def init_schema(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()
