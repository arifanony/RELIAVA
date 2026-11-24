"""FR-15: canonical PDF data model, insert-only (actions move via states)."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any

from psycopg_pool import AsyncConnectionPool

MAX_ARTIFACT_BYTES = 1024 * 1024


class ArtifactTooLarge(Exception):
    pass


SCHEMA = """
CREATE TABLE IF NOT EXISTS actions (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  patch_digest TEXT NOT NULL,
  target_env TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'proposed',
  version INT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS hypotheses (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  text TEXT NOT NULL,
  evidence_ids JSONB NOT NULL DEFAULT '[]',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS diagnoses (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  status TEXT NOT NULL,
  revisions INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS artifacts (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  kind TEXT NOT NULL,
  content BYTEA NOT NULL,
  sha256 TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS verifications (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL REFERENCES incidents(id),
  verdict TEXT NOT NULL,
  error_rate DOUBLE PRECISION,
  p95_ms DOUBLE PRECISION,
  replay_ok BOOLEAN,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_model(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()


@dataclass
class ArtifactRef:
    id: int
    sha256: str
    size: int


async def insert_artifact(
    pool: AsyncConnectionPool, incident_id: str, kind: str, content: bytes
) -> ArtifactRef:
    if len(content) > MAX_ARTIFACT_BYTES:
        raise ArtifactTooLarge(f"{len(content)} bytes exceeds 1 MiB cap")
    digest = hashlib.sha256(content).hexdigest()
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO artifacts (incident_id, kind, content, sha256)"
                " VALUES (%s, %s, %s, %s) RETURNING id",
                (incident_id, kind, content, digest),
            )
            row = await cur.fetchone()
            assert row is not None
        await conn.commit()
    return ArtifactRef(id=int(row[0]), sha256=digest, size=len(content))


async def insert_record(
    pool: AsyncConnectionPool, table: str, row: dict[str, Any]
) -> int:
    if table not in {"hypotheses", "diagnoses", "actions", "verifications"}:
        raise ValueError(f"not an insert-only canonical table: {table}")
    columns = ", ".join(row.keys())
    placeholders = ", ".join(["%s"] * len(row))
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                " RETURNING id",
                tuple(row.values()),
            )
            new_id = await cur.fetchone()
            assert new_id is not None
        await conn.commit()
    return int(new_id[0])
