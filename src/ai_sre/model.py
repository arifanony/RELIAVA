"""FR-15: canonical PDF data model, insert-only (actions move via states)."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from ai_sre.patches import PatchCandidate

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
async def get_action(
    pool: AsyncConnectionPool, action_id: int
) -> dict[str, Any] | None:
    """The canonical action row (id, incident, digest, env, status, version)."""
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, incident_id, patch_digest, target_env, status, version"
            " FROM actions WHERE id = %s",
            (action_id,),
        )
        row = await cur.fetchone()
    if row is None:
        return None
    return {
        "id": int(row[0]),
        "incident_id": row[1],
        "patch_digest": str(row[2]),
        "target_env": str(row[3]),
        "status": str(row[4]),
        "version": int(row[5]),
    }


async def load_patch_by_digest(
    pool: AsyncConnectionPool, incident_id: str, digest: str
) -> PatchCandidate | None:
    """Reload a stored patch candidate by its PatchCandidate digest (FR-16).

    The artifact store is content-addressed by SHA-256 of the stored bytes;
    the candidate digest (SHA-256 of diff + base revision) travels in the
    envelope and is recomputed on load — a mismatch means the stored bytes
    are not the approved candidate and None is returned.
    """
    from ai_sre.patches import PatchCandidate

    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT content FROM artifacts WHERE incident_id = %s AND kind = 'patch'"
            " ORDER BY id",
            (incident_id,),
        )
        rows = await cur.fetchall()
    for (content,) in rows:
        try:
            payload = json.loads(bytes(content).decode())
        except (ValueError, UnicodeDecodeError):
            continue
        if payload.get("digest") != digest:
            continue
        candidate = PatchCandidate(
            path=str(payload["path"]),
            old=str(payload["old"]),
            new=str(payload["new"]),
            base_revision=str(payload["base_revision"]),
        )
        if candidate.digest != digest:
            return None
        return candidate
    return None
