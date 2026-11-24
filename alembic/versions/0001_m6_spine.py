"""M6 spine: canonical tables, optimistic version columns, received default.

Revision ID: 0001
Revises: None
"""

from __future__ import annotations

from alembic import op

revision = "0001"
down_revision = None

UP = """
-- The chain must run from an empty database: first ensure the pre-M6 base
-- tables this migration alters or references exist, in their M0 shapes
-- (mirrors db.py / approvals.py / dispatch.py runtime DDL; IF NOT EXISTS
-- makes this a no-op on databases the app already initialised).
CREATE TABLE IF NOT EXISTS incidents (
  id UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  service TEXT NOT NULL,
  environment TEXT NOT NULL,
  payload JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
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
ALTER TABLE incidents ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
ALTER TABLE dispatches ADD COLUMN IF NOT EXISTS version INT NOT NULL DEFAULT 1;
ALTER TABLE incidents ALTER COLUMN status SET DEFAULT 'received';
UPDATE incidents SET status = 'received' WHERE status = 'open';
"""

DOWN = """
DROP TABLE IF EXISTS verifications;
DROP TABLE IF EXISTS artifacts;
DROP TABLE IF EXISTS diagnoses;
DROP TABLE IF EXISTS hypotheses;
DROP TABLE IF EXISTS actions;
ALTER TABLE dispatches DROP COLUMN IF EXISTS version;
ALTER TABLE incidents DROP COLUMN IF EXISTS version;
ALTER TABLE incidents ALTER COLUMN status SET DEFAULT 'open';
UPDATE incidents SET status = 'open' WHERE status = 'received';
"""


def upgrade() -> None:
    op.execute(UP)


def downgrade() -> None:
    op.execute(DOWN)
