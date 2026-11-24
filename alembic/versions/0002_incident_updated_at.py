"""Add updated_at to incidents for versioned state transitions.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"


def upgrade() -> None:
    op.execute(
        "ALTER TABLE incidents ADD COLUMN IF NOT EXISTS updated_at"
        " TIMESTAMPTZ NOT NULL DEFAULT now();"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE incidents DROP COLUMN IF EXISTS updated_at;")
