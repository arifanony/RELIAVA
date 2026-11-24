"""Alembic env: URL comes from DATABASE_URL (same store as the app)."""

from __future__ import annotations

import os

from sqlalchemy import create_engine

from alembic import context

config = context.config


def url() -> str:
    raw = os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/aisre"
    )
    return raw.replace("postgresql://", "postgresql+psycopg://", 1)


def run_migrations_online() -> None:
    engine = create_engine(url())
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=None)
        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
