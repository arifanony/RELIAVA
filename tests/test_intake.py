"""M0 intake gate: fixture alert in, 202 + outbox dispatch + SSE timeline out."""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any

from httpx import ASGITransport, AsyncClient
from psycopg_pool import AsyncConnectionPool

from ai_sre.db import database_url, init_schema
from ai_sre.main import create_app
from ai_sre.relay import relay_once

FIXTURE_ALERT: dict[str, Any] = {
    "service": "checkout",
    "environment": "staging",
    "alert": {
        "alertname": "CheckoutErrorRate",
        "severity": "critical",
        "error_rate": 0.42,
        "window": "5m",
    },
}
IDEMPOTENCY_KEY = "fixture-checkout-0001"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def fresh_app() -> tuple[AsyncClient, AsyncConnectionPool]:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False,
        kwargs={"connect_timeout": 10}
    )
    await pool.open()
    await init_schema(pool)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE incident_events, outbox, incidents")
        await conn.commit()
    app = create_app(pool)
    client = AsyncClient(transport=ASGITransport(app=app), base_url="http://m0")
    return client, pool


def test_fixture_alert_accepted_and_dispatched() -> None:
    async def scenario() -> None:
        client, pool = await fresh_app()
        try:
            started = time.monotonic()
            first = await client.post(
                "/v1/incidents",
                json=FIXTURE_ALERT,
                headers={"Idempotency-Key": IDEMPOTENCY_KEY},
            )
            elapsed = time.monotonic() - started
            assert first.status_code == 202, first.text
            incident_id = first.json()["id"]
            uuid.UUID(incident_id)
            assert elapsed < 5.0  # generous bound, not a p95 claim

            # FR-2 dedupe: same key returns the same incident, no second row.
            second = await client.post(
                "/v1/incidents",
                json=FIXTURE_ALERT,
                headers={"Idempotency-Key": IDEMPOTENCY_KEY},
            )
            assert second.status_code == 202
            assert second.json()["id"] == incident_id
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute("SELECT count(*) FROM incidents")
                assert (await cur.fetchone())[0] == 1  # type: ignore[index]
                await cur.execute(
                    "SELECT count(*) FROM outbox WHERE dispatched_at IS NULL"
                )
                assert (await cur.fetchone())[0] == 1  # type: ignore[index]

            # Relay dispatches exactly one workflow request for the incident.
            assert await relay_once(pool) is not None
            assert await relay_once(pool) is None
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT workflow_id FROM outbox WHERE incident_id = %s",
                    (incident_id,),
                )
                assert (await cur.fetchone())[0] == f"incident-{incident_id}"  # type: ignore[index]

            # FR-12: SSE timeline carries created + dispatch, and resumes.
            events: list[tuple[str, str]] = []
            async with client.stream(
                "GET",
                f"/v1/incidents/{incident_id}/timeline",
                headers={"Accept": "text/event-stream"},
            ) as stream:
                assert stream.status_code == 200
                buf = ""
                async for chunk in stream.aiter_text():
                    buf += chunk
                for block in buf.strip().split("\n\n"):
                    lines = dict(
                        line.split(": ", 1) for line in block.splitlines() if ": " in line
                    )
                    events.append((lines.get("id", ""), lines.get("event", "")))
            kinds = [kind for _, kind in events]
            assert "ai-sre.incident.created" in kinds
            assert "ai-sre.incident.dispatch_requested" in kinds

            last_id = events[-1][0]
            resumed: list[str] = []
            async with client.stream(
                "GET",
                f"/v1/incidents/{incident_id}/timeline",
                headers={
                    "Accept": "text/event-stream",
                    "Last-Event-ID": last_id,
                },
            ) as stream:
                async for chunk in stream.aiter_text():
                    resumed.append(chunk)
            assert "".join(resumed).strip() == ""

            missing = await client.get(
                f"/v1/incidents/{uuid.uuid4()}/timeline",
                headers={"Accept": "text/event-stream"},
            )
            assert missing.status_code == 404
        finally:
            await client.aclose()
            await pool.close()

    run(scenario())
