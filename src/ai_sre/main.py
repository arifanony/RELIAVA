"""FastAPI intake (FR-1) and SSE timeline (FR-12)."""

from __future__ import annotations

import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import StreamingResponse
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, Field

from ai_sre.approvals import (
    ApprovalExpired,
    ApprovalNotFound,
    decide,
    init_approvals,
    request_approval,
)
from ai_sre.db import get_pool, init_schema
from ai_sre.dispatch import (
    MissingCredential,
    NotApproved,
    dispatch_action,
    init_dispatch,
)
from ai_sre.relay import OUTBOX_CHANNEL


async def _noop_executor(action: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "action": action}


EXECUTORS = {"noop": _noop_executor}


class IncidentIn(BaseModel):
    service: str = Field(min_length=1)
    environment: str = Field(min_length=1)
    alert: dict[str, Any] = Field(default_factory=dict)


class ApprovalIn(BaseModel):
    action_digest: str = Field(min_length=1)
    target_env: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    policy_version: str = Field(min_length=1)
    approver: str = Field(min_length=1)


class DispatchIn(BaseModel):
    action_digest: str = Field(min_length=1)
    target_env: str = Field(min_length=1)
    source_revision: str = Field(min_length=1)
    action: dict[str, Any] = Field(default_factory=dict)


def create_app(pool: AsyncConnectionPool) -> FastAPI:
    app = FastAPI(title="AI SRE intake")

    @app.post("/v1/incidents", status_code=202)
    async def post_incident(
        body: IncidentIn,
        idempotency_key: str = Header(alias="Idempotency-Key"),
    ) -> dict[str, str]:
        incident_id = uuid.uuid4()
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    """
                    INSERT INTO incidents (id, idempotency_key, service, environment, payload)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    RETURNING id
                    """,
                    (incident_id, idempotency_key, body.service, body.environment,
                     Json(body.alert)),
                )
                row = await cur.fetchone()
                if row is None:
                    await cur.execute(
                        "SELECT id, status FROM incidents WHERE idempotency_key = %s",
                        (idempotency_key,),
                    )
                    found = await cur.fetchone()
                    assert found is not None
                    await conn.commit()
                    return {"id": str(found[0]), "status": str(found[1])}
                await cur.execute(
                    """
                    INSERT INTO outbox (incident_id, type, payload)
                    VALUES (%s, 'incident.created', %s)
                    RETURNING id
                    """,
                    (incident_id, Json(body.alert)),
                )
                outbox_row = await cur.fetchone()
                assert outbox_row is not None
                await cur.execute(
                    """
                    INSERT INTO incident_events (incident_id, type, source, data)
                    VALUES (%s, 'ai-sre.incident.created', 'intake', %s)
                    """,
                    (incident_id, Json({"service": body.service,
                                        "environment": body.environment})),
                )
                await cur.execute(
                    "SELECT pg_notify(%s, %s)", (OUTBOX_CHANNEL, str(outbox_row[0]))
                )
            await conn.commit()
        return {"id": str(incident_id), "status": "received"}

    @app.get("/v1/incidents/{incident_id}/timeline")
    async def timeline(
        incident_id: uuid.UUID,
        last_event_id: int = Header(default=0, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT 1 FROM incidents WHERE id = %s", (incident_id,)
            )
            if await cur.fetchone() is None:
                raise HTTPException(status_code=404, detail="unknown incident")

        async def gen() -> AsyncIterator[str]:
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    """
                        SELECT id, type, source, data, created_at
                        FROM incident_events
                        WHERE incident_id = %s AND id > %s
                        ORDER BY id
                        """,
                    (incident_id, last_event_id),
                )
                async for event_id, typ, source, data, created in cur:
                    payload = json.dumps(
                        {
                            "specversion": "1.0",
                            "type": typ,
                            "source": source,
                            "data": data,
                        }
                    )
                    yield f"id: {event_id}\nevent: {typ}\ndata: {payload}\n\n"

        return StreamingResponse(gen(), media_type="text/event-stream")

    @app.post("/v1/approvals", status_code=201)
    async def post_approval(body: ApprovalIn) -> dict[str, Any]:
        approval = await request_approval(
            pool,
            body.action_digest,
            body.target_env,
            body.source_revision,
            body.policy_version,
            body.approver,
        )
        return {"id": approval.id, "expires_at": approval.expires_at.isoformat()}

    @app.post("/v1/approvals/{approval_id}/grant")
    async def grant_approval(approval_id: int) -> dict[str, Any]:
        try:
            approval = await decide(pool, approval_id, True)
        except ApprovalNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ApprovalExpired as exc:
            raise HTTPException(status_code=410, detail=str(exc)) from exc
        return {
            "id": approval.id,
            "decision": approval.decision,
            "valid_until": approval.valid_until.isoformat() if approval.valid_until else None,
        }

    @app.post("/v1/approvals/{approval_id}/reject")
    async def reject_approval(approval_id: int) -> dict[str, Any]:
        try:
            approval = await decide(pool, approval_id, False)
        except ApprovalNotFound as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except ApprovalExpired as exc:
            raise HTTPException(status_code=410, detail=str(exc)) from exc
        return {"id": approval.id, "decision": approval.decision}

    @app.post("/v1/dispatch")
    async def post_dispatch(body: DispatchIn) -> dict[str, Any]:
        executor = EXECUTORS.get(body.action.get("type", ""))
        if executor is None:
            raise HTTPException(status_code=400, detail="unknown action type")
        try:
            dispatch = await dispatch_action(
                pool,
                body.action_digest,
                body.target_env,
                body.source_revision,
                body.action,
                executor,
            )
        except MissingCredential as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except NotApproved as exc:
            raise HTTPException(status_code=409, detail=f"not approved: {exc}") from exc
        return {
            "id": dispatch.id,
            "status": dispatch.status,
            "attempts": dispatch.attempts,
            "approval_id": dispatch.approval_id,
        }

    return app


def run() -> None:
    import asyncio

    import uvicorn

    from ai_sre.relay import relay_forever

    async def lifespan_app() -> None:
        pool = get_pool()
        await pool.open()
        await init_schema(pool)
        await init_approvals(pool)
        await init_dispatch(pool)
        relay = asyncio.ensure_future(relay_forever(pool))
        config = uvicorn.Config(create_app(pool), host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()
        relay.cancel()

    asyncio.run(lifespan_app())
