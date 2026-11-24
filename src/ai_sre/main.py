"""FastAPI intake (FR-1) and SSE timeline (FR-12)."""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import AsyncIterator
from typing import Any

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse
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
from ai_sre.model import get_action, init_model, load_patch_by_digest
from ai_sre.relay import OUTBOX_CHANNEL
from ai_sre.states import advance


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


def _approvers_for(service: str) -> set[str]:
    """RBAC: identities allowed to approve actions for a service.

    APPROVER_ROLES is a JSON object {service: [identity, ...]}; APPROVERS
    is a comma-separated global fallback. Empty means nobody (fail closed).
    """
    raw = os.environ.get("APPROVER_ROLES")
    if raw:
        try:
            mapping = json.loads(raw)
        except ValueError:
            mapping = {}
        if isinstance(mapping, dict) and service in mapping:
            return {str(x) for x in mapping[service]}
    return {x.strip() for x in os.environ.get("APPROVERS", "").split(",") if x.strip()}


async def _signal_workflow(incident_id: str, decision: str) -> bool:
    """Best-effort approval signal; PostgreSQL remains the source of truth."""
    import asyncio as _asyncio

    try:
        from temporalio.client import Client

        from ai_sre.worker import temporal_address
        from ai_sre.workflows import IncidentWorkflow

        async def _send() -> None:
            client = await Client.connect(temporal_address())
            handle = client.get_workflow_handle(f"incident-{incident_id}")
            await handle.signal(IncidentWorkflow.approval, decision)

        await _asyncio.wait_for(_send(), timeout=5)
        return True
    except Exception:  # noqa: BLE001 - signal is advisory; PG record governs
        return False


class ActionApprovalIn(BaseModel):
    decision: str = Field(pattern="^(granted|rejected)$")
    expected_digest: str = Field(min_length=1)


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

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> dict[str, str]:
        try:
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute("SELECT 1")
                await cur.fetchone()
        except Exception as exc:
            raise HTTPException(status_code=503, detail=f"db: {exc}") from exc
        return {"status": "ready"}

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        lines = [
            "# HELP ai_sre_incidents Incidents by status.",
            "# TYPE ai_sre_incidents gauge",
        ]
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT status, count(*) FROM incidents GROUP BY status"
            )
            async for status, count in cur:
                lines.append(f'ai_sre_incidents{{status="{status}"}} {count}')
            lines += [
                "# HELP ai_sre_actions Actions by status.",
                "# TYPE ai_sre_actions gauge",
            ]
            await cur.execute(
                "SELECT status, count(*) FROM actions GROUP BY status"
            )
            async for status, count in cur:
                lines.append(f'ai_sre_actions{{status="{status}"}} {count}')
        return PlainTextResponse("\n".join(lines) + "\n", media_type="text/plain")

    @app.get("/v1/incidents/{incident_id}")
    async def get_incident(incident_id: uuid.UUID) -> dict[str, Any]:
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT id, service, environment, status, version, created_at"
                " FROM incidents WHERE id = %s",
                (incident_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise HTTPException(status_code=404, detail="unknown incident")
            await cur.execute(
                "SELECT id, patch_digest, target_env, status FROM actions"
                " WHERE incident_id = %s ORDER BY id",
                (incident_id,),
            )
            actions = [
                {"id": r[0], "patch_digest": r[1], "target_env": r[2],
                 "status": r[3]}
                async for r in cur
            ]
        return {
            "id": str(row[0]), "service": row[1], "environment": row[2],
            "status": row[3], "version": row[4],
            "created_at": row[5].isoformat(), "actions": actions,
        }

    @app.get("/v1/incidents/{incident_id}/events")
    async def events(
        incident_id: uuid.UUID,
        last_event_id: int = Header(default=0, alias="Last-Event-ID"),
    ) -> StreamingResponse:
        return await timeline(incident_id, last_event_id)  # same stream

    @app.post("/v1/incidents/{incident_id}/cancel", status_code=202)
    async def cancel_incident(incident_id: uuid.UUID) -> dict[str, Any]:
        async with pool.connection() as conn:
            async with conn.cursor() as cur:
                await cur.execute(
                    "SELECT 1 FROM incidents WHERE id = %s", (incident_id,)
                )
                if await cur.fetchone() is None:
                    raise HTTPException(status_code=404, detail="unknown incident")
                await cur.execute(
                    "INSERT INTO incident_events (incident_id, type, source, data)"
                    " VALUES (%s, 'ai-sre.incident.cancelled', 'api', %s)",
                    (incident_id, Json({})),
                )
            await conn.commit()
        signalled = False
        try:
            import asyncio as _asyncio

            from temporalio.client import Client

            from ai_sre.worker import temporal_address

            async def _cancel() -> None:
                client = await Client.connect(temporal_address())
                await client.get_workflow_handle(
                    f"incident-{incident_id}"
                ).cancel()

            await _asyncio.wait_for(_cancel(), timeout=5)
            signalled = True
        except Exception:  # noqa: BLE001 - event record stands on its own
            signalled = False
        return {"id": str(incident_id), "cancelled": True, "signalled": signalled}

    @app.get("/v1/actions/{action_id}")
    async def get_action_route(action_id: int) -> dict[str, Any]:
        action = await get_action(pool, action_id)
        if action is None:
            raise HTTPException(status_code=404, detail="unknown action")
        return {
            "id": action["id"],
            "incident_id": str(action["incident_id"]),
            "patch_digest": action["patch_digest"],
            "target_env": action["target_env"],
            "status": action["status"],
            "version": action["version"],
        }

    @app.post("/v1/actions/{action_id}/approvals", status_code=201)
    async def decide_action(
        action_id: int,
        body: ActionApprovalIn,
        approver: str = Header(alias="X-Approver-Identity"),
    ) -> dict[str, Any]:
        """Record a human decision for an action (FR-18 RBAC).

        The approver identity comes from the authenticated header, never
        the body; it must be on the service's approver list, and the body's
        expected_digest must equal the stored patch digest.
        """
        action = await get_action(pool, action_id)
        if action is None:
            raise HTTPException(status_code=404, detail="unknown action")
        incident_id = str(action["incident_id"])
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT service FROM incidents WHERE id = %s", (incident_id,)
            )
            svc = await cur.fetchone()
        service = str(svc[0]) if svc else ""
        if approver not in _approvers_for(service):
            raise HTTPException(
                status_code=403,
                detail=f"{approver} is not an approver for {service}",
            )
        if body.expected_digest != action["patch_digest"]:
            raise HTTPException(
                status_code=409, detail="expected_digest does not match the action"
            )
        if action["status"] != "ready_for_review":
            raise HTTPException(
                status_code=409,
                detail=f"action is {action['status']}, not ready_for_review",
            )
        candidate = await load_patch_by_digest(
            pool, incident_id, action["patch_digest"]
        )
        if candidate is None:
            raise HTTPException(status_code=409, detail="stored patch missing")
        approval = await request_approval(
            pool,
            action["patch_digest"],
            action["target_env"],
            candidate.base_revision,
            os.environ.get("POLICY_VERSION", "v1"),
            approver,
        )
        try:
            decided = await decide(pool, approval.id, body.decision == "granted")
        except ApprovalExpired as exc:
            raise HTTPException(status_code=410, detail=str(exc)) from exc
        if body.decision == "granted":
            await advance(
                pool, "actions", action_id, "approved", action["version"]
            )
        signalled = await _signal_workflow(incident_id, body.decision)
        return {
            "approval_id": decided.id,
            "action_id": action_id,
            "decision": decided.decision,
            "approver": approver,
            "valid_until": (
                decided.valid_until.isoformat() if decided.valid_until else None
            ),
            "signalled": signalled,
        }

    return app


def run() -> None:
    import asyncio

    import uvicorn

    async def lifespan_app() -> None:
        from ai_sre.worker import run_embedded_relay

        pool = get_pool()
        await pool.open()
        await init_schema(pool)
        await init_approvals(pool)
        await init_dispatch(pool)
        await init_model(pool)
        relay = asyncio.ensure_future(run_embedded_relay())
        config = uvicorn.Config(create_app(pool), host="0.0.0.0", port=8000)
        server = uvicorn.Server(config)
        await server.serve()
        relay.cancel()

    asyncio.run(lifespan_app())
