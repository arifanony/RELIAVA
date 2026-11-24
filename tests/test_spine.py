"""M6 eval: state machines, Alembic spine, LISTEN relay, Temporal workflow."""

from __future__ import annotations

import asyncio
import itertools
import os
import subprocess
import sys
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from temporalio import activity
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

from ai_sre.approvals import init_approvals
from ai_sre.db import database_url, init_schema
from ai_sre.dispatch import init_dispatch
from ai_sre.main import create_app
from ai_sre.model import (
    ArtifactTooLarge,
    init_model,
    insert_artifact,
    insert_record,
)
from ai_sre.relay import OUTBOX_CHANNEL, relay_listen, relay_sweep
from ai_sre.states import (
    IllegalTransition,
    UnknownRecord,
    VersionConflict,
    advance,
    check,
)
from ai_sre.workflows import (
    ACTIVITY_TIMEOUT,
    MAX_ACTIVITY_ATTEMPTS,
    TASK_QUEUE_INVESTIGATION,
    TASK_QUEUE_SANDBOX,
    TASK_QUEUE_TRUSTED,
    WORKFLOW_EXECUTION_TIMEOUT,
    IncidentWorkflow,
)

ROOT = Path(__file__).resolve().parent.parent


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def test_incident_state_machine() -> None:
    walk = ["received", "investigating", "diagnosed", "awaiting_approval",
            "remediating", "verifying", "resolved"]
    for old, new in itertools.pairwise(walk):
        check("incidents", old, new)
    for bad in [("received", "resolved"), ("resolved", "investigating"),
                ("verifying", "received"), ("escalated", "received")]:
        with pytest.raises(IllegalTransition):
            check("incidents", *bad)
    with pytest.raises(IllegalTransition):
        check("nope", "a", "b")


def test_action_state_machine() -> None:
    for old, new in [("proposed", "testing"), ("testing", "ready_for_review"),
                     ("ready_for_review", "approved"), ("approved", "dispatching"),
                     ("dispatching", "unknown"), ("unknown", "succeeded"),
                     ("dispatching", "failed")]:
        check("actions", old, new)
    for bad in [("proposed", "approved"), ("succeeded", "testing"),
                ("failed", "dispatching")]:
        with pytest.raises(IllegalTransition):
            check("actions", *bad)


def test_alembic_upgrade_downgrade_cycle() -> None:
    def alembic(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, "-m", "alembic", *args],
            capture_output=True, text=True, timeout=120, cwd=ROOT, check=False,
        )

    async def has_table(name: str) -> bool:
        pool = AsyncConnectionPool(
            os.environ.get("DATABASE_URL", database_url()), open=False,
        kwargs={"connect_timeout": 10}
        )
        await pool.open()
        try:
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute("SELECT to_regclass(%s)", (f"public.{name}",))
                row = await cur.fetchone()
                assert row is not None
                return row[0] == name
        finally:
            await pool.close()

    assert alembic("upgrade", "head").returncode == 0
    assert alembic("upgrade", "head").returncode == 0  # idempotent rerun
    current = alembic("current")
    assert current.returncode == 0 and "0002" in current.stdout
    assert run(has_table("actions"))
    assert alembic("downgrade", "base").returncode == 0
    assert not run(has_table("actions"))
    assert alembic("upgrade", "head").returncode == 0
    assert run(has_table("actions"))


async def fresh_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False,
        kwargs={"connect_timeout": 10}
    )
    await pool.open()
    await init_schema(pool)
    await init_approvals(pool)
    await init_dispatch(pool)
    await init_model(pool)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "TRUNCATE dispatch_events, dispatches, approvals,"
                " incident_events, outbox, incidents,"
                " actions, hypotheses, diagnoses, artifacts, verifications"
            )
        await conn.commit()
    return pool


async def mk_incident(pool: AsyncConnectionPool) -> str:
    incident_id = str(uuid.uuid4())
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO incidents (id, idempotency_key, service,"
                " environment, payload) VALUES (%s, %s, %s, %s, %s)",
                (incident_id, f"spine-{incident_id}", "checkout", "staging", "{}"),
            )
        await conn.commit()
    return incident_id


def test_advance_enforces_table_version_and_order() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id = await mk_incident(pool)
            assert await advance(pool, "incidents", incident_id, "investigating", 1) == 2
            with pytest.raises(VersionConflict):
                await advance(pool, "incidents", incident_id, "diagnosed", 1)
            with pytest.raises(IllegalTransition):
                await advance(pool, "incidents", incident_id, "resolved", 2)
            with pytest.raises(UnknownRecord):
                await advance(pool, "incidents", str(uuid.uuid4()), "investigating", 1)
            with pytest.raises(IllegalTransition):
                await advance(pool, "dispatches", incident_id, "x", 1)
        finally:
            await pool.close()

    run(scenario())


def test_canonical_records_and_artifact_cap() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id = await mk_incident(pool)
            hyp = await insert_record(
                pool, "hypotheses",
                {"incident_id": incident_id, "text": "coupon bug",
                 "evidence_ids": Json(["E1"])},
            )
            assert hyp > 0
            diag = await insert_record(
                pool, "diagnoses",
                {"incident_id": incident_id, "status": "accepted", "revisions": 1},
            )
            assert diag > 0
            with pytest.raises(ValueError):
                await insert_record(pool, "incidents", {"status": "x"})
            with pytest.raises(ArtifactTooLarge):
                await insert_artifact(pool, incident_id, "bundle", b"x" * (1024 * 1024 + 1))
            ref = await insert_artifact(pool, incident_id, "bundle", b"evidence-bytes")
            import hashlib as _hashlib

            assert ref.sha256 == _hashlib.sha256(b"evidence-bytes").hexdigest()
        finally:
            await pool.close()

    run(scenario())


def test_relay_listen_starts_workflow_on_notify() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        client = AsyncClient(
            transport=ASGITransport(app=create_app(pool)), base_url="http://m6"
        )
        started: list[tuple[int, str]] = []

        async def start_workflow(outbox_id: int, incident_id: str) -> None:
            started.append((outbox_id, incident_id))

        relay = asyncio.ensure_future(
            relay_listen(database_url(), start_workflow, stop_after=1)
        )
        try:
            await asyncio.sleep(0.3)  # let LISTEN attach before the insert
            posted = await client.post(
                "/v1/incidents",
                json={"service": "checkout", "environment": "staging", "alert": {}},
                headers={"Idempotency-Key": f"spine-{uuid.uuid4().hex}"},
            )
            assert posted.status_code == 202
            handled = await asyncio.wait_for(asyncio.shield(relay), timeout=10)
            assert handled == 1
            assert started[0][1] == posted.json()["id"]
            uuid.UUID(started[0][1])  # relay hands a usable id to incident-<id>
        finally:
            if not relay.done():
                relay.cancel()
            await client.aclose()
            await pool.close()

    run(scenario())


def test_relay_sweep_covers_missed_notify() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            sweeps = 0

            async def sweep() -> None:
                nonlocal sweeps
                sweeps += 1

            async def never_called(outbox_id: int, incident_id: str) -> None:
                raise AssertionError("no notify expected")

            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(
                    relay_listen(database_url(), never_called, sweep=sweep), timeout=3
                )
            assert sweeps >= 1  # 2 s fallback sweep ran with no notifies
            assert OUTBOX_CHANNEL == "incident_outbox"
        finally:
            await pool.close()

    run(scenario())


async def _outbox_row(pool: AsyncConnectionPool, incident_id: str) -> tuple[Any, ...]:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, dispatched_at, workflow_id FROM outbox"
            " WHERE incident_id = %s",
            (incident_id,),
        )
        row = await cur.fetchone()
        assert row is not None
        return row


def test_relay_notify_marks_row_dispatched() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        client = AsyncClient(
            transport=ASGITransport(app=create_app(pool)), base_url="http://m6"
        )

        async def start_workflow(outbox_id: int, incident_id: str) -> None:
            pass

        relay = asyncio.ensure_future(
            relay_listen(database_url(), start_workflow, stop_after=1)
        )
        try:
            await asyncio.sleep(0.3)
            posted = await client.post(
                "/v1/incidents",
                json={"service": "checkout", "environment": "staging", "alert": {}},
                headers={"Idempotency-Key": f"spine-{uuid.uuid4().hex}"},
            )
            assert posted.status_code == 202
            assert await asyncio.wait_for(asyncio.shield(relay), timeout=10) == 1
            _id, dispatched_at, workflow_id = await _outbox_row(
                pool, posted.json()["id"]
            )
            assert dispatched_at is not None
            assert workflow_id == f"incident-{posted.json()['id']}"
        finally:
            if not relay.done():
                relay.cancel()
            await client.aclose()
            await pool.close()

    run(scenario())


def test_relay_sweep_starts_and_marks_without_notify() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id = await mk_incident(pool)
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO outbox (incident_id, type, payload)"
                        " VALUES (%s, 'incident.created', '{}')",
                        (incident_id,),
                    )
                await conn.commit()
            started: list[tuple[int, str]] = []

            async def start_workflow(outbox_id: int, incident_id_: str) -> None:
                started.append((outbox_id, incident_id_))

            assert await relay_sweep(pool, start_workflow) == 1
            assert started[0][1] == incident_id
            _id, dispatched_at, workflow_id = await _outbox_row(pool, incident_id)
            assert dispatched_at is not None
            assert workflow_id == f"incident-{incident_id}"
            assert await relay_sweep(pool, start_workflow) == 0  # nothing left
        finally:
            await pool.close()

    run(scenario())


def test_relay_sweep_leaves_row_when_start_fails() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id = await mk_incident(pool)
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO outbox (incident_id, type, payload)"
                        " VALUES (%s, 'incident.created', '{}')",
                        (incident_id,),
                    )
                await conn.commit()

            async def failing(outbox_id: int, incident_id_: str) -> None:
                raise RuntimeError("temporal down")

            assert await relay_sweep(pool, failing) == 0
            _id, dispatched_at, _wf = await _outbox_row(pool, incident_id)
            assert dispatched_at is None  # still pending for a later sweep

            async def working(outbox_id: int, incident_id_: str) -> None:
                pass

            assert await relay_sweep(pool, working) == 1
        finally:
            await pool.close()

    run(scenario())


def test_workflow_deadlines_queues_and_attempts() -> None:
    assert WORKFLOW_EXECUTION_TIMEOUT == timedelta(minutes=8)
    assert ACTIVITY_TIMEOUT == timedelta(seconds=25)
    assert MAX_ACTIVITY_ATTEMPTS == 3
    assert (TASK_QUEUE_INVESTIGATION, TASK_QUEUE_SANDBOX, TASK_QUEUE_TRUSTED) == (
        "investigation", "sandbox", "trusted",
    )


@activity.defn(name="collect_evidence")
async def t_collect(incident_id: str) -> str:
    return '[{"id": "E1", "source": "logs", "window": "5m", "text": "boom"}]'


@activity.defn(name="diagnose")
async def t_diagnose(evidence_json: str) -> str:
    assert "E1" in evidence_json
    return '{"status": "ACCEPTED", "patch": {"path": "demo/app.py"}}'


@activity.defn(name="test_patch")
async def t_patch(diagnosis_json: str) -> str:
    assert "ACCEPTED" in diagnosis_json
    return '{"digest": "abc123"}'


@activity.defn(name="dispatch_action_activity")
async def t_dispatch(patch_json: str) -> str:
    assert "abc123" in patch_json
    return '{"dispatch_id": 7, "status": "applied"}'


@activity.defn(name="verify")
async def t_verify(dispatch_json: str) -> str:
    assert "applied" in dispatch_json
    return "resolved"


async def _run_workflow(decision: str) -> str:
    async with await WorkflowEnvironment.start_time_skipping() as env:
        investigation = Worker(
            env.client, task_queue=TASK_QUEUE_INVESTIGATION,
            workflows=[IncidentWorkflow], activities=[t_collect, t_diagnose],
        )
        sandbox = Worker(
            env.client, task_queue=TASK_QUEUE_SANDBOX, activities=[t_patch]
        )
        trusted = Worker(
            env.client, task_queue=TASK_QUEUE_TRUSTED,
            activities=[t_dispatch, t_verify],
        )
        async with investigation, sandbox, trusted:
            handle = await env.client.start_workflow(
                IncidentWorkflow.run, "inc-1", id=f"incident-test-{decision}",
                task_queue=TASK_QUEUE_INVESTIGATION,
                execution_timeout=WORKFLOW_EXECUTION_TIMEOUT,
            )
            await handle.signal(IncidentWorkflow.approval, decision)
            return await asyncio.wait_for(handle.result(), timeout=120)


def test_workflow_grant_resolves_reject_escalates() -> None:
    assert run(_run_workflow("granted")) == "resolved"
    assert run(_run_workflow("rejected")) == "escalated"


def test_worker_entry_imports() -> None:
    import ai_sre.worker as _worker

    assert _worker.temporal_address().endswith("7233")
    assert callable(_worker.main)
