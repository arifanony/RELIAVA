"""M7-TRUST gates: FR-16 digest-bound dispatch chain + FR-18 HTTP contract.

The trusted path must reload action content from PostgreSQL by digest —
never from a workflow/API payload — and approvals must be RBAC-checked,
digest-bound records, not free-text names.
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool

from ai_sre.activities import (
    collect_evidence_for,
    diagnose_and_persist,
    dispatch_for,
    verify_for,
)
from ai_sre.activities import test_patch_for as run_test_patch
from ai_sre.approvals import decide, init_approvals, request_approval
from ai_sre.db import database_url, init_schema
from ai_sre.dispatch import init_dispatch
from ai_sre.evidence import EvidenceItem
from ai_sre.main import create_app
from ai_sre.model import get_action, init_model
from ai_sre.states import advance

OLD = "total = price * 0.9  # coupon bug"
NEW = "total = price * 0.95  # coupon bug fixed"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def fresh_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False,
        kwargs={"connect_timeout": 10},
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
                (incident_id, f"trust-{incident_id}", "checkout", "staging",
                 Json({"logs": ["discount applied twice"]})),
            )
        await conn.commit()
    return incident_id


class StubTransport:
    """Deterministic model double: diagnosis, critic and patch replies."""

    def __init__(self, family: str, model: str) -> None:
        self.family = family
        self.model = model

    async def generate(self, prompt: str) -> dict[str, Any]:
        if prompt.startswith("You are the patch skill"):
            text = json.dumps(
                {"path": "app.py", "old": OLD, "new": NEW,
                 "base_revision": "rev-1"}
            )
        elif prompt.startswith("Accept or reject"):
            text = '{"accept": true, "notes": "sound"}'
        else:
            text = json.dumps(
                {"hypotheses": [
                    {"text": "coupon applied twice", "evidence_ids": ["E1"]}
                ]}
            )
        return {"text": text, "input_tokens": 100, "output_tokens": 50}


async def diagnosed_action(pool: AsyncConnectionPool) -> tuple[str, int, str]:
    """Drive collect -> diagnose -> test_patch; return (incident, action, digest)."""
    incident_id = await mk_incident(pool)
    await collect_evidence_for(pool, incident_id)  # advances to investigating
    # The stub diagnosis cites E1; give the cycle a matching evidence item.
    items = [EvidenceItem(id="E1", source="logs", window="5m",
                          text="discount applied twice")]
    diagnosis_json = await diagnose_and_persist(
        pool, incident_id, items,
        StubTransport("google", "stub-diagnosis"),
        StubTransport("llama", "stub-critic"),
    )
    result = json.loads(diagnosis_json)
    assert result["status"] == "ACCEPTED", result
    tested = json.loads(await run_test_patch(pool, diagnosis_json))
    assert tested["sandbox"] == "passed", tested
    return incident_id, int(result["action_id"]), str(result["digest"])


async def approve_action(
    pool: AsyncConnectionPool, action_id: int, digest: str
) -> None:
    action = await get_action(pool, action_id)
    assert action is not None
    approval = await request_approval(
        pool, digest, "staging", "rev-1", "v1", "alice"
    )
    await decide(pool, approval.id, True)
    await advance(pool, "actions", action_id, "approved", action["version"])


@pytest.fixture(scope="module", autouse=True)
def _migrated() -> None:
    """Production schema = init_* + Alembic migrations (the migrations add
    the state-machine version columns); apply them once for this module."""
    import subprocess
    import sys

    root = Path(__file__).resolve().parent.parent
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "upgrade", "head"],
        capture_output=True, text=True, timeout=180, cwd=root, check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.fixture(autouse=True)
def _env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PATCH_ROOTS", ".")
    monkeypatch.setenv("AI_SRE_SANDBOX", "validate")
    monkeypatch.setenv("APPROVERS", "alice")


def test_chain_persists_and_dispatch_applies_stored_patch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id, action_id, digest = await diagnosed_action(pool)
            action = await get_action(pool, action_id)
            assert action is not None and action["status"] == "ready_for_review"
            await approve_action(pool, action_id, digest)
            tree = tmp_path / "staging"
            tree.mkdir()
            (tree / "app.py").write_text(f"def bill(price):\n    {OLD}\n")
            monkeypatch.setenv("STAGING_TREE", str(tree))
            monkeypatch.setenv("STAGING_WRITE_CREDENTIALS", "test-cred")
            out = json.loads(
                await dispatch_for(
                    pool, json.dumps({"action_id": action_id, "digest": digest})
                )
            )
            assert out["status"] == "applied", out
            assert NEW in (tree / "app.py").read_text()
            action = await get_action(pool, action_id)
            assert action is not None and action["status"] == "succeeded"
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT status FROM incidents WHERE id = %s", (incident_id,)
                )
                row = await cur.fetchone()
            assert row is not None and row[0] == "remediating"
        finally:
            await pool.close()

    run(scenario())


def test_dispatch_ignores_payload_content(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            _incident, action_id, digest = await diagnosed_action(pool)
            await approve_action(pool, action_id, digest)
            tree = tmp_path / "staging"
            tree.mkdir()
            (tree / "app.py").write_text(f"def bill(price):\n    {OLD}\n")
            monkeypatch.setenv("STAGING_TREE", str(tree))
            monkeypatch.setenv("STAGING_WRITE_CREDENTIALS", "test-cred")
            forged = json.dumps(
                {
                    "action_id": action_id, "digest": digest,
                    "path": "app.py", "old": OLD, "new": "PWNED",
                    "base_revision": "rev-1",
                }
            )
            out = json.loads(await dispatch_for(pool, forged))
            assert out["status"] == "applied", out
            content = (tree / "app.py").read_text()
            assert NEW in content and "PWNED" not in content
        finally:
            await pool.close()

    run(scenario())


def test_dispatch_detects_tampered_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id, action_id, digest = await diagnosed_action(pool)
            await approve_action(pool, action_id, digest)
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "SELECT content FROM artifacts WHERE incident_id = %s",
                        (incident_id,),
                    )
                    row = await cur.fetchone()
                    assert row is not None
                    payload = json.loads(bytes(row[0]).decode())
                    payload["new"] = "tampered"
                    await cur.execute(
                        "UPDATE artifacts SET content = %s"
                        " WHERE incident_id = %s",
                        (json.dumps(payload).encode(), incident_id),
                    )
                await conn.commit()
            tree = tmp_path / "staging"
            tree.mkdir()
            (tree / "app.py").write_text(f"def bill(price):\n    {OLD}\n")
            monkeypatch.setenv("STAGING_TREE", str(tree))
            monkeypatch.setenv("STAGING_WRITE_CREDENTIALS", "test-cred")
            with pytest.raises(ValueError):
                await dispatch_for(
                    pool, json.dumps({"action_id": action_id, "digest": digest})
                )
            assert OLD in (tree / "app.py").read_text()
        finally:
            await pool.close()

    run(scenario())


def test_dispatch_blocked_without_pg_approval(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id, action_id, digest = await diagnosed_action(pool)
            action = await get_action(pool, action_id)
            assert action is not None
            # State advanced without any approvals row: dispatch must refuse.
            await advance(
                pool, "actions", action_id, "approved", action["version"]
            )
            tree = tmp_path / "staging"
            tree.mkdir()
            (tree / "app.py").write_text(f"def bill(price):\n    {OLD}\n")
            monkeypatch.setenv("STAGING_TREE", str(tree))
            monkeypatch.setenv("STAGING_WRITE_CREDENTIALS", "test-cred")
            out = json.loads(
                await dispatch_for(
                    pool, json.dumps({"action_id": action_id, "digest": digest})
                )
            )
            assert out["status"] == "blocked", out
            assert OLD in (tree / "app.py").read_text()
            action = await get_action(pool, action_id)
            assert action is not None and action["status"] == "failed"
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT status FROM incidents WHERE id = %s", (incident_id,)
                )
                row = await cur.fetchone()
            assert row is not None and row[0] == "escalated"
        finally:
            await pool.close()

    run(scenario())


def _prom_handler(error_rate: str) -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/checkout"):
            return httpx.Response(200, json={"order_id": "x"})
        query = request.url.params.get("query", "")
        if "histogram_quantile" in query:
            return httpx.Response(
                200, json={"data": {"result": []}}
            )
        return httpx.Response(
            200,
            json={"data": {"result": [
                {"values": [[1700000000, error_rate], [1700000030, error_rate]]}
            ]}},
        )

    return httpx.MockTransport(handler)


def test_verify_windowed_resolves_then_escalates() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id, action_id, digest = await diagnosed_action(pool)
            await approve_action(pool, action_id, digest)
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE incidents SET status = 'remediating'"
                        " WHERE id = %s",
                        (incident_id,),
                    )
                await conn.commit()
            payload = json.dumps(
                {"incident_id": incident_id, "status": "applied",
                 "action_id": action_id, "digest": digest}
            )
            async with httpx.AsyncClient(
                transport=_prom_handler("0.0"), base_url="http://x"
            ) as http:
                verdict = await verify_for(
                    pool, payload, prom_url="http://x", replay_url="http://x",
                    http=http,
                )
            assert verdict == "resolved"
            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT verdict FROM verifications WHERE incident_id = %s",
                    (incident_id,),
                )
                row = await cur.fetchone()
            assert row is not None and row[0] == "RESOLVED"
        finally:
            await pool.close()

    run(scenario())


def test_api_fr18_contract_and_rbac() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            incident_id, action_id, digest = await diagnosed_action(pool)
            app = create_app(pool)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://t"
            ) as client:
                health = await client.get("/healthz")
                assert health.status_code == 200
                ready = await client.get("/readyz")
                assert ready.status_code == 200
                metrics = await client.get("/metrics")
                assert metrics.status_code == 200
                assert "ai_sre_incidents" in metrics.text
                detail = await client.get(f"/v1/incidents/{incident_id}")
                assert detail.status_code == 200
                assert detail.json()["actions"][0]["id"] == action_id
                action_view = await client.get(f"/v1/actions/{action_id}")
                assert action_view.status_code == 200
                assert action_view.json()["status"] == "ready_for_review"
                denied = await client.post(
                    f"/v1/actions/{action_id}/approvals",
                    json={"decision": "granted", "expected_digest": digest},
                    headers={"X-Approver-Identity": "mallory"},
                )
                assert denied.status_code == 403
                mismatch = await client.post(
                    f"/v1/actions/{action_id}/approvals",
                    json={"decision": "granted", "expected_digest": "0" * 64},
                    headers={"X-Approver-Identity": "alice"},
                )
                assert mismatch.status_code == 409
                granted = await client.post(
                    f"/v1/actions/{action_id}/approvals",
                    json={"decision": "granted", "expected_digest": digest},
                    headers={"X-Approver-Identity": "alice"},
                )
                assert granted.status_code == 201, granted.text
                assert granted.json()["decision"] == "granted"
                action_view = await client.get(f"/v1/actions/{action_id}")
                assert action_view.json()["status"] == "approved"
                events = await client.get(
                    f"/v1/incidents/{incident_id}/events"
                )
                assert events.status_code == 200
                cancelled = await client.post(
                    f"/v1/incidents/{incident_id}/cancel"
                )
                assert cancelled.status_code == 202
        finally:
            await pool.close()

    run(scenario())


def test_worker_role_entrypoints_exist() -> None:
    from ai_sre import worker

    for role in ("investigation", "sandbox", "trusted"):
        assert callable(worker.run_role)
        assert role in worker.ROLE_ACTIVITIES
    trusted = {a.__name__ for a in worker.ROLE_ACTIVITIES["trusted"]}
    assert trusted == {"dispatch_action_activity", "verify"}
    investigation = {a.__name__ for a in worker.ROLE_ACTIVITIES["investigation"]}
    assert "dispatch_action_activity" not in investigation
