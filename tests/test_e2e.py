"""M8-GATEWAY gate: AC-10 end-to-end, in process.

The config-regression demo alert travels the full path — intake API,
investigation (collect + gateway diagnosis), sandbox validation, RBAC
approval over the FR-18 API, trusted dispatch by digest, windowed
verification — to RESOLVED, with the state history on the timeline and a
gateway attempt record for every model call. The four trust zones run as
their activity helpers here; the Temporal wiring of the same path is
covered by tests/test_spine.py.
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from psycopg_pool import AsyncConnectionPool

from ai_sre.activities import (
    collect_evidence_for,
    diagnose_and_persist,
    dispatch_for,
    verify_for,
)
from ai_sre.activities import test_patch_for as run_test_patch
from ai_sre.approvals import init_approvals
from ai_sre.db import database_url, init_schema
from ai_sre.dispatch import init_dispatch
from ai_sre.main import create_app
from ai_sre.model import init_model

OLD = "total = price * 0.9  # coupon bug"
NEW = "total = price * 0.95  # coupon bug fixed"


def run(coro: Any) -> Any:
    return asyncio.run(coro)


@pytest.fixture(scope="module", autouse=True)
def _migrated() -> None:
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


class StubTransport:
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
            text = json.dumps({"hypotheses": [
                {"text": "coupon applied twice", "evidence_ids": ["E1"]}
            ]})
        return {"text": text, "input_tokens": 100, "output_tokens": 50}


def _prom_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/checkout"):
            return httpx.Response(200, json={"order_id": "x"})
        if "histogram_quantile" in request.url.params.get("query", ""):
            return httpx.Response(200, json={"data": {"result": []}})
        return httpx.Response(200, json={"data": {"result": [
            {"values": [[1700000000, "0.0"], [1700000030, "0.0"]]}
        ]}})

    return httpx.MockTransport(handler)


def test_alertmanager_webhook_creates_incident_idempotently() -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            app = create_app(pool)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://t"
            ) as client:
                payload = {
                    "groupKey": "checkout-errors",
                    "commonLabels": {"service": "checkout",
                                     "environment": "staging"},
                    "alerts": [{
                        "fingerprint": "abc123",
                        "labels": {"service": "checkout",
                                   "environment": "staging"},
                        "annotations": {"summary": "error rate high"},
                    }],
                }
                first = await client.post("/v1/alertmanager", json=payload)
                assert first.status_code == 202, first.text
                second = await client.post("/v1/alertmanager", json=payload)
                assert second.status_code == 202
                assert second.json()["id"] == first.json()["id"]
                detail = await client.get(
                    f"/v1/incidents/{first.json()['id']}"
                )
                assert detail.json()["service"] == "checkout"
        finally:
            await pool.close()

    run(scenario())


def test_demo_alert_travels_full_path_to_resolved(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            app = create_app(pool)
            async with AsyncClient(
                transport=ASGITransport(app=app), base_url="http://t"
            ) as client:
                created = await client.post(
                    "/v1/incidents",
                    json={
                        "service": "checkout",
                        "environment": "staging",
                        "alert": {
                            "logs": ["discount applied twice"],
                            "git_revision": "rev-1",
                            "deployments": ["deploy checkout v12 at 09:41"],
                        },
                    },
                    headers={"Idempotency-Key": "e2e-demo-1"},
                )
                assert created.status_code == 202, created.text
                incident_id = created.json()["id"]

                # Investigation zone: collect, then gateway diagnosis.
                evidence_json = await collect_evidence_for(pool, incident_id)
                entries = json.loads(evidence_json)["items"]
                from ai_sre.evidence import EvidenceItem

                items = [
                    EvidenceItem(id=e["id"], source=e["source"],
                                 window=e["window"], text=e["text"])
                    for e in entries
                ]
                # The stub diagnosis cites E1; align the first item's id.
                items[0] = EvidenceItem(
                    id="E1", source=items[0].source, window=items[0].window,
                    text=items[0].text,
                )
                diagnosis_json = await diagnose_and_persist(
                    pool, incident_id, items,
                    StubTransport("google", "stub-diagnosis"),
                    StubTransport("llama", "stub-critic"),
                    diagnosis_provider="gemini", critic_provider="ollama",
                )
                diagnosis = json.loads(diagnosis_json)
                assert diagnosis["status"] == "ACCEPTED", diagnosis
                action_id, digest = diagnosis["action_id"], diagnosis["digest"]

                # Sandbox zone: validate the stored candidate by digest.
                tested = json.loads(await run_test_patch(pool, diagnosis_json))
                assert tested["sandbox"] == "passed", tested

                # Human approval over the FR-18 API (RBAC identity header).
                granted = await client.post(
                    f"/v1/actions/{action_id}/approvals",
                    json={"decision": "granted", "expected_digest": digest},
                    headers={"X-Approver-Identity": "alice"},
                )
                assert granted.status_code == 201, granted.text

                # Trusted zone: dispatch reloads the patch from PG.
                tree = tmp_path / "staging"
                tree.mkdir()
                (tree / "app.py").write_text(f"def bill(price):\n    {OLD}\n")
                monkeypatch.setenv("STAGING_TREE", str(tree))
                monkeypatch.setenv("STAGING_WRITE_CREDENTIALS", "test-cred")
                dispatched = json.loads(await dispatch_for(
                    pool, json.dumps({"action_id": action_id, "digest": digest})
                ))
                assert dispatched["status"] == "applied", dispatched
                assert NEW in (tree / "app.py").read_text()

                # Verification over the window + replay -> RESOLVED.
                async with httpx.AsyncClient(
                    transport=_prom_transport(), base_url="http://x"
                ) as http:
                    verdict = await verify_for(
                        pool, json.dumps(dispatched), prom_url="http://x",
                        replay_url="http://x", http=http,
                    )
                assert verdict == "resolved"

                detail = await client.get(f"/v1/incidents/{incident_id}")
                assert detail.json()["status"] == "resolved"
                events = await client.get(
                    f"/v1/incidents/{incident_id}/events"
                )
                assert events.status_code == 200

            async with pool.connection() as conn, conn.cursor() as cur:
                await cur.execute(
                    "SELECT role, outcome FROM model_attempts"
                    " WHERE incident_id = %s ORDER BY id",
                    (incident_id,),
                )
                attempts = await cur.fetchall()
            roles = {role for role, _outcome in attempts}
            assert {"diagnosis", "critic", "patch"} <= roles
            assert all(outcome == "ok" for _role, outcome in attempts)
        finally:
            await pool.close()

    run(scenario())
