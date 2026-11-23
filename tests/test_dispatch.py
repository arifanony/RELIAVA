"""M4 eval: digest-bound approval, idempotent dispatch, UNKNOWN reconcile, verify."""

from __future__ import annotations

import asyncio
import os
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from psycopg_pool import AsyncConnectionPool

from ai_sre.approvals import (
    ApprovalExpired,
    decide,
    find_valid,
    init_approvals,
    request_approval,
)
from ai_sre.cli import main as cli_main
from ai_sre.db import database_url, init_schema
from ai_sre.dispatch import (
    CREDENTIAL_ENV,
    MissingCredential,
    NotApproved,
    UnknownDispatch,
    dispatch_action,
    init_dispatch,
    reconcile,
)
from ai_sre.main import create_app
from ai_sre.patches import PatchCandidate
from ai_sre.verify import MetricSample, evaluate_verification, verify_dispatch


def run(coro: Any) -> Any:
    return asyncio.run(coro)


DIGEST = PatchCandidate(
    path="demo/app.py",
    old="return total + total * 0.10",
    new="return total - total * 0.10",
    base_revision="m4-fixture-v1",
).digest
TARGET = "staging"
REVISION = "m4-fixture-v1"


async def fresh_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False
    )
    await pool.open()
    await init_schema(pool)
    await init_approvals(pool)
    await init_dispatch(pool)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute("TRUNCATE dispatch_events, dispatches, approvals")
        await conn.commit()
    return pool


async def approved(pool: AsyncConnectionPool, digest: str = DIGEST) -> int:
    approval = await request_approval(
        pool, digest, TARGET, REVISION, "policy-v1", "operator"
    )
    granted = await decide(pool, approval.id, True)
    assert granted.valid_until is not None
    return granted.id


class Calls:
    def __init__(self) -> None:
        self.count = 0

    async def ok(self, action: dict[str, Any]) -> dict[str, Any]:
        self.count += 1
        return {"ok": True, "action": action}

    async def slow(self, action: dict[str, Any]) -> dict[str, Any]:
        self.count += 1
        await asyncio.sleep(30)
        return {"ok": True}

    async def boom(self, action: dict[str, Any]) -> dict[str, Any]:
        self.count += 1
        raise RuntimeError("executor blew up")


def test_approval_grant_reject_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            approval = await request_approval(
                pool, DIGEST, TARGET, REVISION, "policy-v1", "operator"
            )
            granted = await decide(pool, approval.id, True)
            assert granted.decision == "granted"
            assert granted.valid_until is not None
            assert await find_valid(pool, DIGEST, TARGET, REVISION) is not None

            rejected = await decide(
                pool,
                (await request_approval(pool, "other", TARGET, REVISION, "policy-v1", "op")).id,
                False,
            )
            assert rejected.decision == "rejected"
            assert await find_valid(pool, "other", TARGET, REVISION) is None

            stale = await request_approval(pool, "stale", TARGET, REVISION, "policy-v1", "op")
            async with pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "UPDATE approvals SET requested_at = now() - interval '26 hours',"
                        " expires_at = now() - interval '2 hours' WHERE id = %s",
                        (stale.id,),
                    )
                await conn.commit()
            with pytest.raises(ApprovalExpired):
                await decide(pool, stale.id, True)
        finally:
            await pool.close()

    run(scenario())


def test_dispatch_requires_approval_and_credential(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            calls = Calls()
            with pytest.raises(NotApproved):
                await dispatch_action(pool, DIGEST, TARGET, REVISION, {}, calls.ok)
            await approved(pool)
            monkeypatch.delenv(CREDENTIAL_ENV)
            with pytest.raises(MissingCredential):
                await dispatch_action(pool, DIGEST, TARGET, REVISION, {}, calls.ok)
            assert calls.count == 0
        finally:
            await pool.close()

    run(scenario())


def test_dispatch_idempotent_no_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            approval_id = await approved(pool)
            calls = Calls()
            first = await dispatch_action(pool, DIGEST, TARGET, REVISION, {}, calls.ok)
            assert first.status == "applied"
            assert first.attempts == 1
            assert first.approval_id == approval_id  # AC-9 binding
            second = await dispatch_action(pool, DIGEST, TARGET, REVISION, {}, calls.ok)
            assert second.id == first.id
            assert second.attempts == 1
            assert calls.count == 1
        finally:
            await pool.close()

    run(scenario())


def test_timeout_unknown_then_reconcile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            await approved(pool, "slow-action")
            calls = Calls()
            unknown = await dispatch_action(
                pool, "slow-action", TARGET, REVISION, {}, calls.slow, timeout_s=1
            )
            assert unknown.status == "unknown"
            assert unknown.attempts == 1
            repeat = await dispatch_action(
                pool, "slow-action", TARGET, REVISION, {}, calls.slow, timeout_s=1
            )
            assert repeat.id == unknown.id
            assert calls.count == 1  # AC-4: never auto-retried
            resolved = await reconcile(pool, unknown.id, lambda: True)
            assert resolved.status == "applied"
            assert resolved.attempts == 1

            await approved(pool, "absent-action")
            missing = await dispatch_action(
                pool, "absent-action", TARGET, REVISION, {}, calls.slow, timeout_s=1
            )
            rolled = await reconcile(pool, missing.id, lambda: False)
            assert rolled.status == "failed"

            with pytest.raises(UnknownDispatch):
                await reconcile(pool, resolved.id, lambda: True)
        finally:
            await pool.close()

    run(scenario())


def test_executor_failure_marks_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            await approved(pool, "boom-action")
            calls = Calls()
            failed = await dispatch_action(
                pool, "boom-action", TARGET, REVISION, {}, calls.boom
            )
            assert failed.status == "failed"
            assert failed.attempts == 1
        finally:
            await pool.close()

    run(scenario())


def test_verify_resolved_and_escalated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        try:
            await approved(pool, "verify-action")
            calls = Calls()
            dispatch = await dispatch_action(
                pool, "verify-action", TARGET, REVISION, {}, calls.ok
            )
            good = [MetricSample(error_rate=0.005, p95_ms=110) for _ in range(5)]
            assert evaluate_verification(good, 100.0, True) == "RESOLVED"
            verdict = await verify_dispatch(
                pool, dispatch.id, lambda: good, lambda: True, 100.0
            )
            assert verdict == "RESOLVED"
            bad = [MetricSample(error_rate=0.05, p95_ms=110) for _ in range(5)]
            assert evaluate_verification(bad, 100.0, True) == "ESCALATED"
            slow = [MetricSample(error_rate=0.001, p95_ms=150) for _ in range(5)]
            assert evaluate_verification(slow, 100.0, True) == "ESCALATED"
            assert evaluate_verification(good, 100.0, False) == "ESCALATED"
            assert evaluate_verification([], 100.0, True) == "ESCALATED"
        finally:
            await pool.close()

    run(scenario())


def test_http_approval_and_dispatch(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        client = AsyncClient(transport=ASGITransport(app=create_app(pool)), base_url="http://m4")
        try:
            requested = await client.post(
                "/v1/approvals",
                json={
                    "action_digest": DIGEST,
                    "target_env": TARGET,
                    "source_revision": REVISION,
                    "policy_version": "policy-v1",
                    "approver": "operator",
                },
            )
            assert requested.status_code == 201
            approval_id = requested.json()["id"]
            granted = await client.post(f"/v1/approvals/{approval_id}/grant")
            assert granted.status_code == 200
            assert granted.json()["valid_until"] is not None

            body = {
                "action_digest": DIGEST,
                "target_env": TARGET,
                "source_revision": REVISION,
                "action": {"type": "noop"},
            }
            first = await client.post("/v1/dispatch", json=body)
            assert first.status_code == 200, first.text
            assert first.json()["status"] == "applied"
            assert first.json()["approval_id"] == approval_id
            again = await client.post("/v1/dispatch", json=body)
            assert again.json()["id"] == first.json()["id"]
            assert again.json()["attempts"] == 1

            unapproved = await client.post(
                "/v1/dispatch",
                json={**body, "action_digest": "never-approved"},
            )
            assert unapproved.status_code == 409
            bad_type = await client.post(
                "/v1/dispatch", json={**body, "action": {"type": "warp-drive"}}
            )
            assert bad_type.status_code == 400
            missing = await client.post("/v1/approvals/999999/grant")
            assert missing.status_code == 404
        finally:
            await client.aclose()
            await pool.close()

    run(scenario())


def test_cli_request_grant_show(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setenv(CREDENTIAL_ENV, "test-cred")

    async def scenario() -> None:
        pool = await fresh_pool()
        await pool.close()

    run(scenario())
    assert cli_main(["request", "--digest", "cli-digest", "--target", TARGET,
                     "--revision", REVISION, "--policy", "policy-v1",
                     "--approver", "operator"]) == 0
    import json as _json

    approval_id = _json.loads(capsys.readouterr().out)["id"]
    assert cli_main(["grant", "--id", str(approval_id)]) == 0
    granted = _json.loads(capsys.readouterr().out)
    assert granted["decision"] == "granted"
    assert cli_main(["show", "--digest", "cli-digest", "--target", TARGET,
                     "--revision", REVISION]) == 0
    shown = _json.loads(capsys.readouterr().out)
    assert shown["approver"] == "operator"
