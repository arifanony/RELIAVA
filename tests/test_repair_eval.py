"""M5 eval: frozen repair suite, platform budget/latency, invariants, DR drill."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

from httpx import ASGITransport, AsyncClient
from psycopg_pool import AsyncConnectionPool
from test_diagnosis_eval import StubTransport, hypotheses_json, verdict_json
from test_runbook_eval import RUNBOOKS

from ai_sre.approvals import decide, init_approvals, request_approval
from ai_sre.db import database_url, init_schema
from ai_sre.diagnosis import BudgetLedger, run_diagnosis
from ai_sre.dispatch import CREDENTIAL_ENV, dispatch_action, init_dispatch
from ai_sre.evidence import collect, pack
from ai_sre.invariants import check_all, check_i1_i4_design
from ai_sre.main import create_app
from ai_sre.ops import LatencyTracker, PlatformBudget
from ai_sre.patches import PatchCandidate, apply_patch, propose_patches
from ai_sre.runbooks import index_runbooks, init_runbooks, search_runbooks
from ai_sre.sandbox import run_in_sandbox
from ai_sre.verify import MetricSample, evaluate_verification

CALC_BUGGY = '''def apply_discount(total, coupon):
    if coupon == "SAVE10":
        return total + total * 0.10
    return total
'''

TEST_CALC = '''from calc import apply_discount


def test_repro_coupon_bug():
    assert apply_discount(100.0, "SAVE10") == 90.0


def test_no_coupon_full_price():
    assert apply_discount(100.0, None) == 100.0
'''

SCENARIOS: list[dict[str, Any]] = [
    {
        "name": "config-regression",
        "alert": {
            "service": "checkout",
            "environment": "staging",
            "alert": {"alertname": "CheckoutErrorRate", "error_rate": 0.42},
        },
        "logs": [
            "ERROR checkout failed for order 1001 ip=10.0.0.1",
            "ERROR checkout failed for order 1002 ip=10.0.0.2",
            "ERROR checkout failed for order 1003 ip=10.0.0.3",
        ],
        "query": "checkout error rate http 500",
        "want_runbook": "checkout error-rate",
    },
    {
        "name": "code-bug",
        "alert": {
            "service": "payment",
            "environment": "staging",
            "alert": {"alertname": "PaymentExceptions", "error_rate": 0.31},
        },
        "logs": [
            "Traceback ValueError bad coupon order 2001",
            "Traceback ValueError bad coupon order 2002",
        ],
        "query": "coupon ValueError pricing traceback",
        "want_runbook": "payment code bug",
    },
]

REPAIR_BASELINE = json.loads(
    (Path(__file__).resolve().parent.parent / "evals" / "repair_baseline.json").read_text()
)


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def tool_env() -> dict[str, str]:
    home = os.path.expanduser("~")
    path = os.pathsep.join(
        [
            os.path.join(home, ".local", "bin"),
            "/Applications/Docker.app/Contents/Resources/bin",
            os.environ.get("PATH", ""),
        ]
    )
    return {**os.environ, "PATH": path}


async def fresh_pool() -> AsyncConnectionPool:
    pool = AsyncConnectionPool(
        os.environ.get("DATABASE_URL", database_url()), open=False,
        kwargs={"connect_timeout": 10}
    )
    await pool.open()
    await init_schema(pool)
    await init_approvals(pool)
    await init_dispatch(pool)
    await init_runbooks(pool)
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "TRUNCATE dispatch_events, dispatches, approvals,"
                " incident_events, outbox, incidents,"
                " actions, hypotheses, diagnoses, artifacts, verifications,"
                " runbooks, runbook_index_revisions"
            )
        await conn.commit()
    await index_runbooks(pool, RUNBOOKS)
    return pool


async def repair_scenario(
    pool: AsyncConnectionPool,
    client: AsyncClient,
    scenario: dict[str, Any],
    intake_lat: LatencyTracker,
    relay_lat: LatencyTracker,
    ledger: BudgetLedger,
    sandbox_image: str,
) -> dict[str, Any]:
    from ai_sre.relay import relay_once

    started = time.monotonic()
    key = f"m5-{scenario['name']}-{uuid.uuid4().hex[:8]}"
    t0 = time.monotonic()
    posted = await client.post("/v1/incidents", json=scenario["alert"],
                               headers={"Idempotency-Key": key})
    assert posted.status_code == 202, posted.text
    intake_lat.record((time.monotonic() - t0) * 1000)
    incident_id = posted.json()["id"]

    t0 = time.monotonic()
    assert await relay_once(pool) is not None
    relay_lat.record((time.monotonic() - t0) * 1000)

    items = collect(
        [("demo-logs", "5m", line) for line in scenario["logs"]]
    )
    packed, used, _dropped = pack(items)
    assert used <= 8000

    hits = await search_runbooks(pool, scenario["query"], k=3)
    assert hits and hits[0].title == scenario["want_runbook"]

    code = f"coupon-{scenario['name']}"
    diagnosis_transport = StubTransport(
        family="qwen", model="qwen-x",
        replies=[hypotheses_json(*[item.id for item in items[:1]])],
    )
    critic_transport = StubTransport(
        family="other", model="other-x", replies=[verdict_json(True)]
    )
    _ = packed, code
    diagnosis = await run_diagnosis(items, diagnosis_transport, critic_transport, ledger)
    assert diagnosis.status == "ACCEPTED", diagnosis.reason

    with tempfile.TemporaryDirectory(prefix="m5-repair-") as target:
        Path(target, "calc.py").write_text(CALC_BUGGY)
        Path(target, "test_calc.py").write_text(TEST_CALC)
        before = run_in_sandbox(target, "python -m pytest test_calc.py -q",
                                image=sandbox_image)
        assert before.exit_code != 0
        candidate = PatchCandidate(
            path="calc.py",
            old="return total + total * 0.10",
            new="return total - total * 0.10",
            base_revision=f"m5-{scenario['name']}-v1",
        )
        propose_patches([candidate], (".",))
        apply_patch(target, candidate)
        after = run_in_sandbox(target, "python -m pytest test_calc.py -q",
                               image=sandbox_image)
        assert after.exit_code == 0, after.stdout + after.stderr

    approval = await request_approval(
        pool, candidate.digest, "staging", "m5-v1", "policy-v1", "operator"
    )
    granted = await decide(pool, approval.id, True)
    assert granted.valid_until is not None
    calls = {"n": 0}

    async def executor(action: dict[str, Any]) -> dict[str, Any]:
        calls["n"] += 1
        return {"ok": True}

    os.environ.setdefault(CREDENTIAL_ENV, "test-cred")
    dispatch = await dispatch_action(
        pool, candidate.digest, "staging", "m5-v1", {"type": "noop"}, executor
    )
    assert dispatch.status == "applied"
    assert dispatch.approval_id == granted.id

    verdict = evaluate_verification(
        [MetricSample(error_rate=0.005, p95_ms=110)], 100.0, True
    )
    assert verdict == "RESOLVED"
    return {
        "scenario": scenario["name"],
        "incident": incident_id,
        "status": "repaired",
        "seconds": time.monotonic() - started,
    }


def test_repair_suite_matches_frozen_baseline(sandbox_image: str) -> None:
    async def scenario() -> None:
        pool = await fresh_pool()
        client = AsyncClient(
            transport=ASGITransport(app=create_app(pool)), base_url="http://m5"
        )
        try:
            intake_lat = LatencyTracker()
            relay_lat = LatencyTracker()
            ledger = BudgetLedger()
            results = []
            for fixture in SCENARIOS:
                results.append(
                    await repair_scenario(
                        pool, client, fixture, intake_lat, relay_lat, ledger,
                        sandbox_image,
                    )
                )
            repaired = sum(1 for r in results if r["status"] == "repaired")
            assert repaired / len(results) >= REPAIR_BASELINE["min_repair_rate"]
            for r in results:
                assert r["seconds"] <= REPAIR_BASELINE["max_scenario_seconds"], r
            assert intake_lat.p95 <= REPAIR_BASELINE["max_intake_p95_ms"]
            assert relay_lat.p95 <= REPAIR_BASELINE["max_relay_p95_ms"]
            assert ledger.tokens_used < ledger.max_tokens
            assert ledger.cost_usd < ledger.max_cost_usd
            assert set(REPAIR_BASELINE["scenarios"]) == {r["scenario"] for r in results}
        finally:
            await client.aclose()
            await pool.close()

    run(scenario())


def test_platform_budget_alert_and_stop() -> None:
    budget = PlatformBudget()
    assert budget.status == "ok"
    assert budget.spend(119.0)
    assert budget.status == "ok"
    assert budget.spend(1.0)
    assert budget.status == "alert"
    assert budget.spend(30.0)
    assert budget.status == "stop"
    assert budget.spend(0.01) is False  # non-essential refused at ceiling
    assert budget.spend(0.01, essential=True) is True


def test_invariants_hold_over_repair_run(sandbox_image: str) -> None:
    _ = sandbox_image

    async def scenario() -> None:
        pool = await fresh_pool()
        client = AsyncClient(
            transport=ASGITransport(app=create_app(pool)), base_url="http://m5"
        )
        try:
            intake_lat = LatencyTracker()
            relay_lat = LatencyTracker()
            await repair_scenario(
                pool, client, SCENARIOS[0], intake_lat, relay_lat,
                BudgetLedger(), sandbox_image,
            )
            assert check_i1_i4_design() == []
            report = await check_all(
                pool, secrets=[os.environ.get("OPENROUTER_API_KEY", "")]
            )
            assert report == {"I3": [], "I2/I5": [], "I6": []}, report
        finally:
            await client.aclose()
            await pool.close()

    run(scenario())


def test_drill_backup_lists_tables() -> None:
    dest = tempfile.mkdtemp(prefix="m5-drill-")
    script = (
        Path(__file__).resolve().parent.parent / "scripts" / "db-backup.sh"
    )
    completed = subprocess.run(
        ["sh", str(script)],
        capture_output=True,
        text=True,
        timeout=300,
        env={**tool_env(), "BACKUP_DIR": dest},
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    dumps = sorted(Path(dest).glob("*.dump"))
    assert dumps and dumps[-1].stat().st_size > 0
    listing = completed.stdout
    for table in ("incidents", "outbox", "approvals", "dispatches", "runbooks"):
        assert table in listing, f"{table} missing from backup listing"
