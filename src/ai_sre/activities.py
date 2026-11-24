"""FR-14 activities: thin, real integrations behind the workflow.

Each activity does one trust-zone job using the existing modules. The eval
registers deterministic doubles at this boundary; production registers these.
Timeouts and retries are owned by Temporal (see workflows.py), never here.
"""

from __future__ import annotations

import json
import os
from typing import Any

from psycopg_pool import AsyncConnectionPool
from temporalio import activity

from ai_sre.db import get_pool
from ai_sre.diagnosis import BudgetLedger, OllamaTransport, run_diagnosis
from ai_sre.dispatch import dispatch_action
from ai_sre.evidence import collect
from ai_sre.patches import propose_patches
from ai_sre.verify import MetricSample, evaluate_verification


def _pool() -> AsyncConnectionPool:
    return get_pool()


@activity.defn
async def collect_evidence(incident_id: str) -> str:
    """Read the incident alert from PG and return evidence items as JSON."""
    pool = _pool()
    await pool.open()
    try:
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT service, environment, payload FROM incidents WHERE id = %s",
                (incident_id,),
            )
            row = await cur.fetchone()
            if row is None:
                raise ValueError(f"unknown incident {incident_id}")
            service, environment, payload = row
    finally:
        await pool.close()
    lines = payload.get("logs", [json.dumps(payload)]) if isinstance(payload, dict) else [str(payload)]
    items = collect(
        [(f"{service}-logs", "5m", line) for line in lines]
        + [(f"{service}-meta", "5m", f"service={service} env={environment}")]
    )
    return json.dumps(
        [
            {"id": i.id, "source": i.source, "window": i.window, "text": i.text}
            for i in items
        ]
    )


@activity.defn
async def diagnose(evidence_json: str) -> str:
    """Run the tiered cited diagnosis (models from env, Ollama dev default)."""
    from ai_sre.evidence import EvidenceItem

    raw = json.loads(evidence_json)
    items = [
        EvidenceItem(id=e["id"], source=e["source"], window=e["window"], text=e["text"])
        for e in raw
    ]
    diagnosis_transport = OllamaTransport(
        model=os.environ.get("DIAGNOSIS_MODEL", "qwen2.5-coder:7b"),
        family=os.environ.get("DIAGNOSIS_FAMILY", "qwen"),
    )
    critic_transport = OllamaTransport(
        model=os.environ.get("CRITIC_MODEL", "llama3.2:1b"),
        family=os.environ.get("CRITIC_FAMILY", "llama"),
    )
    result = await run_diagnosis(items, diagnosis_transport, critic_transport, BudgetLedger())
    return json.dumps(
        {
            "status": result.status,
            "hypotheses": [
                {"text": h.text, "evidence_ids": h.evidence_ids}
                for h in result.hypotheses
            ],
            "revisions": result.revisions,
            "reason": result.reason,
        }
    )


@activity.defn
async def test_patch(patch_json: str) -> str:
    """Validate the candidate and return its digest (sandbox runs in M3/M8)."""
    candidate = json.loads(patch_json)
    allowed = tuple(os.environ.get("PATCH_ROOTS", ".").split(":"))
    from ai_sre.patches import PatchCandidate

    patch = PatchCandidate(
        path=candidate["path"],
        old=candidate["old"],
        new=candidate["new"],
        base_revision=candidate["base_revision"],
    )
    propose_patches([patch], allowed)
    return json.dumps({"digest": patch.digest, "diff": patch.diff})


@activity.defn
async def dispatch_action_activity(dispatch_json: str) -> str:
    """Trusted dispatch: applies the approved patch to the staging tree."""
    request = json.loads(dispatch_json)
    staging = os.environ.get("STAGING_TREE")
    if not staging:
        raise ValueError("STAGING_TREE is not set")
    from ai_sre.patches import PatchCandidate, apply_patch

    patch = PatchCandidate(
        path=request["path"],
        old=request["old"],
        new=request["new"],
        base_revision=request["base_revision"],
    )
    apply_patch(staging, patch)
    pool = _pool()
    await pool.open()
    try:
        result = await dispatch_action(
            pool,
            patch.digest,
            request["target_env"],
            request["base_revision"],
            {"type": "patch", "path": request["path"]},
            _recording_executor,
        )
    finally:
        await pool.close()
    return json.dumps({"dispatch_id": result.id, "status": result.status})


async def _recording_executor(action: dict[str, Any]) -> dict[str, Any]:
    return {"ok": True, "action": action}


@activity.defn
async def verify(dispatch_json: str) -> str:
    """Independent verification over the window (Prometheus + replay)."""
    request = json.loads(dispatch_json)
    prom_url = os.environ.get("PROM_URL", "http://localhost:9090")
    replay_url = os.environ.get("REPLAY_URL", "http://localhost:8000")
    import httpx

    async with httpx.AsyncClient(timeout=25.0) as client:
        resp = await client.get(
            f"{prom_url}/api/v1/query",
            params={"query": 'sum(rate(demo_checkout_requests_total{status="500"}[10m]))'},
        )
        error_rate = float(resp.json()["data"]["result"][0]["value"][1])
        replay = await client.post(f"{replay_url}/checkout", json={"product_id": 1})
    samples = [MetricSample(error_rate=error_rate, p95_ms=100.0)]
    baseline = float(request.get("baseline_p95_ms", 100.0))
    return evaluate_verification(samples, baseline, replay.status_code == 200)
