"""FR-14 activities: thin, real integrations behind the workflow.

Each activity does one trust-zone job using the existing modules. The eval
registers deterministic doubles at this boundary; production registers these.
Timeouts and retries are owned by Temporal (see workflows.py), never here.

FR-16 trust rule: after diagnosis, action content lives only in PostgreSQL,
addressed by the patch digest. Workflow payloads between the sandbox and
trusted zones carry identifiers ({action_id, digest}) — never patch content.
The trusted activity reloads the candidate from the artifact store and
recomputes its digest before anything is applied.
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx
from psycopg.types.json import Json
from psycopg_pool import AsyncConnectionPool
from temporalio import activity

from ai_sre.db import get_pool
from ai_sre.diagnosis import (
    BudgetLedger,
    Transport,
    estimate_tokens,
    run_diagnosis,
    transport_from_env,
)
from ai_sre.dispatch import dispatch_action
from ai_sre.evidence import EvidenceItem, collect
from ai_sre.model import (
    get_action,
    insert_artifact,
    insert_record,
    load_patch_by_digest,
)
from ai_sre.patches import PatchCandidate, apply_patch, propose_patches
from ai_sre.states import advance
from ai_sre.verify import MetricSample, evaluate_verification, p95


def _pool() -> AsyncConnectionPool:
    return get_pool()


async def _advance(
    pool: AsyncConnectionPool, table: str, record_id: int | str, new_status: str
) -> None:
    """Advance a state machine row, reading its current version first.

    A single workflow drives each incident/action, so read-then-advance is
    safe here; advance() still enforces the transition table and version.
    """
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            f"SELECT version FROM {table} WHERE id = %s", (record_id,)
        )
        row = await cur.fetchone()
    if row is None:
        raise ValueError(f"unknown {table} record {record_id}")
    await advance(pool, table, record_id, new_status, int(row[0]))


async def _log_event(
    pool: AsyncConnectionPool, incident_id: str, type_: str, source: str,
    data: dict[str, Any],
) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(
                "INSERT INTO incident_events (incident_id, type, source, data)"
                " VALUES (%s, %s, %s, %s)",
                (incident_id, type_, source, Json(data)),
            )
        await conn.commit()


# ---------------------------------------------------------------------------
# Investigation zone (agent-worker: read-only, zero write credentials to
# staging; its PostgreSQL writes are evidence and proposals, never actions
# on the target).
# ---------------------------------------------------------------------------


async def collect_evidence_for(pool: AsyncConnectionPool, incident_id: str) -> str:
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT service, environment, payload FROM incidents WHERE id = %s",
            (incident_id,),
        )
        row = await cur.fetchone()
        if row is None:
            raise ValueError(f"unknown incident {incident_id}")
        service, environment, payload = row
    lines = (
        payload.get("logs", [json.dumps(payload)])
        if isinstance(payload, dict)
        else [str(payload)]
    )
    items = collect(
        [(f"{service}-logs", "5m", line) for line in lines]
        + [(f"{service}-meta", "5m", f"service={service} env={environment}")]
    )
    await _advance(pool, "incidents", incident_id, "investigating")
    return json.dumps(
        {
            "incident_id": incident_id,
            "items": [
                {"id": i.id, "source": i.source, "window": i.window, "text": i.text}
                for i in items
            ],
        }
    )


@activity.defn
async def collect_evidence(incident_id: str) -> str:
    """Read the incident alert from PG and return evidence items as JSON."""
    pool = _pool()
    await pool.open()
    try:
        return await collect_evidence_for(pool, incident_id)
    finally:
        await pool.close()


PATCH_PROMPT = """You are the patch skill of an incident-response agent.
Diagnosis (accepted by an independent critic):
{hypotheses}

Evidence:
{evidence}

Write ONE minimal patch that fixes the root cause. Reply with only JSON:
{{"path": "<repo-relative path>", "old": "<exact current text>",
  "new": "<replacement text>", "base_revision": "<git revision>"}}
The old text must occur exactly once in the file. No commentary."""


async def _propose_patch(
    items: list[EvidenceItem],
    hypotheses: list[Any],
    transport: Transport,
    ledger: BudgetLedger,
) -> PatchCandidate:
    packed = "\n".join(f"[{i.id}] {i.text}" for i in items)
    prompt = PATCH_PROMPT.format(
        hypotheses="\n".join(f"- {h.text}" for h in hypotheses),
        evidence=packed,
    )
    if not ledger.check(transport.model, estimate_tokens(prompt), 1000):
        raise ValueError("budget exhausted before patch call")
    reply = await transport.generate(prompt)
    ledger.spend(
        transport.model, int(reply["input_tokens"]), int(reply["output_tokens"])
    )
    payload = json.loads(str(reply["text"]))
    candidate = PatchCandidate(
        path=str(payload["path"]),
        old=str(payload["old"]),
        new=str(payload["new"]),
        base_revision=str(payload["base_revision"]),
    )
    allowed = tuple(os.environ.get("PATCH_ROOTS", ".").split(":"))
    propose_patches([candidate], allowed)
    return candidate


async def diagnose_and_persist(
    pool: AsyncConnectionPool,
    incident_id: str,
    items: list[EvidenceItem],
    diagnosis_transport: Transport,
    critic_transport: Transport,
) -> str:
    """Run the diagnosis cycle and persist its outcome (FR-15 + FR-16).

    Hypotheses and the diagnosis verdict become canonical records; an
    accepted diagnosis also produces one patch candidate whose content is
    stored as a digest-addressed artifact plus an actions row in PROPOSED.
    Only identifiers travel onward.
    """
    ledger = BudgetLedger()
    result = await run_diagnosis(
        items, diagnosis_transport, critic_transport, ledger
    )
    for hypothesis in result.hypotheses:
        await insert_record(
            pool,
            "hypotheses",
            {
                "incident_id": incident_id,
                "text": hypothesis.text,
                "evidence_ids": Json(hypothesis.evidence_ids),
            },
        )
    out: dict[str, Any] = {
        "status": result.status,
        "incident_id": incident_id,
        "hypotheses": [
            {"text": h.text, "evidence_ids": h.evidence_ids}
            for h in result.hypotheses
        ],
        "revisions": result.revisions,
        "reason": result.reason,
    }
    if result.status != "ACCEPTED":
        await insert_record(
            pool,
            "diagnoses",
            {"incident_id": incident_id, "status": result.status,
             "revisions": result.revisions},
        )
        await _advance(pool, "incidents", incident_id, "inconclusive")
        return json.dumps(out)
    try:
        candidate = await _propose_patch(
            items, result.hypotheses, diagnosis_transport, ledger
        )
    except Exception as exc:  # noqa: BLE001 - no patch, no action: inconclusive
        out["status"] = "INCONCLUSIVE"
        out["reason"] = f"patch generation failed: {exc}"
        await insert_record(
            pool,
            "diagnoses",
            {"incident_id": incident_id, "status": "INCONCLUSIVE",
             "revisions": result.revisions},
        )
        await _advance(pool, "incidents", incident_id, "inconclusive")
        return json.dumps(out)
    await insert_record(
        pool,
        "diagnoses",
        {"incident_id": incident_id, "status": result.status,
         "revisions": result.revisions},
    )
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT environment FROM incidents WHERE id = %s", (incident_id,)
        )
        env_row = await cur.fetchone()
    target_env = str(env_row[0]) if env_row else "staging"
    content = json.dumps(
        {
            "path": candidate.path,
            "old": candidate.old,
            "new": candidate.new,
            "base_revision": candidate.base_revision,
            "digest": candidate.digest,
        },
        sort_keys=True,
    ).encode()
    await insert_artifact(pool, incident_id, "patch", content)
    action_id = await insert_record(
        pool,
        "actions",
        {
            "incident_id": incident_id,
            "patch_digest": candidate.digest,
            "target_env": target_env,
        },
    )
    await _advance(pool, "incidents", incident_id, "diagnosed")
    out["action_id"] = action_id
    out["digest"] = candidate.digest
    return json.dumps(out)


@activity.defn
async def diagnose(evidence_json: str) -> str:
    """Run the tiered cited diagnosis (models from env; Gemini API default
    for diagnosis, local Llama critic default — see .env.example) and
    persist the outcome; only identifiers travel onward."""
    raw = json.loads(evidence_json)
    if isinstance(raw, dict):
        incident_id = str(raw["incident_id"])
        entries = raw["items"]
    else:  # legacy bare-list payloads carry no incident to persist against
        raise TypeError("evidence payload must name its incident")
    items = [
        EvidenceItem(id=e["id"], source=e["source"], window=e["window"], text=e["text"])
        for e in entries
    ]
    diagnosis_transport = transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")
    critic_transport = transport_from_env("CRITIC", "ollama:llama3.2:1b")
    pool = _pool()
    await pool.open()
    try:
        return await diagnose_and_persist(
            pool, incident_id, items, diagnosis_transport, critic_transport
        )
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Sandbox zone (sandbox-runner: no credentials; loads by digest, validates,
# tests the patch in the sandbox, never touches staging).
# ---------------------------------------------------------------------------


async def test_patch_for(pool: AsyncConnectionPool, diagnosis_json: str) -> str:
    """Validate the stored candidate and move it to READY_FOR_REVIEW."""
    diagnosis = json.loads(diagnosis_json)
    action_id = diagnosis.get("action_id")
    digest = diagnosis.get("digest")
    if action_id is None or digest is None:
        raise ValueError("diagnosis produced no action to test")
    action = await get_action(pool, int(action_id))
    if action is None or action["patch_digest"] != digest:
        raise ValueError("action/digest mismatch")
    candidate = await load_patch_by_digest(pool, str(action["incident_id"]), digest)
    if candidate is None:
        raise ValueError(f"no stored patch for digest {digest}")
    allowed = tuple(os.environ.get("PATCH_ROOTS", ".").split(":"))
    propose_patches([candidate], allowed)
    await _advance(pool, "actions", int(action_id), "testing")
    mode = os.environ.get("AI_SRE_SANDBOX", "docker")
    if mode == "docker":
        staging = os.environ.get("STAGING_TREE")
        if not staging:
            raise ValueError("STAGING_TREE is not set")
        import shutil
        import tempfile

        from ai_sre.sandbox import run_in_sandbox

        with tempfile.TemporaryDirectory() as tmp:
            work = os.path.join(tmp, "tree")
            shutil.copytree(staging, work)
            apply_patch(work, candidate)
            test_cmd = os.environ.get("PATCH_TEST_CMD", "python -m pytest -q")
            result = run_in_sandbox(work, test_cmd)
        if result.exit_code != 0:
            await _advance(pool, "actions", int(action_id), "failed")
            await _advance(
                pool, "incidents", str(action["incident_id"]), "inconclusive"
            )
            await _log_event(
                pool, str(action["incident_id"]), "ai-sre.action.sandbox_failed",
                "sandbox", {"action_id": action_id, "digest": digest,
                            "stderr": result.stderr[-500:]},
            )
            return json.dumps(
                {"action_id": action_id, "digest": digest, "sandbox": "failed"}
            )
    elif mode != "validate":
        raise ValueError(f"unknown AI_SRE_SANDBOX mode: {mode}")
    await _advance(pool, "actions", int(action_id), "ready_for_review")
    await _advance(
        pool, "incidents", str(action["incident_id"]), "awaiting_approval"
    )
    return json.dumps({"action_id": action_id, "digest": digest, "sandbox": "passed"})


@activity.defn
async def test_patch(patch_json: str) -> str:
    """Load the candidate by digest from PG, validate and sandbox it."""
    pool = _pool()
    await pool.open()
    try:
        return await test_patch_for(pool, patch_json)
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Trusted zone (trusted-worker: sole holder of the staging write credential;
# reloads the approved action from PostgreSQL by digest — action content is
# never accepted from a workflow payload or the API).
# ---------------------------------------------------------------------------


async def _apply_executor(
    staging: str, candidate: PatchCandidate
) -> Any:
    async def executor(action: dict[str, Any]) -> dict[str, Any]:
        apply_patch(staging, candidate)
        return {"ok": True, "action": action}

    return executor


async def dispatch_for(pool: AsyncConnectionPool, patch_json: str) -> str:
    """Dispatch the approved action, reloading its content from PG."""
    request = json.loads(patch_json)
    action_id = request.get("action_id")
    digest = request.get("digest")
    if action_id is None or digest is None:
        raise ValueError("dispatch payload must carry action_id and digest only")
    if request.get("sandbox") == "failed":
        raise ValueError("sandbox failed; nothing to dispatch")
    action = await get_action(pool, int(action_id))
    if action is None or action["patch_digest"] != digest:
        raise ValueError("action/digest mismatch")
    incident_id = str(action["incident_id"])
    candidate = await load_patch_by_digest(pool, incident_id, digest)
    if candidate is None:
        raise ValueError(f"no stored patch for digest {digest}")
    staging = os.environ.get("STAGING_TREE")
    if not staging:
        raise ValueError("STAGING_TREE is not set")
    if action["status"] != "approved":
        # The human grant is recorded by the FR-18 endpoint (approvals row +
        # state advance); the workflow signal alone never authorises this.
        raise ValueError(
            f"action {action_id} is {action['status']}, not approved"
        )
    await _advance(pool, "actions", int(action_id), "dispatching")
    executor = await _apply_executor(staging, candidate)
    from ai_sre.dispatch import MissingCredential, NotApproved

    try:
        result = await dispatch_action(
            pool,
            candidate.digest,
            str(action["target_env"]),
            candidate.base_revision,
            {"type": "patch", "path": candidate.path, "digest": candidate.digest},
            executor,
        )
    except (NotApproved, MissingCredential) as exc:
        await _advance(pool, "actions", int(action_id), "failed")
        await _advance(pool, "incidents", incident_id, "escalated")
        return json.dumps(
            {
                "dispatch_id": None,
                "status": "blocked",
                "reason": f"{type(exc).__name__}: {exc}",
                "action_id": action_id,
                "incident_id": incident_id,
                "digest": digest,
            }
        )
    await _advance(pool, "incidents", incident_id, "remediating")
    outcome = {"applied": "succeeded", "failed": "failed", "unknown": "unknown"}
    await _advance(
        pool, "actions", int(action_id), outcome.get(result.status, "failed")
    )
    return json.dumps(
        {
            "dispatch_id": result.id,
            "status": result.status,
            "action_id": action_id,
            "incident_id": incident_id,
            "digest": digest,
        }
    )


@activity.defn
async def dispatch_action_activity(dispatch_json: str) -> str:
    """Trusted dispatch: applies the approved patch to the staging tree.

    The patch content is reloaded from PostgreSQL by digest; the payload
    carries only {action_id, digest} (FR-16).
    """
    pool = _pool()
    await pool.open()
    try:
        return await dispatch_for(pool, dispatch_json)
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# Verification (trusted zone): windowed sampling + replay (FR-11/FR-16).
# ---------------------------------------------------------------------------


def _range_values(payload: dict[str, Any]) -> list[float]:
    values: list[float] = []
    for series in payload.get("data", {}).get("result", []):
        for _ts, value in series.get("values", []):
            try:
                values.append(float(value))
            except (TypeError, ValueError):
                continue
    return values


async def verify_for(
    pool: AsyncConnectionPool,
    dispatch_json: str,
    prom_url: str | None = None,
    replay_url: str | None = None,
    window_s: int | None = None,
    http: httpx.AsyncClient | None = None,
) -> str:
    """Judge the fix over the trailing window and record the verdict.

    Samples Prometheus over the window (error rate per step; p95 from the
    configured query when the target exposes one), replays the original
    failing request, and persists a verifications record. RESOLVED only
    when every signal is clean (see verify.evaluate_verification).
    """
    request = json.loads(dispatch_json)
    incident_id = request.get("incident_id")
    if incident_id is None:
        raise ValueError("verify payload must name its incident")
    if request.get("status") != "applied":
        async with pool.connection() as conn, conn.cursor() as cur:
            await cur.execute(
                "SELECT status FROM incidents WHERE id = %s", (incident_id,)
            )
            row = await cur.fetchone()
        if row is not None and row[0] in {"awaiting_approval", "remediating"}:
            await _advance(pool, "incidents", str(incident_id), "escalated")
        return "escalated"
    prom = prom_url or os.environ.get("PROM_URL", "http://localhost:9090")
    replay_base = replay_url or os.environ.get("REPLAY_URL", "http://localhost:8000")
    window = window_s or int(os.environ.get("VERIFY_WINDOW_S", "600"))
    baseline = float(request.get("baseline_p95_ms", os.environ.get("BASELINE_P95_MS", "1000")))
    error_query = os.environ.get(
        "PROM_ERROR_QUERY",
        'sum(rate(demo_checkout_requests_total{status="500"}[1m]))'
        ' / sum(rate(demo_checkout_requests_total[1m]))',
    )
    p95_query = os.environ.get(
        "PROM_P95_QUERY",
        "histogram_quantile(0.95, sum(rate("
        "demo_checkout_request_duration_seconds_bucket[1m])) by (le)) * 1000",
    )
    await _advance(pool, "incidents", str(incident_id), "verifying")
    owns_client = http is None
    client = http or httpx.AsyncClient(timeout=25.0)
    try:
        err_resp = await client.get(
            f"{prom}/api/v1/query_range",
            params={"query": error_query, "start": f"now-{window}s",
                    "end": "now", "step": "30s"},
        )
        error_rates = _range_values(err_resp.json())
        p95_resp = await client.get(
            f"{prom}/api/v1/query_range",
            params={"query": p95_query, "start": f"now-{window}s",
                    "end": "now", "step": "30s"},
        )
        p95_values = _range_values(p95_resp.json())
        replay = await client.post(
            f"{replay_base}/checkout", json={"product_id": 1}
        )
    finally:
        if owns_client:
            await client.aclose()
    replay_ok = replay.status_code == 200
    if p95_values:
        window_p95 = p95(p95_values)
    elif replay_ok:
        # No latency series on the target: judge p95 from the replay itself.
        try:
            elapsed = replay.elapsed
        except Exception:  # noqa: BLE001 - mock transports may not set it
            elapsed = None
        window_p95 = float(elapsed.total_seconds() * 1000) if elapsed else 0.0
    else:
        window_p95 = baseline * 10  # no latency signal at all: cannot pass
    samples = [MetricSample(error_rate=r, p95_ms=window_p95) for r in error_rates]
    verdict = evaluate_verification(samples, baseline, replay_ok)
    await insert_record(
        pool,
        "verifications",
        {
            "incident_id": incident_id,
            "verdict": verdict,
            "error_rate": max(error_rates) if error_rates else None,
            "p95_ms": window_p95,
            "replay_ok": replay_ok,
        },
    )
    await _advance(
        pool, "incidents", str(incident_id),
        "resolved" if verdict == "RESOLVED" else "escalated",
    )
    return "resolved" if verdict == "RESOLVED" else "escalated"


@activity.defn
async def verify(dispatch_json: str) -> str:
    """Independent verification over the window (Prometheus + replay)."""
    pool = _pool()
    await pool.open()
    try:
        return await verify_for(pool, dispatch_json)
    finally:
        await pool.close()
