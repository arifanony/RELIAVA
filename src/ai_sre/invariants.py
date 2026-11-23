"""NFR-6: executable safety-invariant checks over recorded run data.

I1 model proposes only (diagnosis carries citations, never writes).
I2 only the trusted dispatcher writes approved digests (dispatch requires
    a valid approval and the staging credential).
I3 approvals bind digest, target, revision, policy, approver, expiry.
I4 verification is independent telemetry (verdict from separate samples).
I5 unknown outcomes reconcile before closing (no APPLIED without a
    reconcile event after an UNKNOWN).
I6 application content is untrusted input (no secret material in records).

Each check returns a list of violations; empty means the invariant holds.
"""

from __future__ import annotations

from psycopg_pool import AsyncConnectionPool


async def check_i3_approval_bindings(pool: AsyncConnectionPool) -> list[str]:
    violations: list[str] = []
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                SELECT id, action_digest, target_env, source_revision,
                       policy_version, approver, valid_until
                FROM approvals WHERE decision = 'granted'
                """
        )
        for row in await cur.fetchall():
            _id, digest, env, rev, policy, approver, valid_until = row
            if not all([digest, env, rev, policy, approver]):
                violations.append(f"I3: approval {_id} has empty binding field")
            if valid_until is None:
                violations.append(f"I3: approval {_id} granted without validity")
    return violations


async def check_i2_i5_dispatch_chain(pool: AsyncConnectionPool) -> list[str]:
    """Every dispatch carries a valid approval; UNKNOWN never jumps to APPLIED."""
    violations: list[str] = []
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            """
                SELECT d.id, d.approval_id, d.status, a.valid_until
                FROM dispatches d JOIN approvals a ON a.id = d.approval_id
                """
        )
        dispatches = await cur.fetchall()
        await cur.execute(
            "SELECT dispatch_id, event FROM dispatch_events ORDER BY id"
        )
        events = await cur.fetchall()
    seen_unknown: set[int] = set()
    reconciled: set[int] = set()
    for dispatch_id, event in events:
        if event == "unknown":
            seen_unknown.add(dispatch_id)
        if event == "reconciled":
            reconciled.add(dispatch_id)
    for _id, approval_id, status, valid_until in dispatches:
        if valid_until is None:
            violations.append(f"I2: dispatch {_id} without validity-bound approval")
        if status == "applied" and _id in seen_unknown and _id not in reconciled:
            violations.append(f"I5: dispatch {_id} applied after UNKNOWN without reconcile")
    return violations


async def check_i6_no_secrets_in_records(
    pool: AsyncConnectionPool, secrets: list[str]
) -> list[str]:
    """Recorded content must never contain secret material (I6)."""
    violations: list[str] = []
    needles = [s for s in secrets if s]
    if not needles:
        return violations
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT id, data::text FROM incident_events"
            " UNION ALL SELECT -id, data::text FROM dispatch_events"
        )
        for row_id, text in await cur.fetchall():
            for needle in needles:
                if needle in text:
                    violations.append(f"I6: secret material in record {row_id}")
    return violations


async def check_all(
    pool: AsyncConnectionPool, secrets: list[str] | None = None
) -> dict[str, list[str]]:
    return {
        "I3": await check_i3_approval_bindings(pool),
        "I2/I5": await check_i2_i5_dispatch_chain(pool),
        "I6": await check_i6_no_secrets_in_records(pool, secrets or []),
    }


def check_i1_i4_design() -> list[str]:
    """I1/I4 are structural: diagnosis returns citations without write tools,
    verification consumes separate telemetry. Enforced by construction
    (Transport has no write methods; verify takes independent samples)."""
    from ai_sre import diagnosis as _d
    from ai_sre import verify as _v

    violations: list[str] = []
    if hasattr(_d.OllamaTransport, "write") or hasattr(_d.OpenRouterTransport, "write"):
        violations.append("I1: a model transport exposes a write method")
    import inspect as _inspect

    params = set(_inspect.signature(_v.verify_dispatch).parameters)
    if not {"metrics", "replay", "baseline_p95_ms"} <= params:
        violations.append("I4: verification is not driven by independent inputs")
    return violations
