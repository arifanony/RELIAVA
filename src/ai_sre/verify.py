"""FR-11: independent verification over a window, then RESOLVED or escalate.

Close RESOLVED only when, over the window: error rate stays below 1%,
p95 latency stays within 20% of baseline, and the original failing request
replays cleanly. Anything else escalates. The window is sampled by the
caller (production default 600 s); the evaluator here judges the samples.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from psycopg_pool import AsyncConnectionPool

MAX_ERROR_RATE = 0.01
P95_TOLERANCE = 0.20


@dataclass
class MetricSample:
    error_rate: float
    p95_ms: float


def p95(values: list[float]) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(0.95 * len(ordered)))]


def evaluate_verification(
    samples: list[MetricSample], baseline_p95_ms: float, replay_ok: bool
) -> str:
    """RESOLVED only on all three signals; empty or degraded samples escalate."""
    if not samples or not replay_ok:
        return "ESCALATED"
    if any(sample.error_rate >= MAX_ERROR_RATE for sample in samples):
        return "ESCALATED"
    if p95([sample.p95_ms for sample in samples]) > baseline_p95_ms * (1 + P95_TOLERANCE):
        return "ESCALATED"
    return "RESOLVED"


async def verify_dispatch(
    pool: AsyncConnectionPool,
    dispatch_id: int,
    metrics: Callable[[], list[MetricSample]],
    replay: Callable[[], bool],
    baseline_p95_ms: float,
) -> str:
    from ai_sre.dispatch import log_event

    verdict = evaluate_verification(metrics(), baseline_p95_ms, replay())
    await log_event(
        pool,
        dispatch_id,
        "verified" if verdict == "RESOLVED" else "escalated",
        {"verdict": verdict, "baseline_p95_ms": baseline_p95_ms},
    )
    return verdict
