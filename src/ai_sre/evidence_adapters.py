"""FR-17 evidence adapters: one per evidence kind, all scoped to the
incident's service and time window.

Each adapter turns part of the incident payload (the alert as delivered)
into (source, window, text) tuples for evidence.collect. Kinds: logs,
metrics, traces, deployment history and git revision. Adapters are pure
functions of the payload — a live metrics pull belongs in the caller,
which can pass the series text in under "metrics".
"""

from __future__ import annotations

import json
from typing import Any

WINDOW = "5m"


def _lines(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [
            entry if isinstance(entry, str) else json.dumps(entry)
            for entry in value
        ]
    return [json.dumps(value)]


def items_from_payload(
    payload: Any, service: str, environment: str
) -> list[tuple[str, str, str]]:
    """All adapter tuples for one incident payload, logs first."""
    if not isinstance(payload, dict):
        return [(f"{service}-logs", WINDOW, str(payload))]
    tuples: list[tuple[str, str, str]] = []
    logs = _lines(payload.get("logs")) or [json.dumps(payload)]
    tuples += [(f"{service}-logs", WINDOW, line) for line in logs]
    for line in _lines(payload.get("metrics")):
        tuples.append((f"{service}-metrics", WINDOW, line))
    for line in _lines(payload.get("traces")):
        tuples.append((f"{service}-traces", WINDOW, line))
    for line in _lines(payload.get("deployments")):
        tuples.append((f"{service}-deployments", WINDOW, line))
    revision = payload.get("git_revision")
    if revision:
        tuples.append((f"{service}-git", WINDOW, f"git_revision={revision}"))
    tuples.append(
        (f"{service}-meta", WINDOW, f"service={service} env={environment}")
    )
    return tuples
