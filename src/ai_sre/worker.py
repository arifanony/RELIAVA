"""Agent-worker entry points: Temporal workers and the embedded relay.

Run a local Temporal dev server with `temporal server start-dev`
(download the temporal CLI separately), then::

    python -m ai_sre.worker worker   # three task-queue workers
    python -m ai_sre.worker relay    # LISTEN/NOTIFY relay -> start workflows
"""

from __future__ import annotations

import asyncio
import os
import sys

from temporalio.client import Client
from temporalio.worker import Worker

from ai_sre.activities import (
    collect_evidence,
    diagnose,
    dispatch_action_activity,
    test_patch,
    verify,
)
from ai_sre.db import database_url, get_pool
from ai_sre.relay import relay_listen, relay_sweep
from ai_sre.workflows import (
    TASK_QUEUE_INVESTIGATION,
    TASK_QUEUE_SANDBOX,
    TASK_QUEUE_TRUSTED,
    WORKFLOW_EXECUTION_TIMEOUT,
    IncidentWorkflow,
)


def temporal_address() -> str:
    return os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")


async def run_workers() -> None:
    client = await Client.connect(temporal_address())
    investigation = Worker(
        client,
        task_queue=TASK_QUEUE_INVESTIGATION,
        workflows=[IncidentWorkflow],
        activities=[collect_evidence, diagnose],
    )
    sandbox = Worker(
        client, task_queue=TASK_QUEUE_SANDBOX, activities=[test_patch]
    )
    trusted = Worker(
        client,
        task_queue=TASK_QUEUE_TRUSTED,
        activities=[dispatch_action_activity, verify],
    )
    await asyncio.gather(
        investigation.run(), sandbox.run(), trusted.run()
    )


async def run_relay() -> None:
    client = await Client.connect(temporal_address())
    pool = get_pool()
    await pool.open()

    async def start_workflow(outbox_id: int, incident_id: str) -> None:
        await client.start_workflow(
            IncidentWorkflow.run,
            incident_id,
            id=f"incident-{incident_id}",
            task_queue=TASK_QUEUE_INVESTIGATION,
            execution_timeout=WORKFLOW_EXECUTION_TIMEOUT,  # 8 min deadline
        )

    async def sweep() -> None:
        await relay_sweep(pool, start_workflow)

    try:
        await relay_listen(database_url(), start_workflow, sweep=sweep)
    finally:
        await pool.close()


async def run_embedded_relay() -> None:
    """The api process's relay (FR-14): retry the Temporal connection so
    intake keeps serving while Temporal is down; rows stay undispatched
    until a start succeeds."""
    while True:
        try:
            await run_relay()
        except Exception:  # noqa: BLE001 - Temporal unreachable: wait, retry
            await asyncio.sleep(2.0)


def main(argv: list[str]) -> int:
    mode = argv[1] if len(argv) > 1 else "worker"
    if mode == "worker":
        asyncio.run(run_workers())
    elif mode == "relay":
        asyncio.run(run_relay())
    else:
        print(f"unknown mode: {mode}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
