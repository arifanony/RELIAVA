"""FR-14: Temporal IncidentWorkflow on the agent-worker.

Task queues mirror the trust zones: investigation (collect + diagnose),
sandbox (patch testing), trusted (dispatch + verify). 8 min workflow
deadline, 25 s activity timeouts, approval by signal, retries owned by
Temporal activity retry policies — never hand-rolled in the workflow.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from ai_sre.activities import (
        collect_evidence,
        diagnose,
        dispatch_action_activity,
        test_patch,
        verify,
    )

WORKFLOW_EXECUTION_TIMEOUT = timedelta(minutes=8)
ACTIVITY_TIMEOUT = timedelta(seconds=25)
MAX_ACTIVITY_ATTEMPTS = 3

TASK_QUEUE_INVESTIGATION = "investigation"
TASK_QUEUE_SANDBOX = "sandbox"
TASK_QUEUE_TRUSTED = "trusted"


@workflow.defn
class IncidentWorkflow:
    def __init__(self) -> None:
        self._approval: str | None = None

    @workflow.run
    async def run(self, incident_id: str) -> str:
        evidence = await workflow.execute_activity(
            collect_evidence,
            incident_id,
            task_queue=TASK_QUEUE_INVESTIGATION,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=MAX_ACTIVITY_ATTEMPTS),
        )
        diagnosis = await workflow.execute_activity(
            diagnose,
            evidence,
            task_queue=TASK_QUEUE_INVESTIGATION,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=MAX_ACTIVITY_ATTEMPTS),
        )
        patch = await workflow.execute_activity(
            test_patch,
            diagnosis,
            task_queue=TASK_QUEUE_SANDBOX,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=MAX_ACTIVITY_ATTEMPTS),
        )
        await workflow.wait_condition(lambda: self._approval is not None)
        if self._approval != "granted":
            return "escalated"
        dispatch = await workflow.execute_activity(
            dispatch_action_activity,
            patch,
            task_queue=TASK_QUEUE_TRUSTED,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return await workflow.execute_activity(
            verify,
            dispatch,
            task_queue=TASK_QUEUE_TRUSTED,
            start_to_close_timeout=ACTIVITY_TIMEOUT,
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    @workflow.signal
    async def approval(self, decision: str) -> None:
        self._approval = decision

    @workflow.query
    def approval_state(self) -> str | None:
        return self._approval
