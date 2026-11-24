"""M8-GATEWAY gates: FR-17 model gateway + critic-revision loop + adapters.

The gateway is the only path to a model: it reserves worst-case budget
before a call, caps concurrency per incident, refuses calls after the
investigation deadline, and records every attempt. A critic rejection
must produce a REVISED diagnosis (the notes are fed back), not a re-ask.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from ai_sre.diagnosis import run_diagnosis
from ai_sre.evidence import EvidenceItem
from ai_sre.evidence_adapters import items_from_payload
from ai_sre.gateway import (
    BudgetRefused,
    DeadlineExceeded,
    Gateway,
    ReservingLedger,
    provider_of,
)


def run(coro: Any) -> Any:
    return asyncio.run(coro)


class StubTransport:
    def __init__(
        self, family: str = "google", model: str = "stub-model",
        delay: float = 0.0,
    ) -> None:
        self.family = family
        self.model = model
        self.delay = delay
        self.active = 0
        self.max_active = 0

    async def generate(self, prompt: str) -> dict[str, Any]:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.delay:
                await asyncio.sleep(self.delay)
            return {"text": "ok", "input_tokens": 100, "output_tokens": 50}
        finally:
            self.active -= 1


def _gateway(ledger: ReservingLedger, deadline: datetime) -> Gateway:
    return Gateway(incident_id="inc-1", ledger=ledger, deadline=deadline)


def test_reserving_ledger_counts_holds() -> None:
    ledger = ReservingLedger(max_tokens=1000, max_cost_usd=10.0)
    hold = ledger.reserve("stub-model", 400, 400)
    assert not ledger.check("stub-model", 100, 200)  # 800 held + 300 > 1000
    ledger.release(hold)
    assert ledger.check("stub-model", 100, 200)


def test_gateway_records_ok_attempt() -> None:
    async def scenario() -> None:
        gateway = _gateway(
            ReservingLedger(), datetime.now(UTC) + timedelta(minutes=8)
        )
        wrapped = gateway.wrap(StubTransport(), "diagnosis", "gemini")
        reply = await wrapped.generate("diagnose this")
        assert reply["text"] == "ok"
        assert len(gateway.attempts) == 1
        attempt = gateway.attempts[0]
        assert attempt["outcome"] == "ok"
        assert attempt["role"] == "diagnosis"
        assert attempt["provider"] == "gemini"
        assert attempt["prompt_version"] == "diagnosis-v1"
        assert attempt["input_tokens"] == 100

    run(scenario())


def test_gateway_refuses_after_deadline() -> None:
    async def scenario() -> None:
        gateway = _gateway(
            ReservingLedger(), datetime.now(UTC) - timedelta(seconds=1)
        )
        wrapped = gateway.wrap(StubTransport(), "diagnosis", "gemini")
        with pytest.raises(DeadlineExceeded):
            await wrapped.generate("too late")
        assert gateway.attempts[0]["outcome"] == "refused_deadline"

    run(scenario())


def test_gateway_refuses_when_budget_exhausted() -> None:
    async def scenario() -> None:
        ledger = ReservingLedger(max_tokens=10**9, max_cost_usd=0.0001)
        gateway = _gateway(ledger, datetime.now(UTC) + timedelta(minutes=8))
        wrapped = gateway.wrap(StubTransport(), "critic", "ollama")
        with pytest.raises(BudgetRefused):
            await wrapped.generate("judge this")
        assert gateway.attempts[0]["outcome"] == "refused_budget"

    run(scenario())


def test_gateway_caps_incident_concurrency_at_two() -> None:
    async def scenario() -> None:
        gateway = _gateway(
            ReservingLedger(), datetime.now(UTC) + timedelta(minutes=8)
        )
        stub = StubTransport(delay=0.2)
        wrapped = gateway.wrap(stub, "diagnosis", "gemini")
        await asyncio.gather(*(wrapped.generate(f"call {i}") for i in range(4)))
        assert stub.max_active == 2
        assert len(gateway.attempts) == 4

    run(scenario())


def test_provider_of_specs() -> None:
    assert provider_of("gemini:gemini-2.5-flash") == "gemini"
    assert provider_of("openrouter:deepseek/x") == "openrouter"
    assert provider_of("llama3.2:1b") == "ollama"  # bare name w/ tag
    assert provider_of("qwen") == "ollama"


class RevisionDiagnosis:
    family = "google"
    model = "diag-stub"

    def __init__(self) -> None:
        self.prompts: list[str] = []

    async def generate(self, prompt: str) -> dict[str, Any]:
        self.prompts.append(prompt)
        if prompt.startswith("Revise your diagnosis."):
            assert "no rollback step" in prompt  # critic notes fed back
            text = json.dumps({"hypotheses": [
                {"text": "coupon applied twice, rollback missing",
                 "evidence_ids": ["E1"]}
            ]})
        else:
            text = json.dumps({"hypotheses": [
                {"text": "coupon applied twice", "evidence_ids": ["E1"]}
            ]})
        return {"text": text, "input_tokens": 10, "output_tokens": 5}


class RejectOnceCritic:
    family = "llama"
    model = "critic-stub"

    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, prompt: str) -> dict[str, Any]:
        self.calls += 1
        if self.calls == 1:
            text = '{"accept": false, "notes": "no rollback step"}'
        else:
            text = '{"accept": true, "notes": "sound now"}'
        return {"text": text, "input_tokens": 10, "output_tokens": 5}


def test_critic_notes_feed_a_revised_diagnosis() -> None:
    async def scenario() -> None:
        diagnosis = RevisionDiagnosis()
        critic = RejectOnceCritic()
        items = [EvidenceItem(id="E1", source="logs", window="5m",
                              text="discount applied twice")]
        result = await run_diagnosis(items, diagnosis, critic)
        assert result.status == "ACCEPTED", result.reason
        assert result.revisions == 1
        assert result.hypotheses[0].text.endswith("rollback missing")
        assert any(p.startswith("Revise your diagnosis.")
                   for p in diagnosis.prompts)

    run(scenario())


def test_adapters_cover_all_evidence_kinds() -> None:
    payload = {
        "logs": ["discount applied twice"],
        "metrics": ["error_rate=0.42 window=5m"],
        "traces": ["trace 9f3: checkout -> pricing 820ms"],
        "deployments": ["deploy pricing v12 at 09:41"],
        "git_revision": "rev-1",
    }
    tuples = items_from_payload(payload, "checkout", "staging")
    sources = {source for source, _window, _text in tuples}
    assert sources == {
        "checkout-logs", "checkout-metrics", "checkout-traces",
        "checkout-deployments", "checkout-git", "checkout-meta",
    }
    git_text = next(t for s, _w, t in tuples if s == "checkout-git")
    assert "rev-1" in git_text
