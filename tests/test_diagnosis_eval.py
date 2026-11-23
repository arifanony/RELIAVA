"""M1 eval: Drain3 + AST pre-processing, cited diagnosis, critic, budget."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import pytest

from ai_sre.diagnosis import (
    MAX_EVIDENCE_CALLS,
    BudgetLedger,
    Hypothesis,
    OllamaTransport,
    run_diagnosis,
    validate_citations,
)
from ai_sre.evidence import MAX_PACK_TOKENS, cluster_logs, collect, pack, slice_traceback


def run(coro: Any) -> Any:
    return asyncio.run(coro)


FIXTURE_LOGS = [
    "ERROR checkout failed for order 1001 ip=10.0.0.1",
    "ERROR checkout failed for order 1002 ip=10.0.0.2",
    "ERROR checkout failed for order 1003 ip=10.0.0.3",
    "INFO health ok",
    "ERROR checkout failed for order 1004 ip=10.0.0.4",
]

FIXTURE_TRACEBACK = """Traceback (most recent call last):
  File "app/checkout.py", line 2, in charge
    total = apply_discount(order)
  File "app/pricing.py", line 3, in apply_discount
    raise ValueError("bad coupon")
ValueError: bad coupon
"""

FIXTURE_SOURCES = {
    "app/checkout.py": "def charge(order):\n    total = apply_discount(order)\n    return total\n",
    "app/pricing.py": (
        "def apply_discount(order):\n"
        "    if order.coupon:\n"
        '        raise ValueError("bad coupon")\n'
        "    return order.total\n"
    ),
}


@dataclass
class StubTransport:
    family: str
    model: str
    replies: list[str]
    calls: int = 0

    async def generate(self, prompt: str) -> dict[str, Any]:
        self.calls += 1
        text = self.replies[min(self.calls - 1, len(self.replies) - 1)]
        return {"text": text, "input_tokens": 10, "output_tokens": 20}


def hypotheses_json(*ids: str) -> str:
    return json.dumps({"hypotheses": [{"text": "coupon bug", "evidence_ids": list(ids)}]})


def verdict_json(accept: bool) -> str:
    return json.dumps({"accept": accept, "notes": "stub"})


def test_pack_capped_with_hashes() -> None:
    items = collect([("logs", "5m", "x" * 500) for _ in range(200)])
    packed, used, dropped = pack(items)
    assert used <= MAX_PACK_TOKENS
    assert dropped > 0
    for line in packed.split("\n\n"):
        header = line.split("\n", 1)[0]
        assert "source=" in header and "window=" in header and "hash=" in header


def test_drain3_clusters_fixture_logs() -> None:
    clusters = cluster_logs(FIXTURE_LOGS)
    assert len(clusters) <= 3
    assert any("<" in template for template, _, _ in clusters)


def test_ast_slices_enclosing_function() -> None:
    chunks = slice_traceback(FIXTURE_TRACEBACK, FIXTURE_SOURCES)
    assert len(chunks) == 2
    assert "def charge" in chunks[0]
    assert "def apply_discount" in chunks[1]


def test_hallucinated_citations_rejected() -> None:
    evidence = collect([("logs", "5m", "boom")])
    diagnosis = StubTransport(family="qwen", model="qwen-x", replies=[hypotheses_json("E999")])
    critic = StubTransport(family="other", model="other-x", replies=[verdict_json(True)])
    result = run(run_diagnosis(evidence, diagnosis, critic))
    assert result.status == "INCONCLUSIVE"
    assert "unknown" in result.reason
    assert diagnosis.calls == 3  # all step attempts exhausted


def test_critic_accepts_after_revision() -> None:
    evidence = collect([("logs", "5m", "boom")])
    diagnosis = StubTransport(family="qwen", model="qwen-x", replies=[hypotheses_json("E1")])
    critic = StubTransport(
        family="other", model="other-x",
        replies=[verdict_json(False), verdict_json(True)],
    )
    result = run(run_diagnosis(evidence, diagnosis, critic))
    assert result.status == "ACCEPTED"
    assert result.revisions == 1
    assert result.hypotheses[0].evidence_ids == ["E1"]


def test_critic_rejection_caps_at_two_revisions() -> None:
    evidence = collect([("logs", "5m", "boom")])
    diagnosis = StubTransport(family="qwen", model="qwen-x", replies=[hypotheses_json("E1")])
    critic = StubTransport(family="other", model="other-x", replies=[verdict_json(False)])
    result = run(run_diagnosis(evidence, diagnosis, critic))
    assert result.status == "INCONCLUSIVE"
    assert result.revisions == 2
    assert critic.calls == 3  # initial review plus at most 2 revisions


def test_budget_refusal_leaves_ledger_proof() -> None:
    evidence = collect([("logs", "5m", "boom")])
    diagnosis = StubTransport(family="qwen", model="qwen-x", replies=[hypotheses_json("E1")])
    critic = StubTransport(family="other", model="other-x", replies=[verdict_json(True)])
    ledger = BudgetLedger(max_tokens=5)
    result = run(run_diagnosis(evidence, diagnosis, critic, ledger=ledger))
    assert result.status == "INCONCLUSIVE"
    assert "budget" in result.reason
    assert diagnosis.calls == 0
    assert result.ledger.tokens_used == 0  # refused before spend: AC-5 proof


def test_evidence_calls_bounded() -> None:
    evidence = collect([("logs", "5m", "boom")])
    seen: list[int] = []

    def fetch_more() -> list[Any]:
        seen.append(1)
        return []

    diagnosis = StubTransport(
        family="qwen", model="qwen-x",
        replies=[hypotheses_json("E999"), hypotheses_json("E999"), hypotheses_json("E1")],
    )
    critic = StubTransport(family="other", model="other-x", replies=[verdict_json(True)])
    result = run(run_diagnosis(evidence, diagnosis, critic, fetch_more=fetch_more))
    assert result.status == "ACCEPTED"
    assert result.evidence_calls == len(seen) <= MAX_EVIDENCE_CALLS


def test_same_family_critic_rejected() -> None:
    evidence = collect([("logs", "5m", "boom")])
    diagnosis = StubTransport(family="qwen", model="qwen-x", replies=[hypotheses_json("E1")])
    critic = StubTransport(family="qwen", model="qwen-y", replies=[verdict_json(True)])
    with pytest.raises(ValueError, match="different model family"):
        run(run_diagnosis(evidence, diagnosis, critic))


def test_validate_citations_flags_uncited() -> None:
    hyps = [Hypothesis(text="a claim with no backing", evidence_ids=[])]
    assert validate_citations(hyps, {"E1"}) != []


def test_ollama_transport_smoke() -> None:
    async def scenario() -> None:
        transport = OllamaTransport()
        try:
            reply = await transport.generate("Reply with exactly: {}")
        except Exception as exc:  # noqa: BLE001 - server may be down
            pytest.skip(f"ollama unavailable: {exc}")
        assert isinstance(reply["text"], str)
        assert reply["input_tokens"] > 0

    run(scenario())
