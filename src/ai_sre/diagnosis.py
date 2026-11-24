"""FR-4/FR-5: tiered cited diagnosis with cross-family critic, caps, budget.

Caps (all enforced, all covered by the eval): at most 20 evidence tool
calls, 3 generation attempts per step, 25 s per model call, 2 critic
revisions. The critic must belong to a different model family than the
diagnosis model. Costs are rough per-1k-token estimates for ledger math;
local Ollama tiers cost 0.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from typing import Any, Protocol

import httpx

from ai_sre.evidence import EvidenceItem, estimate_tokens

MODEL_CALL_TIMEOUT = 25.0
MAX_EVIDENCE_CALLS = 20
MAX_STEP_ATTEMPTS = 3
MAX_REVISIONS = 2
MAX_INCIDENT_TOKENS = 80_000
MAX_INCIDENT_COST_USD = 0.08

COST_PER_1K_USD = {  # rough estimates for ledger math, not billing
    "gemini-2.5-flash": 0.0003,
    "deepseek-flash": 0.0006,
    "deepseek-v3": 0.0003,
    "qwen3.8-flash": 0.0005,
    "qwen-2.5-coder-32b": 0.0004,
    "claude-haiku-5-5": 0.0005,
    "claude-3.5-haiku": 0.001,
    "ollama": 0.0,
}


class Transport(Protocol):
    family: str
    model: str

    async def generate(self, prompt: str) -> dict[str, Any]: ...


@dataclass
class OllamaTransport:
    model: str = "qwen2.5-coder:7b"
    family: str = "qwen"
    host: str = "http://127.0.0.1:11434"

    async def generate(self, prompt: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=MODEL_CALL_TIMEOUT) as client:
            response = await client.post(
                f"{self.host}/api/generate",
                json={"model": self.model, "prompt": prompt, "stream": False},
            )
            response.raise_for_status()
            body = response.json()
        text = str(body.get("response", ""))
        return {
            "text": text,
            "input_tokens": estimate_tokens(prompt),
            "output_tokens": estimate_tokens(text),
        }


@dataclass
class OpenRouterTransport:
    api_key: str
    model: str
    family: str
    site: str = "https://openrouter.ai/api/v1"

    async def generate(self, prompt: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=MODEL_CALL_TIMEOUT) as client:
            response = await client.post(
                f"{self.site}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={"model": self.model, "messages": [{"role": "user", "content": prompt}]},
            )
            response.raise_for_status()
            text = str(response.json()["choices"][0]["message"]["content"])
        return {
            "text": text,
            "input_tokens": estimate_tokens(prompt),
            "output_tokens": estimate_tokens(text),
        }


@dataclass
class GeminiTransport:
    api_key: str
    model: str = "gemini-2.5-flash"
    family: str = "google"
    site: str = "https://generativelanguage.googleapis.com/v1beta/openai"

    async def generate(self, prompt: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=MODEL_CALL_TIMEOUT) as client:
            response = await client.post(
                f"{self.site}/chat/completions",
                headers={"Authorization": f"Bearer {self.api_key}"},
                json={"model": self.model, "messages": [{"role": "user", "content": prompt}]},
            )
            response.raise_for_status()
            text = str(response.json()["choices"][0]["message"]["content"])
        return {
            "text": text,
            "input_tokens": estimate_tokens(prompt),
            "output_tokens": estimate_tokens(text),
        }


def _family_for(model: str) -> str:
    """Derive the model family from a model name or OpenRouter slug."""
    slug = model.lower()
    if "gemini" in slug or "gemma" in slug:
        return "google"
    if "claude" in slug:
        return "anthropic"
    for hint in ("llama", "qwen", "deepseek", "mistral"):
        if hint in slug:
            return hint
    return slug.split("/")[0].split("-")[0].split(":")[0]


def _required_key(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    raise ValueError(
        f"no API key in environment (looked for {', '.join(names)}); "
        "export it in the operator shell (AC-8), never in a repository file"
    )


def transport_from_env(role: str, default: str) -> Transport:
    """Build the transport for a role (DIAGNOSIS, CRITIC, TRIAGE) from env.

    ``<ROLE>_MODEL`` is a spec: ``gemini:<model>``, ``openrouter:<model>``
    or ``ollama:<model>``; a bare model name means Ollama (the historical
    behaviour). ``<ROLE>_FAMILY`` overrides the derived model family.
    API keys come from the operator shell only (AC-8).
    """
    spec = os.environ.get(f"{role}_MODEL", default)
    provider, model = "ollama", spec
    for prefix in ("gemini:", "openrouter:", "ollama:"):
        if spec.startswith(prefix):
            provider, model = prefix[:-1], spec[len(prefix):]
            break
    else:
        head, sep, _ = spec.partition(":")
        if sep and head.isalpha():
            raise ValueError(
                f"unknown model provider {head!r} in {role}_MODEL "
                "(use gemini:, openrouter: or ollama:)"
            )
    family = os.environ.get(f"{role}_FAMILY") or _family_for(model)
    if provider == "gemini":
        return GeminiTransport(
            api_key=_required_key("GEMINI_API_KEY", "GOOGLE_API_KEY"),
            model=model,
            family=family,
        )
    if provider == "openrouter":
        return OpenRouterTransport(
            api_key=_required_key("OPENROUTER_API_KEY"), model=model, family=family
        )
    return OllamaTransport(model=model, family=family)


def cost_per_1k(model: str) -> float:
    for key, price in COST_PER_1K_USD.items():
        if key in model:
            return price
    return 0.001


@dataclass
class BudgetLedger:
    max_tokens: int = MAX_INCIDENT_TOKENS
    max_cost_usd: float = MAX_INCIDENT_COST_USD
    tokens_used: int = 0
    cost_usd: float = 0.0

    def check(self, model: str, prompt_tokens: int, max_out_tokens: int) -> bool:
        """Worst-case reserve before a call; False means the call is refused."""
        worst_tokens = self.tokens_used + prompt_tokens + max_out_tokens
        worst_cost = self.cost_usd + (prompt_tokens + max_out_tokens) / 1000 * cost_per_1k(model)
        return worst_tokens <= self.max_tokens and worst_cost <= self.max_cost_usd

    def spend(self, model: str, input_tokens: int, output_tokens: int) -> None:
        self.tokens_used += input_tokens + output_tokens
        self.cost_usd += (input_tokens + output_tokens) / 1000 * cost_per_1k(model)


@dataclass
class Hypothesis:
    text: str
    evidence_ids: list[str]


@dataclass
class DiagnosisResult:
    status: str  # ACCEPTED or INCONCLUSIVE
    hypotheses: list[Hypothesis] = field(default_factory=list)
    revisions: int = 0
    reason: str = ""
    ledger: BudgetLedger = field(default_factory=BudgetLedger)
    evidence_calls: int = 0


def validate_citations(hypotheses: list[Hypothesis], known_ids: set[str]) -> list[str]:
    """Every claim must cite stored evidence IDs; returns unknown IDs."""
    unknown: list[str] = []
    for hypothesis in hypotheses:
        if not hypothesis.evidence_ids:
            unknown.append(f"<uncited:{hypothesis.text[:40]}>")
            continue
        for evidence_id in hypothesis.evidence_ids:
            if evidence_id not in known_ids and evidence_id not in unknown:
                unknown.append(evidence_id)
    return unknown


def parse_hypotheses(text: str) -> list[Hypothesis]:
    payload = json.loads(text)
    return [
        Hypothesis(text=str(h["text"]), evidence_ids=[str(e) for e in h.get("evidence_ids", [])])
        for h in payload.get("hypotheses", [])
    ]


async def run_diagnosis(
    evidence: list[EvidenceItem],
    diagnosis: Transport,
    critic: Transport,
    ledger: BudgetLedger | None = None,
    fetch_more: Any = None,
) -> DiagnosisResult:
    """Generate a cited diagnosis, reviewed by a cross-family critic."""
    if critic.family == diagnosis.family:
        raise ValueError("critic must belong to a different model family")
    ledger = ledger or BudgetLedger()
    known_ids = {item.id for item in evidence}
    packed = "\n".join(f"[{i.id}] {i.text}" for i in evidence)
    evidence_calls = 0

    result = DiagnosisResult(status="INCONCLUSIVE", ledger=ledger)
    hypotheses: list[Hypothesis] = []
    for _ in range(MAX_STEP_ATTEMPTS):
        if not ledger.check(diagnosis.model, estimate_tokens(packed), 2000):
            result.reason = "budget exhausted before diagnosis call"
            return result
        if fetch_more is not None and evidence_calls < MAX_EVIDENCE_CALLS:
            extra = fetch_more()
            evidence_calls += 1
            if extra:
                for item in extra:
                    known_ids.add(item.id)
                packed += "\n" + "\n".join(f"[{i.id}] {i.text}" for i in extra)
        try:
            reply = await diagnosis.generate(
                "Diagnose from the evidence. Reply ONLY as JSON "
                '{"hypotheses": [{"text": ..., "evidence_ids": [...]}]}.\n' + packed
            )
        except Exception as exc:  # noqa: BLE001 - a failed call is a failed attempt
            result.reason = f"diagnosis call failed: {exc}"
            continue
        ledger.spend(diagnosis.model, int(reply["input_tokens"]), int(reply["output_tokens"]))
        try:
            hypotheses = parse_hypotheses(str(reply["text"]))
        except (ValueError, KeyError, TypeError):
            result.reason = "diagnosis reply was not valid hypotheses JSON"
            continue
        if unknown := validate_citations(hypotheses, known_ids):
            result.reason = f"unknown evidence citations: {unknown}"
            continue
        break
    else:
        return result
    if not hypotheses:
        result.reason = "no valid hypotheses after 3 attempts"
        return result

    notes = ""
    for revision in range(MAX_REVISIONS + 1):
        if not ledger.check(critic.model, estimate_tokens(packed), 1000):
            result.reason = "budget exhausted before critic call"
            return result
        try:
            verdict = await critic.generate(
                "Accept or reject this diagnosis as JSON "
                '{"accept": true/false, "notes": ...}. Diagnosis: '
                + json.dumps([{"text": h.text, "evidence_ids": h.evidence_ids} for h in hypotheses])
            )
        except Exception as exc:  # noqa: BLE001
            result.reason = f"critic call failed: {exc}"
            return result
        ledger.spend(critic.model, int(verdict["input_tokens"]), int(verdict["output_tokens"]))
        try:
            verdict_payload = json.loads(str(verdict["text"]))
            accepted = bool(verdict_payload["accept"])
            notes = str(verdict_payload.get("notes", ""))
        except (ValueError, KeyError, TypeError):
            result.reason = "critic reply was not valid JSON"
            return result
        if accepted:
            result.status = "ACCEPTED"
            result.hypotheses = hypotheses
            result.revisions = revision
            result.evidence_calls = evidence_calls
            return result
        result.revisions = min(revision + 1, MAX_REVISIONS)
        if revision >= MAX_REVISIONS:
            break
        # FR-17: feed the critic's notes into a REVISED diagnosis instead
        # of re-asking the critic about the same one.
        if not ledger.check(diagnosis.model, estimate_tokens(packed), 2000):
            result.reason = "budget exhausted before revision call"
            return result
        try:
            reply = await diagnosis.generate(
                "Revise your diagnosis. Reply ONLY as JSON "
                '{"hypotheses": [{"text": ..., "evidence_ids": [...]}]}.\n'
                f"Critic notes: {notes}\n"
                "Previous hypotheses: "
                + json.dumps([{"text": h.text, "evidence_ids": h.evidence_ids} for h in hypotheses])
                + "\nEvidence:\n" + packed
            )
        except Exception as exc:  # noqa: BLE001
            result.reason = f"revision call failed: {exc}"
            return result
        ledger.spend(diagnosis.model, int(reply["input_tokens"]), int(reply["output_tokens"]))
        try:
            revised = parse_hypotheses(str(reply["text"]))
        except (ValueError, KeyError, TypeError):
            result.reason = "revision reply was not valid hypotheses JSON"
            return result
        if unknown := validate_citations(revised, known_ids):
            result.reason = f"unknown evidence citations: {unknown}"
            return result
        if revised:
            hypotheses = revised
    result.reason = "critic rejected after 2 revisions; escalating"
    result.evidence_calls = evidence_calls
    return result
