"""FR-17: the model gateway — every model call in the system goes through
here, so budget, concurrency, deadlines and audit live in exactly one place.

A GatewayTransport wraps a role's Transport and, per call:
  * refuses the call once the investigation deadline has passed,
  * takes one of the incident's 2 call slots (16 across the process),
  * reserves the worst-case budget BEFORE calling (the hold is released
    when the call returns; the calling skill remains the spender of
    record on the shared ledger, so tokens are never double-counted),
  * records the attempt (provider, model, prompt version, tokens, cost,
    outcome) in model_attempts for the incident timeline and evals.

Skills stay separate (FR-17): triage, diagnosis, critic and patch each get
their own wrapped transport, model and prompt version.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool

from ai_sre.diagnosis import BudgetLedger, Transport, cost_per_1k
from ai_sre.evidence import estimate_tokens

PROMPT_VERSIONS = {
    "triage": "triage-v1",
    "diagnosis": "diagnosis-v1",
    "critic": "critic-v1",
    "patch": "patch-v1",
}

MAX_CALLS_PER_INCIDENT = 2
MAX_CALLS_GLOBAL = 16

SCHEMA = """
CREATE TABLE IF NOT EXISTS model_attempts (
  id SERIAL PRIMARY KEY,
  incident_id UUID NOT NULL,
  role TEXT NOT NULL,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  prompt_version TEXT NOT NULL,
  input_tokens INT NOT NULL DEFAULT 0,
  output_tokens INT NOT NULL DEFAULT 0,
  cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  outcome TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""


async def init_gateway(pool: AsyncConnectionPool) -> None:
    async with pool.connection() as conn:
        async with conn.cursor() as cur:
            await cur.execute(SCHEMA)
        await conn.commit()


class DeadlineExceeded(Exception):
    pass


class BudgetRefused(Exception):
    pass


@dataclass
class Reservation:
    model: str
    tokens: int
    cost_usd: float


class ReservingLedger(BudgetLedger):
    """BudgetLedger with worst-case holds: check() counts outstanding
    reservations, so concurrent calls cannot both spend the same budget."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.holds: list[Reservation] = []

    def check(  # type: ignore[override]
        self, model: str, prompt_tokens: int, max_out_tokens: int
    ) -> bool:
        held_tokens = sum(h.tokens for h in self.holds)
        held_cost = sum(h.cost_usd for h in self.holds)
        worst_tokens = self.tokens_used + held_tokens + prompt_tokens + max_out_tokens
        worst_cost = (
            self.cost_usd
            + held_cost
            + (prompt_tokens + max_out_tokens) / 1000 * cost_per_1k(model)
        )
        return worst_tokens <= self.max_tokens and worst_cost <= self.max_cost_usd

    def reserve(self, model: str, prompt_tokens: int, max_out_tokens: int) -> Reservation:
        hold = Reservation(
            model=model,
            tokens=prompt_tokens + max_out_tokens,
            cost_usd=(prompt_tokens + max_out_tokens) / 1000 * cost_per_1k(model),
        )
        self.holds.append(hold)
        return hold

    def release(self, hold: Reservation) -> None:
        if hold in self.holds:
            self.holds.remove(hold)


@dataclass
class Gateway:
    """Per-incident gateway state shared by the incident's skill transports."""

    incident_id: str
    ledger: ReservingLedger
    deadline: datetime
    pool: AsyncConnectionPool | None = None
    attempts: list[dict[str, Any]] = field(default_factory=list)
    _slots: asyncio.Semaphore = field(
        default_factory=lambda: asyncio.Semaphore(MAX_CALLS_PER_INCIDENT)
    )

    def wrap(
        self, transport: Transport, role: str, provider: str
    ) -> GatewayTransport:
        return GatewayTransport(self, transport, role, provider)

    async def record(self, attempt: dict[str, Any]) -> None:
        self.attempts.append(attempt)
        if self.pool is not None:
            async with self.pool.connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute(
                        "INSERT INTO model_attempts (incident_id, role, provider,"
                        " model, prompt_version, input_tokens, output_tokens,"
                        " cost_usd, outcome) VALUES (%s, %s, %s, %s, %s, %s, %s,"
                        " %s, %s)",
                        (
                            self.incident_id, attempt["role"], attempt["provider"],
                            attempt["model"], attempt["prompt_version"],
                            attempt["input_tokens"], attempt["output_tokens"],
                            attempt["cost_usd"], attempt["outcome"],
                        ),
                    )
                await conn.commit()


_GLOBAL_SLOTS = asyncio.Semaphore(MAX_CALLS_GLOBAL)


@dataclass
class GatewayTransport:
    """A Transport that routes one role's calls through the gateway."""

    gateway: Gateway
    inner: Transport
    role: str
    provider: str
    max_out_tokens: int = 2000
    family: str = field(init=False)
    model: str = field(init=False)

    def __post_init__(self) -> None:
        self.family = self.inner.family
        self.model = self.inner.model

    async def generate(self, prompt: str) -> dict[str, Any]:
        gw = self.gateway
        base = {
            "role": self.role,
            "provider": self.provider,
            "model": self.model,
            "prompt_version": PROMPT_VERSIONS.get(self.role, "unknown"),
            "input_tokens": 0,
            "output_tokens": 0,
            "cost_usd": 0.0,
        }
        if datetime.now(UTC) > gw.deadline:
            await gw.record({**base, "outcome": "refused_deadline"})
            raise DeadlineExceeded(f"investigation deadline passed for {gw.incident_id}")
        async with _GLOBAL_SLOTS, gw._slots:
            prompt_tokens = estimate_tokens(prompt)
            if not gw.ledger.check(self.model, prompt_tokens, self.max_out_tokens):
                await gw.record({**base, "outcome": "refused_budget"})
                raise BudgetRefused(f"budget refused for {self.model}")
            hold = gw.ledger.reserve(self.model, prompt_tokens, self.max_out_tokens)
            started = time.monotonic()
            try:
                reply = await self.inner.generate(prompt)
            except Exception:
                gw.ledger.release(hold)
                await gw.record({**base, "outcome": "error"})
                raise
            input_tokens = int(reply["input_tokens"])
            output_tokens = int(reply["output_tokens"])
            # The caller (run_diagnosis / the patch skill) remains the
            # spender of record on the shared ledger; the gateway releases
            # its worst-case hold and records the actuals here.
            gw.ledger.release(hold)
            cost = (input_tokens + output_tokens) / 1000 * cost_per_1k(self.model)
            await gw.record(
                {
                    **base,
                    "input_tokens": input_tokens,
                    "output_tokens": output_tokens,
                    "cost_usd": cost,
                    "outcome": "ok",
                    "latency_s": round(time.monotonic() - started, 3),
                }
            )
            return reply


def provider_of(spec: str) -> str:
    """Provider label for an env model spec, mirroring transport_from_env:
    only the gemini:/openrouter:/ollama: prefixes name a provider; a bare
    model name (with or without a :tag) means Ollama."""
    for prefix in ("gemini:", "openrouter:", "ollama:"):
        if spec.startswith(prefix):
            return prefix[:-1]
    return "ollama"


async def triage_brief(
    transport: Transport, evidence_text: str
) -> str:
    """The triage skill (FR-17): a cheap first pass that classifies the
    incident and compresses the evidence before diagnosis spends on it."""
    reply = await transport.generate(
        "You are the triage skill of an incident-response agent. Classify "
        "the incident and summarise the evidence in at most 5 short lines: "
        "likely failure class, affected component, strongest signal, "
        "missing evidence, urgency. Evidence:\n" + evidence_text
    )
    return str(reply["text"]).strip()
