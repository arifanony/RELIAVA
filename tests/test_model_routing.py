"""Model routing: env-spec transport selection (SPEC NFR-4) and the
Gemini API transport. No network, no keys: env is monkeypatched and
httpx is faked for the generate() shape test.
"""

from __future__ import annotations

import asyncio
from typing import Any, ClassVar, Self

import pytest

from ai_sre.diagnosis import (
    GeminiTransport,
    OllamaTransport,
    OpenRouterTransport,
    cost_per_1k,
    transport_from_env,
)

KEY_VARS = ("GEMINI_API_KEY", "GOOGLE_API_KEY", "OPENROUTER_API_KEY")


@pytest.fixture(autouse=True)
def clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in KEY_VARS:
        monkeypatch.delenv(var, raising=False)
    for role in ("DIAGNOSIS", "CRITIC", "TRIAGE"):
        monkeypatch.delenv(f"{role}_MODEL", raising=False)
        monkeypatch.delenv(f"{role}_FAMILY", raising=False)


def test_bare_model_name_means_ollama(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DIAGNOSIS_MODEL", "qwen2.5-coder:7b")
    transport = transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")
    assert isinstance(transport, OllamaTransport)
    assert transport.model == "qwen2.5-coder:7b"
    assert transport.family == "qwen"


def test_ollama_prefix_keeps_model_tag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRITIC_MODEL", "ollama:llama3.2:1b")
    transport = transport_from_env("CRITIC", "ollama:llama3.2:1b")
    assert isinstance(transport, OllamaTransport)
    assert transport.model == "llama3.2:1b"
    assert transport.family == "llama"


def test_gemini_spec(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    transport = transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")
    assert isinstance(transport, GeminiTransport)
    assert transport.model == "gemini-2.5-flash"
    assert transport.family == "google"


def test_gemini_key_falls_back_to_google_api_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GOOGLE_API_KEY", "test-key")
    transport = transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")
    assert isinstance(transport, GeminiTransport)


def test_gemini_missing_key_fails_loud() -> None:
    with pytest.raises(ValueError, match="GEMINI_API_KEY"):
        transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")


def test_openrouter_spec_derives_family_from_slug(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OPENROUTER_API_KEY", "test-key")
    monkeypatch.setenv("CRITIC_MODEL", "openrouter:deepseek/deepseek-v4.1-flash")
    transport = transport_from_env("CRITIC", "ollama:llama3.2:1b")
    assert isinstance(transport, OpenRouterTransport)
    assert transport.family == "deepseek"


def test_family_override_wins(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CRITIC_MODEL", "ollama:llama3.2:1b")
    monkeypatch.setenv("CRITIC_FAMILY", "meta")
    transport = transport_from_env("CRITIC", "ollama:llama3.2:1b")
    assert transport.family == "meta"


def test_unknown_provider_fails_loud() -> None:
    with pytest.raises(ValueError, match="unknown model provider"):
        transport_from_env("DIAGNOSIS", "azure:gpt-4")


def test_activity_defaults_are_cross_family(monkeypatch: pytest.MonkeyPatch) -> None:
    """The defaults wired in activities.diagnose must satisfy FR-5."""
    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    diagnosis = transport_from_env("DIAGNOSIS", "gemini:gemini-2.5-flash")
    critic = transport_from_env("CRITIC", "ollama:llama3.2:1b")
    assert diagnosis.family != critic.family


class _FakeResponse:
    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict[str, Any]:
        return {"choices": [{"message": {"content": "hello world"}}]}


class _FakeClient:
    seen: ClassVar[dict[str, Any]] = {}

    def __init__(self, timeout: float | None = None) -> None:
        self.timeout = timeout

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc: object) -> bool:
        return False

    async def post(
        self, url: str, headers: dict[str, str], json: dict[str, Any]
    ) -> _FakeResponse:
        _FakeClient.seen = {"url": url, "headers": headers, "json": json}
        return _FakeResponse()


def test_gemini_generate_shape(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("ai_sre.diagnosis.httpx.AsyncClient", _FakeClient)
    transport = GeminiTransport(api_key="test-key", model="gemini-2.5-flash")
    reply = asyncio.run(transport.generate("diagnose this"))
    assert reply["text"] == "hello world"
    assert reply["input_tokens"] > 0
    assert reply["output_tokens"] > 0
    assert _FakeClient.seen["url"].endswith("/chat/completions")
    assert "generativelanguage.googleapis.com" in _FakeClient.seen["url"]
    assert _FakeClient.seen["headers"]["Authorization"] == "Bearer test-key"
    assert _FakeClient.seen["json"]["model"] == "gemini-2.5-flash"


def test_ledger_covers_routed_models() -> None:
    assert cost_per_1k("gemini-2.5-flash") == 0.0003
    assert cost_per_1k("qwen/qwen3.8-flash") == 0.0005
    assert cost_per_1k("deepseek/deepseek-flash") == 0.0006
    assert cost_per_1k("claude-haiku-5-5") == 0.0005
    assert cost_per_1k("ollama") == 0.0
