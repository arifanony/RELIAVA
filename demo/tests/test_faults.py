"""DEMO-1 faults gate: each injector behaves per the incident catalog."""

from __future__ import annotations

import asyncio
from typing import Any

from app import app as demo_app
from httpx import ASGITransport, AsyncClient


def run(coro: Any) -> Any:
    return asyncio.run(coro)


async def fresh_client() -> AsyncClient:
    await AsyncClient(
        transport=ASGITransport(app=demo_app), base_url="http://demo"
    ).post("/faults/clear")
    return AsyncClient(transport=ASGITransport(app=demo_app), base_url="http://demo")


def test_healthy_browse_and_checkout() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            browse = await client.get("/browse")
            assert browse.status_code == 200
            assert len(browse.json()["products"]) == 2
            checkout = await client.post("/checkout", json={"product_id": 1})
            assert checkout.status_code == 200
            body = checkout.json()
            assert body["trace_id"]
            assert checkout.headers["x-trace-id"] == body["trace_id"]
            metrics = await client.get("/metrics")
            assert 'demo_checkout_requests_total{status="200"}' in metrics.text
        finally:
            await client.aclose()

    run(scenario())


def test_config_regression_fault() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            activated = await client.post("/faults/activate", json={"fault": "config-regression"})
            assert activated.status_code == 200
            for _ in range(2):
                failed = await client.post("/checkout", json={"product_id": 1})
                assert failed.status_code == 500
                assert "config value" in failed.json()["error"]
            metrics = await client.get("/metrics")
            assert 'demo_checkout_requests_total{status="500"}' in metrics.text
            await client.post("/faults/clear")
            recovered = await client.post("/checkout", json={"product_id": 1})
            assert recovered.status_code == 200
        finally:
            await client.aclose()

    run(scenario())


def test_code_bug_fault() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            await client.post("/faults/activate", json={"fault": "code-bug"})
            failed = await client.post("/checkout", json={"product_id": 1})
            assert failed.status_code == 500
            assert "TypeError" in failed.json()["error"]
            await client.post("/faults/clear")
        finally:
            await client.aclose()

    run(scenario())


def test_inconclusive_evidence_fault() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            await client.post("/faults/activate", json={"fault": "no-evidence"})
            before = await client.get("/metrics")
            degraded = await client.post("/checkout", json={"product_id": 1})
            assert degraded.status_code == 200
            assert degraded.json()["trace_id"] is None
            assert "x-trace-id" not in degraded.headers
            after = await client.get("/metrics")

            def errors_500(text: str) -> str:
                return next(
                    line for line in text.splitlines() if 'status="500"' in line
                )

            assert errors_500(after.text) == errors_500(before.text)  # no new errors
            await client.post("/faults/clear")
        finally:
            await client.aclose()

    run(scenario())


def test_unknown_fault_rejected() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            rejected = await client.post("/faults/activate", json={"fault": "meteor"})
            assert rejected.status_code == 400
            await client.post("/faults/clear")
        finally:
            await client.aclose()

    run(scenario())


def test_alertmanager_webhook_sink() -> None:
    async def scenario() -> None:
        client = await fresh_client()
        try:
            payload = {
                "receiver": "demo-sink",
                "alerts": [
                    {
                        "labels": {
                            "alertname": "CheckoutErrorRate",
                            "severity": "critical",
                            "service": "checkout",
                            "environment": "staging",
                        }
                    }
                ],
            }
            sunk = await client.post("/alerts", json=payload)
            assert sunk.status_code == 200
            received = await client.get("/alerts/received")
            assert received.json()["alerts"][-1]["alerts"][0]["labels"]["alertname"] == (
                "CheckoutErrorRate"
            )
        finally:
            await client.aclose()

    run(scenario())
