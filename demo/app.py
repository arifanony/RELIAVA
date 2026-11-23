"""DEMO-1: disposable minimal Python e-commerce (browse, checkout).

Three fault injectors: config-regression (bad config value -> HTTP 500),
code-bug (exception in pricing -> HTTP 500), no-evidence (degraded checkout
with no trace and no errors -> inconclusive signals). /alerts is the
Alertmanager webhook sink until intake wiring lands.
"""

from __future__ import annotations

import time
import uuid
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse

FAULTS = ("config-regression", "code-bug", "no-evidence")

PRODUCTS = [
    {"id": 1, "name": "widget", "price": 9.99},
    {"id": 2, "name": "gadget", "price": 24.50},
]

state: dict[str, Any] = {"fault": None}
counters: dict[str, int] = {"200": 0, "500": 0}
alerts_received: list[Any] = []

app = FastAPI(title="demo store")


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


@app.get("/browse")
def browse() -> dict[str, Any]:
    return {"products": PRODUCTS}


@app.post("/checkout")
def checkout() -> Any:
    fault = state["fault"]
    if fault == "config-regression":
        counters["500"] += 1
        return JSONResponse(
            status_code=500,
            content={"error": "checkout failed: bad config value CHECKOUT_TAX_RATE"},
        )
    if fault == "code-bug":
        counters["500"] += 1
        return JSONResponse(
            status_code=500,
            content={"error": "TypeError: unsupported operand in apply_discount"},
        )
    counters["200"] += 1
    order_id = str(uuid.uuid4())
    if fault == "no-evidence":
        time.sleep(0.3)
        return JSONResponse(
            status_code=200,
            content={"order_id": order_id, "trace_id": None, "note": "trace unavailable"},
        )
    trace_id = str(uuid.uuid4())
    return JSONResponse(
        status_code=200,
        content={"order_id": order_id, "trace_id": trace_id},
        headers={"X-Trace-Id": trace_id},
    )


@app.get("/metrics")
def metrics() -> PlainTextResponse:
    lines = [
        "# HELP demo_checkout_requests_total Checkout requests by status.",
        "# TYPE demo_checkout_requests_total counter",
    ]
    lines += [f'demo_checkout_requests_total{{status="{k}"}} {v}' for k, v in counters.items()]
    return PlainTextResponse("\n".join(lines) + "\n")


@app.post("/faults/activate")
def activate_fault(body: dict[str, str]) -> dict[str, Any]:
    fault = body.get("fault")
    if fault not in FAULTS:
        raise HTTPException(status_code=400, detail=f"unknown fault: {fault}")
    state["fault"] = fault
    return {"fault": fault}


@app.post("/faults/clear")
def clear_fault() -> dict[str, Any]:
    state["fault"] = None
    return {"fault": None}


@app.get("/faults")
def current_fault() -> dict[str, Any]:
    return {"fault": state["fault"]}


@app.post("/alerts")
async def alert_sink(request: Request) -> dict[str, bool]:
    alerts_received.append(await request.json())
    return {"ok": True}


@app.get("/alerts/received")
def alerts_list() -> dict[str, Any]:
    return {"alerts": alerts_received}
