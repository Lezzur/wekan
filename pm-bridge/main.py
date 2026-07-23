"""pm-bridge — the spine. Routes webhooks between WeKan/Alertmanager and
exports flow metrics for Prometheus. This is a SKELETON: routes exist and
respond, but board mutation and real metric computation are stubbed (TODO).
"""
import asyncio
import os
import time

import httpx
from fastapi import FastAPI, Request
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

WEKAN_URL = os.environ.get("WEKAN_URL", "http://wekan:8080")
# Optional ChatOps sink. Empty = Mattermost module disabled (decision deferred).
MATTERMOST_WEBHOOK = os.environ.get("MATTERMOST_WEBHOOK", "")
FLOW_INTERVAL_SECONDS = int(os.environ.get("FLOW_INTERVAL_SECONDS", "300"))

app = FastAPI(title="pm-bridge", version="0.0.1")

# Honest flow metrics only — no velocity points, no burndown, no leaderboards.
WIP = Gauge("pm_wip", "Cards currently in progress", ["board", "column"])
CYCLE_TIME = Gauge("pm_cycle_time_seconds", "Cycle time of completed cards", ["board"])
BLOCKED_AGE = Gauge("pm_blocked_age_seconds", "Age of the oldest blocked card", ["board"])
THROUGHPUT = Gauge("pm_throughput_cards", "Cards completed in the last interval", ["board"])

_client: httpx.AsyncClient | None = None
_alert_lock = asyncio.Lock()
_seen_fingerprints: set[str] = set()


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(timeout=10.0)
    asyncio.create_task(_flow_metrics_loop())


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _client is not None:
        await _client.aclose()


@app.get("/healthz")
async def healthz() -> dict:
    return {"ok": True, "ts": time.time()}


@app.get("/metrics")
async def metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.post("/wekan")
async def wekan_webhook(request: Request) -> dict:
    """Board event ingress (card moved/created/etc). TODO: translate to actions."""
    payload = await request.json()
    # TODO: react to card transitions; optionally notify ChatOps.
    return {"received": True, "keys": sorted(payload.keys())}


@app.post("/alert")
async def alert_webhook(request: Request) -> dict:
    """Alertmanager -> Triage card. Dedup by fingerprint (single-node lock)."""
    payload = await request.json()
    created = 0
    for alert in payload.get("alerts", []):
        fp = alert.get("fingerprint", "")
        async with _alert_lock:
            if fp in _seen_fingerprints:
                continue
            _seen_fingerprints.add(fp)
        # TODO: create a Triage card in WeKan via REST.
        created += 1
    return {"created": created}


async def _flow_metrics_loop() -> None:
    """Every FLOW_INTERVAL_SECONDS, recompute flow metrics from the board.
    TODO: pull real board state via WeKan REST and populate the gauges."""
    while True:
        try:
            # TODO: replace stub with real WeKan REST queries.
            pass
        except Exception:  # keep the loop alive; a scrape gap is not fatal
            pass
        await asyncio.sleep(FLOW_INTERVAL_SECONDS)
