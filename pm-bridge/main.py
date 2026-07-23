"""pm-bridge — the spine. Routes webhooks between WeKan/Alertmanager and
exports flow metrics for Prometheus.

Flow loop status: REST client wired (login -> boards -> lists -> cards).
WIP-per-column is exact. Cycle-time / throughput / blocked-age depend on
board date + label conventions and are computed from card dates
(startAt/endAt) and a configurable blocked label — verify against a real
board before trusting the numbers.
"""
import asyncio
import os
import time

import httpx
from fastapi import FastAPI, Request
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

WEKAN_URL = os.environ.get("WEKAN_URL", "http://wekan:8080")
WEKAN_USER = os.environ.get("WEKAN_USER", "")
WEKAN_PASS = os.environ.get("WEKAN_PASS", "")
# Cards carrying this label (case-insensitive) count as blocked. Board convention.
WEKAN_BLOCKED_LABEL = os.environ.get("WEKAN_BLOCKED_LABEL", "blocked").lower()
# Optional ChatOps sink. Empty = ChatOps disabled (provider-agnostic).
CHATOPS_WEBHOOK = os.environ.get("CHATOPS_WEBHOOK", "") or os.environ.get("MATTERMOST_WEBHOOK", "")
FLOW_INTERVAL_SECONDS = int(os.environ.get("FLOW_INTERVAL_SECONDS", "300"))

app = FastAPI(title="pm-bridge", version="0.1.0")

# Honest flow metrics only — no velocity points, no burndown, no leaderboards.
WIP = Gauge("pm_wip", "Cards currently in progress", ["board", "column"])
CYCLE_TIME = Gauge("pm_cycle_time_seconds", "Mean cycle time of cards completed in the last interval", ["board"])
BLOCKED_AGE = Gauge("pm_blocked_age_seconds", "Age of the oldest blocked card", ["board"])
THROUGHPUT = Gauge("pm_throughput_cards", "Cards completed in the last interval", ["board"])

_client: httpx.AsyncClient | None = None
_alert_lock = asyncio.Lock()
_seen_fingerprints: set[str] = set()


@app.on_event("startup")
async def _startup() -> None:
    global _client
    _client = httpx.AsyncClient(base_url=WEKAN_URL, timeout=15.0)
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
        # TODO: create a Triage card in WeKan via REST (needs a target board/list).
        created += 1
    return {"created": created}


# --- WeKan REST client -----------------------------------------------------

class WekanClient:
    """Thin WeKan REST wrapper. Auth is username/password -> bearer token."""

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._c = client
        self._token: str | None = None
        self._user_id: str | None = None

    async def login(self) -> None:
        # WeKan: POST /users/login (form-encoded) -> {id, token, tokenExpires}
        r = await self._c.post(
            "/users/login",
            data={"username": WEKAN_USER, "password": WEKAN_PASS},
        )
        r.raise_for_status()
        body = r.json()
        self._token = body["token"]
        self._user_id = body["id"]

    def _headers(self) -> dict:
        return {"Authorization": f"Bearer {self._token}"}

    async def _get(self, path: str) -> object:
        r = await self._c.get(path, headers=self._headers())
        if r.status_code == 401:  # token expired -> re-auth once
            await self.login()
            r = await self._c.get(path, headers=self._headers())
        r.raise_for_status()
        return r.json()

    async def boards(self) -> list[dict]:
        return await self._get(f"/api/users/{self._user_id}/boards")

    async def lists(self, board_id: str) -> list[dict]:
        return await self._get(f"/api/boards/{board_id}/lists")

    async def cards(self, board_id: str, list_id: str) -> list[dict]:
        return await self._get(f"/api/boards/{board_id}/lists/{list_id}/cards")

    async def card(self, board_id: str, list_id: str, card_id: str) -> dict:
        return await self._get(f"/api/boards/{board_id}/lists/{list_id}/cards/{card_id}")


def _parse_ts(value: object) -> float | None:
    """WeKan dates come as ISO strings. Return epoch seconds or None."""
    if not value or not isinstance(value, str):
        return None
    try:
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


async def _collect_board_metrics(wk: WekanClient, board: dict, now: float) -> None:
    board_id = board["_id"]
    board_title = board.get("title", board_id)
    window_start = now - FLOW_INTERVAL_SECONDS

    completed_cycles: list[float] = []
    throughput = 0
    oldest_blocked_ts: float | None = None

    for lst in await wk.lists(board_id):
        list_id = lst["_id"]
        list_title = lst.get("title", list_id)
        card_stubs = await wk.cards(board_id, list_id)

        # WIP: exact count of cards per column.
        WIP.labels(board=board_title, column=list_title).set(len(card_stubs))

        # Per-card detail is needed for dates/labels.
        for stub in card_stubs:
            card = await wk.card(board_id, list_id, stub["_id"])
            start = _parse_ts(card.get("startAt"))
            end = _parse_ts(card.get("endAt"))

            if end is not None and end >= window_start:
                throughput += 1
                if start is not None and end >= start:
                    completed_cycles.append(end - start)

            labels = [str(x).lower() for x in (card.get("labels") or [])]
            if WEKAN_BLOCKED_LABEL in labels:
                # No native "blocked since" — use last activity as the proxy age anchor.
                anchor = _parse_ts(card.get("dateLastActivity")) or _parse_ts(card.get("createdAt"))
                if anchor is not None and (oldest_blocked_ts is None or anchor < oldest_blocked_ts):
                    oldest_blocked_ts = anchor

    THROUGHPUT.labels(board=board_title).set(throughput)
    CYCLE_TIME.labels(board=board_title).set(
        sum(completed_cycles) / len(completed_cycles) if completed_cycles else 0.0
    )
    BLOCKED_AGE.labels(board=board_title).set(
        now - oldest_blocked_ts if oldest_blocked_ts is not None else 0.0
    )


async def _flow_metrics_loop() -> None:
    """Every FLOW_INTERVAL_SECONDS, recompute flow metrics from the live board."""
    assert _client is not None
    wk = WekanClient(_client)
    while True:
        try:
            if WEKAN_USER and WEKAN_PASS:
                if wk._token is None:
                    await wk.login()
                now = time.time()
                for board in await wk.boards():
                    await _collect_board_metrics(wk, board, now)
        except Exception:  # keep the loop alive; a scrape gap is not fatal
            wk._token = None  # force re-auth next tick
        await asyncio.sleep(FLOW_INTERVAL_SECONDS)
