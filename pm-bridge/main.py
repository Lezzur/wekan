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
import pathlib
import time
from datetime import datetime

import httpx
from fastapi import FastAPI, Request
from prometheus_client import Gauge, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import FileResponse, JSONResponse, Response

WEKAN_URL = os.environ.get("WEKAN_URL", "http://wekan:8080")
PROM_URL = os.environ.get("PROM_URL", "http://prometheus:9090")
# Which board the UI renders. Empty = first board the service user can see.
WEKAN_BOARD_ID = os.environ.get("WEKAN_BOARD_ID", "")
STATIC_DIR = pathlib.Path(__file__).parent / "static"
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
BLOCKED_AGE = Gauge("pm_blocked_age_seconds", "Idle time (since last activity) of the oldest blocked card; lower bound on true block duration", ["board"])
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

    async def board_detail(self, board_id: str) -> dict:
        # Full board doc — carries the label definitions ({_id, name, color}).
        return await self._get(f"/api/boards/{board_id}")

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

    # WeKan cards reference labels by id, not name. Build id -> lowercased-name
    # so we can tell whether a card carries the configured blocked label.
    detail = await wk.board_detail(board_id)
    blocked_label_ids = {
        lab["_id"]
        for lab in (detail.get("labels") or [])
        if str(lab.get("name", "")).lower() == WEKAN_BLOCKED_LABEL
    }

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

            card_label_ids = set(card.get("labelIds") or [])
            if blocked_label_ids & card_label_ids:
                # WeKan records no "blocked since" (its activity log has no
                # addedLabel events), so anchor on last activity. This measures
                # idle time — a lower bound on true block duration, since any
                # touch resets it. Oldest anchor = the blocked card idle longest.
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


# --- UI layer (Soft Clinical board + flow dashboard) -----------------------
# Two screens served as static HTML, wired to live data: the board reads WeKan
# REST (reusing WekanClient above); the dashboard reads Prometheus.

COLUMN_ORDER = ["To Do", "In Progress", "Review", "Done"]
# CFD stack order is bottom->top: Done, Review, In Progress, To Do.
CFD_ORDER = ["Done", "Review", "In Progress", "To Do"]
COLUMN_COLOR = {
    "To Do": "#9b51e0",
    "In Progress": "#e08a1e",
    "Review": "#2f80c4",
    "Done": "#4a9e3f",
}
# Label name -> hex from the design palette (used when the board's labels are
# named like the handoff, so pills match the mock exactly).
HANDOFF_LABEL_HEX = {
    "compliance": "#9b51e0", "data": "#2f80c4", "recruitment": "#4a9e3f",
    "lab": "#e08a1e", "stats": "#6b7280", "writing": "#b8912a",
    "qc": "#17a2a2", "blocked": "#e0453e",
}
# WeKan stores label colors as named swatches — map the palette to hex so the
# frontend can do the alpha-suffix pill trick.
WEKAN_COLOR_HEX = {
    "green": "#4a9e3f", "yellow": "#e0b91e", "orange": "#e08a1e", "red": "#e0453e",
    "purple": "#9b51e0", "blue": "#2f80c4", "sky": "#17a2a2", "lime": "#4a9e3f",
    "pink": "#d1477f", "black": "#4d4d4d", "silver": "#8b929c", "peachpuff": "#e0a06b",
    "crimson": "#d13b46", "plum": "#9b51e0", "darkgreen": "#2f6e2a", "slateblue": "#6b7280",
    "magenta": "#d1477f", "gold": "#b8912a", "navy": "#274b78", "gray": "#6b7280",
    "saddlebrown": "#8b5a2b", "paleturquoise": "#17a2a2", "mistyrose": "#d1477f",
    "indigo": "#5a3db0",
}


def _safe_hex(value: str) -> str:
    v = (value or "").strip()
    if v.startswith("#") and all(ch in "0123456789abcdefABCDEF" for ch in v[1:]) and len(v) in (4, 7, 9):
        return v
    return "#6b7280"


def _label_hex(name: str, color: str) -> str:
    n = (name or "").lower()
    if n in HANDOFF_LABEL_HEX:
        return HANDOFF_LABEL_HEX[n]
    c = (color or "").lower()
    if c.startswith("#"):
        return _safe_hex(color)
    return WEKAN_COLOR_HEX.get(c, "#6b7280")


def _fmt_day(iso: object) -> str:
    ts = _parse_ts(iso)
    if ts is None:
        return "—"
    return datetime.utcfromtimestamp(ts).strftime("%b %d")


def _fmt_age(seconds: float) -> str:
    s = int(seconds)
    return f"{s // 86400}d {(s % 86400) // 3600:02d}h"


async def _pick_board(wk: "WekanClient") -> dict | None:
    boards = await wk.boards()
    if not boards:
        return None
    if WEKAN_BOARD_ID:
        for b in boards:
            if b.get("_id") == WEKAN_BOARD_ID:
                return b
    return boards[0]


@app.get("/ui")
async def ui_board_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "board.html")


@app.get("/ui/dashboard")
async def ui_dashboard_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/ui/settings")
async def ui_settings_page() -> FileResponse:
    return FileResponse(STATIC_DIR / "settings.html")


@app.get("/ui/api/board")
async def ui_api_board() -> JSONResponse:
    assert _client is not None
    wk = WekanClient(_client)
    await wk.login()
    board = await _pick_board(wk)
    if board is None:
        return JSONResponse({"title": "No board", "boardId": None, "cardCount": 0, "columns": [], "blockedText": None})

    board_id = board["_id"]
    detail = await wk.board_detail(board_id)
    label_map = {
        lab["_id"]: {"name": lab.get("name", ""), "color": _label_hex(lab.get("name", ""), lab.get("color", ""))}
        for lab in (detail.get("labels") or [])
    }
    blocked_ids = {
        lab["_id"] for lab in (detail.get("labels") or [])
        if str(lab.get("name", "")).lower() == WEKAN_BLOCKED_LABEL
    }

    now = time.time()
    oldest_blocked: float | None = None
    lists = await wk.lists(board_id)
    ordered = sorted(lists, key=lambda l: COLUMN_ORDER.index(l["title"]) if l.get("title") in COLUMN_ORDER else 99)

    columns = []
    total = 0
    for lst in ordered:
        list_id = lst["_id"]
        name = lst.get("title", list_id)
        cards = []
        for stub in await wk.cards(board_id, list_id):
            card = await wk.card(board_id, list_id, stub["_id"])
            lids = card.get("labelIds") or []
            labels = [label_map[lid] for lid in lids if lid in label_map]
            blocked = bool(blocked_ids & set(lids))
            if blocked:
                anchor = _parse_ts(card.get("dateLastActivity")) or _parse_ts(card.get("createdAt"))
                if anchor is not None and (oldest_blocked is None or anchor < oldest_blocked):
                    oldest_blocked = anchor
            cards.append({
                "id": card["_id"],
                "title": card.get("title", ""),
                "desc": card.get("description", "") or "",
                "labels": labels,
                "start": _fmt_day(card.get("startAt")),
                "end": _fmt_day(card.get("endAt")),
                "blocked": blocked,
            })
        columns.append({"name": name, "listId": list_id, "count": len(cards), "done": name == "Done", "cards": cards})
        total += len(cards)

    return JSONResponse({
        "title": board.get("title", board_id),
        "boardId": board_id,
        "cardCount": total,
        "columns": columns,
        "blockedText": _fmt_age(now - oldest_blocked) if oldest_blocked is not None else None,
    })


@app.put("/ui/api/move")
async def ui_api_move(request: Request) -> JSONResponse:
    assert _client is not None
    body = await request.json()
    board_id, from_list, to_list, card_id = body.get("board"), body.get("fromList"), body.get("toList"), body.get("card")
    if not all([board_id, from_list, to_list, card_id]):
        return JSONResponse({"ok": False, "error": "missing ids"}, status_code=400)
    wk = WekanClient(_client)
    await wk.login()
    r = await _client.put(
        f"/api/boards/{board_id}/lists/{from_list}/cards/{card_id}",
        json={"listId": to_list},
        headers=wk._headers(),
    )
    return JSONResponse({"ok": r.status_code < 300, "status": r.status_code})


async def _prom_range(c: httpx.AsyncClient, query: str, minutes: int = 1440, step: int = 300) -> list[dict]:
    now = time.time()
    r = await c.get(
        f"{PROM_URL}/api/v1/query_range",
        params={"query": query, "start": now - minutes * 60, "end": now, "step": step},
    )
    r.raise_for_status()
    return r.json().get("data", {}).get("result", [])


def _series_vals(result: list[dict]) -> list[float]:
    if not result:
        return []
    return [float(v[1]) for v in result[0].get("values", [])]


@app.get("/ui/api/flow")
async def ui_api_flow() -> JSONResponse:
    async with httpx.AsyncClient(timeout=15.0) as c:
        wip = await _prom_range(c, "pm_wip")
        cyc = await _prom_range(c, "pm_cycle_time_seconds")
        tpt = await _prom_range(c, "pm_throughput_cards")
        blk = await _prom_range(c, "pm_blocked_age_seconds")

    col_vals: dict[str, list[float]] = {}
    for s in wip:
        col = s.get("metric", {}).get("column")
        if col:
            col_vals[col] = [float(v[1]) for v in s.get("values", [])]

    cfd_series = [
        {"name": n, "color": COLUMN_COLOR.get(n, "#6b7280"), "v": [round(x) for x in col_vals.get(n, [0])] or [0]}
        for n in CFD_ORDER
    ]
    last = {n: (col_vals.get(n, [0]) or [0])[-1] for n in COLUMN_ORDER}

    cyc_vals = _series_vals(cyc)
    cyc_last = cyc_vals[-1] if cyc_vals else 0.0
    tpt_vals = [round(x) for x in _series_vals(tpt)]
    tpt_last = tpt_vals[-1] if tpt_vals else 0
    blk_vals = _series_vals(blk)
    blk_last = blk_vals[-1] if blk_vals else 0.0

    return JSONResponse({
        "cfd": {"series": cfd_series, "last": {k: round(v) for k, v in last.items()}},
        "cycle": {
            "valueText": f"{cyc_last / 86400:.2f} d",
            "secondsText": f"{int(cyc_last):,} s",
            "v": cyc_vals or [0],
        },
        "tput": {"value": tpt_last, "v": tpt_vals or [0]},
        "blocked": {
            "text": _fmt_age(blk_last),
            "seconds": int(blk_last),
            "secondsText": f"{int(blk_last):,} s",
            "v": blk_vals or [0],
        },
    })
