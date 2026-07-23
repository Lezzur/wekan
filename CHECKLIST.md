# WeKan PM-layer — Original Scope Checklist

Living list of everything originally scoped, including items bypassed/deferred.
Nothing here is deleted just because we skipped it — if a need surfaces, it gets picked back up.

Legend: [x] done & verified · [~] partial/stubbed · [ ] not started · [cut] intentionally dropped (revivable)

## Core v1 (the lean spine)
- [x] WeKan + MongoDB — board + datastore, live on worker4080:8090
- [x] pm-bridge FastAPI spine — routes `/healthz` `/metrics` `/wekan` `/alert` respond
- [x] Prometheus — scraping pm-bridge, verified `health=up`
- [x] Alertmanager — config wired to pm-bridge `/alert`
- [x] Grafana — running on :3000 (no dashboards yet — see S1/S2)
- [~] Alert dedup — works single-node (asyncio lock + fingerprint set)

## In flight
- [x] **S1** — WeKan REST client in the flow loop: populates `pm_wip`, `pm_cycle_time_seconds`, `pm_blocked_age_seconds`, `pm_throughput_cards` from the live board (owner: Sanchez). Merged to `main` (b3d2947). Verified live against the seeded demo board: WIP 5/4/3/6 exact, throughput 6, mean cycle 244800s (2.83d), blocked-age ~3d. Fixed the label bug (cards carry `labelIds`; names live on the board doc — resolve via `board_detail`). Service login `pm-bridge` (least-priv) wired via `WEKAN_USER`/`WEKAN_PASS`.
- [x] **S2** — Grafana dashboards: CFD + cycle-time + throughput + blocked-age dashboard provisioned as code (`grafana/provisioning` + `grafana/dashboards/flow-metrics.json`), verified rendering live data (not just loaded).
- [ ] WeKan REST board mutation — `/wekan` translate board events; `/alert` create Triage cards

## Bypassed / deferred (revivable)
- [cut] **Mattermost (ChatOps)** — bypassed in favor of Slack incoming webhook. Env is provider-agnostic (`CHATOPS_WEBHOOK`). Revive path: if a self-hosted chat sink is ever wanted, drop Mattermost + its Postgres back into compose and point the webhook at it. Nothing in the code assumes Slack specifically.
- [cut] **Redis** — not needed at single-node scale. Revive path: needed only if alert dedup must survive a restart or go multi-node (move `_seen_fingerprints` into Redis).
- [cut] **Uptime-Kuma** — external uptime pinger. Revive path: add as its own container if we want black-box "is it up" checks separate from Prometheus.
- [cut] **bge-m3 / vector store** — semantic search over cards. Deferred; note a `/data/scientific-rag` dir already exists on the 4080 (possible home if revived).

## Deploy hardening (not v1, gates nothing yet)
- [ ] Egress control — Squid **domain** allowlist (NOT IP; outbound targets are cloud CDNs with rotating IPs)
- [ ] Secrets — `.env` off-repo (already gitignored), rotate any tokens
- [ ] Backups — restic on the data volumes (mongo-data, grafana-data, prometheus-data)

## UI (new ask — 2 visual layers on live backends)
- [x] Layer 1 — **Flow metrics UI** (Grafana): CFD + cycle time + throughput + blocked-age, backed by Prometheus ← pm-bridge. Live at `:3000/d/pm-flow`, all four panels rendering real data from the seeded board.
- [x] Layer 2 — **Board UI** (WeKan): the Kanban board itself, backed by Mongo — live on worker4080:8090. Admin `rockadmin` registered; least-priv `pm-bridge` service user minted; demo board `ECYETuFPdCyRSoHCP` seeded (5/4/3/6 across To Do/In Progress/Review/Done).

## Design handoff — "Soft Clinical" (#1d board + #2b dashboard)
- [x] **Board (#1d)** — pixel-faithful Soft Clinical Kanban served by pm-bridge at `:8000/ui`, wired to live WeKan REST (`/ui/api/board`). Rounded cards, 4 columns (5/4/3/6), green Done column w/ ✓ glyphs, blocked-label pulse banner. Drag-to-move (`PUT /ui/api/move` → WeKan) + card-detail modal. `prefers-reduced-motion` honored. Verified live: WIP 5/4/3/6 exact, blocked card detected.
- [x] **Dashboard (#2b)** — Soft Clinical re-skin of PM Flow — Honest Metrics at `:8000/ui/dashboard`, wired to Prometheus (`/ui/api/flow`). CFD stacked area + mean cycle + throughput + oldest-blocked, SVG builders ported from the mock, 30s refresh, threshold coloring. Verified live: cycle 2.83 d / 244,800 s, throughput 6, blocked 3d 05h. No velocity/burndown/leaderboards.
- Vanilla HTML/CSS/JS (no build toolchain) served by the existing FastAPI spine — no new container. Design-mock content was sample data; screens render real board/Prometheus state.
- [x] **Numeral polish** — killed IBM Plex Mono's dotted zero on `.mono` (`font-feature-settings: "zero" 0`) across board + dashboard + settings, so figures like `0.00 d` / `05h` read as plain zeros (Rick's ask).
- [x] **Settings page + Live toggle** — `:8000/ui/settings` with a "Live" switch. On → dashboard reads real data (`/ui/api/flow`); Off → renders a built-in smooth `DUMMY` dataset so the layout demos without live board activity. State persists per-browser in `localStorage.pmLive` (default on); header shows a LIVE/DEMO mode chip. Verified both modes live.
