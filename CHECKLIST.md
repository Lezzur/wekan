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
- [ ] **S1** — WeKan REST client in the flow loop: populate `pm_wip`, `pm_cycle_time_seconds`, `pm_blocked_age_seconds`, `pm_throughput_cards` from the live board (owner: Sanchez, ETA 2026-07-24)
- [ ] **S2** — Grafana dashboards: CFD + cycle-time views on the real gauges
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
- [ ] Layer 1 — **Flow metrics UI** (Grafana): CFD + cycle time, backed by Prometheus ← pm-bridge
- [ ] Layer 2 — **Board UI** (WeKan): the Kanban board itself, backed by Mongo
