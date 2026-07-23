# WeKan PM-layer

Lean v1 scaffold. **Skeleton only — nothing here computes real metrics yet.**

## Stack (v1 core)
- **WeKan + MongoDB** — the board and its datastore
- **pm-bridge** — FastAPI spine: webhook router + flow-metrics exporter (`pm-bridge/`)
- **Prometheus + Alertmanager** — time-series store + alert routing
- **Grafana** — flow dashboards (CFD, cycle time)

Deferred / out of v1: Redis, Mattermost (ChatOps), Uptime-Kuma, bge-m3.
See `../light/wekan/discovery/GLOSSARY-WeKan-Stack.md` for the full dissection.

## Run
```bash
docker compose up --build
```
- WeKan       → http://localhost:8080
- pm-bridge   → http://localhost:8000/healthz  /metrics
- Prometheus  → http://localhost:9090
- Alertmanager→ http://localhost:9093
- Grafana     → http://localhost:3000 (admin/admin)

## Status — what's real vs stubbed
| Piece | State |
|---|---|
| Routes (`/wekan`, `/alert`, `/metrics`, `/healthz`) | respond |
| Alert dedup (fingerprint lock) | works, single-node |
| Flow-metrics loop | loops, computes nothing yet (TODO) |
| WeKan REST board mutation | not wired (TODO) |
| ChatOps notify | gated behind `MATTERMOST_WEBHOOK` (decision deferred) |

## Open decisions (module-level, not blocking)
1. Mattermost — org-existing or fresh? Gates the ChatOps leaf module only.
2. Egress — domain (Squid) vs IP (nftables/Tailscale ACLs). Deploy-time.
3. Repo hosting — where this lands long-term.
