# platform-api — agent context

## agent-hub retired (2026-08-16)

This repo was previously coordinated through `agent-hub`. That system was
retired on 2026-08-16: the server is gone, the MCP registration removed, and
`~/.agent-hub` (task database, handoffs, blockers) deleted. Do not look for
hub tasks, do not attempt to register/heartbeat, and do not call
`mcp__agent-hub__*` tools — they no longer exist. `hub#<id>` references in
docs and commit history are historical breadcrumbs, not live tickets.
Current state and open work live in this repo (constitution/roadmap/CHANGELOG).

Application platform API service. Manages users, content, documents,
pipelines, ingest/ETL, social features, and live video sessions. Sits
behind identity-server for authentication.

**Read `.agent/CONSTITUTION.md` first** — agent's north star and
backlog priorities. Read `ROADMAP.md` for Phase P1–P7 progression
(M1 event-catalog adoption, M2 ingest durable-backbone flip, M4
pipeline-worker extraction).

## Architecture

- **Express HTTP API** — all domain routes; single deployable.
- **Pipeline executor runs in-process** — pipeline-worker was absorbed
  back into platform-api on 2026-05-19. The HTTPPipelineBridge in
  `@connorhoehn/pipeline-runtime` is still the abstraction, but the
  bridge talks to the local in-process executor today, not a separate
  worker pod. M4 (re-extraction) is gated on the SLO defined in
  `.agent/SLO-M4-TRIGGER.md`.
- **distributed-core** — PipelineModule, EventBus, Cluster primitives,
  CircuitBreaker (used by the shared `src/lib/sfu-breaker.ts`), durable
  transport (consumed via `@connorhoehn/event-catalog`).
- **Outbox table is THE durability primitive.** `src/services/outbox-
  drainer.ts` is lease-coordinated (DDB `OUTBOX_LOCK#drainer` row) and
  publishes to the durable MQ via `catalog.publish`. Observability
  series at `/internal/metrics`: `outbox_published_total`,
  `outbox_publish_failures_total`, `outbox_lease_acquired_total`,
  `outbox_oldest_pending_seconds` (hub#1002).
- **Redis pub/sub** — publishes events consumed by websocket-gateway.
- **DynamoDB** — persistence layer.

## Key event channels (Redis pub/sub)

| Channel | Data |
|---------|------|
| `dcore:topic:pipeline.events.*` | Pipeline lifecycle events |
| `ingest:events` | Ingest progress events |

## Dependencies

- **`distributed-core`** — GitHub-pinned via SHA at `v0.39.0`
  (`github:connorhoehn/distributed-core#93e891f9d21a5ae3`, bumped from
  v0.38.1 in commit fba8a90). Imports route through the public subpath
  exports (`distributed-core/applications/pipeline`,
  `distributed-core/messaging-queue`, `distributed-core/transport`).
  Note: hub#971 (AbortSignal listener leak in the durable-transport
  poll loop) is blocked pending an upstream fix; see the handoff to
  distributed-core for the suspected root cause.
- **`@connorhoehn/event-catalog`** (`github:connorhoehnslalom/event-catalog#v0.3.48`) — durable + signal event registry
- **`@connorhoehn/pipeline-runtime`** (`github:connorhoehn/pipeline-runtime#v0.4.0`) — HTTPPipelineBridge (currently bridges to the in-process executor; see Architecture note above)
- **`@connorhoehn/service-runtime`** (`github:connorhoehn/service-runtime#v0.2.4`) — service-auth (`signEnvelope`/`verifyEnvelope`), `createLogger`, `makeHealthHandler`
- **`identity-server`** — OIDC provider (future auth migration from Cognito)

## Cross-cutting patterns

- **Shared SFU CircuitBreaker** — every outbound LVS call (videoSessions,
  recordings playback-token mint, future LVS proxies) goes through
  `callSfu` from `src/lib/sfu-breaker.ts`. One process-wide breaker
  state, so a sick LVS opens the circuit for every consuming route
  (hub#1001).
- **5xx structured logs** — the request-logger emits `errorMessage` /
  `errorName` / `errorStackTop` on >=500 responses, sourced from
  `res.locals.__loggedError` (stashed by the central errorHandler).
  Operators diagnose 5xx without cross-referencing a separate
  "unhandled error" entry (hub#972).
- **LVS webhook HMAC** — `/hooks/lvs/recording` requires
  `LVS_WEBHOOK_SECRET` in production. With the secret unset, the
  receiver rejects (401) unless `SKIP_AUTH=true` is set as an explicit
  dev opt-in (hub#1006).
- **M4 SLO trigger gate** — `.agent/SLO-M4-TRIGGER.md` declares the
  PromQL fire condition for the pipeline-worker re-extraction decision.
  Metrics: `pipeline_executor_busy_seconds_total` and
  `http_handler_duration_seconds{route_group=...}` (hub#1005).

## OpenAPI coverage

- `openapi/pipelines.yaml` covers the pipelines surface (~30 endpoints,
  OpenAPI 3.1 with full schemas).
- Documents / social / video prefixes have no OpenAPI yet — frontend
  mirrors types by hand under `frontend/src/types/`. hub#1009 (filing
  the three new YAML files + a drift-detection lint) is blocked pending
  operator scope decision; see blocker #27 for A/B/C/D options.

## Local dev

```bash
npm run dev
```

Requires Redis and DynamoDB-local running (shared services from agent-hub
docker-compose, or standalone).

## Conventions

- Solo project, no PRs. Stage, commit, push to `main` directly.
- TypeScript strict mode.
- Repositories are DynamoDB-backed singletons exported from `src/repositories/index.ts`.
- Agent coordination via the agent-hub MCP (back online 2026-05-30 after
  a brief outage). Cross-repo work goes through `create_handoff`; never
  modify a sibling repo directly.
