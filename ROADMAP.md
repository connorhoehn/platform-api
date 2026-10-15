# platform-api Roadmap

> Drafted 2026-05-13 alongside the 6-project audit that established the M1–M4
> platform milestones. That audit lived in one operator's local agent memory
> under the retired websocket-gateway repo (now realtime-examples) and is not
> part of this tree; this file is the surviving record.

## Charter (one paragraph)

The application platform API. Owns HTTP CRUD over all business state
(documents, typed documents, pipelines, social, video sessions)
backed by DynamoDB. Owns the **outbox path** — durable events are written
atomically alongside state mutations and drained to `message-queue` for
at-least-once delivery. Embeds the `distributed-core` PipelineModule for
in-process pipeline execution today; that responsibility is slated for
extraction at M4 scale. Composes `live-video-streaming` channel + token
primitives into application-level video "sessions" tied to documents.

## Platform context

platform-api is one of seven services in the platform target topology
(drafted in the same 2026-05-13 audit; the realtime gateway it feeds is now
the realtime-examples gateway on realtime-modules, formerly websocket-gateway).
Two of today's responsibilities are extraction candidates at scale:

| Today | Future | Milestone | Trigger |
|---|---|---|---|
| Embeds pipeline executor (Raft SM, in-process Cluster) | `pipeline-worker` (separate deployable) | M4 | HTTP p99 incidents under pipeline load, OR multi-node executor for crash isolation |

**Do not pre-build these splits.** Wait for trigger.

## Reality check (today)

What's working:

- ✅ Express HTTP service on `:3001` with 180 endpoints across 48 route modules.
- ✅ 14 DDB-backed repository singletons.
- ✅ Outbox table (`social-outbox`) with TransactWrite atomicity for social
  events. Outbox drainer at `src/services/outbox-drainer.ts` uses a DDB
  conditional lease — already multi-node safe.
- ✅ Distributed-core PipelineModule embedded for pipeline execution
  (`src/pipeline/bootstrap.ts`).
- ✅ Video session composition over live-video-streaming primitives.
- ✅ Cognito JWT auth with SKIP_AUTH bypass for local dev (`src/middleware/auth.ts`).
- ✅ Schedule evaluator for cron-triggered pipelines.
- ✅ OpenTelemetry tracing with OTLP HTTP exporter (`src/tracing.ts`).
- ✅ pino structured logger + Prometheus metrics endpoint.
- ✅ 173 test files (50%+ test/source ratio).

What's not:

- ✅ (2026-10-14) Schedule evaluator is single-flight across replicas on a
  fenced lease plus a per-fire claim (P2.1); it was a per-tick lock that two
  offset replicas could both win in the same minute.
- ⚠ `publishSignal()` in `src/lib/event-catalog.ts:430` bypasses the event-
  catalog library and writes directly to Redis — schemas aren't validated on
  the WS-bound path.
- ⚠ `pipeline.run.completed` has two producers: distributed-core's
  `PipelineExecutor` (colon-form, dual-emit dot-form) AND `bootstrap.ts:745`
  (durable, different payload). Drift hazard.
- ⚠ `src/routes/videoSessions.ts:55,91,165` makes synchronous HTTP calls
  to `VIDEO_API_URL` — un-circuit-broken, un-retried, un-observed. First
  outage bites.
- ⚠ Pipeline definitions cache (`src/pipeline/definitions-cache.ts:65`)
  polls DDB every 60s; webhook lookup is per-replica. Newly-created pipelines
  take up to 60s to propagate across replicas.
- ⚠ OpenAPI spec at `openapi/pipelines.yaml` covers only pipelines (not
  documents, social, video). Incomplete.

## Roadmap

### Phase P1 — `.agent/CONSTITUTION.md` + ROADMAP (1 day)

**Goal:** platform-api has stated direction matching the platform audit.

- [x] Write `.agent/CONSTITUTION.md` (charter, anti-goals, platform context,
  self-driven backlog).
- [x] Write `ROADMAP.md` (this file).
- [x] Update `CLAUDE.md` to reference the new docs.

### Phase P2 — Multi-replica safety (1–2 weeks)

**Goal:** platform-api can run with 2 replicas behind a load balancer without
duplicate work. **The cheapest meaningful scaling win.**

- [x] **P2.1 — Leader-elect `scheduleEvaluator`.** Done 2026-10-14 (lane P1):
  `src/services/scheduler-fenced-leadership.ts` holds a distributed-core
  `FencedLease` (renewed every 10 s, 30 s lease) and claims every fire with an
  atomic "still my lease + first fire of this minute" script, so two replicas
  or a mid-minute failover never double-fire. Contracts:
  `scheduleEvaluator.fenced-leadership.test.ts` (fake Redis) and the opt-in
  real-Redis `scheduler-fenced-leadership.redis.test.ts`.
- [x] **P2.2 — Pipeline definitions cache invalidation.** Done 2026-10-14
  (lane P1): every definition write (including document automations)
  publishes on `platform-api:pipeline-definitions:invalidate`; while that
  subscription is live the cache's DynamoDB Scan drops from 60 s to a 15-minute
  safety poll, a dropped subscription restores the 60 s poll, and the
  reconnect refreshes once. Contracts: `definitions-cache.event-driven.test.ts`.
- [ ] **P2.3 — Idempotency cache: require Redis in prod.** Today
  `src/middleware/idempotency.ts:135` falls back to in-memory `memFallback`
  when Redis is unavailable. Add a startup check that fails fast in prod
  if `REDIS_REQUIRED=true` and Redis isn't reachable.
- [ ] **P2.4 — Rate limit store: same.** `src/middleware/rateLimit.ts:55`
  has the same in-memory fallback. Same fix.
- [ ] **P2.5 — Integration test for 2-replica scenario.** Spin up two
  platform-api processes against a shared LocalStack + Redis; trigger a
  cron-scheduled pipeline; assert exactly one run was created.

**Success criteria:**
- Two replicas can be running for ≥1 hour without any duplicate cron firings,
  duplicate webhook fan-outs, or duplicate outbox-drainer leases.

### Phase P3 — M1 adoption (event-catalog) (2–3 weeks)

**Goal:** Every cross-service event platform-api emits has a declaration in
event-catalog; the `channel_message` envelope is the shared type.

Depends on event-catalog Phases E2.1 (envelope shared type), E3 (transport:
'both', discriminator, aliases).

- [ ] **P3.1 — Import shared `channel_message` envelope.** Replace inline
  construction at `src/lib/event-catalog.ts:443` with import from
  `@connorhoehn/event-catalog`. Add unit test that round-trips through Redis.
- [ ] **P3.3 — Reconcile `pipeline.run.completed`.** Coordinate with
  distributed-core agent: either the PipelineExecutor stops dual-emitting
  AND platform-api stays the durable producer, OR they get a shared
  `EventDeclaration` with `producer: ['distributed-core', 'platform-api']`
  and aliases. Pick a path; ship it.
- [ ] **P3.4 — Strict-mode `declare()` in prod.** Set `EVENT_CATALOG_STRICT=true`
  in production env. Validates that schema changes don't ship without going
  through compat checks.
- [ ] **P3.5 — Make `publishSignal()` go through `catalog.publish()`.**
  Today it bypasses the library and writes directly to Redis. Route everything
  through the library so schemas are actually enforced on the WS-bound path.
  Performance check: must stay below 1ms p99 per publish.

**Success criteria:**
- `GET /api/topology?producer=platform-api` (from event-catalog) returns the
  full set of platform-api-declared events.
- Strict-mode compat checking is on in prod for ≥1 week with zero violations.
- `grep -r 'channel_message' src/` finds only imports.

### Phase P5 — Video session events become durable (2 weeks)

**Goal:** Stop the synchronous HTTP coupling to live-video-streaming in
mutation paths. Replace with durable events.

Depends on live-video-streaming exposing `/sessions` contract (M4, owned by
LVS agent — **coordinate via handoff**).

- [ ] **P5.1 — Declare `video.channel.requested` (command) and
  `video.channel.created`, `video.session.started`, `video.session.ended`
  (events)** in event-catalog. The channel-lifecycle events should already
  exist on the LVS side as webhooks — collaborate to make those declarations
  authoritative.
- [ ] **P5.2 — Move channel creation from synchronous HTTP to durable command.**
  `src/routes/videoSessions.ts:91` POSTs to LVS; replace with publish of
  `video.channel.requested`. LVS subscribes, creates channel, publishes
  `video.channel.created`. platform-api's session-creation route awaits the
  reply (correlation-id keyed) or returns a session-pending status.
- [ ] **P5.3 — Circuit breaker on remaining synchronous calls.** Token
  minting at `src/routes/videoSessions.ts:165` may need to stay synchronous
  (user is waiting to join). Wrap in `distributed-core` `CircuitBreaker`
  with a sane timeout (3s) and retry budget.
- [ ] **P5.4 — Webhook receiver for LVS events.** Confluence-style webhook
  endpoint that ingests LVS-side events and republishes them as platform
  events. Already declared in P5.1.

**Success criteria:**
- An LVS outage doesn't fail platform-api session-create requests; they
  queue and complete when LVS recovers.
- All video lifecycle events are visible in event-catalog's topology view.

### Phase P6 — M4 prep: pipeline-worker extraction (gated on trigger)

**Goal:** Pipeline executor + Raft SM + PipelineExecutor + PipelineRunRepository
+ run-related routes become a separate deployable.

**Trigger gate** (per constitution): proceed only when one of these fires:
- HTTP p99 incidents observed under pipeline load.
- Multi-node pipeline executor required for crash isolation.
- ≥ 2 platform-api replicas required for non-pipeline reasons (i.e., when
  M3 in gateway forces 2-replica everything).

- [ ] **P6.1 — Extract `src/pipeline/` into its own npm package + Dockerfile.**
  Shared types via event-catalog.
- [ ] **P6.2 — RPC contract: pipeline-worker exposes a minimal HTTP API**
  for "submit run" and "cancel run"; everything else flows through durable
  events.
- [ ] **P6.3 — platform-api routes (pipelineRetry, pipelineResume,
  pipelineDLQ, pipelineInspector) become thin proxies** to pipeline-worker
  via the contract.
- [ ] **P6.4 — Raft cluster: pipeline-worker boots with multi-node Raft
  by default** (today's default is in-memory transport).
- [ ] **P6.5 — Migration: live-cutover script.** Snapshot run state from
  platform-api's in-process Raft, restore in pipeline-worker, then flip
  the gateway/UI to talk to pipeline-worker.

**Success criteria:**
- HTTP p99 of platform-api is unchanged when pipeline-worker is under load
  (because they're separate processes).
- pipeline-worker can restart without dropping in-flight runs (durable run
  state).

### Phase P7 — OpenAPI completeness (1 week)

**Goal:** Generate documentation + SDK-friendly spec for the full HTTP surface.

- [ ] **P7.1 — Extend `openapi/` to cover documents, social, video
  domains.** Today only pipelines.yaml exists.
- [ ] **P7.2 — Auto-generate from route declarations.** Use a route-decoration
  pattern (express-openapi-validator or similar) so spec doesn't drift from
  code.
- [ ] **P7.3 — Publish spec via `/openapi.json` endpoint.** Bundle Swagger UI
  in dev only.

## Decisions made

| Decision | Why | When |
|---|---|---|
| Outbox is mandatory for durable events | Zero event loss; tested via v4.0 Phase 43 | Pre-existing |
| Repositories are singletons | Connection pooling; consistent error handling | Pre-existing |
| Pipeline executor embedded today, extracted at M4 | Constitution's "no premature multi-node"; trigger-gated | 2026-05-13 audit |
| TypedDocument and CRDT Document stay parallel | Phase 51 Phase F "Option B chosen, no bridge" | 2026-04-27 (gateway constitution) |
| Sync HTTP to LVS is tech debt; move to durable events | Outage-blast-radius reduction | This roadmap |
| event-catalog is the source of truth for event schemas | M1 platform milestone | 2026-05-13 audit |
| Don't add WebSocket endpoints to this service | Real-time is gateway's job | Pre-existing |

## Risks

- **R1: distributed-core pin drift.** This repo floats on `#main`; gateway
  pins `#v0.14.2`. M1/M2 work touches `RetryManager`, `CircuitBreaker`,
  `DistributedLock`, `MetricsRegistry` — all subject to v0.16.0 deprecation
  cull. **Mitigation:** coordinate with gateway agent to align pins before
  P2 ships.
- **R2: P3.5 (publishSignal through catalog) introduces latency.** Today
  it's a direct Redis publish; routing through library adds validation +
  envelope build. **Mitigation:** benchmark before and after; target <1ms
  p99 for in-process catalog publish.
- **R3: P4 migration race.** During cutover, items might exist in both the
  in-process queue AND message-queue. **Mitigation:** P4.4's drain-first
  script + dedup via item-id.
- **R4: P5 depends on LVS agent.** Cannot make progress without coordination.
  **Mitigation:** file handoff with proposed event declarations as soon as
  event-catalog Phase E3 ships; don't wait until P5 starts.
- **R5: P6 trigger ambiguity.** "HTTP p99 incidents" is vague — need a metric.
  **Mitigation:** before P6, define SLO: "platform-api HTTP p99 > 500ms for
  >5min during a pipeline run" → trigger fired.

## Out of scope

- Multi-tenancy. Single-tenant invariants are load-bearing.
- WebSocket endpoints. Real-time is gateway.
- A "platform-api SDK". OpenAPI spec is sufficient.
- Replacing DynamoDB. Repository abstraction allows swap, but no consumer
  pull signal.
- Auth migration to identity-server. Per CLAUDE.md, "future" — wait.
