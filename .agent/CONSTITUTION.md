# Constitution: platform-api

## agent-hub retired (2026-08-16)

This repo was previously coordinated through `agent-hub`. That system was
retired on 2026-08-16: the server is gone, the MCP registration removed, and
`~/.agent-hub` (task database, handoffs, blockers) deleted. Do not look for
hub tasks, do not attempt to register/heartbeat, and do not call
`mcp__agent-hub__*` tools — they no longer exist. `hub#<id>` references in
docs and commit history are historical breadcrumbs, not live tickets.
Current state and open work live in this repo (constitution/roadmap/CHANGELOG).

> A living document. Edit deliberately and explain the reason in commit messages.
> Workers re-read this on every `/clear` between tasks; it is the north star
> when no task has been dispatched and self-driven work needs to be chosen.

## Mission

The application platform API — HTTP CRUD over the durable model (users, content,
documents, typed documents, pipelines, ingest/ETL, social, video sessions).
Owns the outbox path for durable event publishing; embeds the
`distributed-core` PipelineModule for in-process pipeline execution today.

## Scope

**This project IS:**
- An Express TypeScript HTTP service on port `3001`.
- The persistence layer for all platform business state (DynamoDB-backed
  repositories).
- The outbox writer — atomically writes durable events alongside state
  mutations; a drainer service publishes to message-queue.
- The host (for now) of the pipeline executor — embeds a single-node
  `distributed-core` Raft cluster + `PipelineModule`.
- The orchestrator of video sessions — composes `live-video-streaming`'s
  channel + token primitives into application-level "sessions" tied to
  documents.

**This project IS NOT:**
- A WebSocket gateway. Real-time delivery is `websocket-gateway`'s job;
  platform-api publishes durable events and signal envelopes that the
  gateway fans out.
- A media service. Video sessions are application-level metadata over the
  SFU; the SFU itself is `live-video-streaming`.
- A library. Cross-repo abstractions belong in `distributed-core` or
  `event-catalog`. If you're tempted to copy a function from there and
  modify it locally, STOP and file a task for the upstream repo.
- A multi-tenant API. Single-tenant invariants are load-bearing.

## Platform context (added 2026-05-13)

The 2026-05-13 audit established platform-api as one of seven long-running
services in the target topology. Two of platform-api's current responsibilities
are explicitly slated for extraction at scale:

- **Pipeline executor → `pipeline-worker`** (M4 milestone). Today's
  `src/pipeline/bootstrap.ts` stands up an in-process `distributed-core`
  Cluster with the PipelineExecutor and Raft state machine running co-resident
  with the HTTP server. At 10x scale, a Raft snapshot rebuild (seconds) shares
  a process with HTTP routes (200ms p99 budget). Extract to a separate
  deployable when M4's trigger fires (HTTP p99 incidents observed under
  pipeline load, OR multi-node executor required for crash isolation).

- **Ingest engine source-of-truth → message-queue** (M2 milestone). Today
  `src/ingest/bootstrap.ts` uses an in-process `InMemoryBatchedQueue`; items
  arriving moments before a crash are lost. Flip the source-of-truth to
  message-queue (POST to `/api/v1/queues/ingest-items/messages`); the
  in-process queue stays as a batching buffer. Crash-isolation win.

These are described in `ROADMAP.md`. Do not pre-build for them.

The four platform milestones (M1–M4) and the 7-service target topology are
documented in audit memory files at
`~/.claude/projects/-Users-connorhoehn-Projects-websocket-gateway/memory/`:
`platform-target-topology.md`, `platform-milestones-m1-m4.md`,
`event-catalog-phases-e1-e5.md`, `critical-drift-hazards.md`.

## Principles (non-negotiable)

1. **Outbox or bust.** Every durable event is written atomically with its
   state mutation (`TransactWriteCommand`). Fire-and-forget direct publishes
   are forbidden in mutation paths.
2. **Repositories are singletons.** Exported from `src/repositories/index.ts`.
   Routes call repositories; repositories own DDB access. No DDB SDK calls
   in route handlers.
3. **TypeScript strict mode.** No `any` in committed code without a recorded
   reason in a comment.
4. **Event-catalog is the source of truth for event schemas.** Every cross-
   service event is declared in `src/lib/event-catalog.ts` (or another module
   that calls `catalog.declare()`); the declaration carries the JSON Schema.
   Today this is partially in place; M1 adoption is the priority.
5. **No premature multi-node.** The platform's "stay single-node until
   trigger" posture (established in `websocket-gateway/.agent/CONSTITUTION.md`)
   applies here too. Don't pre-build pipeline-worker; wait for M4 trigger.
6. **Linear history.** Rebase, don't merge. Commit small + atomic. Reference
   hub task ids.

## Anti-goals

- **Don't recreate distributed-core abstractions.** No "we just need something
  simpler" forks of Raft / Cluster / Queue. File a task for distributed-core
  instead.
- **Don't add WebSocket endpoints.** Real-time delivery is `websocket-gateway`.
  This service is HTTP only.
- **Don't bridge TypedDocument ↔ CRDT Document.** Phase 51 Phase F (in
  websocket-gateway) chose "parallel systems, no bridge" deliberately.
  Respect the decision.
- **Don't make synchronous HTTP calls to `live-video-streaming` in mutation
  paths.** Today `src/routes/videoSessions.ts:55,91,165` does this. Move to
  durable events when M4 + event-catalog adoption permits.
- **Don't deploy to AWS / K8s.** CI is intentionally disabled; cloud apply
  is operator-only.

## Current phase

**Phase 4-A: PipelineModule integrated.** Single-node in-process Cluster,
ApplicationModuleContext, PipelineBridge contract routes. 155 tests
(+23 from Phase 0–1). See `.planning/HANDOFF.md` for the extraction history.

**Next planned phase: M1 adoption (event-catalog).** See `ROADMAP.md`.

## Self-driven backlog (in priority order, ranked)

When the dispatched queue is empty AND no unread handoffs are open, draw from
these ranked items. Each item must still satisfy the "Good-enhancement
criteria" below.

1. **Leader-elect `scheduleEvaluator` via `DistributedLock`** (~80 LOC).
   Today `src/services/scheduleEvaluator.ts:158-174` is a process-singleton
   `setInterval` that fires cron pipelines — 2 replicas would fire each
   scheduled pipeline twice. The `outbox-drainer.ts:99,224` pattern (DDB
   conditional lease) is the working precedent. Or adopt `distributed-core`'s
   `DistributedLock`. Unblocks safe 2-replica platform-api.
2. **Channel-message envelope migration to event-catalog shared type**
   (depends on event-catalog Phase E2.1). Today `src/lib/event-catalog.ts:443`
   hand-rolls the `{type:'channel_message', channel, message, ...}` envelope;
   gateway parses it. Replace with import from `@connorhoehn/event-catalog`
   once that ship.
3. **Discriminator declaration for `ingest:progress`** (depends on event-catalog
   Phase E3.6). Today the envelope carries 10 sub-types via `payload.type`
   (`src/ingest/types.ts:253-262`); the registry sees one schema. Declare
   `discriminator: { field: 'type', values: [...] }` to enable per-sub-type
   validation.
4. **Reconcile `pipeline.run.completed` dual-source.** Today distributed-core's
   `PipelineExecutor` emits the event AND `src/pipeline/bootstrap.ts:745`
   re-publishes it durably with a different payload. Pick one canonical
   source; the other becomes a consumer. Coordinate with distributed-core
   agent if the executor side needs to change.
5. **`platform-api/.agent/CONSTITUTION.md` first review at 5 done events.**
   Re-read this file + last 5 done summaries; propose edits via handoff if
   the document drifts from reality.

The list IS NOT exhaustive. Refile when a top item ships.

## Good-enhancement criteria

A self-driven task is worth claiming if it satisfies AT LEAST ONE of:

- **Hardens an outbox surface** (outbox writes, drainer reliability, idempotency).
- **Adopts event-catalog** for an event that's currently published ad-hoc.
- **Removes a synchronous coupling** to a sibling service (e.g., `VIDEO_API_URL`
  fetches that should be durable events).
- **Closes a drift hazard** documented in audit memory `critical-drift-hazards.md`.
- **Adds an integration test** for a wire-up with only unit-level coverage.
- **Improves operator visibility** (a metric, a log, a dashboard panel).

A self-driven task is **not** worth claiming if it:

- Pre-builds for multi-node (M4 trigger hasn't fired).
- Adds a new external API without an existing consumer.
- Refactors a green code path with no measured benefit.
- Spends more than ~200 LOC. Larger → file a blocker for operator scope review.

## User-facing framing

"User" for platform-api means three personas:

- **Frontend client agent** (the React SPA consuming the HTTP API):
  "Frontend can now query `/api/topology` for the set of pipeline events
  it needs to handle, instead of hardcoding the list."
- **Operator** (you, on-call):
  "Operators can see which schedule was duplicated when 2 replicas ran the
  same cron, because the duplicate-fire incident is now impossible
  (DistributedLock prevents it)."
- **Sibling-service agent** (gateway, event-catalog, message-queue):
  "Gateway no longer hand-rolls the `channel_message` envelope; it imports
  the shared type from event-catalog. Schema drift is now a compile error."

**Good user-impact statements:**
- "Schedule pipelines can now run with 2 replicas of platform-api without
  duplicate firings — the leader election handles failover transparently."
- "Ingest events from a partial-crash scenario survive because message-queue
  is the source of truth, not the in-process buffer."

**Bad user-impact statements (REJECTED):**
- "Adopts T1 envelope shared type." (Names the type, not the capability.)
- "Improves outbox reliability." (Vague.)

## Affected consumers

- **websocket-gateway** — subscribes to platform-api's Redis pub/sub channels
  (`websocket:route:{channelId}`, `dcore:topic:pipeline.events.*`, `ingest:events`,
  `social:*`, `doc:*`). Any envelope-shape change requires a coordinated
  bump.
- **frontend SPA (inside websocket-gateway repo)** — consumes ~180 HTTP
  endpoints across `/api/*` routes. Type contracts mirrored in
  `frontend/src/types/` — drift risk; track via event-catalog adoption.
- **event-catalog server** — receives schema declarations via
  `syncToRemoteRegistry`. Today only platform-api participates; M1 priority
  is to keep doing so.
- **message-queue** — receives durable events via outbox-drainer. Per-queue
  config (retention, FIFO, partition key) is currently a convention; event-
  catalog Phase E3 makes it declared.
- **live-video-streaming** — accessed via three HTTP calls from
  `src/routes/videoSessions.ts`. **Coordinate via handoff** — that repo is
  owned by another agent.

## Cross-repo contracts this project depends on

- **`distributed-core`** — pinned to `github:connorhoehn/distributed-core#distributed-core-v0.37.2`.
  Imports `PipelineModule`, `Cluster`, `RaftStateMachineBase`,
  `InMemoryDeadLetterQueue`, `MetricsRegistry`, `HintedHandoffQueue`,
  `InMemoryDedupStore`, `DEFAULT_RAFT_CONFIG`, `FixtureLLMClient`,
  `RetryManager`. ~46 imports.
- **`@connorhoehn/event-catalog`** — pinned to `github:connorhoehnslalom/event-catalog#v0.3.48`.
  Imports `EventCatalog`, `EventEnvelope`. ~6 imports. Single consumer of
  this library today.
- **`@connorhoehn/service-runtime`** — pinned to `github:connorhoehn/service-runtime#v0.2.4`.
  Imports HMAC `signEnvelope`/`verifyEnvelope`, `createLogger`,
  `makeHealthHandler`, `requestContextMiddleware`, lifecycle helpers.
- **`@connorhoehn/pipeline-runtime`** — pinned to `github:connorhoehn/pipeline-runtime#v0.4.0`.
  Imports `HTTPPipelineBridge`, `PipelineApprovalStateMachine`,
  `ApprovalStore`, canonical pipeline contracts.
- **Docker build auth**: image uses BuildKit SSH agent forwarding
  (`--ssh default`) so `npm install` can fetch the four github: pins
  from private repos via `RUN --mount=type=ssh`. Aligned with the
  websocket-gateway Tilt detox (gateway commit 6783f89 / platform-api
  commit e9ccbb5). See `Dockerfile` header for the build invocation.
- **DDB table prefix**: `DDB_TABLE_PREFIX` env var to namespace tables in
  shared-services mode. Helper at `src/lib/ddb-table-name.ts:9` (duplicated
  in gateway — known drift, low priority to unify).
- **Redis envelope shape**: hand-rolled `channel_message` at
  `src/lib/event-catalog.ts:443`. Target: import from event-catalog (M1).

## Workflow conventions

- **Solo project, no PRs.** Stage, commit, push to `main` directly.
- **No destructive ops without explicit ask.**
- **TypeScript strict mode.** Jest for tests; coverage 50%+ source/test ratio.
- **Repositories are singletons.** Exported from `src/repositories/index.ts`.

## Daily cap

**Default: up to 30 self-driven tasks per UTC day per agent session.**
Tracked via the hub: count `task.done` activity rows where `agent_name=self`
AND `detail` contains `[self-driven]` over the last 24 hours.

**Operator override:** if `$AGENT_HUB_ROOT/.budget-platform-api` exists, read
its integer as today's cap. Read every `/clear` cycle.

When the cap is reached:
1. `append_log("daily cap reached at <N>; idling until next UTC day")`.
2. Optionally file a "daily summary" handoff to operator.
3. `/clear` and stop.

## Constitution review cadence

After every **5 self-driven `task.done` events**, run a constitution review:
1. Read this file.
2. Read the last 5 self-driven done summaries from the hub.
3. Decide: does the constitution still describe what's being built?
4. If YES → log it and continue.
5. If NO → propose an edit via a handoff to operator. No silent edits.

## Kill-switch

If `$AGENT_HUB_ROOT/.no_self_driven` exists, do NOT enter the self-generation
step. Only execute dispatched work. Idle when the dispatched queue is empty.
