# Handoff — platform-api

## Current State

platform-api has been fully extracted from websocket-gateway/social-api into a standalone Express TypeScript service. All domain routes (documents, pipelines, ingest, social, video) are operational and the service runs independently on port 3001.

### What was just completed

- Extraction of all route handlers, repositories, services, and middleware from websocket-gateway into this standalone project
- Redis pub/sub event bus wiring so websocket-gateway can subscribe and fan out events without being in-process
- Video session routes composing live-video-streaming primitives into app-level sessions
- Pipeline module bootstrap with distributed-core (PipelineModule, Raft, WAL, DLQ persistence)
- Ingest engine with Redis-backed event bus, adapter framework (Google Drive, Confluence, Jira, S3), mapping pipeline, and materialization
- Dockerfile, jest config, and independent test suite
- OpenTelemetry tracing integration
- Prometheus metrics endpoint
- Schedule evaluator for cron-triggered pipelines

## Architecture Decisions

### Video: primitives composed into sessions
live-video-streaming is kept as a thin primitives service (channels + participant tokens). platform-api composes those primitives into "sessions" with DynamoDB-backed state (participants, lifecycle, transcript status). This avoids coupling app-level concepts into the video infrastructure layer.

### Ingest: Redis event bus (not in-process)
The ingest engine publishes all progress events to the `ingest:events` Redis channel. websocket-gateway subscribes and handles per-source fan-out to WebSocket clients. This decouples the two services and allows them to scale independently.

### No BFF layer needed yet
The frontend talks directly to platform-api for REST and to websocket-gateway for real-time. Adding a BFF would increase latency without meaningful benefit at current scale. Revisit if auth/aggregation patterns become complex enough to warrant it.

### Pipeline distributed-core absorption (Phase A)
A v2 pipeline router (`/api/v2`) implements the "module-shaped" contract that will eventually move into distributed-core itself. Legacy `/api/pipelines/*` routes remain unchanged during migration.

### DynamoDB table prefix for multi-tenancy
`DDB_TABLE_PREFIX` env var prevents table name collisions when multiple services share DynamoDB-local in development.

## What's Working

- All social routes (rooms, posts, comments, likes, reactions, activity, groups, profiles)
- Document management (typed documents, sections, items, reviews, approvals, import/export)
- Pipeline orchestration (trigger, cancel, retry, replay, resume, breakpoints, approvals, DLQ, audit, LLM streaming, cost tracking)
- Pipeline definitions CRUD with scan-backed cache
- Pipeline schedule evaluator (cron-based triggers)
- Ingest engine with adapters, mapping, materialization, DLQ, audit
- Ingest webhooks (Confluence, Jira, S3, generic)
- Video session CRUD (create, join, end, list)
- Redis broadcast to websocket-gateway for real-time social events
- Health check and Prometheus metrics
- OpenTelemetry tracing
- CORS configured for frontend dev server
- Rate limiting on pipeline GET routes
- Idempotency middleware

## What's Not Yet Wired

- **identity-server OIDC auth** — currently uses Cognito JWTs (with `SKIP_AUTH=true` for local dev). Migration planned but not started.
- **Video transcript/summary** — `transcriptStatus`, `transcript`, and `aiSummary` fields exist on VideoSessionRecord but no processing pipeline populates them yet.
- **Ingest write-back conflict resolution** — stub-only; persists resolutions in-memory. Full implementation pending slice F.
- **Pipeline schedule evaluator Phase 4** — currently logs-only on cron fire. Actually creating runs requires ResourceRouter integration.
- **Production deployment** — Dockerfile works, Helm chart references exist in websocket-gateway, but no standalone production deployment has been validated.

## Integration Points with Sibling Services

| Service | Integration | Protocol |
|---------|-------------|----------|
| **websocket-gateway** | Subscribes to Redis channels; fans out to WebSocket clients | Redis pub/sub |
| **live-video-streaming** | Channel create/delete, participant token minting | HTTP REST (`VIDEO_API_URL`) |
| **distributed-core** | PipelineModule, EventBus, Cluster, Raft, WAL, metrics | npm file: dependency |
| **identity-server** | Future OIDC auth provider (replaces Cognito) | OIDC/JWT (planned) |
| **agent-hub** | Shared backing services (DynamoDB-local, Redis) in local dev | Docker Compose |

## Known Issues / Tech Debt

### Pre-existing test failures
- **Google Drive adapter tests** — fail when Google API credentials are not configured (expected in CI without secrets)
- **S3 adapter tests** — fail without LocalStack or real AWS credentials configured
- These are environment-dependent and pass in appropriately configured environments

### distributed-core interface mismatch
- Some distributed-core interfaces have drifted from what platform-api expects (e.g. `readonly` modifiers on arrays, optional fields becoming required). Bridge code includes compatibility shims (mutable array copies, null coalescing) that should be cleaned up when the interfaces stabilize.

### Other debt
- `stub-run-store.ts` still used as fallback when PipelineModule bootstrap fails — should be removed once bootstrap is reliable
- BroadcastService creates its own Redis client instead of sharing the one from `redis-client.ts`
- Some route files are large (pipelineTriggers.ts) and could be further decomposed
- Pipeline v2 router duplicates some logic from legacy routes during migration period
- Ingest engine processes items serially — acceptable now but will need parallelism for production scale

## Next Steps

### identity-server auth migration
Replace Cognito JWT verification with identity-server OIDC. The middleware (`src/middleware/auth.ts`) is the single point of change. Frontend will switch token issuance to identity-server simultaneously.

### Component library extraction
Shared UI components (used by frontend against both platform-api and other services) should move into a dedicated package. Not blocking but reduces duplication.

### Production deployment
- Validate Dockerfile in CI (multi-stage build with SSH mount for distributed-core)
- Configure Helm values for platform-api pod (resource limits, HPA, readiness/liveness probes)
- Set up DynamoDB table provisioning (CDK or Terraform)
- Configure Redis cluster endpoint for production pub/sub
- Set up monitoring dashboards from Prometheus metrics and OpenTelemetry traces

### Video transcript pipeline
Wire a post-session job that fetches recording from IVS, runs transcription (Whisper or AWS Transcribe), and generates AI summary. Populate `transcriptStatus`/`transcript`/`aiSummary` fields on VideoSessionRecord.

### Ingest write-back (Slice F)
Full bi-directional sync: changes made in platform-api written back to source systems (Google Drive, Confluence). Conflict resolution UI already has stub routes.
