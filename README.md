# platform-api

Application platform API — manages users, documents, pipelines, ingest/ETL, social features, and video sessions. Extracted from websocket-gateway/social-api into a standalone Express TypeScript service (port 3001).

## Architecture

```
                         +-------------------+
                         |    Frontend SPA   |
                         +--------+----------+
                                  |
                    HTTP (REST)   |   WebSocket
                  +--------------++--------------+
                  |                               |
          +-------v--------+           +---------v-----------+
          |  platform-api  |           | websocket-gateway   |
          |  (port 3001)   |           | (WebSocket fan-out) |
          +--+----+----+---+           +---------^-----------+
             |    |    |                         |
             |    |    |     Redis pub/sub       |
             |    |    +-------------------------+
             |    |
             |    |    +-------------------------+
             |    +--->| live-video-streaming    |
             |         | (IVS channel mgmt)     |
             |         +-------------------------+
             |
             v
     +---------------+
     |   DynamoDB    |
     | (persistence) |
     +---------------+
```

**Data flow:** platform-api handles all CRUD and business logic, persists to DynamoDB, and publishes events to Redis pub/sub channels. websocket-gateway subscribes to those channels and fans out real-time updates to connected WebSocket clients.

## Quick Start

```bash
# Prerequisites: Redis and DynamoDB-local running
# (shared services from agent-hub docker-compose, or standalone)

npm install
npm run dev
```

### Required Environment Variables

| Variable | Description | Dev default |
|----------|-------------|-------------|
| `COGNITO_REGION` | AWS region for Cognito user pool | `us-east-1` |
| `COGNITO_USER_POOL_ID` | Cognito user pool ID | `us-east-1_localdev000` |
| `SKIP_AUTH` | Skip JWT verification in local dev | `true` |
| `REDIS_ENDPOINT` | Redis host for signal transport pub/sub | `redis` |
| `REDIS_PORT` | Redis port | `6379` |
| `EVENT_CATALOG_URL` | URL of the event-catalog registry server. When set, platform-api registers all its events with the catalog at startup for topology visibility. | (empty = disabled) |
| `MESSAGE_QUEUE_URL` | URL of the message-queue service for durable event delivery | `http://localhost:3004` |
| `LOCALSTACK_ENDPOINT` | DynamoDB-local endpoint | `http://localhost:8000` |
| `AWS_REGION` | AWS region for DynamoDB | `us-east-1` |
| `VIDEO_API_URL` | live-video-streaming service URL | (empty = disabled) |
| `PORT` | HTTP listen port | `3001` |
| `DDB_TABLE_PREFIX` | Table name prefix for shared DDB | (none) |
| `PIPELINE_RUN_PERSISTENCE_ENABLED` | Persist pipeline runs to DDB | `true` |
| `PIPELINE_DLQ_PERSISTENCE_ENABLED` | Persist DLQ entries to DDB | `true` |
| `PLATFORM_API_INGEST_TRANSPORT` | Ingest item handoff transport: `memory` (in-process FIFO per sourceId; lost on restart) or `mq` (durable via message-queue; survives restart, multi-replica safe). See `src/ingest/transport.ts`. | `memory` |

## Service Domains

### Documents
Typed document CRUD, sections, section items, reviews, approvals, comments, import/export.

### Pipelines
Distributed pipeline orchestration via `distributed-core` PipelineModule. Supports triggers, scheduling (cron), approvals, breakpoints, replay, retry, resume, DLQ, LLM streaming, cost tracking, and audit logging. Multiple LLM backends (Anthropic direct, Bedrock, Claude Code).

### Ingest / ETL
Source-adapter framework for ingesting content from external systems (Google Drive, Confluence, Jira, S3). Includes mapping rules, type generation, materialization, dead-letter queue, conflict resolution, audit trail, and webhook receivers for push-based sync.

### Social
Rooms, posts, comments, likes, reactions, activity feeds, groups, group membership, profiles. Real-time updates broadcast to websocket-gateway via Redis.

### Video Sessions
Composes live-video-streaming primitives (channels + participant tokens) into application-level video sessions tied to documents. Tracks participants, session lifecycle, and transcript/summary state.

## Redis Event Channels

| Channel Pattern | Publisher | Description |
|----------------|-----------|-------------|
| `dcore:topic:pipeline.events.*` | Pipeline module | Pipeline lifecycle (run started, step complete, failed, etc.) |
| `ingest:events` | Ingest engine | Ingest progress (item fetched, mapped, materialized, failed) |
| `social:post`, `social:comment`, `social:like`, `social:member_joined`, `social:member_left` | Social routes | Social activity events |
| `doc:comment_added`, `doc:comment_resolved`, `doc:comment_deleted` | Document routes | Document collaboration events |
| `websocket:route:{channelId}` | BroadcastService | Targeted delivery to gateway nodes with subscribed clients |

## API Routes

All authenticated routes require a valid Cognito JWT (or `SKIP_AUTH=true` for local dev).

### Public (no auth)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/internal/metrics` | Prometheus metrics |
| POST | `/hooks/pipeline/:webhookId` | External webhook pipeline triggers |
| POST | `/api/ingest/webhooks` | External ingest push notifications |
| POST | `/api/ingest/webhook/confluence/:sourceId` | Confluence page-updated callbacks |

### Documents (`/api/documents`)

| Method | Path | Description |
|--------|------|-------------|
| * | `/api/document-types` | Document type schema CRUD |
| * | `/api/typed-documents` | Typed document instance CRUD |
| * | `/api/documents/:id/sections/:sectionId/items` | Section items |
| * | `/api/documents/:id/sections/:sectionId/reviews` | Section reviews |
| * | `/api/documents/:id/reviews` | Document-level reviews |
| GET | `/api/reviews` | My pending reviews |
| GET | `/api/items` | My assigned items |
| * | `/api/documents/:id` | Import/export (JSON, CSV) |
| * | `/api/documents` | Document comments |
| * | `/api/approvals` | Approval log |

### Pipelines (`/api/pipelines`)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/pipelines/metrics` | Pipeline metrics |
| GET | `/api/pipelines/dashboard-data` | Dashboard KPIs and charts |
| * | `/api/pipelines/defs` | Pipeline definition CRUD |
| GET | `/api/pipelines/health` | Pipeline health introspection |
| POST | `/api/pipelines/validate` | Validate pipeline definition |
| GET | `/api/pipelines/runs/active` | Active runs listing |
| GET | `/api/pipelines/approvals` | Pending approval queue |
| GET | `/api/pipelines/inspector` | Run-queue introspection |
| * | `/api/pipelines/dlq` | Dead-letter queue inspect/redrive |
| POST | `/api/pipelines/llm/stream` | LLM streaming proxy |
| GET | `/api/pipelines/audit/search` | Audit log search |
| POST | `/api/pipelines/:id/runs` | Trigger a pipeline run |
| POST | `/api/pipelines/:id/runs/:runId/replay` | Replay a run |
| POST | `/api/pipelines/:id/runs/:runId/retry/:nodeId` | Retry from node |
| POST | `/api/pipelines/:id/runs/:runId/resume` | Resume failed run |
| GET | `/api/pipelines/:id/runs/:runId/audit` | Per-run audit timeline |
| GET | `/api/pipelines/:id/cost` | Per-pipeline cost aggregation |
| POST | `/api/pipelines/:runId/cancel` | Cancel a run |
| POST | `/api/pipelines/:runId/approvals` | Approve/reject gate |

### Ingest (`/api/ingest`)

| Method | Path | Description |
|--------|------|-------------|
| * | `/api/ingest/sources` | Ingest source CRUD |
| GET | `/api/ingest/sources/:id/metrics` | Per-source SLO metrics |
| * | `/api/ingest/items` | Ingested item tracking |
| * | `/api/ingest/mapping` | Mapping rule management |
| * | `/api/ingest/mapping/proposals` | DocType proposals |
| * | `/api/ingest/templates` | Pipeline templates |
| * | `/api/ingest/dlq` | Ingest dead-letter queue |
| GET | `/api/ingest/audit` | Ingest audit log |
| * | `/api/sync/conflicts` | Write-back conflict queue |
| POST | `/api/ingest/dev/seed` | Dev-only seed data |

### Social (`/api`)

| Method | Path | Description |
|--------|------|-------------|
| * | `/api/profiles` | User profiles |
| * | `/api/social` | Social feed |
| * | `/api/groups` | Groups CRUD |
| * | `/api/groups/:id/members` | Group membership |
| * | `/api/groups/:id/rooms` | Group rooms |
| * | `/api/rooms` | Rooms CRUD + my rooms |
| * | `/api/rooms/:id/members` | Room membership |
| * | `/api/rooms/:id/posts` | Posts in room |
| * | `/api/rooms/:id/posts/:postId/comments` | Comments on post |
| * | `/api/rooms/:id/posts/:postId/likes` | Post likes |
| * | `/api/rooms/:id/posts/:postId` | Reactions |
| GET | `/api/activity` | Activity feed |

### Video (`/api/video`)

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/video/sessions` | Create video session |
| POST | `/api/video/sessions/:id/join` | Join session (mint token) |
| POST | `/api/video/sessions/:id/end` | End session (teardown) |
| GET | `/api/video/sessions/document/:docId` | List sessions for document |
| GET | `/api/video/sessions/:id` | Get session details |

### v2 Pipeline Router (`/api/v2`)

Module-shaped pipeline router (Phase A of distributed-core absorption). New routes land here first during migration.

## Dependencies

Shared libraries are pinned by published version in `package.json` — `npm install` fetches them from GitHub directly. No side-by-side clone is required, and no `file:../` pins are used. Current pins (also documented in `.agent/CONSTITUTION.md`):

### distributed-core
`github:connorhoehn/distributed-core#distributed-core-v0.37.2`. Provides PipelineModule, EventBus, Cluster primitives, Raft state machine, WAL, and metrics infrastructure.

### @connorhoehn/event-catalog
`github:connorhoehnslalom/event-catalog#v0.3.48`. Durable + signal event registry; source of truth for cross-service event schemas (M1 adoption).

### @connorhoehn/service-runtime
`github:connorhoehn/service-runtime#v0.2.4`. HMAC envelope sign/verify, logger, health handler, request-context middleware, lifecycle helpers.

### @connorhoehn/pipeline-runtime
`github:connorhoehn/pipeline-runtime#v0.4.0`. HTTPPipelineBridge, PipelineApprovalStateMachine, ApprovalStore, canonical pipeline contracts.

### live-video-streaming
Separate service providing IVS-backed video channel management. platform-api composes its primitives (create channel, mint participant token, delete channel) into application-level video sessions. Connected via `VIDEO_API_URL` env var.

### Redis
Used for pub/sub event delivery to websocket-gateway. Not used for caching or session storage.

### DynamoDB
Primary persistence layer for all domains. Tables are prefixed with `DDB_TABLE_PREFIX` when running against shared DynamoDB-local.

### Docker build auth
The image is built with BuildKit and uses SSH agent forwarding so `npm install` can fetch the four github: pins above from the private repos. Invoke with `DOCKER_BUILDKIT=1 docker build --ssh default -f platform-api/Dockerfile -t platform-api .` so the host's `ssh-agent` is forwarded into the `RUN --mount=type=ssh` steps. See the `Dockerfile` header for the exact build invocation.

## Testing

```bash
npm test              # Run full test suite (Jest)
npm run typecheck     # TypeScript type checking only
```

Tests use Jest with `ts-jest` transformer. Test files live in `src/__tests__/` and `test/`.

## Deployment

### Docker

```bash
docker build -t platform-api .
docker run -p 3001:3001 \
  -e COGNITO_REGION=us-east-1 \
  -e COGNITO_USER_POOL_ID=<pool-id> \
  -e REDIS_ENDPOINT=<redis-host> \
  -e AWS_REGION=us-east-1 \
  platform-api
```

### Helm

Deployed via the websocket-gateway Helm chart as a sidecar/companion service. The chart manages both the gateway and platform-api pods, sharing the same Redis and DynamoDB backing services.

### Health Check

`GET /health` returns 200 when the service is ready. The Dockerfile includes a built-in healthcheck on 30s intervals.
