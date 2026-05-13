# platform-api — agent context

Application platform API service. Manages users, content, documents,
pipelines, ingest/ETL, and social features. Sits behind identity-server
for authentication.

**Read `.agent/CONSTITUTION.md` first** — agent's north star and
backlog priorities. Read `ROADMAP.md` for Phase P1–P7 progression
(M1 event-catalog adoption, M2 ingest durable-backbone flip, M4
pipeline-worker extraction).

## Architecture

- **Express HTTP API** — all domain routes
- **distributed-core** — PipelineModule, EventBus, Cluster primitives
- **Redis pub/sub** — publishes events consumed by websocket-gateway
- **DynamoDB** — persistence layer

## Key event channels (Redis pub/sub)

| Channel | Data |
|---------|------|
| `dcore:topic:pipeline.events.*` | Pipeline lifecycle events |
| `ingest:events` | Ingest progress events |

## Dependencies

- **`distributed-core`** — local pin via `file:../distributed-core`
- **`identity-server`** — OIDC provider (future auth migration from Cognito)

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
