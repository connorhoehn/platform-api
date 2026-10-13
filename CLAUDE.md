# platform-api — agent context

Application platform API: users, content, documents, pipelines, social
features and live video sessions. Sits behind identity-server for
authentication. Read `.agent/CONSTITUTION.md` first, then `ROADMAP.md`
(P1–P7; M4 pipeline-worker re-extraction is gated on `.agent/SLO-M4-TRIGGER.md`).
The aws-agentcore workspace consumes this service through its Tilt stack; its
current handoff is `../aws-agentcore/docs/claude-code-handoff-2026-10-12.md`.

The `agent-hub` coordination system was retired on 2026-08-16. `hub#<id>`
references in docs and commits are historical breadcrumbs, not tickets; do not
look for hub tasks or `mcp__agent-hub__*` tools.

## Architecture

- **Express HTTP API**, single deployable. Repositories are DynamoDB-backed
  singletons from `src/repositories/index.ts`.
- **Pipeline executor runs in-process.** The `HTTPPipelineBridge` from
  `@connorhoehn/pipeline-runtime` bridges to the local executor, not a worker pod.
- **Outbox table is the durability primitive.** `src/services/outbox-drainer.ts`
  is lease-coordinated (`OUTBOX_LOCK#drainer`) and publishes to the durable MQ
  via `catalog.publish`; metrics at `/internal/metrics` (`outbox_*`).
- **Redis pub/sub** (`dcore:topic:pipeline.events.*`) feeds websocket-gateway.
- **Shared SFU circuit breaker**: every outbound LVS call goes through
  `callSfu` in `src/lib/sfu-breaker.ts`, one process-wide state.
- **Lobby admission asks the OrgIQ BFF** when `LOBBY_AUTHORITY_REQUIRED=true`
  (`docs/lobby-authority.md`); the BFF route must exist before the flag is set.

## Dependencies

Pinned by full git sha in `package.json`; read it rather than this file for
versions. `distributed-core` is the **connorhoehn** lineage; `event-catalog`
is connorhoehnslalom; `pipeline-runtime`, `realtime-modules` and
`service-runtime` are connorhoehn. Cross-repo fixes go upstream in the owning
repo, then the pin moves here.

## Conventions

- Solo project, no PRs: stage, commit, push to `main` directly. TypeScript strict.
- 5xx responses log `errorMessage`/`errorName`/`errorStackTop` from
  `res.locals.__loggedError`.
- `/hooks/lvs/recording` requires `LVS_WEBHOOK_SECRET` in production; with it
  unset the receiver rejects unless `SKIP_AUTH=true` is an explicit dev opt-in.
- OpenAPI lives in `openapi/{pipelines,documents,social,video}.yaml`;
  `npm run lint:openapi` fails on route/YAML drift.

## Local dev

```bash
npm run dev      # needs Redis and DynamoDB-local
npm test && npm run lint && npm run build
```
