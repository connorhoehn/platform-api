# Changelog

Dated entries for changes other repos or operators need to know about. Commit
messages remain the detailed record.

## 2026-11-07 — 0.1.4, media org backfill and 24 hour recording tokens

- At startup, catalog rows with no `orgId` are stamped with the walker's org when exactly one is configured (`MEDIA_WALKER_ORG_ID`; `default-org` under the insecure dev switch), in batches of 100; otherwise the count of hidden rows is logged once. Recording playback tokens are now minted for 24 hours (LVS's cap) on every read of the detail, list, share and new `GET /api/recordings/:id/card` routes, list rows and the chat card get a tokenised `thumbnailUrl` on read, and no token is ever stored.

## 2026-11-07 — 0.1.3, response security headers (fix package 20)

- Every response now carries nosniff, `Referrer-Policy: strict-origin-when-cross-origin`, `X-Frame-Options: DENY`, `Content-Security-Policy: default-src 'none'; frame-ancestors 'none'`, a minimal Permissions-Policy and CORP `same-site`; `X-Powered-By` is removed. HSTS is sent over TLS (`req.secure`, or `x-forwarded-proto: https` with `TRUST_PROXY=1`). No new dependency.

## 2026-11-07 — 0.1.2, follows live-video-streaming's auth hardening

- Recording media URLs (composed MP4, tracks, thumbnail, share playback) now end in `?token=<LVS playback token for that channel>`; treat those URLs as secrets.
- **New required env `VIDEO_SERVICE_TOKEN`** (= LVS `LVS_SERVICE_TOKEN`): session create/join/refresh/end refuse with 503 when unset; the caller's bearer is no longer forwarded to LVS.
- The webhook bootstrap sends `VIDEO_OPS_TOKEN`/`LVS_OPS_TOKEN` (admin token fallback) and logs a hint when LVS refuses the target (add its host to LVS `LVS_WEBHOOK_ALLOWED_HOSTS`).

## 2026-11-07 — 0.1.1, security fixes (review packages 1 and 9)

Fails closed where it used to fail open. **Deployments must set new env before
taking this version**, or they lose Cognito sign-in, CORS, call lobbies and LVS
admin calls.

| Env | Needed by | Unset now means |
|-----|-----------|-----------------|
| `COGNITO_CLIENT_ID` | every deployment taking Cognito tokens (comma list of app client ids) | every Cognito token is refused (401); boot fails where `COGNITO_REQUIRED` applies |
| `CORS_ALLOWED_ORIGINS` | any browser origin calling with credentials (comma list of exact origins) | no CORS grant for any origin |
| `LOBBY_AUTHORITY_URL` | OrgIQ / Assessment calls (the BFF's `/api/realtime/lobby-access`) | every OrgIQ-token call lobby is refused; `LOBBY_AUTHORITY_REQUIRED` is now **on** unless set to `false` |
| `VIDEO_ADMIN_TOKEN` (or `LVS_ADMIN_TOKEN`) | LVS admin calls, the recording webhook bootstrap, playback tokens, captions | no Authorization header (LVS refuses); the bootstrap skips; there is no `'admin'` default |
| `INSECURE_DEV_MODE=true` | local development only, beside `SKIP_AUTH=true` | `SKIP_AUTH=true` without it refuses to start; never honoured with `NODE_ENV=production` |

- **Image sets `NODE_ENV=production`.** A local container stack that wants
  `SKIP_AUTH` must set `NODE_ENV=development` and `INSECURE_DEV_MODE=true`.
- **Agent steps.** A pipeline definition a person writes (run trigger body, `PUT
  /pipelines/defs/:id`) may not add or change agent steps that request tools
  (400 `agent_tools_not_allowed`). A re-save keeps a tool step the stored
  definition already has, unchanged, on `runtime: 'devbox'`. Tool-using steps run
  only on devbox. The `claude` and `codex` children (agent, LLM, vision, Codex
  summary) get an allowlisted environment (`src/pipeline/cliChildEnv.ts`): no
  keys, tokens or secrets; in Bedrock mode the AWS credential-provider pointers
  pass so the CLI can use the pod's role. `ANTHROPIC_API_KEY` is not passed:
  local CLI auth is the mounted `~/.claude` session.
- **Lobbies.** Every tenant-prefixed lobby needs the caller's partition, not only
  `dm:` and `room:` shapes.
- **Cognito.** Only ID tokens (`token_use: 'id'`) whose `aud` is in
  `COGNITO_CLIENT_ID`; access tokens are refused.
- **Admin.** `requireAdmin` compares in constant time, answers 401 for a missing
  or wrong token (was 403 for wrong), and `SKIP_AUTH` no longer bypasses it.
- **Media catalog.** Rows are per organisation (stored key `<orgId>#<hash>`;
  the API's `fileId` stays the hash). Rows written before (bucket `all`) are no
  longer returned; re-walk to rebuild. Batches: 20 per user per minute, 8 MB of
  thumbnails per batch, 12mb body. The walker waits out a 429.

## 2026-11-04 — shared pipeline stack

- **Pins.** `distributed-core` 0.48.0 → v0.50.1
  (`eea5c874766dea15b8b1f58b969af991b289a42c`), `@connorhoehn/pipeline-runtime`
  1.2.1 → v1.4.0 (`f8d4c07c4ba77f1474684385c66a006f00c8593f`). event-catalog,
  realtime-modules, service-runtime and message-queue still depend on 0.48.0;
  an `overrides` entry resolves them to the root pin so the process loads one
  distributed-core.
- **Boot resume runs on pipeline-runtime's `recoverInterruptedRuns`**
  (`/durable`), through `RunRecordRecoveryStore` (`src/pipeline/runRecordStore.ts`)
  over a new `RunRecordRepository` interface that `PipelineRunRepository`
  implements. Same outcomes as before (window, terminal evidence, conditional
  claim, seeded persistence, `module.resumeRun`).
- **`POST …/runs/:runId/resume?fromNodeId` works after a restart.** With no
  executor holding the run, the row's definition snapshot and checkpoint go to
  distributed-core's `resumeFromStep` fallback. A forwarded resume nobody
  acknowledges is rewound on the receiving replica instead of answering 409
  `run-not-resumable` (still the answer when the row has no checkpoint).
- **Action idempotency is real now.** The `onBeforeAction` hook was never read
  upstream; every executor now shares one process-wide `actionDedupStore`
  (still in memory, so a restart forgets it).
- **A forwarded resume-from-step runs once.** The replica holding the run now
  publishes `accepted` on `pipeline.controls` before the re-run starts (it used
  to ack only at the terminal snapshot, so a re-run over the route's 5 s wait,
  or one parked at an approval or breakpoint, was also rewound on the receiving
  replica and action steps fired twice). The route answers 202
  `{applied:false, acceptedBy}` on `accepted`. Both sides take a one-time claim
  `rerun:<runId>:<controlId>` in `idempotency-claims` (TTL 7 days, past any
  plausible re-run) before executing: the route falls back locally only when it
  wins, else 202; claims table down → 503. Still open: two *different*
  controlIds for one run while its owner is dead both fall back (needs a
  run-level claim).

## 2026-10-14 — upstream refinement (lane P1)

- **Pins (lane P1).** `@connorhoehn/realtime-modules` 0.92.0 → v0.117.0
  (`dc7eb68611946a5079dc59c618534c465c6d8ff8`) and `distributed-core`
  0.46.5 → v0.48.0 (`077d7594cc412e3153ace47ce0f0b4be60df84d5`). The
  realtime-modules copy now dedupes onto the root distributed-core;
  `@connorhoehn/pipeline-runtime` (peer) resolves the same 0.48.0. Nothing in
  `/work-graph`, `/server` or `/work-graph/server` needed a source change.
- **Schedule evaluator is single-flight across replicas (ROADMAP P2.1, lane P1).**
  `src/services/scheduler-fenced-leadership.ts`: a distributed-core
  `FencedLease` (`distributed-core/cluster/leases`, 30 s lease over a
  `BoundedRedis`) picks the one evaluating replica, and every fire is claimed by
  an atomic script that requires the lease key to still name this owner and the
  `(pipeline, minute)` marker to be unset. The old per-tick DynamoDB lock let two
  offset replicas both win in one minute. Without Redis no replica fires
  (fail closed). Contracts: `scheduleEvaluator.fenced-leadership.test.ts`
  (two evaluators, fake Redis: one fire per tick, takeover on fence, no double
  fire across a crash or hand-off, stale owner refused at the claim) and the
  opt-in real-Redis `scheduler-fenced-leadership.redis.test.ts`
  (`SCHEDULER_LEASE_REDIS_URL`, never db 0).
- **Definitions cache is event-invalidated (ROADMAP P2.2, lane P1).** While the
  `platform-api:pipeline-definitions:invalidate` subscription is live the
  DynamoDB Scan runs every 15 minutes as a safety poll instead of every 60 s; a
  dropped subscription restores the 60 s poll and the reconnect refreshes once.
  Document-automation writes now publish the invalidation too. Contracts:
  `definitions-cache.event-driven.test.ts`.
- **Unit-suite Redis leak (lane P1).** node-redis rejects `connect()` on the
  first socket error but keeps the client reconnecting; `getRedisClient()`
  dropped that client without closing it, so with no Redis it kept dialling
  `redis:6379` once a second after its caller (and its test file) had moved on.
  Route tests reach it through unmocked writes (definitions invalidation, video
  ARN index). A failed connect now destroys its client before anything else in
  the error path can throw, `closeRedisClients()` also cancels a connect in
  progress, and `jest.after-env.js` closes the shared clients after every test
  file. `--detectOpenHandles` on the five suites that made such clients reports
  nothing.
- **Recording-status heap soak (lane P1).** `scripts/soak/recording-status-soak.ts`
  drives the real long-poll route in-process (woken, deadline, changed,
  refused, expired and aborted cycles) and writes
  `docs/soak/<date>-recording-status.md`. First run
  (`docs/soak/2026-10-14-recording-status.md`): 125 minutes, 673,845 cycles
  (about 5,400 a minute), post-warm-up heap slope +0.12 MB/h against a 1 MB/h
  bound, no orphaned bus subscription — `verdict: pass`. Local only; the
  deployed media stack is still unconfirmed (cloud blocked).
- **Docs (lane P1).** CLAUDE.md and ROADMAP.md no longer point at the retired
  websocket-gateway name or its private memory paths.
