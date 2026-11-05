# Changelog

Dated entries for changes other repos or operators need to know about. Commit
messages remain the detailed record.

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
