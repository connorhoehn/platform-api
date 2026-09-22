# DynamoDB access patterns — platform-api

## The rule

**Never add a query that depends on a GSI without checking the table that
actually exists.** Local DDB (and the deployed table) are created by the
bootstrap in `realtime-examples`, not by this repo, so a GSI that is in a
repository method's signature is not evidence that it is in the table.

`PipelineRunRepository.listByTriggeredBy` reads
`triggeredBy-startedAt-index`. That index is **not provisioned**. Calling it
answers:

```
ValidationException: The table does not have the specified index: triggeredBy-startedAt-index
```

which surfaces as a 500. `listByStatus` reads `status-startedAt-index`, which
IS provisioned. Check before you reach for one.

And never reach for a Scan as the fix: "this person's loops today" would read
every run of every pipeline to answer.

## The pattern: a pointer row instead of an index

When a read pattern needs a key the table does not have, pay for it on the
WRITE side — one extra row, under a synthetic partition key, in the same
table. Both sides then use the table's own key schema and no index at all.

`/agent` loops do this for the Computer Lab tab
(`src/chat/agentLoopRunner.ts`):

| | |
|---|---|
| Partition (`pipelineId`) | `loops-by-person#<userId>#<YYYY-MM-DD utc>` |
| Sort (`runId`) | the loop id |
| `status` | `index` |
| `triggerType` | `agent-loop-index` |
| `ttl` | 90 days, matching the run rows it points at |

Read: `listByPipeline('loops-by-person#<userId>#<day>')` for each UTC day the
caller's local-day window touches, then `get()` the loop rows those name.
Two key-only Queries and N bounded Gets.

### Two things that are easy to get wrong

**Partition by UTC day, not local day.** The requester's timezone is not known
when the row is written — the chat command does not carry one — so a "local
day" written at 202 time is a guess. The UTC day is a fact. The reader knows
the timezone, asks for the one to three UTC days its local window touches
(`utcDaysSpanned`), and filters precisely with the half-open window from
`src/chat/localDay.ts`.

**Give pointer rows an inert `status`.** They live in the runs table but are
not runs. `status: 'index'` keeps them out of every status-keyed query in the
service — the orphan scans ask for `pending`/`running`, the reconcile sweep
for the terminal ones. A pointer row that answered to `pending` would be
"resumed" as an interrupted run.

### When to add a GSI instead

A pointer row suits a write-light, read-specific pattern. Prefer a real GSI
when the access pattern is general (arbitrary date ranges, many consumers) —
but then add it to the table bootstrap in `realtime-examples` FIRST, confirm
it exists in local DDB, and record it here.

## Tables this service owns a key pattern in

| Table | Key | Notes |
|---|---|---|
| `pipeline-runs` | `(pipelineId, runId)` | `status-startedAt-index` exists; `triggeredBy-startedAt-index` does NOT |
| `pipeline-runs` | `(loops-by-person#…, loopId)` | pointer rows, above |
| `chat-members` | `(channel, userId)` | membership; `removedAt` present means removed |

## The work graph: one read per table per pass, not per node

A v2 graph request authorizes every node it will show and then reads the
source record behind each one. Both used to be per-node, and both are joins on
a NON-KEY attribute:

| What is looked up | By | Why it costs a Scan |
|---|---|---|
| a run (`authorizeRef`, `activityReader.pipelineRuns`) | `runId` | `pipeline-runs` is keyed `(pipelineId, runId)`; there is no `runId` GSI |
| a recording (`authorizeRef`) | `callId` / `recordingId` / `transcriptId` | only `recordingId-index` exists, and a meeting is addressed three ways |

Per-node, that is a filtered Scan for every run, every STEP of every run (a
step is `runId::stepId` and resolves to the same row), every workspace and
every meeting — issued sequentially. `scanAll(…, 2)` made it worse: the page
limit is `min(100, maximum - read)`, so a two-row cap walked the table two
items per round trip. dev-frank's ~100-node day took 25–80 s.

**The pattern:** collect every resource id the pass will ask about FIRST, then

- one Scan per table per pass, `FilterExpression: runId IN (:r0 … :rN)`
  (chunked at 90 operands, page size 100), indexed into a `Map<id, row|null>`;
- one `BatchGetItem` for the day's documents (`crdt-documents` is keyed by
  `documentId`, so this one is a real batch, ≤100 keys per call);
- memoize `chat-members` membership by `(channel, userId)` — the owner and the
  viewer are asked once, not once per revision node of the same document;
- memoize the compute source's HTTP authorization by resource.

**Keep ambiguity a denial.** The per-node form refused when a lookup returned
anything but exactly one row. The batched form must too: index a second row
for the same id as `null`, and treat `null` and absent the same way. A batch
that quietly picked the first of two rows would authorize by guess.

Guarded by `src/work-graph/__tests__/sourceAclBatching.test.ts`, which counts
reads rather than timing them. `WORK_GRAPH_PROFILE=1` logs per-phase
durations (`authorizeMs` / `activityMs`) for one request.
