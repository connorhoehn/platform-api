# Judge v2 — Operator Provisioning Runbook

This document captures the DynamoDB resources the `llm:judge` step
depends on. The application code degrades gracefully when these aren't
provisioned (see _Fallback behavior_), but operators should provision
them before going live.

The application **does NOT** auto-create any of these — IaC happens
out-of-band per the project's standing convention. All substrate is
DynamoDB; there are no cloud-only services in the judge feature.

## Tables

### `pipeline-judgments` (v1 — already required)

Per-judgment immutable record. Provisioned in the v1 wave. Schema as
defined in `src/pipeline/judge/JudgmentRepository.ts`.

### `pipeline-judgment-aggregates` (v2 — NEW)

Per-`(pipelineId, rubricId, dayBucket)` rollup row. Read shape on the
type `AggregateRow` in `JudgmentAggregateRepository.ts`.

#### Schema

| Attribute | Type | Role |
| --- | --- | --- |
| `aggregateKey` | S | Primary key — `${pipelineId}#${rubricId}#${dayBucket}` |
| `pipelineId` | S | de-normalized; GSI1 partition key |
| `rubricId` | S | de-normalized; GSI2 partition key |
| `dayBucket` | S | yyyy-mm-dd UTC; both GSI sort keys |
| `numJudgments`, `numPassed`, `numFailed` | N | Counters (atomic ADD) |
| `sumOverallScore`, `sumTokensIn`, `sumTokensOut`, `sumCostUsd` | N | Sums (atomic ADD) |
| `minOverallScore`, `maxOverallScore` | N | Best-effort extremes (conditional SET) |
| `firstJudgedAt`, `lastJudgedAt` | S | Bookend ISO timestamps |
| `ttlEpoch` | N | Unix-seconds — DDB native TTL attribute (default 365 days) |

#### `aws ddb create-table` command

```bash
aws dynamodb create-table \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-judgment-aggregates" \
  --attribute-definitions \
    AttributeName=aggregateKey,AttributeType=S \
    AttributeName=pipelineId,AttributeType=S \
    AttributeName=rubricId,AttributeType=S \
    AttributeName=dayBucket,AttributeType=S \
  --key-schema \
    AttributeName=aggregateKey,KeyType=HASH \
  --global-secondary-indexes \
    'IndexName=pipeline-day-index,
     KeySchema=[{AttributeName=pipelineId,KeyType=HASH},{AttributeName=dayBucket,KeyType=RANGE}],
     Projection={ProjectionType=ALL},
     ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
    'IndexName=rubric-day-index,
     KeySchema=[{AttributeName=rubricId,KeyType=HASH},{AttributeName=dayBucket,KeyType=RANGE}],
     Projection={ProjectionType=ALL},
     ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
  --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

Drop `--endpoint-url` for the default AWS DDB endpoint. DDB-local
ignores `--billing-mode` and the throughput args but accepts them
without error.

Enable native TTL once the table is `ACTIVE` (DDB-local enforces TTL
on read, real AWS DDB on background sweep):

```bash
aws dynamodb update-time-to-live \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-judgment-aggregates" \
  --time-to-live-specification "Enabled=true, AttributeName=ttlEpoch"
```

### `pipeline-judgment-rawtext` (v2 — NEW)

Spillover table for `rawModelText` bodies ≥ 32KB. Without this, oversized
bodies are dropped on persist (only the `rawTextTruncated:true` flag is
kept on the parent row in `pipeline-judgments`). The live
`pipeline:judge:scored` event still carries the full body for session
observers — only post-hoc audit drill-in depends on the spillover.

Bodies larger than ~390KB still get dropped (DDB's 400KB per-item cap).
Tail case for LLM responses; not addressed here.

#### Schema

| Attribute | Type | Role |
| --- | --- | --- |
| `judgmentId` | S | Primary key — matches the parent row's `judgmentId` |
| `body` | S | Verbatim `rawModelText` |
| `ttlEpoch` | N | Unix-seconds — DDB native TTL (default 90 days, matches parent) |

#### `aws ddb create-table` command

```bash
aws dynamodb create-table \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-judgment-rawtext" \
  --attribute-definitions AttributeName=judgmentId,AttributeType=S \
  --key-schema AttributeName=judgmentId,KeyType=HASH \
  --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb update-time-to-live \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-judgment-rawtext" \
  --time-to-live-specification "Enabled=true, AttributeName=ttlEpoch"
```

## Environment variables

| Var | Default | Purpose |
| --- | --- | --- |
| `DDB_TABLE_PREFIX` | _empty_ | Local-dev table-name prefix when sharing one DDB-local instance |
| `JUDGE_RAW_TEXT_ENABLED` | `true` | Set to `false` to skip the spillover write entirely (oversized bodies always drop) |
| `JUDGE_AGGREGATOR_ENABLED` | `true` | Set to `false` to skip the subscriber when the aggregates table isn't provisioned yet |
| `JUDGE_DAILY_BUDGET_USD_DEFAULT` | `10` | Per-pipeline-per-day USD cap on judge LLM spend |

## Fallback behavior — what happens without each resource

| Missing | Effect |
| --- | --- |
| `pipeline-judgment-aggregates` table | Aggregator subscriber writes fail and log a warning. Per-judgment row in `pipeline-judgments` still lands. Trend UI shows empty state. Set `JUDGE_AGGREGATOR_ENABLED=false` to silence the warnings. |
| `pipeline-judgment-rawtext` table | Spillover writes fail; JudgmentRepository's soft-fail path drops the body and sets `rawTextTruncated:true`. `GET /api/judgments/:id/raw-text` returns 404. Live `pipeline:judge:scored` event still carries the body for session observers. |
| Both above | Judge runs work end-to-end; trend + audit-drill-in features show "not available" in the UI. |

## Local-dev (Tilt + DDB-local)

Run DynamoDB Local on `http://localhost:8000` (the `dev` script points
`LOCALSTACK_ENDPOINT` there). Create the v2 tables at boot with the
commands above (they default to `--endpoint-url http://localhost:8000`).

Mount a volume on the DynamoDB Local data directory if the tables should
survive container restarts.
