# Judge v2 — Operator Provisioning Runbook

This document captures the AWS resources the `llm:judge` step depends on
in production. The application code degrades gracefully when these
aren't provisioned (see _Fallback behavior_ at the bottom), but
operators should provision them before going live.

The application **does NOT** auto-create any of this — IaC happens
out-of-band per the project's standing convention.

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

Switch `--billing-mode PAY_PER_REQUEST` (and drop both
`ProvisionedThroughput=…` blocks) for on-demand billing.

Enable native TTL once the table is `ACTIVE`:

```bash
aws dynamodb update-time-to-live \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-judgment-aggregates" \
  --time-to-live-specification "Enabled=true, AttributeName=ttlEpoch"
```

## S3 bucket

### `JUDGE_RAW_TEXT_BUCKET` (v2 — NEW, optional)

Spillover for `rawModelText` bodies ≥ 32KB. Without this, oversized
bodies are dropped on persist (only the `rawTextTruncated:true` flag is
kept on the DDB row). The live `pipeline:judge:scored` event still
carries the full body — only post-hoc audit drill-in depends on the
spillover.

#### Create the bucket

```bash
aws s3api create-bucket \
  --bucket your-org-judge-rawtext-prod \
  --region us-east-1
```

#### Recommended bucket-level controls

| Control | Why |
| --- | --- |
| Block all public access | Bodies may contain prompt/response data |
| Server-side encryption (SSE-S3 or SSE-KMS) | Same as above |
| Lifecycle rule: expire after 90 days | Match `pipeline-judgments` retention |
| Versioning OFF | Bodies are immutable per judgmentId; versioning would just inflate cost |

#### Bucket policy — IAM permissions for platform-api

The platform-api service role needs `s3:PutObject` + `s3:GetObject` for
keys under `judgments/*`:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject"],
      "Resource": "arn:aws:s3:::your-org-judge-rawtext-prod/judgments/*"
    }
  ]
}
```

## Environment variables

| Var | Default | Purpose |
| --- | --- | --- |
| `DDB_TABLE_PREFIX` | _empty_ | Local-dev table-name prefix when sharing one DDB-local instance |
| `JUDGE_RAW_TEXT_BUCKET` | _unset_ | Enables S3 spillover; without it, oversized rawText is dropped |
| `JUDGE_AGGREGATOR_ENABLED` | `true` | Set to `false` to skip the subscriber when the aggregates table isn't provisioned yet |
| `JUDGE_DAILY_BUDGET_USD_DEFAULT` | `10` | Per-pipeline-per-day USD cap on judge LLM spend |

## Fallback behavior — what happens without each resource

| Missing | Effect |
| --- | --- |
| `pipeline-judgment-aggregates` table | Aggregator subscriber writes fail and log a warning. Per-judgment row in `pipeline-judgments` still lands. Trend UI shows empty state. Set `JUDGE_AGGREGATOR_ENABLED=false` to silence the warnings. |
| `JUDGE_RAW_TEXT_BUCKET` unset | Oversized rawText is dropped on persist; `rawTextTruncated:true` is set on the row. `GET /api/judgments/:id/raw-text` returns 404. Live `pipeline:judge:scored` event still carries the body for session observers. |
| `S3:GetObject` denied | `GET /api/judgments/:id/raw-text` returns 404 (the store swallows S3 errors silently). Persist still succeeds. |
| Both above | Judge runs work end-to-end; trend + audit-drill-in features show "not available" in the UI. |

## Local-dev (Tilt)

DDB-local is already wired in `agent-hub/services/docker-compose.yml`.
Create the aggregates table at boot with the `aws ddb create-table`
command above plus `--endpoint-url http://localhost:8000`.

S3 spillover is opt-in for local-dev; leave `JUDGE_RAW_TEXT_BUCKET` unset
and the v1 truncate-flag path handles oversized bodies fine. If you do
want spillover locally, point at a LocalStack or MinIO endpoint via the
standard `AWS_ENDPOINT_URL_S3` env var.
