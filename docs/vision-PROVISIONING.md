# Vision (image:vision-analyze) — Operator Provisioning Runbook

This document captures the DynamoDB resources the `image:vision-analyze`
step depends on. The application code degrades gracefully when these
aren't provisioned (see _Fallback behavior_), but operators should
provision them before going live.

The application **does NOT** auto-create any of these — IaC happens
out-of-band per the project's standing convention. All substrate is
DynamoDB; there are no cloud-only services in the vision feature
(no S3, no Bedrock, no Rekognition).

## Tables

### `pipeline-images` (v1 — required)

DDB-backed blob store for image bytes uploaded via `POST /api/images`.
Read shape on `ImageRow` in `src/pipeline/image/ImageBlobStore.ts`.

Bodies are capped at **350 KB** server-side (`IMAGE_BLOB_SIZE_CAP_BYTES`)
to fit comfortably inside DDB's 400 KB per-item limit alongside metadata
and the TTL attribute. The upload route resizes images to a 1568px
long edge before persisting; typical phone photos land at 50–250 KB.

#### Schema

| Attribute | Type | Role |
| --- | --- | --- |
| `imageId` | S | Primary key — UUID generated server-side on upload |
| `body` | B | Re-encoded image bytes (EXIF-stripped via Sharp) |
| `mimeType` | S | `image/jpeg` \| `image/png` \| `image/webp` |
| `sizeBytes` | N | Body length after re-encode |
| `width`, `height` | N | Pixel dimensions after resize |
| `uploadedAt` | S | ISO-8601 timestamp |
| `uploadedBy` | S | `req.user.sub` of the uploader (optional) |
| `ttlEpoch` | N | Unix-seconds — DDB native TTL (default 30 days) |

#### `aws ddb create-table` command

```bash
aws dynamodb create-table \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-images" \
  --attribute-definitions AttributeName=imageId,AttributeType=S \
  --key-schema AttributeName=imageId,KeyType=HASH \
  --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb update-time-to-live \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-images" \
  --time-to-live-specification "Enabled=true, AttributeName=ttlEpoch"
```

Drop `--endpoint-url` for the default AWS DDB endpoint. DDB-local
ignores `--billing-mode` and the throughput args but accepts them
without error.

### `pipeline-vision-results` (v1 — required)

Per-call immutable record of each vision analysis. Read shape on
`StoredVisionResult` in `src/pipeline/image/VisionResultRepository.ts`.

#### Schema

| Attribute | Type | Role |
| --- | --- | --- |
| `visionId` | S | Primary key — UUID generated in `VisionStepHandler` |
| `pipelineId` | S | de-normalized; `pipeline-time-index` partition key |
| `runId` | S | de-normalized; `run-time-index` partition key |
| `stepId`, `imageId` | S | Refs back to the pipeline node + source image |
| `caption` | S | Freeform model analysis (always present) |
| `extracted` | M | Structured-output object when `outputSchema` was set |
| `rawModelText` | S | Verbatim model response (≤32 KB; stripped above) |
| `rawTextTruncated` | BOOL | True when the body was stripped |
| `rawTextKey` | S | Spillover key when a `pipeline-vision-rawtext` was wired |
| `model`, `tokensIn`, `tokensOut` | S/N | Telemetry for cost rollups |
| `imageTokens`, `cacheReadTokens`, `cacheCreationTokens` | N | Anthropic usage subset |
| `latencyMs`, `costUsd`, `cacheHit` | N/N/BOOL | Per-call metrics |
| `analyzedAt` | S | ISO-8601 — both GSI sort key |
| `ttlEpoch` | N | Unix-seconds — DDB native TTL (default 30 days) |

#### `aws ddb create-table` command

```bash
aws dynamodb create-table \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-vision-results" \
  --attribute-definitions \
    AttributeName=visionId,AttributeType=S \
    AttributeName=pipelineId,AttributeType=S \
    AttributeName=runId,AttributeType=S \
    AttributeName=analyzedAt,AttributeType=S \
  --key-schema AttributeName=visionId,KeyType=HASH \
  --global-secondary-indexes \
    'IndexName=pipeline-time-index,
     KeySchema=[{AttributeName=pipelineId,KeyType=HASH},{AttributeName=analyzedAt,KeyType=RANGE}],
     Projection={ProjectionType=ALL},
     ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
    'IndexName=run-time-index,
     KeySchema=[{AttributeName=runId,KeyType=HASH},{AttributeName=analyzedAt,KeyType=RANGE}],
     Projection={ProjectionType=ALL},
     ProvisionedThroughput={ReadCapacityUnits=5,WriteCapacityUnits=5}' \
  --billing-mode PROVISIONED \
  --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5

aws dynamodb update-time-to-live \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-vision-results" \
  --time-to-live-specification "Enabled=true, AttributeName=ttlEpoch"
```

### `pipeline-vision-rawtext` (v2 — optional spillover)

Not wired in v1: vision responses are typically small (caption + minimal
structured JSON), well under the 32 KB inline cap. If a real workload
starts tripping the cap, create this table and pass a `DdbRawTextStore`
pointing at it to `new VisionResultRepository({ rawTextStore })` in
`visionServices.ts`.

Schema + create command are identical to `pipeline-judgment-rawtext` —
swap the table name. See `docs/judge-PROVISIONING.md`.

## Environment variables

| Var | Default | Purpose |
| --- | --- | --- |
| `DDB_TABLE_PREFIX` | _empty_ | Local-dev table-name prefix when sharing one DDB-local instance |
| `ANTHROPIC_API_KEY` | _unset_ | Required — `VisionClient` calls Anthropic Messages API directly (the Claude Code CLI does not forward image content blocks, so we can't reuse the judge's OAuth path). Without this set, every vision step returns `failureReason: http-error` and emits `pipeline:image:vision:failed`. |
| `ANTHROPIC_API_URL` | `https://api.anthropic.com/v1/messages` | Override for Bedrock proxy or testing |

## Fallback behavior — what happens without each resource

| Missing | Effect |
| --- | --- |
| `pipeline-images` table | `POST /api/images` returns 500 on upload; `GET /api/images/:id` returns 404; vision steps emit `pipeline:image:vision:failed` with `reason: image-fetch`. |
| `pipeline-vision-results` table | Vision steps run end-to-end; `pipeline:image:vision:analyzed` events still fire (UI gets live results), but no historical record persists for later drill-in. `VisionResultRepository.persistVisionResult` logs a warning per `VisionStepHandler.ts` — persistence failure does NOT fail the step. |
| `ANTHROPIC_API_KEY` env var | Every call returns `failureReason: http-error` before fetch is attempted; the step emits `pipeline:image:vision:failed` with `reason: unknown` and `error: http-error`. |

## Local-dev (Tilt + DDB-local)

DDB-local is wired in `agent-hub/services/docker-compose.yml`. Bring it
up via `start-shared-services.sh`. Create both v1 tables at boot with
the commands above (they default to `--endpoint-url http://localhost:8000`).

Both tables persist across container restarts via the `agent-hub-ddb-data`
volume.

Set `ANTHROPIC_API_KEY` in the platform-api deployment's env. In Tilt
local-dev, the env is sourced from `.env` (not committed) — add the
key there and restart the platform-api process.
