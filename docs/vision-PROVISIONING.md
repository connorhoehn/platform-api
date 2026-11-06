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
| `CLAUDE_CLI_PATH` | `claude` | Override the Claude Code CLI binary path. Defaults to whatever `claude` resolves to on PATH. |
| `CLAUDE_CODE_USE_BEDROCK` | `false` | When `true`, the CLI is invoked with Bedrock model aliases instead of the public Anthropic API. Useful when the host has Bedrock credentials but not an Anthropic key. |
| `ALLOW_INTERNAL_IMAGE_FETCH` | `false` | When `true`, the `POST /api/images/from-url` route allows fetching from private/loopback addresses. Required for test environments hitting in-cluster fixtures. Production should keep `false` (SSRF mitigation). |

## Auth (CLI subprocess)

`VisionClient` spawns `claude -p` and inherits whatever credentials
the host's CLI has. There is **no separate `ANTHROPIC_API_KEY`** to
manage — the same OAuth session that powers the operator's Claude Code
shell is what drives the vision step. In containerized deploys this
means mounting `~/.claude` (or the equivalent keychain volume) into
the pod; in local-dev it's automatic.

If the CLI can't authenticate (no session, expired token), the vision
step emits `pipeline:image:vision:failed` with the CLI's stderr in the
event's `error` field. Re-run `claude login` to refresh.

## Image input plumbing

The CLI's stream-json input format does NOT forward image content
blocks to the model (empirically verified — base64 images get
"I can't view the image." back). Instead, the client:

1. Writes the image bytes to a per-call tempdir under
   `/tmp/pipeline-vision/<uuid>/img.<ext>`.
2. Spawns the CLI with `--allowedTools Read --add-dir <tempdir>
   --permission-mode acceptEdits` and a prompt that references the
   file path: `"Read the image at <path> and ..."`.
3. The CLI's Read tool surfaces the image to the model as a base64
   content block automatically (Claude Code 2.1.x+).
4. Tempdir is cleaned up in `finalize()` on both success and failure.

## HTTP routes

### POST /api/images

Multipart upload of a raw image file. Server re-encodes via Sharp (strips
EXIF, resizes to 1568px long edge), validates Content-Type, and persists to
`pipeline-images`. Returns `UploadResponse`: `{imageId, mimeType, sizeBytes,
width, height}`.

### POST /api/images/from-url

Server-side fetch by URL. Useful when the client already has a public
image URL and doesn't want to re-upload bytes.

Body: `{ "url": "https://..." }` (JSON, max 4 KB)

Server-side: validates the URL's protocol (http/https only), rejects
private/loopback hosts (configurable via `ALLOW_INTERNAL_IMAGE_FETCH`),
follows up to 5 redirects with re-validation at every hop, fetches with
a 5s timeout, validates Content-Type against the same ALLOWED_MIME set
as POST /, caps body size before passing to Sharp.

Response: same `UploadResponse` shape as POST / — `{imageId, mimeType,
sizeBytes, width, height}`. Callers can treat both paths identically.

## Fallback behavior — what happens without each resource

| Missing | Effect |
| --- | --- |
| `pipeline-images` table | `POST /api/images` returns 500 on upload; `GET /api/images/:id` returns 404; vision steps emit `pipeline:image:vision:failed` with `reason: image-fetch`. |
| `pipeline-vision-results` table | Vision steps run end-to-end; `pipeline:image:vision:analyzed` events still fire (UI gets live results), but no historical record persists for later drill-in. `VisionResultRepository.persistVisionResult` logs a warning per `VisionStepHandler.ts` — persistence failure does NOT fail the step. |
| Claude Code CLI not on PATH | Every call returns `failureReason: unknown` with the spawn error in `rawText`. Set `CLAUDE_CLI_PATH` to an absolute path. |
| Claude Code CLI auth expired | Steps emit `pipeline:image:vision:failed` with the CLI's authentication error in `rawText`. Re-run `claude login`. |
| Bad `imageRefPath` config | Vision step emits `pipeline:image:vision:failed` with `reason: input-resolution` and the jsonpath traversal error in `error`. Common cause: typo in the JSONPath, or the path resolves through a non-object. Verify the path starts with `$.` and the resolved value is either a string imageId or an `{imageId}` object. |

## Local-dev (Tilt + DDB-local)

Run DynamoDB Local on `http://localhost:8000` (the `dev` script points
`LOCALSTACK_ENDPOINT` there). Create both v1 tables at boot with
the commands above (they default to `--endpoint-url http://localhost:8000`).

Mount a volume on the DynamoDB Local data directory if the tables should
survive container restarts.

For the CLI to work in the platform-api container, the image must
include the Claude Code binary and a mounted `~/.claude` directory.
The current Dockerfile installs `@anthropic-ai/claude-code` globally;
the operator mounts their host keychain volume at `/root/.claude` so
the OAuth session flows through.
