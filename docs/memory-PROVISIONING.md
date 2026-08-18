# Pipeline `memory` node — Operator Provisioning Runbook

The `memory` step handler (`src/pipeline/memory/`) gives a pipeline a
key/value store it can read from and write to mid-run. It has two backing
modes, selected by `PIPELINE_MEMORY_MODE`.

The application **does NOT** auto-create the table — IaC happens out-of-band
per the project's standing convention.

## Modes

| `PIPELINE_MEMORY_MODE` | Store | Use |
| --- | --- | --- |
| `memory` (**default**) | `InMemoryMemoryStore` | Local dev, tests, and any deployment that has not provisioned the table |
| `ddb` | `DdbMemoryStore` → `pipeline-memory` | Production |

The default is deliberately **not** `ddb`. Defaulting to DynamoDB would make
every `memory` node fail on a deployment that has not created the table yet.
The in-process default degrades to "memory works but does not survive a
restart", which is the better failure for a node that is opt-in per graph.

Set `PIPELINE_MEMORY_MODE=ddb` once the table below exists.

## Scopes

Scope is part of the **partition key**, not a filter, so one scope physically
cannot read another's rows.

| Scope | Partition key | Lifetime | Default TTL |
| --- | --- | --- | --- |
| `session` | `run/${runId}` | Private to a single run — two concurrent runs of the same pipeline cannot see each other's values | 1 day |
| `long-term` | `pipeline/${pipelineId}` | Shared across every run of the pipeline | 90 days |

There is no `sessionId` in the pipeline runtime — scoping is `runId` +
`pipelineId` — so `session` binds to the `runId`. The editor's scope help text
in `ui-components` (`MemoryConfig.tsx`) describes exactly this; the two must
not drift apart.

A `long-term` step whose `pipelineId` cannot be resolved **fails**. It does not
fall back to session scope, which would write a value no later run could ever
read.

## Table: `pipeline-memory`

### Schema

| Attribute | Type | Role |
| --- | --- | --- |
| `scopeKey` | S | Partition key — `run/${runId}` or `pipeline/${pipelineId}` |
| `itemKey` | S | Sort key — `${namespace}#${key}` |
| `value` | — | The stored value, at its original type (not stringified) |
| `ttlEpoch` | N | Unix-seconds — DDB native TTL attribute |

Namespaces are validated to exclude `#`, so `a` + `b#c` and `a#b` + `c` cannot
collide on the same sort key.

`DdbMemoryStore.get` also checks `ttlEpoch` on read. DynamoDB's TTL sweeper is
best-effort and can lag by up to ~48h; without the read-side check a `session`
value could outlive its own contract just because the sweeper is behind.

### `create-table`

```bash
aws dynamodb create-table \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-memory" \
  --attribute-definitions \
    AttributeName=scopeKey,AttributeType=S \
    AttributeName=itemKey,AttributeType=S \
  --key-schema \
    AttributeName=scopeKey,KeyType=HASH \
    AttributeName=itemKey,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

### Enable TTL

```bash
aws dynamodb update-time-to-live \
  --endpoint-url "${DDB_ENDPOINT:-http://localhost:8000}" \
  --table-name "${DDB_TABLE_PREFIX:-}pipeline-memory" \
  --time-to-live-specification "Enabled=true,AttributeName=ttlEpoch"
```

## Fallback behavior

- **Table missing while `PIPELINE_MEMORY_MODE=ddb`** — the DDB call throws and
  the handler returns `{status:'failed'}` for that step. It does not throw out
  of the executor, and it does not silently succeed.
- **Read miss** — a normal `completed` outcome with `{found: false, value: null}`.
  A first-run lookup of a long-term key legitimately finds nothing, so callers
  branch on `found` rather than on the step having failed.
- **Unresolved `{{context.*}}` path in the key expression** — the step
  **fails**. The executor's own `interpolate()` renders a missing path as an
  empty string, which is right for an LLM prompt but catastrophic for a key:
  every run with a missing path would collapse onto the same key and read back
  each other's values.
