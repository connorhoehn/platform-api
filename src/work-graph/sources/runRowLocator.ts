/**
 * Finds `pipeline-runs` rows by run id alone.
 *
 * The table is keyed `(pipelineId, runId)`; the work graph names a run only by
 * its id. Lookups Query the `runId-index` GSI (hash `runId`, projection ALL):
 * one indexed read per run, the same on every replica, with no per-process
 * memory to warm or to disagree (NFR #43 follow-up — this replaced a
 * per-process runId → pipelineId map that each replica learned from Scans).
 *
 * A table without the index (an environment provisioned before it was
 * declared) falls back to the filtered, paged Scan, logs that once, and looks
 * for the index again after `INDEX_RECHECK_MS` — so adding the GSI to a live
 * table takes effect without a restart.
 *
 * Every read returns the row as it is now; nothing is cached. More than one
 * row claiming an id resolves to null (ambiguous provenance discloses nothing).
 */
import { BatchGetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import log from '../../lib/logger';
import { buildMemoActive, fullReadsRequested, memoHas, memoOnce } from '../buildMemo';
import { runKeyStore, type RunKeyStore } from './runKeyMap';

const memoKey = (table: string, runId: string): string => `run-row\u0000${table}\u0000${runId}`;

type Row = Record<string, unknown>;

/** The GSI on `pipeline-runs` by `runId` (infra/dynamodb-schemas.json, k8s init-dynamodb.sh). */
export const PIPELINE_RUNS_RUN_ID_INDEX = 'runId-index';

/** DynamoDB's cap on `IN (…)` operands per expression, less headroom. */
const MAX_IN_OPERANDS = 90;
/** Stop a Scan after this many rows read, whatever it found. */
const MAX_SCANNED_ROWS = 20_000;
/** Index Queries in flight at once for one call. */
const QUERY_CONCURRENCY = 16;
/** How long a table found without the index is scanned before the index is tried again. */
const INDEX_RECHECK_MS = 5 * 60_000;
/** DynamoDB's cap on keys per `BatchGetItem`. */
const BATCH_GET_KEYS = 100;

/**
 * What a graph build reads of a run row (NFR #157): every attribute a run row
 * carries except the two a build never looks at — `definitionSnapshot` (the
 * pipeline's full definition, copied onto each run) and `checkpoint` beyond
 * `checkpoint.context.plan` (the engine's resume state). They are about half
 * of a row's bytes, and a 100-run BatchGetItem took ~100 ms p50 in full
 * against ~40 ms projected. Only the build memo's keyed path uses it; every
 * other caller still reads whole rows. An attribute added to run rows later
 * that a graph reader needs must be named here.
 */
const GRAPH_RUN_ATTRIBUTES = [
  'pipelineId', 'runId', 'status', 'triggeredBy', 'triggerType', 'organizationId', 'ownerNodeId',
  'startedAt', 'completedAt', 'lastEventAt', 'interruptedAt', 'resumedAt', 'resumeCount', 'resumeClaimedBy',
  'stepsSummary', 'steps', 'error', 'pause', 'review', 'workGraph', 'finalizedFromEvidence',
  'agentLoopRunId', 'runOverrides', 'estimatedCostUsd', 'tokenUsage', 'ttl',
  'createdBy', 'startedBy', 'participants', 'workItem', 'summary', 'title',
] as const;
const GRAPH_RUN_PROJECTION = {
  ProjectionExpression: [...GRAPH_RUN_ATTRIBUTES.map((_, index) => `#a${index}`), '#ck.#ctx.#plan'].join(', '),
  ExpressionAttributeNames: {
    ...Object.fromEntries(GRAPH_RUN_ATTRIBUTES.map((name, index) => [`#a${index}`, name])),
    '#ck': 'checkpoint', '#ctx': 'context', '#plan': 'plan',
  },
};
/** Rounds of `UnprocessedKeys` retried before those runs go back to the index. */
const BATCH_GET_RETRIES = 4;

/** DynamoDB's answer when the named index does not exist on the table. */
export function isMissingIndexError(err: unknown): boolean {
  const e = err as { name?: string; message?: string } | null;
  return !!e && (e.name === 'ValidationException' || e.name === 'ResourceNotFoundException')
    && /index/i.test(e.message ?? '');
}

export class RunRowLocator {
  /** Tables seen without the index, and when — scanned until the recheck. */
  private readonly missingIndexAt = new Map<string, number>();
  private readonly warned = new Set<string>();

  /**
   * `keys` — remembered `runId → pipelineId` (NFR #147): a remembered run is
   * read by its table key in a `BatchGetItem`, the rest by the index. Null
   * reads every run through the index.
   */
  constructor(
    private readonly now: () => number = Date.now,
    private readonly keys: RunKeyStore | null = null,
  ) {}

  /** For tests and logs: is this table currently served by the index? */
  usesIndex(table: string): boolean {
    const at = this.missingIndexAt.get(table);
    return at === undefined || this.now() - at >= INDEX_RECHECK_MS;
  }

  /**
   * Every asked run's row, or null when there is none — or more than one row
   * claims the id, which discloses nothing (ambiguous provenance).
   */
  async rows(client: DynamoDBDocumentClient, table: string, runIds: readonly string[]): Promise<Map<string, Row | null>> {
    const wanted = [...new Set(runIds)].filter(Boolean);
    if (!wanted.length) return new Map();
    if (buildMemoActive()) {
      // Within one graph build a run is read once: the ACL pass and the
      // activity layer both ask for every run (NFR #16). Ids no one in this
      // build has asked for are read together, as one call.
      const missing = wanted.filter((id) => !memoHas(memoKey(table, id)));
      if (missing.length) {
        const batch = this.readRows(client, table, missing, !fullReadsRequested());
        for (const id of missing) {
          memoOnce(memoKey(table, id), () => batch.then((found) => found.get(id) ?? null)).catch(() => undefined);
        }
      }
      const answers = await Promise.all(wanted.map(async (id) => [id, await memoOnce(memoKey(table, id), () => this.readRows(client, table, [id], !fullReadsRequested()).then((found) => found.get(id) ?? null))] as const));
      return new Map(answers);
    }
    return this.readRows(client, table, wanted);
  }

  private async readRows(
    client: DynamoDBDocumentClient,
    table: string,
    wanted: string[],
    projected = false,
  ): Promise<Map<string, Row | null>> {
    if (!this.keys || !this.usesIndex(table)) return this.readUnkeyed(client, table, wanted);
    const known = await this.keys.get(table, wanted).catch(() => new Map<string, string>());
    const found = new Map<string, Row | null>();
    if (known.size) {
      const byKey = await this.viaKeys(
        client, table, [...known].map(([runId, pipelineId]) => ({ pipelineId, runId })), projected,
      );
      for (const [runId, row] of byKey) found.set(runId, row);
    }
    // Not remembered, or remembered and not where the key said (row gone,
    // throttled past the retries): the index answers, and fills the map.
    const rest = wanted.filter((id) => !found.has(id));
    if (!rest.length) return found;
    const read = await this.readUnkeyed(client, table, rest);
    const fills: Array<[string, string]> = [];
    const stale: string[] = [];
    for (const [runId, row] of read) {
      found.set(runId, row);
      if (row && typeof row.pipelineId === 'string') fills.push([runId, row.pipelineId]);
      else if (known.has(runId)) stale.push(runId);
    }
    if (this.usesIndex(table)) {
      // Only an index answer proves a key unique; a Scan fallback fills nothing.
      void this.keys.fill(table, fills);
      void this.keys.drop(table, stale);
    }
    return found;
  }

  /** Remembered runs by their table key, 100 a call; `UnprocessedKeys` retried. */
  private async viaKeys(
    client: DynamoDBDocumentClient,
    table: string,
    keys: Array<{ pipelineId: string; runId: string }>,
    projected = false,
  ): Promise<Map<string, Row>> {
    const found = new Map<string, Row>();
    const pipelineOf = new Map(keys.map((key) => [key.runId, key.pipelineId]));
    const chunks: Array<Array<Record<string, unknown>>> = [];
    for (let index = 0; index < keys.length; index += BATCH_GET_KEYS) chunks.push(keys.slice(index, index + BATCH_GET_KEYS));
    await Promise.all(chunks.map(async (chunk) => {
      let pending: Array<Record<string, unknown>> | undefined = chunk;
      for (let round = 0; pending?.length && round <= BATCH_GET_RETRIES; round += 1) {
        if (round > 0) await new Promise((resolve) => setTimeout(resolve, 20 * 2 ** round));
        const result = await client.send(new BatchGetCommand({
          RequestItems: { [table]: { Keys: pending, ...(projected ? GRAPH_RUN_PROJECTION : {}) } },
        }));
        for (const item of (result?.Responses?.[table] ?? []) as Row[]) {
          const runId = typeof item.runId === 'string' ? item.runId : undefined;
          if (!runId || pipelineOf.get(runId) !== item.pipelineId || isIndexRow(item)) continue;
          found.set(runId, item);
        }
        pending = result?.UnprocessedKeys?.[table]?.Keys as Array<Record<string, unknown>> | undefined;
      }
    }));
    return found;
  }

  private async readUnkeyed(client: DynamoDBDocumentClient, table: string, wanted: string[]): Promise<Map<string, Row | null>> {
    if (this.usesIndex(table)) {
      try {
        const found = await this.viaIndex(client, table, wanted);
        this.missingIndexAt.delete(table);
        return found;
      } catch (err) {
        if (!isMissingIndexError(err)) throw err;
        this.missingIndexAt.set(table, this.now());
        if (!this.warned.has(table)) {
          this.warned.add(table);
          log.warn({ table, index: PIPELINE_RUNS_RUN_ID_INDEX, err: (err as Error).message },
            'pipeline-runs has no runId index: run lookups fall back to a filtered Scan (add the GSI; see infra/dynamodb-schemas.json)');
        }
      }
    }
    return this.viaScan(client, table, wanted);
  }

  private async viaIndex(client: DynamoDBDocumentClient, table: string, wanted: string[]): Promise<Map<string, Row | null>> {
    const found = new Map<string, Row | null>();
    let next = 0;
    const worker = async (): Promise<void> => {
      while (next < wanted.length) {
        const runId = wanted[next++]!;
        const matches: Row[] = [];
        let startKey: Record<string, unknown> | undefined;
        do {
          const result = await client.send(new QueryCommand({
            TableName: table,
            IndexName: PIPELINE_RUNS_RUN_ID_INDEX,
            KeyConditionExpression: 'runId = :r',
            ExpressionAttributeValues: { ':r': runId },
            ...(startKey ? { ExclusiveStartKey: startKey } : {}),
          }));
          matches.push(...((result?.Items ?? []) as Row[]).filter((item) => item.runId === runId && !isIndexRow(item)));
          startKey = result?.LastEvaluatedKey as Record<string, unknown> | undefined;
        } while (startKey && matches.length < 2);
        found.set(runId, matches.length === 1 ? matches[0]! : null);
      }
    };
    await Promise.all(Array.from({ length: Math.min(QUERY_CONCURRENCY, wanted.length) }, worker));
    return found;
  }

  private async viaScan(client: DynamoDBDocumentClient, table: string, wanted: string[]): Promise<Map<string, Row | null>> {
    const found = new Map<string, Row | null>();
    for (let index = 0; index < wanted.length; index += MAX_IN_OPERANDS) {
      const chunk = wanted.slice(index, index + MAX_IN_OPERANDS);
      const values = Object.fromEntries(chunk.map((id, position) => [`:r${position}`, id]));
      const matches = new Map<string, Row | null>();
      let startKey: Record<string, unknown> | undefined;
      let scanned = 0;
      do {
        // Paged: one Scan call stops at 1 MB READ, before the filter applies.
        const result = await client.send(new ScanCommand({
          TableName: table,
          FilterExpression: `runId IN (${Object.keys(values).join(', ')})`,
          ExpressionAttributeValues: values,
          ...(startKey ? { ExclusiveStartKey: startKey } : {}),
        }));
        for (const item of (result?.Items ?? []) as Row[]) {
          const id = typeof item.runId === 'string' ? item.runId : undefined;
          if (!id || !chunk.includes(id) || isIndexRow(item)) continue;
          matches.set(id, matches.has(id) ? null : item);
        }
        scanned += result?.ScannedCount ?? 0;
        startKey = result?.LastEvaluatedKey as Record<string, unknown> | undefined;
      } while (startKey && scanned < MAX_SCANNED_ROWS);
      for (const id of chunk) found.set(id, matches.get(id) ?? null);
    }
    return found;
  }
}

/**
 * An agent loop's per-person-day pointer (`personDayIndexRow`,
 * `PA/chat/agentLoopRunner.ts`) carries the loop's run id under a
 * `loops-by-person#…` partition with `status: 'index'`. It is not a run.
 * Counted as one, every agent loop had two rows for its id, the ambiguity
 * rule below answered "no row", and the loop was withheld from the work
 * graph (realtime-examples NFR #134).
 */
function isIndexRow(item: Row): boolean {
  return item.status === 'index';
}

/**
 * One per process: it holds no rows, only whether each table has the index;
 * run keys live in the shared cache Redis (`runKeyMap`).
 */
export const runRowLocator = new RunRowLocator(Date.now, runKeyStore);
