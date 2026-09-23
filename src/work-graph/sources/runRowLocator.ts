/**
 * Finds `pipeline-runs` rows by run id alone.
 *
 * The table is keyed `(pipelineId, runId)` and has no index on `runId`, so the
 * work graph — which names a run only by its id — found every run with a
 * filtered Scan of the whole table. A revision preview did that TWICE (once
 * for the source ACL, once for the revision listing), and the table is past
 * 1 MB, so every preview paid several Scan pages for one row.
 *
 * A run's `pipelineId` never changes once written, so the id → key mapping is
 * safe to remember: after a run has been found once, it is read by its key
 * (`BatchGetItem`). This remembers only that immutable mapping — never a row,
 * never a decision — so every read still returns the row as it is now, and
 * authorization is decided from it exactly as before.
 *
 * With two or more platform replicas each process learns its own mapping; a
 * miss (a fresh replica, an evicted id, a row that is gone) falls back to the
 * Scan, so a replica never needs another one's memory to be correct. The
 * proper fix is a `runId` GSI on the table (NFR #43); this is the bridge.
 */
import { BatchGetCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

type Row = Record<string, unknown>;

/** DynamoDB's cap on `IN (…)` operands per expression, less headroom. */
const MAX_IN_OPERANDS = 90;
/** `BatchGetItem` takes at most 100 keys. */
const MAX_BATCH_KEYS = 100;
/** Stop a Scan after this many rows read, whatever it found. */
const MAX_SCANNED_ROWS = 20_000;

export class RunRowLocator {
  private readonly pipelineIds = new Map<string, string>();

  constructor(private readonly capacity = 10_000) {}

  /** For tests: how many run ids this process can read by key. */
  get known(): number {
    return this.pipelineIds.size;
  }

  /**
   * Every asked run's row, or null when there is none — or more than one row
   * claims the id, which discloses nothing (ambiguous provenance).
   */
  async rows(client: DynamoDBDocumentClient, table: string, runIds: readonly string[]): Promise<Map<string, Row | null>> {
    const wanted = [...new Set(runIds)].filter(Boolean);
    const found = new Map<string, Row | null>();
    const byKey = wanted.filter((id) => this.pipelineIds.has(id));
    const unknown = new Set(wanted.filter((id) => !this.pipelineIds.has(id)));

    for (let index = 0; index < byKey.length; index += MAX_BATCH_KEYS) {
      const chunk = byKey.slice(index, index + MAX_BATCH_KEYS);
      const response = await client.send(new BatchGetCommand({
        RequestItems: { [table]: { Keys: chunk.map((runId) => ({ pipelineId: this.pipelineIds.get(runId), runId })) } },
      }));
      for (const row of (response?.Responses?.[table] ?? []) as Row[]) {
        if (typeof row.runId === 'string' && chunk.includes(row.runId)) found.set(row.runId, row);
      }
      // Not returned (gone, or left unprocessed): forget the key and look again.
      for (const runId of chunk) {
        if (found.has(runId)) continue;
        this.pipelineIds.delete(runId);
        unknown.add(runId);
      }
    }

    const scan = [...unknown];
    for (let index = 0; index < scan.length; index += MAX_IN_OPERANDS) {
      const chunk = scan.slice(index, index + MAX_IN_OPERANDS);
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
          if (!id || !chunk.includes(id)) continue;
          matches.set(id, matches.has(id) ? null : item);
        }
        scanned += result?.ScannedCount ?? 0;
        startKey = result?.LastEvaluatedKey as Record<string, unknown> | undefined;
      } while (startKey && scanned < MAX_SCANNED_ROWS);
      for (const id of chunk) {
        const row = matches.get(id) ?? null;
        found.set(id, row);
        if (row && typeof row.pipelineId === 'string') this.remember(id, row.pipelineId);
      }
    }
    for (const id of wanted) if (!found.has(id)) found.set(id, null);
    return found;
  }

  private remember(runId: string, pipelineId: string): void {
    this.pipelineIds.delete(runId);
    this.pipelineIds.set(runId, pipelineId);
    while (this.pipelineIds.size > this.capacity) {
      const oldest = this.pipelineIds.keys().next().value;
      if (oldest === undefined) break;
      this.pipelineIds.delete(oldest);
    }
  }
}

/** One per process: the mapping it holds is immutable, so sharing it is safe. */
export const runRowLocator = new RunRowLocator();
