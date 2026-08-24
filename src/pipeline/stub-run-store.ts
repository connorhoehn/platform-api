import type { PipelineRunStatus } from '@connorhoehn/pipeline-runtime';

/**
 * Statuses this in-memory fallback store can hold.
 *
 * Derived from the shared `PipelineRunStatus` rather than re-declared, so this
 * store cannot drift from the canonical vocabulary again. It previously had
 * its own spellings (`succeeded`, `canceled`, `awaiting-approval`) — a third
 * dialect on top of the DynamoDB/executor split.
 *
 * `stuck` is excluded because nothing sets it on this path (StuckRunDetector
 * writes through the repository, not here).
 */
export type StubRunStatus = Exclude<PipelineRunStatus, 'stuck'>;

export interface StubRunRecord {
  runId: string;
  pipelineId: string;
  userId: string;
  status: StubRunStatus;
  triggeredAt: string;
  updatedAt: string;
  triggerPayload?: unknown;
  reason?: string;
}

const runRecords = new Map<string, StubRunRecord>();
const MAX_STUB_RUNS = 1000;

function evictOldestIfNeeded(): void {
  if (runRecords.size <= MAX_STUB_RUNS) return;
  const entries = Array.from(runRecords.entries()).sort((a, b) =>
    a[1].triggeredAt.localeCompare(b[1].triggeredAt),
  );
  const toDelete = entries.slice(0, runRecords.size - MAX_STUB_RUNS);
  for (const [runId] of toDelete) {
    runRecords.delete(runId);
  }
}

/**
 * Non-terminal statuses. Matches the frontend's active-run definition
 * (`PipelineRunsContext`), which also counts a breakpoint-paused run as live.
 */
export const ACTIVE_STATUSES: ReadonlySet<StubRunStatus> = new Set([
  'pending',
  'running',
  'awaiting_approval',
  'paused_at_breakpoint',
]);

export const stubRunStore = {
  put(record: StubRunRecord): void {
    runRecords.set(record.runId, record);
    evictOldestIfNeeded();
  },
  get(runId: string): StubRunRecord | undefined {
    return runRecords.get(runId);
  },
  listForPipeline(
    pipelineId: string,
    userId: string,
    opts: { limit?: number; cursor?: string } = {},
  ): { runs: StubRunRecord[]; nextCursor?: string } {
    const limit = Math.max(1, Math.min(100, opts.limit ?? 20));
    const all = Array.from(runRecords.values())
      .filter((r) => r.pipelineId === pipelineId && r.userId === userId)
      .sort((a, b) => (a.triggeredAt < b.triggeredAt ? 1 : -1));
    let startIdx = 0;
    if (opts.cursor) {
      const idx = all.findIndex((r) => r.runId === opts.cursor);
      startIdx = idx >= 0 ? idx + 1 : 0;
    }
    const page = all.slice(startIdx, startIdx + limit);
    const nextCursor =
      startIdx + limit < all.length ? page[page.length - 1]?.runId : undefined;
    return { runs: page, nextCursor };
  },
  listActive(userId: string): StubRunRecord[] {
    return Array.from(runRecords.values())
      .filter((r) => r.userId === userId && ACTIVE_STATUSES.has(r.status))
      .sort((a, b) => (a.triggeredAt < b.triggeredAt ? 1 : -1));
  },
  cancel(runId: string, userId: string, reason?: string): StubRunRecord | null {
    const r = runRecords.get(runId);
    if (!r || r.userId !== userId) return null;
    if (!ACTIVE_STATUSES.has(r.status)) return null;
    r.status = 'cancelled';
    r.updatedAt = new Date().toISOString();
    if (reason) r.reason = reason;
    return r;
  },
  __resetForTests(): void {
    runRecords.clear();
  },
};
