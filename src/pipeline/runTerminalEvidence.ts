// platform-api/src/pipeline/runTerminalEvidence.ts
//
// Before the orphan scan interrupts a `running` row, and before the resume
// scan continues an `interrupted` one, ask: did this run already finish
// somewhere? The row is only one of several records a run leaves, and it is
// the one that can be stale — deck-revise 15655b83 completed, a late
// checkpoint put turned its row back to `running`, and the next platform
// restart "resumed" a finished run (NFR #100). e29e114 stops that write from
// landing; this stops a stale row that already exists (or one left by any
// other path) from re-running work.
//
// Evidence, cheapest first:
//   1. The row's own checkpoint: every step on the frontier is completed and
//      none of them has an outgoing edge in the run's definition — a resume
//      would have nothing to schedule.
//   2. Idempotency: the run's side effect already exists. deck-revise records
//      `producedBy.requestId` on the revision it writes, and the write is the
//      last stage, so a revision for this request means the run is done.
//   3. The audit trail: a `pipeline.run.completed` / `failed` / `cancelled`
//      event for this runId.
//
// Any lookup that fails counts as "no evidence" — the caller then does what it
// did before (interrupt / resume). A false "finished" would drop a live run's
// remaining work, so every rule answers only on positive proof.

import type { PipelineRunItem } from '../repositories/PipelineRunRepository';

export type TerminalEvidenceSource = 'checkpoint' | 'idempotency' | 'audit';

export interface TerminalEvidence {
  status: 'completed' | 'failed' | 'cancelled';
  /** When the run finished, as best known (the finish event, else the last event). */
  at: string;
  source: TerminalEvidenceSource;
  detail?: string;
}

export type TerminalEvidenceCheck = (item: PipelineRunItem) => Promise<TerminalEvidence | null>;

interface EdgeLike { source?: unknown }

function lastKnownAt(item: PipelineRunItem): string {
  return item.lastEventAt ?? item.checkpoint?.at ?? item.startedAt;
}

/**
 * Rule 1. The checkpoint after a step completes still lists that step on the
 * frontier (dc removes it after emitting), so "finished" is: a non-empty
 * frontier whose every step is completed and is a sink of the definition.
 */
export function checkpointEvidence(item: PipelineRunItem): TerminalEvidence | null {
  const cp = item.checkpoint;
  const definition = item.definitionSnapshot as { edges?: EdgeLike[] } | undefined;
  if (!cp || !Array.isArray(cp.currentStepIds) || cp.currentStepIds.length === 0) return null;
  if (!definition || !Array.isArray(definition.edges)) return null;
  const steps = (cp.steps ?? {}) as Record<string, { status?: unknown } | undefined>;
  for (const stepId of cp.currentStepIds) {
    if (steps[stepId]?.status !== 'completed') return null;
    if (definition.edges.some((edge) => edge?.source === stepId)) return null;
  }
  return {
    status: 'completed',
    at: lastKnownAt(item),
    source: 'checkpoint',
    detail: `frontier ${cp.currentStepIds.join(',')} completed with no outgoing edges`,
  };
}

/** Rule 2 — deck-revise: a revision produced for this request already exists. */
export function deckReviseEvidence(deps: {
  pipelineId: string;
  readRequest: (context: Record<string, unknown>) => { documentId: string; requestId: string };
  findRevisionForRequest: (documentId: string, requestId: string) => Promise<{ id: string; createdAt?: string } | null>;
}): TerminalEvidenceCheck {
  return async (item) => {
    if (item.pipelineId !== deps.pipelineId) return null;
    const context = item.checkpoint?.context;
    if (!context || typeof context !== 'object') return null;
    const { documentId, requestId } = deps.readRequest(context);
    if (!documentId || !requestId) return null;
    const revision = await deps.findRevisionForRequest(documentId, requestId);
    if (!revision) return null;
    return {
      status: 'completed',
      // A revision older than this run (the request was retried under a new
      // run) must not date the finish before the run started.
      at: revision.createdAt && revision.createdAt > item.startedAt ? revision.createdAt : lastKnownAt(item),
      source: 'idempotency',
      detail: `revision ${revision.id} already produced for request ${requestId}`,
    };
  };
}

const AUDIT_TERMINAL: Record<string, TerminalEvidence['status']> = {
  'pipeline.run.completed': 'completed',
  'pipeline.run.failed': 'failed',
  'pipeline.run.cancelled': 'cancelled',
};

/** Rule 3 — a terminal lifecycle event in the run's audit trail. */
export function auditEvidence(listTerminalEvents: (runId: string) => Promise<Array<{ action: string; timestamp?: string }>>): TerminalEvidenceCheck {
  return async (item) => {
    const events = await listTerminalEvents(item.runId);
    // Newest first; a resume-from-step after a failure is a newer run.started,
    // so only a terminal event at or after the row's last event counts.
    const since = item.lastEventAt ?? '';
    const hit = events.find((event) => AUDIT_TERMINAL[event.action] && (event.timestamp ?? '') >= since);
    if (!hit) return null;
    return { status: AUDIT_TERMINAL[hit.action]!, at: hit.timestamp ?? lastKnownAt(item), source: 'audit', detail: hit.action };
  };
}

/** First positive answer wins; a check that throws is "no evidence". */
export function composeTerminalEvidence(...checks: TerminalEvidenceCheck[]): TerminalEvidenceCheck {
  return async (item) => {
    const fromRow = checkpointEvidence(item);
    if (fromRow) return fromRow;
    for (const check of checks) {
      try {
        const found = await check(item);
        if (found) return found;
      } catch {
        // Unknown is not finished.
      }
    }
    return null;
  };
}

/** The row to write when evidence says the run finished. */
export function finalizedRow(item: PipelineRunItem, evidence: TerminalEvidence, finalizedAt: string): PipelineRunItem {
  return {
    ...item,
    status: evidence.status,
    completedAt: evidence.at,
    finalizedFromEvidence: { source: evidence.source, at: finalizedAt, ...(evidence.detail ? { detail: evidence.detail } : {}) },
  } as PipelineRunItem;
}
