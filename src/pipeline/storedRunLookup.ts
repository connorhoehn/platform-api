// pipeline/storedRunLookup.ts
//
// The durable half of `GET /api/pipelines/:pipelineId/runs/:runId`.
//
// It used to list the pipeline's newest 200 rows and filter them for the one
// asked about: every poll of one card read up to 200 rows (the chat's agent
// cards poll every 1.5 s — realtime-examples NFR #136 measured 104 reads in
// 20 s from an idle chat), and a run older than that window was a 404.
// Now it is one keyed read.
//
// An /agent loop answers before its run exists: the command's row is written
// under the loop id and later points at the executor's run
// (`agentLoopRunId`). That row stays `pending` for ever — it is a pointer, not
// the run — so a card that asked about the loop id polled a run that never
// settles. The pointer is followed: the answer is the executor run's own
// state (live if the bridge still has it), under the id the caller asked
// about, with `executorRunId` naming the run whose `pipeline:run:<id>` frames
// carry it.

import type { PipelineRunItem } from '../repositories/PipelineRunRepository';
import { runItemToSnapshot } from './createBridge';

export interface StoredRunLookupDeps {
  get(pipelineId: string, runId: string): Promise<PipelineRunItem | null>;
  /** The bridge's in-memory snapshot, when it still holds the run. */
  live?(runId: string): Promise<Record<string, unknown> | null>;
}

export async function resolveStoredRun(
  pipelineId: string,
  runId: string,
  deps: StoredRunLookupDeps,
): Promise<Record<string, unknown> | null> {
  const item = await deps.get(pipelineId, runId);
  if (!item) return null;
  const executorRunId = typeof item.agentLoopRunId === 'string' && item.agentLoopRunId && item.agentLoopRunId !== runId
    ? item.agentLoopRunId
    : undefined;
  if (executorRunId) {
    const live = deps.live ? await deps.live(executorRunId).catch(() => null) : null;
    if (live) return { ...live, runId, executorRunId };
    const executor = await deps.get(pipelineId, executorRunId);
    if (executor) return { ...storedRunDetail(executor), runId, executorRunId };
  }
  return storedRunDetail(item);
}

/**
 * One stored run for the DETAIL endpoint: the list row plus the definition the
 * run executed (`pipelineDefinitionSnapshot`, as a live or remote snapshot
 * already carries it). A card orders its steps by it; without it the client
 * read `GET /api/pipelines/:pipelineId`, which is the CALLER's definition —
 * a 404 for anyone but the owner, and for a command pipeline (`deck-generate`)
 * until its first run since the pod started (realtime-examples NFR #184).
 */
export function storedRunDetail(item: PipelineRunItem): Record<string, unknown> {
  return {
    ...(runItemToSnapshot(item) as unknown as Record<string, unknown>),
    ...(item.definitionSnapshot !== undefined ? { pipelineDefinitionSnapshot: item.definitionSnapshot } : {}),
  };
}
