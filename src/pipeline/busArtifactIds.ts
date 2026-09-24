// NFR #161 (realtime-examples): stamp `artifactIds` on the pipeline bus events.
//
// A step declares what it produced on its output's `artifacts` array
// (declaredArtifacts.ts). The work-graph events already carry that as
// `artifactIds`; the bus events did not, so a consumer that only hears the bus
// (the gateway's revision lineage: `producedByRunId` on a document revision)
// never learned which run wrote a document.
//
// This wraps the module EventBus's `publish` once, BEFORE the event is built,
// so every lane that carries it — dc pub-sub (the gateway's Redis pattern),
// the Redis relay and the MQ relay, which all read the published BusEvent —
// carries the same `artifactIds`:
//
//   pipeline:step:completed → the ids that step's output declares
//   pipeline:run:completed  → every id any step of the run declared, in order
//
// The run's set is per process: the executor that runs a run publishes all of
// its events from one node. A run resumed on another pod after a restart only
// knows the steps completed since the resume (the step events before it
// already carried their own ids).

import { declaredArtifactRefs } from './declaredArtifacts';

const MAX_RUN_ARTIFACTS = 50;
const MAX_TRACKED_RUNS = 2000;

type Publish = (type: string, payload: unknown, ...rest: unknown[]) => Promise<unknown>;

interface StampableBus {
  publish: Publish;
  __artifactIdsStamped?: boolean;
}

function withArtifactIds(payload: unknown, artifactIds: string[]): unknown {
  if (artifactIds.length === 0 || !payload || typeof payload !== 'object') return payload;
  if (Array.isArray((payload as { artifactIds?: unknown }).artifactIds)) return payload;
  return { ...(payload as Record<string, unknown>), artifactIds };
}

/** Wraps `eventBus.publish` in place; idempotent. Returns the bus. */
export function stampPipelineArtifactIds<T>(eventBus: T): T {
  const bus = eventBus as unknown as StampableBus;
  if (!bus || typeof bus.publish !== 'function' || bus.__artifactIdsStamped) return eventBus;
  const original = bus.publish.bind(bus);
  const byRun = new Map<string, string[]>();

  bus.publish = async (type: string, payload: unknown, ...rest: unknown[]) => {
    const runId = (payload as { runId?: unknown } | undefined)?.runId;
    if (typeof runId !== 'string' || !runId) return original(type, payload, ...rest);

    if (type === 'pipeline:step:completed') {
      const ids = declaredArtifactRefs((payload as { output?: unknown }).output, { pipelineId: '', runId })
        .map((ref) => ref.artifactId);
      if (ids.length > 0) {
        const seen = byRun.get(runId) ?? [];
        for (const artifactId of ids) {
          if (!seen.includes(artifactId) && seen.length < MAX_RUN_ARTIFACTS) seen.push(artifactId);
        }
        byRun.delete(runId); // re-insert: Map order is least recently touched first
        byRun.set(runId, seen);
        while (byRun.size > MAX_TRACKED_RUNS) byRun.delete(byRun.keys().next().value as string);
      }
      return original(type, withArtifactIds(payload, ids), ...rest);
    }

    if (type === 'pipeline:run:completed') {
      const ids = byRun.get(runId) ?? [];
      byRun.delete(runId);
      return original(type, withArtifactIds(payload, ids), ...rest);
    }

    if (type === 'pipeline:run:failed' || type === 'pipeline:run:cancelled') byRun.delete(runId);
    return original(type, payload, ...rest);
  };
  bus.__artifactIdsStamped = true;
  return eventBus;
}
