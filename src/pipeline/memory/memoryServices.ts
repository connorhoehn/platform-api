// platform-api/src/pipeline/memory/memoryServices.ts
//
// Single source of construction for the memory store, so the step handler and
// any future HTTP route share one instance and one piece of env wiring.
// Lazy, matching judgeServices / visionServices: env is read at first use, not
// at import time, so tests can set it per-case.

import {
  DdbMemoryStore,
  InMemoryMemoryStore,
  type MemoryStore,
} from './MemoryStore';

let _store: MemoryStore | null = null;

/**
 * Which backing store to use.
 *
 * Defaults to 'memory' (in-process) rather than 'ddb', because the
 * `pipeline-memory` table is provisioned by hand - see
 * docs/memory-PROVISIONING.md. Defaulting to DDB would make every memory node
 * fail on a deployment that has not created the table yet, whereas the
 * in-process default degrades to "memory works but does not survive a
 * restart", which is the better failure for a node that is opt-in per graph.
 *
 * Set PIPELINE_MEMORY_MODE=ddb once the table exists.
 */
export type PipelineMemoryMode = 'memory' | 'ddb';

export function resolveMemoryMode(): PipelineMemoryMode {
  const raw = (process.env.PIPELINE_MEMORY_MODE ?? 'memory').trim().toLowerCase();
  return raw === 'ddb' ? 'ddb' : 'memory';
}

/** Singleton MemoryStore for the configured mode. */
export function getMemoryStore(): MemoryStore {
  if (!_store) {
    _store =
      resolveMemoryMode() === 'ddb'
        ? new DdbMemoryStore()
        : new InMemoryMemoryStore();
  }
  return _store;
}

/** Test helper: drop the cached singleton so the next call re-reads env. */
export function resetMemoryStoreForTests(): void {
  _store = null;
}
