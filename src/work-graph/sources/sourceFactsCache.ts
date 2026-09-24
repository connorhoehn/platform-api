/**
 * Per-replica caches for the two activity-layer reads that repeated on every
 * work-graph build for data that had not changed (realtime-examples NFR #208,
 * Loop 33 (activity)).
 *
 * - A deck's revision facts: the newest page of `presentation-revisions` and
 *   each revision's slide list (a manifest blob). A revision is immutable once
 *   written — its item is keyed by version and its manifest by content — and
 *   every save moves the row's `revisionCount` / `headRevision`, which the
 *   build reads fresh on every request. Keyed by those, an entry can never be
 *   stale; a new save simply misses. This took the artifact read from three
 *   serial round trips (row → revision page → manifests) to one.
 * - A pipeline definition row, read to name a run's steps and agent. Keyed by
 *   the definitions cache's generation, which every replica bumps when any
 *   replica announces a definition write (Redis pub/sub,
 *   `platform-api:pipeline-definitions:invalidate`) and on its 60 s tick; a
 *   write that bypasses both is bounded by `DEFINITION_TTL_MS`.
 *
 * `x-graph-full-read: 1` builds read and fill neither (the drift check).
 */

import { fullReadsRequested } from '../buildMemo';
import { pipelineDefinitionsCache } from '../../pipeline/definitions-cache';

const MAX_ENTRIES = 2_000;
export const DEFINITION_TTL_MS = 60_000;

export interface SourceFactsCacheCounters { hits: number; misses: number }
export const sourceFactsCacheCounters: SourceFactsCacheCounters = { hits: 0, misses: 0 };

class BoundedCache<V> {
  private readonly entries = new Map<string, { value: Promise<V>; at: number }>();

  constructor(private readonly ttlMs: number, private readonly now: () => number = Date.now) {}

  read(key: string, load: () => Promise<V>): Promise<V> {
    if (fullReadsRequested()) return load();
    const found = this.entries.get(key);
    if (found && this.now() - found.at < this.ttlMs) {
      sourceFactsCacheCounters.hits += 1;
      return found.value;
    }
    sourceFactsCacheCounters.misses += 1;
    const value = load();
    this.entries.delete(key);
    this.entries.set(key, { value, at: this.now() });
    // A failed read is not remembered.
    value.catch(() => { if (this.entries.get(key)?.value === value) this.entries.delete(key); });
    while (this.entries.size > MAX_ENTRIES) this.entries.delete(this.entries.keys().next().value as string);
    return value;
  }

  clear(): void { this.entries.clear(); }
}

const revisionFacts = new BoundedCache<unknown>(Number.POSITIVE_INFINITY);
const definitionRows = new BoundedCache<unknown>(DEFINITION_TTL_MS);

/**
 * A deck's revision facts at one saved state. `revisionCount` must be a
 * number (rows written the item way); any other row is read as before.
 */
export function cachedRevisionFacts<V>(
  documentId: string,
  revisionCount: unknown,
  headRevision: unknown,
  load: () => Promise<V>,
): Promise<V> {
  if (typeof revisionCount !== 'number') return load();
  return revisionFacts.read(
    `${documentId}\u0000${revisionCount}\u0000${typeof headRevision === 'string' ? headRevision : ''}`,
    load,
  ) as Promise<V>;
}

/** One pipeline-definitions row, until the next announced definition write. */
export function cachedDefinitionRow<V>(userId: string, pipelineId: string, load: () => Promise<V>): Promise<V> {
  return definitionRows.read(
    `${pipelineDefinitionsCache.generation()}\u0000${userId}\u0000${pipelineId}`,
    load,
  ) as Promise<V>;
}

/** Tests only. */
export function clearSourceFactsCaches(): void {
  revisionFacts.clear();
  definitionRows.clear();
}
