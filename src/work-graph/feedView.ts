import { randomUUID } from 'crypto';
import {
  diffWorkGraphViewV2,
  type WorkGraphStreamViewV2,
  type WorkGraphViewPatchV2,
} from '@connorhoehn/realtime-modules/work-graph';

/**
 * Incremental views on the platform → gateway hop (the gateway's live feed).
 *
 * Every feed answer used to carry the whole activity view (50–80 KB), once per
 * batch plus once at the top level. Now each view the platform answers with is
 * remembered under a random `viewId`; the gateway names the one it holds
 * (`baseViewId`) on its next call and gets a keyed patch against it — the same
 * `diffWorkGraphViewV2` the gateway uses toward the browser (NFR #66), so both
 * hops share one implementation.
 *
 * Where the state lives: in this process only. With 2+ platform replicas a
 * call that lands on a replica that did not answer the previous one misses and
 * gets the whole view — correct, only bigger. A view is only ever served back
 * to the scope it was computed for (org, viewer, person, day, timezone,
 * window), so a guessed or replayed id cannot cross viewers; ids are random
 * and the route is service-authenticated anyway.
 */

const MAX_ENTRIES = 256;
const TTL_MS = 10 * 60_000;

interface Entry {
  scopeKey: string;
  view: WorkGraphStreamViewV2;
  at: number;
}

export interface FeedViewScope {
  organizationId: string;
  viewerId: string;
  personId: string;
  day: string;
  timezone: string;
  windowStart?: string;
  windowEnd?: string;
  mode?: string;
}

export type FeedViewEncoding =
  | { viewId: string; view: WorkGraphStreamViewV2; viewPatch?: undefined; baseViewId?: undefined }
  | { viewId: string; viewPatch: WorkGraphViewPatchV2; baseViewId: string; view?: undefined };

export class FeedViewCache {
  private readonly entries = new Map<string, Entry>();

  constructor(
    private readonly options: { maxEntries?: number; ttlMs?: number; now?: () => number; id?: () => string } = {},
  ) {}

  /**
   * Remembers `view` and encodes it for the caller: a patch against
   * `baseViewId` when that view is still held for this same scope, otherwise
   * the whole view. The base entry is dropped once superseded — a feed only
   * ever patches against the latest view it was sent.
   */
  encode(scope: FeedViewScope, view: WorkGraphStreamViewV2, baseViewId?: string): FeedViewEncoding {
    const now = (this.options.now ?? Date.now)();
    const scopeKey = JSON.stringify([
      scope.organizationId, scope.viewerId, scope.personId, scope.day, scope.timezone,
      scope.windowStart ?? null, scope.windowEnd ?? null, scope.mode ?? null,
    ]);
    const base = baseViewId ? this.entries.get(baseViewId) : undefined;
    const usable = base && base.scopeKey === scopeKey && now - base.at <= (this.options.ttlMs ?? TTL_MS);
    if (baseViewId) this.entries.delete(baseViewId);
    const viewId = (this.options.id ?? randomUUID)();
    this.entries.set(viewId, { scopeKey, view, at: now });
    this.evict(now);
    if (usable && baseViewId) {
      // baseWatermark is not used on this hop (the view id is the base); 0 keeps the shared type.
      return { viewId, baseViewId, viewPatch: diffWorkGraphViewV2(base.view, view, 0) };
    }
    return { viewId, view };
  }

  get size(): number {
    return this.entries.size;
  }

  private evict(now: number): void {
    const ttl = this.options.ttlMs ?? TTL_MS;
    for (const [id, entry] of this.entries) {
      if (now - entry.at <= ttl) break;
      this.entries.delete(id);
    }
    const max = this.options.maxEntries ?? MAX_ENTRIES;
    while (this.entries.size > max) {
      const oldest = this.entries.keys().next().value;
      if (oldest === undefined) break;
      this.entries.delete(oldest);
    }
  }
}

export const feedViewCache = new FeedViewCache();
