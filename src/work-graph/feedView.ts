import { createHash } from 'crypto';
import {
  diffWorkGraphViewV2,
  type WorkGraphStreamViewV2,
  type WorkGraphViewPatchV2,
} from '@connorhoehn/realtime-modules/work-graph';
import { getRedisCacheClient } from '../lib/redis-client';
import { VIEW_BASE_TTL_SECONDS, viewBaseRedisKey } from './viewBase';

/**
 * Incremental views on the platform → gateway hop (the gateway's live feed).
 *
 * Every feed answer used to carry the whole activity view (50–80 KB), once per
 * batch plus once at the top level. Now each view the platform answers with is
 * named by a content hash (`viewId`); the gateway names the one it holds
 * (`baseViewId`) on its next call and gets a keyed patch against it — the same
 * `diffWorkGraphViewV2` the gateway uses toward the browser (NFR #66), so both
 * hops share one implementation.
 *
 * Where the state lives (loop 13): in this process (recent views, the fast
 * path) AND in the shared cache Redis under the view-base keys
 * (`wg:viewbase:<hash>`, 120 s, NFR #85/#88), written by whichever replica
 * answered. A call that lands on another platform replica reads the base back
 * from Redis instead of answering whole. The hash covers the scope (org,
 * viewer, person, day, timezone, window, mode) as well as the view's bytes, and
 * a base read from Redis is re-hashed before use, so a guessed, replayed or
 * other-scope id is a miss: the answer is whole — correct, only bigger. No
 * Redis, a slow Redis (> 150 ms) or an evicted key degrade the same way.
 */

const MAX_ENTRIES = 256;
const TTL_MS = 10 * 60_000;
const SHARED_TIMEOUT_MS = 150;

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

export interface FeedViewStore {
  get(key: string): Promise<string | null>;
  set(key: string, value: string, ttlSeconds: number): Promise<unknown>;
}

export type FeedViewEncoding =
  | { viewId: string; view: WorkGraphStreamViewV2; viewPatch?: undefined; baseViewId?: undefined }
  | { viewId: string; viewPatch: WorkGraphViewPatchV2; baseViewId: string; view?: undefined };

export interface FeedViewStats {
  whole: number;
  patchLocal: number;
  patchShared: number;
  sharedMiss: number;
}

/** Hex, not base64url: the id travels as `baseViewId`, which the replay route admits only as `[A-Za-z0-9][A-Za-z0-9._:@/-]*`. */
export function hashFeedView(scopeKey: string, serializedView: string): string {
  return createHash('sha256').update('wgfv1\n').update(scopeKey).update('\n').update(serializedView).digest('hex');
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  return Promise.race([
    p,
    new Promise<T>((_, reject) => { timer = setTimeout(() => reject(new Error('timeout')), ms); }),
  ]).finally(() => { if (timer) clearTimeout(timer); });
}

export class FeedViewCache {
  private readonly entries = new Map<string, Entry>();
  readonly stats: FeedViewStats = { whole: 0, patchLocal: 0, patchShared: 0, sharedMiss: 0 };

  constructor(
    private readonly options: {
      maxEntries?: number;
      ttlMs?: number;
      now?: () => number;
      /** The shared store; omitted = this process only. */
      store?: () => Promise<FeedViewStore | null>;
      sharedTimeoutMs?: number;
    } = {},
  ) {}

  /**
   * Remembers `view` (here and in the shared store) and encodes it for the
   * caller: a patch against `baseViewId` when that view is held for this same
   * scope, here or in the shared store, otherwise the whole view.
   */
  async encode(scope: FeedViewScope, view: WorkGraphStreamViewV2, baseViewId?: string): Promise<FeedViewEncoding> {
    const now = (this.options.now ?? Date.now)();
    const scopeKey = JSON.stringify([
      scope.organizationId, scope.viewerId, scope.personId, scope.day, scope.timezone,
      scope.windowStart ?? null, scope.windowEnd ?? null, scope.mode ?? null,
    ]);
    const serialized = JSON.stringify(view);
    const viewId = hashFeedView(scopeKey, serialized);
    const store = this.options.store ? await this.options.store().catch(() => null) : null;

    let base: WorkGraphStreamViewV2 | undefined;
    let shared = false;
    const local = baseViewId ? this.entries.get(baseViewId) : undefined;
    if (baseViewId) {
      if (local && local.scopeKey === scopeKey && now - local.at <= (this.options.ttlMs ?? TTL_MS)) {
        base = local.view;
      } else if (store) {
        base = await this.readShared(store, scopeKey, baseViewId);
        shared = base !== undefined;
        if (!shared) this.stats.sharedMiss += 1;
      }
      // A feed only ever patches against the latest view it was sent (another
      // scope naming the id does not get to drop it).
      if (baseViewId !== viewId && local?.scopeKey === scopeKey) this.entries.delete(baseViewId);
    }

    this.entries.delete(viewId);
    this.entries.set(viewId, { scopeKey, view, at: now });
    this.evict(now);
    if (store) {
      await withTimeout(Promise.resolve(store.set(viewBaseRedisKey(viewId), serialized, VIEW_BASE_TTL_SECONDS)),
        this.options.sharedTimeoutMs ?? SHARED_TIMEOUT_MS).catch(() => undefined);
    }

    if (base && baseViewId) {
      if (shared) this.stats.patchShared += 1; else this.stats.patchLocal += 1;
      // baseWatermark is not used on this hop (the view id is the base); 0 keeps the shared type.
      return { viewId, baseViewId, viewPatch: diffWorkGraphViewV2(base, view, 0) };
    }
    this.stats.whole += 1;
    return { viewId, view };
  }

  get size(): number {
    return this.entries.size;
  }

  private async readShared(store: FeedViewStore, scopeKey: string, baseViewId: string): Promise<WorkGraphStreamViewV2 | undefined> {
    try {
      const raw = await withTimeout(store.get(viewBaseRedisKey(baseViewId)), this.options.sharedTimeoutMs ?? SHARED_TIMEOUT_MS);
      // Re-hash: only the exact view issued for this scope may serve as a base.
      if (!raw || hashFeedView(scopeKey, raw) !== baseViewId) return undefined;
      return JSON.parse(raw) as WorkGraphStreamViewV2;
    } catch {
      return undefined;
    }
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

async function redisFeedViewStore(): Promise<FeedViewStore | null> {
  const client = await getRedisCacheClient().catch(() => null);
  return client
    ? { get: (key) => client.get(key), set: (key, value, ttlSeconds) => client.set(key, value, { EX: ttlSeconds }) }
    : null;
}

export const feedViewCache = new FeedViewCache({ store: redisFeedViewStore });
