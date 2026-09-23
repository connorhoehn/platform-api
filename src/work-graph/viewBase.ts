import { createHash } from 'crypto';
import type { WorkGraphSnapshotV2 } from '@connorhoehn/realtime-modules/work-graph';

/**
 * NFR #85. The browser fetches a v2 snapshot, then subscribes; the first
 * stream frame used to carry the whole activity view (52–74 KB) although the
 * browser had just received that view in the snapshot. A snapshot asked with
 * `viewBase=1` names its view by a content hash and publishes the view to
 * Redis under it; the subscribe echoes the hash, and the gateway — whichever
 * replica holds the socket — reads the view back, recomputes the hash, and
 * patches the first frame against it.
 *
 * The hash covers the scope (organization, viewer, person, day, timezone) and
 * the snapshot's watermark as well as the view, so a hash can only prove the
 * base of the stream it was issued for: another viewer's hash, or one from an
 * older snapshot, is a miss and that stream gets the whole view. Nothing here
 * is process memory — any platform replica may answer the snapshot and any
 * gateway replica may read it.
 *
 * The gateway keeps an identical copy of `hashViewBase` (src/work-graph/
 * viewBase.ts there); the shared test vector in both suites keeps them equal.
 */
/**
 * The base is needed from the snapshot answer to the subscribe (~1–2 s; a
 * reconnect fetches a fresh snapshot). Two minutes covers a slow client
 * without holding a 50–75 KB view per snapshot for long. Identical snapshots
 * share one key. The key lives in the cache Redis (`allkeys-lru`, NFR #88), so
 * memory pressure evicts a base — a whole first frame — never coordination.
 */
export const VIEW_BASE_TTL_SECONDS = 120;
const PUBLISH_TIMEOUT_MS = 150;

export interface ViewBaseScope {
  organizationId: string;
  viewerId: string;
  personId: string;
  day: string;
  timezone: string;
  watermark: number;
}

export interface ViewBaseStore {
  set(key: string, value: string, ttlSeconds: number): Promise<unknown>;
}

export function viewBaseRedisKey(hash: string): string {
  return `wg:viewbase:${hash}`;
}

export function hashViewBase(scope: ViewBaseScope, serializedView: string): string {
  return createHash('sha256')
    .update(['wgvb1', scope.organizationId, scope.viewerId, scope.personId, scope.day, scope.timezone,
      String(scope.watermark)].join('\n'))
    .update('\n')
    .update(serializedView)
    .digest('base64url');
}

/** The fields a v2 stream frame's view carries, in this order: what a patch applies to. */
export function serializeViewBase(snapshot: WorkGraphSnapshotV2): string {
  const { query, temporal, efforts, details, operations, eventBuckets } = snapshot;
  return JSON.stringify({ query, temporal, efforts, details, operations, eventBuckets });
}

/**
 * Publishes the snapshot's view and returns its hash, or null when it could
 * not be stored in time — the snapshot then goes out without a hash and the
 * stream's first frame is whole, exactly as before.
 */
export async function publishViewBase(
  store: ViewBaseStore | null,
  scope: ViewBaseScope,
  snapshot: WorkGraphSnapshotV2,
): Promise<string | null> {
  if (!store) return null;
  const serialized = serializeViewBase(snapshot);
  const hash = hashViewBase(scope, serialized);
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    await Promise.race([
      store.set(viewBaseRedisKey(hash), serialized, VIEW_BASE_TTL_SECONDS),
      new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('timeout')), PUBLISH_TIMEOUT_MS); }),
    ]);
    return hash;
  } catch {
    return null;
  } finally {
    if (timer) clearTimeout(timer);
  }
}
