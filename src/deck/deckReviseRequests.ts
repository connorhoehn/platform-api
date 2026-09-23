// One composer request → one run, across retries and across replicas.
//
// `POST /api/deck/revise-run` claims `(user, document, requestId)` before it
// starts a run: Redis `SET key 'pending' NX EX 24h`, then the run id once the
// engine answers. A retried POST — the browser's, a proxy's, or the same click
// landing on the other platform replica — reads the run id back instead of
// starting a second edit. A retry that arrives while the first is still
// starting waits a few seconds for the id.
//
// Without Redis (tests, a bare local run) the claim is per process, which is
// exactly as strong as the deployment: one replica.

import { getRedisClient } from '../lib/redis-client';

export const REQUEST_CLAIM_TTL_SECONDS = 24 * 60 * 60;
const PENDING = 'pending';

/** The slice of a Redis client this needs. */
export interface ClaimStore {
  set(key: string, value: string, options: { NX?: boolean; XX?: boolean; EX: number }): Promise<string | null>;
  get(key: string): Promise<string | null>;
  del(key: string): Promise<number>;
}

class MemoryClaimStore implements ClaimStore {
  private readonly values = new Map<string, { value: string; until: number }>();
  async set(key: string, value: string, options: { NX?: boolean; XX?: boolean; EX: number }): Promise<string | null> {
    const now = Date.now();
    const live = this.values.get(key);
    const exists = !!live && live.until > now;
    if (options.NX && exists) return null;
    if (options.XX && !exists) return null;
    this.values.set(key, { value, until: now + options.EX * 1000 });
    return 'OK';
  }
  async get(key: string): Promise<string | null> {
    const live = this.values.get(key);
    return live && live.until > Date.now() ? live.value : null;
  }
  async del(key: string): Promise<number> {
    return this.values.delete(key) ? 1 : 0;
  }
}

const memory = new MemoryClaimStore();
let override: ClaimStore | null = null;
/** Tests: a store to claim in (null = back to Redis / memory). */
export function setClaimStore(next: ClaimStore | null): void { override = next; }

async function store(): Promise<ClaimStore> {
  if (override) return override;
  const redis = await getRedisClient().catch(() => null);
  return (redis as unknown as ClaimStore | null) ?? memory;
}

export function requestClaimKey(userId: string, documentId: string, requestId: string): string {
  return `deck-revise:request:${userId}:${documentId}:${requestId}`;
}

export type RequestClaim =
  | { status: 'claimed'; commit(runId: string): Promise<void>; release(): Promise<void> }
  | { status: 'existing'; runId: string }
  | { status: 'in-flight' };

export async function claimDeckReviseRequest(
  args: { userId: string; documentId: string; requestId: string },
  opts: { waitMs?: number; pollMs?: number } = {},
): Promise<RequestClaim> {
  const key = requestClaimKey(args.userId, args.documentId, args.requestId);
  const claims = await store();
  if (await claims.set(key, PENDING, { NX: true, EX: REQUEST_CLAIM_TTL_SECONDS }) === 'OK') {
    return {
      status: 'claimed',
      commit: async (runId) => { await claims.set(key, runId, { EX: REQUEST_CLAIM_TTL_SECONDS }); },
      // The run never started: let a retry start it.
      release: async () => { if (await claims.get(key) === PENDING) await claims.del(key); },
    };
  }
  const waitMs = opts.waitMs ?? 5000;
  const pollMs = opts.pollMs ?? 200;
  const until = Date.now() + waitMs;
  for (;;) {
    const value = await claims.get(key);
    if (value && value !== PENDING) return { status: 'existing', runId: value };
    if (value === null) {
      // The first attempt released its claim (its trigger failed): this one may start.
      return claimDeckReviseRequest(args, { waitMs: Math.max(0, until - Date.now()), pollMs });
    }
    if (Date.now() >= until) return { status: 'in-flight' };
    await new Promise((resolve) => setTimeout(resolve, pollMs));
  }
}
