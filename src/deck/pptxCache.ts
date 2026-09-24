/**
 * The `.pptx` of a revision, built once per content and kept (NFR #178,
 * realtime-examples Loop 29 ppt).
 *
 * A revision is immutable, so its file is a pure function of its spec and the
 * renderer. The key says exactly that: `deck-pptx-<sha256(renderer, spec)>.pptx`
 * — two revisions with the same spec (a revert, a re-save) name one blob, and a
 * renderer change (`PPTX_RENDER_VERSION`) names new ones instead of serving
 * stale bytes. The key matches the blob sweeper's `deck-….pptx` pattern, so a
 * key a row names is referenced — never swept, and its store expiry is cleared
 * (#12) — and one nothing names is swept after the grace period like any other.
 * A file named only by SUPERSEDED revisions counts as unnamed (Loop 33 ppt,
 * `regenerablePptxKey` in blobSweeper.ts): it is rebuilt from the revision's
 * spec on the next download, so old revisions do not pin their files forever.
 *
 * Built where:
 * - eagerly, at write, by the `/deck` step and `POST …/revisions` (they report
 *   the size), stored under the content key;
 * - lazily, on the first download, for a composer edit (`deck-revise`): most
 *   revisions of an edited deck are never downloaded, so the write skips the
 *   render and two blob puts. The first download builds it.
 *
 * A lazy build is single-flighted: same replica, one promise; across replicas,
 * the Redis lock pattern of `fixtureSingleFlight` (platform 9cc9bc4) — the
 * lock holder builds and stores, the others poll for its answer and read the
 * blob. Ten concurrent cold downloads are one build.
 *
 * Only a content key is ever built on read. A revision recorded under the old
 * per-revision key (`deck-<doc>-<rev>-<attempt>.pptx`) whose bytes are gone
 * stays a 404: regenerating those is an operator decision, not a side effect
 * of someone clicking Download.
 *
 * Warm reads come from a small in-process LRU of the bytes (immutable, so it
 * is never invalidated — only evicted).
 */
import log from '../lib/logger';
import { getRedisClient } from '../lib/redis-client';
import type { DeckSpec } from './DeckSpec';
import { contentKey, createSingleFlight, type SingleFlightRole, type SingleFlightStore } from './fixtureSingleFlight';
import { renderPptx } from './renderPptx';

/** Bump when renderPptx's output changes, so old keys are not served for new bytes. */
export const PPTX_RENDER_VERSION = 'pptx-r1';

/** A content-addressed `.pptx` key — the only kind built on read. */
export const PPTX_CONTENT_KEY = /^deck-pptx-[0-9a-f]{64}\.pptx$/;

function stable(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value as Record<string, unknown>).sort()
      .filter((key) => (value as Record<string, unknown>)[key] !== undefined)
      .map((key) => [key, stable((value as Record<string, unknown>)[key])]));
  }
  return value;
}

/** `deck-pptx-<sha256>.pptx` for this spec under this renderer. */
export function pptxContentKey(spec: DeckSpec): string {
  return `deck-pptx-${contentKey(PPTX_RENDER_VERSION, JSON.stringify(stable(spec)))}.pptx`;
}

// ---------------------------------------------------------------------------
// Warm bytes
// ---------------------------------------------------------------------------

const MEMORY_MAX_BYTES = Number(process.env.DECK_PPTX_MEMORY_BYTES ?? 48 * 1024 * 1024);
const memory = new Map<string, Buffer>();
let memoryBytes = 0;

export function rememberPptx(key: string, body: Buffer): void {
  if (body.byteLength > MEMORY_MAX_BYTES) return;
  const had = memory.get(key);
  if (had) { memory.delete(key); memoryBytes -= had.byteLength; }
  memory.set(key, body);
  memoryBytes += body.byteLength;
  for (const [oldest, bytes] of memory) {
    if (memoryBytes <= MEMORY_MAX_BYTES) break;
    memory.delete(oldest);
    memoryBytes -= bytes.byteLength;
  }
}

export function recalledPptx(key: string): Buffer | null {
  const body = memory.get(key);
  if (!body) return null;
  memory.delete(key);
  memory.set(key, body); // most recently used last
  return body;
}

/** Tests and measurements. */
export function resetPptxCache(): void { memory.clear(); memoryBytes = 0; builds = 0; }

// ---------------------------------------------------------------------------
// One build per content
// ---------------------------------------------------------------------------

let builds = 0;
/** Renders on this process since start (or the last reset). */
export function pptxBuildCount(): number { return builds; }

const flight = createSingleFlight<{ key: string; sizeBytes: number; built: boolean }>({
  store: async () => (await getRedisClient()) as unknown as SingleFlightStore | null,
  prefix: 'deck:pptx:flight',
  lockTtlMs: 30_000,
  resultTtlMs: 15_000,
  pollMs: 100,
});

export type PptxBuildSource = 'built' | 'joined-local' | 'joined-remote' | 'stored';

export interface PptxBuildIo {
  /** The bytes under `key` in either store, or null. */
  read(key: string): Promise<Buffer | null>;
  /** Store `body` under `key` (durable copy first). */
  write(key: string, body: Buffer): Promise<void>;
}

/**
 * The `.pptx` for `spec` under `key`, built at most once among everyone asking
 * at the same time on any replica. `source` says whether this caller built it,
 * shared another caller's build, or found it stored after all.
 */
export async function buildPptxOnce(key: string, spec: DeckSpec, io: PptxBuildIo, singleFlight = true): Promise<{ body: Buffer; source: PptxBuildSource }> {
  let role: SingleFlightRole = 'ran';
  const run = async () => {
    // A replica that finished just before this one took the lock left it stored.
    const stored = await io.read(key);
    if (stored) { rememberPptx(key, stored); return { key, sizeBytes: stored.byteLength, built: false }; }
    const started = Date.now();
    const body = await renderPptx(spec);
    builds += 1;
    await io.write(key, body);
    rememberPptx(key, body);
    log.info({ key, sizeBytes: body.byteLength, ms: Date.now() - started }, 'deck pptx built on first download');
    return { key, sizeBytes: body.byteLength, built: true };
  };
  const answer = singleFlight ? await flight(key, run, (r) => { role = r; }) : await run();
  const body = recalledPptx(key) ?? await io.read(key);
  if (!body) throw new Error(`deck pptx ${key} was built but cannot be read back`);
  const source: PptxBuildSource = role === 'ran' ? (answer.built ? 'built' : 'stored') : role;
  return { body, source };
}
