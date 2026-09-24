// realtime-examples NFR #166: the per-slide `.svg` route paid, per slide, a
// DynamoDB row read + the read check, the slide list (a manifest blob read for
// an older revision), then `store.slidePreview` read the row and the manifest
// AGAIN before the bytes. Under N viewers × S slides those reads queue on the
// table (N=30: p50 0.8–0.9 s; the batch route, one pass per reader, 123–135 ms).
//
// What is shared here, per process:
//   - the row and the caller's read decision are JOINED while in flight: the
//     slide requests that arrive together (a reader opening a deck asks for
//     every slide at once; so do the other readers) share one DynamoDB row
//     read and one decision (one membership read for a chat reader). Nothing
//     is kept after it settles (READ_TTL_MS = 0), so a request that starts
//     after a revocation is decided on fresh reads, as every route is.
//   - a revision's slide list (immutable once written), LRU.
//   - slide bytes by (document, revision, slide) (immutable), LRU by size.

import type { PresentationDocumentRow, PresentationDocumentStore, StoredRevision, StoredSlide } from './presentationDocuments';
import { resolveRevisionSlides } from './presentationDocuments';

export const READ_TTL_MS = 0;
const MAX_ROWS = 500;
const MAX_DECISIONS = 2_000;
const MAX_LISTS = 300;
const MAX_BYTES = 16 * 1024 * 1024;

interface Timed<T> { at: number; value: Promise<T>; settled?: boolean }

class Lru<V> {
  private readonly map = new Map<string, V>();
  constructor(private readonly max: number) {}
  get(key: string): V | undefined {
    const value = this.map.get(key);
    if (value === undefined) return undefined;
    this.map.delete(key);
    this.map.set(key, value);
    return value;
  }
  set(key: string, value: V): void {
    this.map.delete(key);
    this.map.set(key, value);
    while (this.map.size > this.max) this.map.delete(this.map.keys().next().value as string);
  }
  delete(key: string): void { this.map.delete(key); }
  clear(): void { this.map.clear(); }
  get size(): number { return this.map.size; }
}

export interface SlideReadCacheStats { rowReads: number; rowJoins: number; decisions: number; decisionHits: number; listReads: number; byteReads: number; byteHits: number }

export class SlideReadCache {
  private readonly rows = new Lru<Timed<PresentationDocumentRow | null>>(MAX_ROWS);
  private readonly decisions = new Lru<Timed<boolean>>(MAX_DECISIONS);
  private readonly lists = new Lru<StoredSlide[]>(MAX_LISTS);
  private readonly bytes = new Map<string, Buffer>();
  private bytesTotal = 0;
  readonly stats: SlideReadCacheStats = { rowReads: 0, rowJoins: 0, decisions: 0, decisionHits: 0, listReads: 0, byteReads: 0, byteHits: 0 };

  constructor(private readonly now: () => number = Date.now, private readonly ttlMs = READ_TTL_MS) {}

  /** The document row; concurrent callers join one read (kept `ttlMs` after it lands, 0 by default). */
  row(store: PresentationDocumentStore, documentId: string): Promise<PresentationDocumentRow | null> {
    const hit = this.rows.get(documentId);
    if (hit && this.fresh(hit)) { this.stats.rowJoins += 1; return hit.value; }
    this.stats.rowReads += 1;
    const entry: Timed<PresentationDocumentRow | null> = { at: this.now(), value: store.get(documentId) };
    this.rows.set(documentId, entry);
    entry.value.then(() => this.settle(this.rows, documentId, entry), () => { entry.settled = true; this.rows.delete(documentId); });
    return entry.value;
  }

  private fresh<T>(entry: Timed<T>): boolean {
    return !entry.settled || this.now() - entry.at < this.ttlMs;
  }

  private settle<T>(lru: Lru<Timed<T>>, key: string, entry: Timed<T>): void {
    entry.settled = true;
    entry.at = this.now();
    if (this.ttlMs <= 0 && lru.get(key) === entry) lru.delete(key);
  }

  /** The caller's read decision, joined while in flight (kept `ttlMs` after, 0 by default); a refusal is never kept. */
  canRead(key: string, decide: () => Promise<boolean>): Promise<boolean> {
    const hit = this.decisions.get(key);
    if (hit && this.fresh(hit)) { this.stats.decisionHits += 1; return hit.value; }
    this.stats.decisions += 1;
    const entry: Timed<boolean> = { at: this.now(), value: decide() };
    this.decisions.set(key, entry);
    entry.value.then(
      (allowed) => (allowed ? this.settle(this.decisions, key, entry) : this.decisions.delete(key)),
      () => { entry.settled = true; this.decisions.delete(key); },
    );
    return entry.value;
  }

  /** A revision's slide list (inline, or its manifest), kept once read. */
  async slides(store: PresentationDocumentStore, documentId: string, revision: StoredRevision): Promise<StoredSlide[] | null> {
    const key = `${documentId}\u0000${revision.id}`;
    const hit = this.lists.get(key);
    if (hit) return hit;
    if (Array.isArray(revision.slides) && revision.slides.length) return revision.slides;
    this.stats.listReads += 1;
    const list = await resolveRevisionSlides(store, revision);
    if (list && list.length) this.lists.set(key, list);
    return list;
  }

  /** One slide's bytes, kept once read (a revision's slide never changes). */
  async slideBytes(key: string, read: () => Promise<Buffer | null>): Promise<Buffer | null> {
    const hit = this.bytes.get(key);
    if (hit) {
      this.stats.byteHits += 1;
      this.bytes.delete(key);
      this.bytes.set(key, hit);
      return hit;
    }
    this.stats.byteReads += 1;
    const body = await read();
    if (!body || body.byteLength > MAX_BYTES / 16) return body;
    this.bytes.set(key, body);
    this.bytesTotal += body.byteLength;
    while (this.bytesTotal > MAX_BYTES) {
      const oldest = this.bytes.keys().next().value as string;
      this.bytesTotal -= this.bytes.get(oldest)!.byteLength;
      this.bytes.delete(oldest);
    }
    return body;
  }

  clear(): void {
    this.rows.clear();
    this.decisions.clear();
    this.lists.clear();
    this.bytes.clear();
    this.bytesTotal = 0;
  }
}

let shared: SlideReadCache | null = null;
export function getSlideReadCache(): SlideReadCache {
  shared ??= new SlideReadCache();
  return shared;
}
/** Tests only. */
export function resetSlideReadCache(): void { shared = null; }
