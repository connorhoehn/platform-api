// The presentation blob sweeper (NFR #12, made reference-safe by NFR #48).
//
// Since the row diet (NFR #4) a deck's slide SVGs and older specs are stored by
// content hash — `deck-svg-<hash>.svg`, `deck-spec-<hash>.json` — so one blob
// backs every revision, and every deck, with that content. Nothing that looks
// at one deck, one revision or one item's age can know a blob is free. This
// sweeper does mark-and-sweep over the live rows instead:
//
//   1. list every `deck-` asset in each store, with its last-write time;
//   2. keep only those last written before `now - graceMs` in every store;
//   3. THEN scan every presentation row and collect every blob key it names;
//   4. delete each candidate no row names — conditionally, so a save that
//      rewrote the key after step 1 keeps it (the store refuses).
//
// Why that order is safe against a save running concurrently: a save either
// writes a blob (moving its last-write time, so the conditional delete refuses
// or the write lands after the delete and recreates it) before its row, or
// reuses a blob its own existing row already names — and that row is read in
// step 3, which starts after step 1. The grace period covers the window
// between a save's blob write and its row write (and a slow save generally).
//
// A revision that is no longer the head keeps its slide list in a manifest
// blob (`deck-slides-<hash>.json`, NFR #36), so the mark step opens every
// manifest a row names and marks the slides inside it as well. A manifest it
// cannot read makes the whole sweep delete nothing: the slides it names are
// unknown, and an unknown reference has to be treated as a live one.
//
// What it cleans up: blobs of a deleted deck no other deck shares, `.pptx`
// files a raced or failed save wrote under its attempt-unique key, and content
// blobs a losing attempt wrote that nobody adopted.
//
// Runs as a ClusterSweeper task (one replica, under the sweep-leader lease).
// Two sweepers overlapping — a lease that lapsed mid-sweep — is still safe:
// every delete is conditional and idempotent.

import { ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../lib/aws-clients';
import log from '../lib/logger';
import {
  DdbImageBlobStore,
  isAssetBlobStore,
  isSweepableAssetStore,
  type SweepableAssetStore,
} from '../pipeline/image/ImageBlobStore';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import { DOCUMENTS_TABLE, isSlideListKey } from './presentationDocuments';
import { ChunkedBlobStore } from './chunkedBlobStore';

/** Every key a presentation stores starts with this; nothing else does. */
export const DECK_BLOB_PREFIX = 'deck-';
/** A deck blob key: the `.pptx`, a slide `.svg`, or a spec `.json`. */
const DECK_BLOB_KEY = /^deck-[A-Za-z0-9._-]+\.(?:pptx|svg|json)$/;

export const DEFAULT_DECK_BLOB_GRACE_MS = 24 * 60 * 60 * 1000;

export function isDeckBlobKey(value: unknown): value is string {
  return typeof value === 'string' && DECK_BLOB_KEY.test(value);
}

/** A deck blob key embedded in a longer string (a URL, a part key, a handle). */
const EMBEDDED_DECK_BLOB_KEY = /deck-[A-Za-z0-9._-]+?\.(?:pptx|svg|json)(?![A-Za-z0-9])/g;

/**
 * Every deck blob key a row names, wherever it names it: `revisions[].asset`,
 * `revisions[].slides[].asset`, `revisions[].specRef`, `revisions[].slidesRef`
 * (whose manifest `sweepDeckBlobs` then opens), legacy
 * `revisionSlides`, and any field a later build adds. Walking every string is
 * deliberately over-inclusive — a false "referenced" only keeps a blob — and
 * a key embedded in a longer string (a URL, a `.pptx~part-N` part key) counts
 * as a reference too.
 */
export function collectBlobReferences(row: unknown, into: Set<string> = new Set()): Set<string> {
  const stack: unknown[] = [row];
  while (stack.length) {
    const value = stack.pop();
    if (typeof value === 'string') {
      if (DECK_BLOB_KEY.test(value)) into.add(value);
      else if (value.includes(DECK_BLOB_PREFIX)) for (const match of value.match(EMBEDDED_DECK_BLOB_KEY) ?? []) into.add(match);
    } else if (Array.isArray(value)) stack.push(...value);
    else if (value && typeof value === 'object') stack.push(...Object.values(value as Record<string, unknown>));
  }
  return into;
}

/**
 * Fields whose value, when present, must name a deck blob the sweeper
 * recognises: a `{ key, contentType, sizeBytes, sha256 }` ref (a bare key
 * string is accepted too).
 */
const KEY_FIELDS = new Set(['specRef', 'slidesRef']);

function namesDeckBlob(ref: unknown): boolean {
  return isDeckBlobKey(ref) || (!!ref && typeof ref === 'object' && isDeckBlobKey((ref as { key?: unknown }).key));
}

/**
 * Why a row (or a moved slide list) is in a shape the mark step cannot vouch
 * for, or null when every place it can name a blob is one the sweeper knows.
 * Any answer makes the whole sweep delete nothing: a reference the sweeper
 * cannot read is a reference it has to assume is live. It still marks what it
 * CAN read (to clear expiries), it just never turns the rest into deletes.
 */
export function unknownReferenceShape(row: unknown, where = 'row'): string | null {
  if (!row || typeof row !== 'object' || Array.isArray(row)) return `${where} is not an object`;
  const record = row as Record<string, unknown>;
  if (record.revisions !== undefined && !Array.isArray(record.revisions)) return `${where}.revisions is not a list`;
  if (record.revisionSlides !== undefined) {
    const slides = record.revisionSlides;
    if (!slides || typeof slides !== 'object' || Array.isArray(slides)) return `${where}.revisionSlides is not a map`;
    for (const [revisionId, list] of Object.entries(slides)) {
      if (!Array.isArray(list)) return `${where}.revisionSlides.${revisionId} is not a list`;
    }
  }
  // Every `asset`, `specRef`, `slidesRef` at any depth: a present value that is
  // not a recognised key is a reference in a form this build does not know.
  const stack: Array<{ value: unknown; path: string }> = [{ value: record, path: where }];
  while (stack.length) {
    const { value, path } = stack.pop()!;
    if (Array.isArray(value)) {
      value.forEach((item, index) => stack.push({ value: item, path: `${path}[${index}]` }));
      continue;
    }
    if (!value || typeof value !== 'object') continue;
    for (const [field, child] of Object.entries(value as Record<string, unknown>)) {
      const at = `${path}.${field}`;
      if (KEY_FIELDS.has(field) && child !== undefined && child !== null && !namesDeckBlob(child)) {
        return `${at} is ${JSON.stringify(child)?.slice(0, 80)}, not a deck blob key`;
      }
      if (field === 'asset' && child !== undefined && child !== null) {
        const key = (child as { key?: unknown })?.key;
        if (typeof child !== 'object' || typeof key !== 'string') return `${at} has no key`;
        if (key.startsWith(DECK_BLOB_PREFIX) && !isDeckBlobKey(key)) return `${at}.key ${JSON.stringify(key).slice(0, 80)} is not a deck blob key`;
      }
      if (child && typeof child === 'object') stack.push({ value: child, path: at });
    }
  }
  return null;
}

/** A source of every live presentation row. Must throw rather than return a partial set. */
export type LiveRowSource = () => AsyncIterable<Record<string, unknown>>;

/**
 * Every row in `crdt-documents`, page by page. Not filtered to presentations:
 * a filter would only drop rows DynamoDB has already read, and a row of some
 * other shape that names a deck blob (a seeded or legacy row, a later build's
 * field) must still protect it.
 */
export function scanPresentationRows(docClient: DynamoDBDocumentClient = defaultDocClient): LiveRowSource {
  return async function* rows() {
    let startKey: Record<string, unknown> | undefined;
    do {
      const page = await docClient.send(new ScanCommand({
        TableName: DOCUMENTS_TABLE,
        ...(startKey ? { ExclusiveStartKey: startKey } : {}),
      }));
      for (const item of page.Items ?? []) yield item as Record<string, unknown>;
      startKey = page.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (startKey);
  };
}

export interface DeckBlobSweepOptions {
  /** Stores to sweep, the durable copy FIRST: a key is removed from the next only once the previous let it go. */
  stores: SweepableAssetStore[];
  rows: LiveRowSource;
  graceMs?: number;
  now?: () => number;
  /** Cap on deletes per sweep, so one pass cannot run away. */
  maxDeletes?: number;
  /**
   * Reads a slide-list manifest a row names. Defaults to the first of `stores`
   * that can read blobs and holds the key. Null (or a throw) means unreadable,
   * and an unreadable manifest stops the sweep from deleting anything.
   */
  readBlob?: (key: string) => Promise<Buffer | null>;
}

export interface DeckBlobSweepResult {
  listed: number;
  candidates: number;
  rowsScanned: number;
  referenced: number;
  deleted: number;
  /** Candidates a store refused because a save touched them since listing. */
  refused: number;
  /** Slide-list manifests opened and marked through (NFR #36). */
  manifestsRead?: number;
  /** Referenced keys whose store-side expiry (a legacy DDB `ttlEpoch`) was cleared. */
  expiryCleared?: number;
  /** Set when the sweep deliberately deleted nothing. */
  skipped?: string;
}

function defaultBlobReader(stores: SweepableAssetStore[]): (key: string) => Promise<Buffer | null> {
  return async (key) => {
    for (const store of stores) {
      if (!isAssetBlobStore(store)) continue;
      const blob = await store.getBlob(key).catch(() => null);
      if (blob) return blob.body;
    }
    return null;
  };
}

export async function sweepDeckBlobs(opts: DeckBlobSweepOptions): Promise<DeckBlobSweepResult> {
  const graceMs = Math.max(opts.graceMs ?? DEFAULT_DECK_BLOB_GRACE_MS, 0);
  const cutoffMs = (opts.now ?? Date.now)() - graceMs;
  const maxDeletes = opts.maxDeletes ?? 5000;

  // 1-2. Candidates: listed in some store, and old in every store that has it.
  const perStore: Array<Map<string, number>> = [];
  const expiring: Array<Set<string>> = [];
  const oldEverywhere = new Map<string, boolean>();
  let listed = 0;
  for (const store of opts.stores) {
    const seen = new Map<string, number>();
    const expires = new Set<string>();
    for await (const entry of store.listAssets(DECK_BLOB_PREFIX)) {
      if (!isDeckBlobKey(entry.key)) continue;
      listed += 1;
      seen.set(entry.key, entry.touchedAtMs);
      if (entry.expiresAtMs !== undefined) expires.add(entry.key);
      const old = entry.touchedAtMs < cutoffMs;
      oldEverywhere.set(entry.key, (oldEverywhere.get(entry.key) ?? true) && old);
    }
    perStore.push(seen);
    expiring.push(expires);
  }
  const candidates = [...oldEverywhere].filter(([, old]) => old).map(([key]) => key);
  const result: DeckBlobSweepResult = { listed, candidates: candidates.length, rowsScanned: 0, referenced: 0, deleted: 0, refused: 0 };
  const anyExpiring = expiring.some((keys) => keys.size > 0);
  if (candidates.length === 0 && !anyExpiring) return result;

  // 3. Mark: every key any live row names. Started only after listing. A row
  // in a shape the sweeper cannot vouch for is still marked, but turns off
  // every delete below (fail closed).
  const referenced = new Set<string>();
  let unknownShape: string | null = null;
  for await (const row of opts.rows()) {
    result.rowsScanned += 1;
    collectBlobReferences(row, referenced);
    if (!unknownShape) {
      const why = unknownReferenceShape(row, `row ${String((row as { documentId?: unknown }).documentId ?? '?')}`);
      if (why) unknownShape = why;
    }
  }
  // 3b. A moved slide list protects its slides: open every manifest named and
  // mark what it names. Any manifest that will not read → delete nothing.
  const readBlob = opts.readBlob ?? defaultBlobReader(opts.stores);
  const opened = new Set<string>();
  let unreadable: string | null = null;
  for (;;) {
    const next = [...referenced].filter((key) => isSlideListKey(key) && !opened.has(key));
    if (!next.length) break;
    for (const key of next) {
      opened.add(key);
      const body = await readBlob(key).catch(() => null);
      let parsed: unknown;
      try { parsed = body ? JSON.parse(body.toString('utf8')) : undefined; } catch { parsed = undefined; }
      if (!Array.isArray(parsed)) {
        unreadable ??= key;
        continue;
      }
      collectBlobReferences(parsed, referenced);
      if (!unknownShape) {
        const why = parsed.map((slide, index) => unknownReferenceShape(slide, `${key}[${index}]`)).find(Boolean);
        if (why) unknownShape = why;
      }
    }
  }
  result.manifestsRead = opened.size - (unreadable ? 1 : 0);
  result.referenced = referenced.size;

  // 3c. A referenced key must not age out on the store's own clock: clear any
  // legacy expiry (DDB ttlEpoch) on it. Done whatever the delete decision is —
  // it only ever keeps bytes.
  let expiryCleared = 0;
  for (let index = 0; index < opts.stores.length; index += 1) {
    const store = opts.stores[index]!;
    if (!store.clearExpiry) continue;
    for (const key of expiring[index]!) {
      if (!referenced.has(key)) continue;
      if (await store.clearExpiry(key).catch(() => false)) expiryCleared += 1;
    }
  }
  if (expiryCleared) {
    result.expiryCleared = expiryCleared;
    log.info({ expiryCleared }, 'deck blob sweep: cleared the store expiry on referenced blobs');
  }

  if (unreadable) {
    result.skipped = `slide-list manifest ${unreadable} could not be read; refusing to delete while the slides it names are unknown`;
    log.warn({ key: unreadable, candidates: candidates.length }, `deck blob sweep: ${result.skipped}`);
    return result;
  }
  if (unknownShape) {
    result.skipped = `${unknownShape}; refusing to delete while a reference is in a shape this build does not know`;
    log.warn({ candidates: candidates.length }, `deck blob sweep: ${result.skipped}`);
    return result;
  }
  if (result.rowsScanned === 0) {
    // Old deck blobs and not one presentation row is far likelier to be a
    // scan of the wrong table (a prefix mismatch) than a store with no decks.
    result.skipped = 'no presentation rows found; refusing to treat every deck blob as unreferenced';
    log.warn({ candidates: candidates.length }, `deck blob sweep: ${result.skipped}`);
    return result;
  }

  // 4. Sweep: unreferenced candidates, conditionally, durable store first.
  // Every key deleted is logged, so a sweep can always be audited afterwards.
  const deletedKeys: string[] = [];
  for (const key of candidates) {
    if (referenced.has(key)) continue;
    if (result.deleted >= maxDeletes) break;
    let removed = false;
    for (let index = 0; index < opts.stores.length; index += 1) {
      const touchedAtMs = perStore[index]!.get(key);
      if (touchedAtMs === undefined) continue;
      if (!(await opts.stores[index]!.deleteAssetIfUntouched(key, touchedAtMs))) {
        // A save wrote it since we listed it: it is live now, leave every copy.
        result.refused += 1;
        removed = false;
        break;
      }
      removed = true;
    }
    if (removed) {
      result.deleted += 1;
      deletedKeys.push(key);
    }
  }
  if (result.deleted || result.refused) log.info({ ...result, deletedKeys }, 'deck blob sweep complete');
  return result;
}

/**
 * The ClusterSweeper task: the same stores the presentation store writes (the
 * configured blob store plus its durable DDB copy), at most once per
 * `everyMs` (the ClusterSweeper ticks every minute; a table scan does not
 * need to). Returns null when the configured store cannot be swept.
 */
export function createDeckBlobSweepTask(opts: {
  graceMs?: number;
  everyMs?: number;
  now?: () => number;
  stores?: SweepableAssetStore[];
  rows?: LiveRowSource;
} = {}): { name: string; run: () => Promise<void> } | null {
  let stores = opts.stores;
  if (!stores) {
    const primary = getAssetBlobStore();
    // Mirrors DdbPresentationDocumentStore's default wiring.
    // Chunked (NFR #61): listing hides a large blob's parts; deleting its head deletes them.
    const mirror = primary instanceof DdbImageBlobStore ? null : new ChunkedBlobStore(new DdbImageBlobStore());
    const all: unknown[] = [mirror, primary instanceof DdbImageBlobStore ? new ChunkedBlobStore(primary) : primary].filter((store) => store !== null);
    if (!all.every(isSweepableAssetStore)) return null;
    stores = all.filter(isSweepableAssetStore);
  }
  const everyMs = opts.everyMs ?? 60 * 60 * 1000;
  const now = opts.now ?? Date.now;
  const rows = opts.rows ?? scanPresentationRows();
  let lastRunMs = -Infinity;
  return {
    name: 'deck-blob-sweep',
    run: async () => {
      if (now() - lastRunMs < everyMs) return;
      lastRunMs = now();
      await sweepDeckBlobs({ stores: stores!, rows, graceMs: opts.graceMs, now });
    },
  };
}
