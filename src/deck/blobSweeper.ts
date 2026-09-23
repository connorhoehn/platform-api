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
import { DOCUMENTS_TABLE, PRESENTATION_DOCUMENT_TYPE, isSlideListKey } from './presentationDocuments';
import { ChunkedBlobStore } from './chunkedBlobStore';

/** Every key a presentation stores starts with this; nothing else does. */
export const DECK_BLOB_PREFIX = 'deck-';
/** A deck blob key: the `.pptx`, a slide `.svg`, or a spec `.json`. */
const DECK_BLOB_KEY = /^deck-[A-Za-z0-9._-]+\.(?:pptx|svg|json)$/;

export const DEFAULT_DECK_BLOB_GRACE_MS = 24 * 60 * 60 * 1000;

export function isDeckBlobKey(value: unknown): value is string {
  return typeof value === 'string' && DECK_BLOB_KEY.test(value);
}

/**
 * Every deck blob key a row names, wherever it names it: `revisions[].asset`,
 * `revisions[].slides[].asset`, `revisions[].specRef`, `revisions[].slidesRef`
 * (whose manifest `sweepDeckBlobs` then opens), legacy
 * `revisionSlides`, and any field a later build adds. Walking every string is
 * deliberately over-inclusive — a false "referenced" only keeps a blob.
 */
export function collectBlobReferences(row: unknown, into: Set<string> = new Set()): Set<string> {
  const stack: unknown[] = [row];
  while (stack.length) {
    const value = stack.pop();
    if (isDeckBlobKey(value)) into.add(value);
    else if (Array.isArray(value)) stack.push(...value);
    else if (value && typeof value === 'object') stack.push(...Object.values(value as Record<string, unknown>));
  }
  return into;
}

/** A source of every live presentation row. Must throw rather than return a partial set. */
export type LiveRowSource = () => AsyncIterable<Record<string, unknown>>;

/** Every presentation row in `crdt-documents`, page by page. */
export function scanPresentationRows(docClient: DynamoDBDocumentClient = defaultDocClient): LiveRowSource {
  return async function* rows() {
    let startKey: Record<string, unknown> | undefined;
    do {
      const page = await docClient.send(new ScanCommand({
        TableName: DOCUMENTS_TABLE,
        // A row with revisions (or the pre-diet revisionSlides) of any type is
        // included: over-including only keeps blobs.
        FilterExpression: '#type = :presentation OR attribute_exists(revisions) OR attribute_exists(revisionSlides)',
        ExpressionAttributeNames: { '#type': 'type' },
        ExpressionAttributeValues: { ':presentation': PRESENTATION_DOCUMENT_TYPE },
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
  const oldEverywhere = new Map<string, boolean>();
  let listed = 0;
  for (const store of opts.stores) {
    const seen = new Map<string, number>();
    for await (const entry of store.listAssets(DECK_BLOB_PREFIX)) {
      if (!isDeckBlobKey(entry.key)) continue;
      listed += 1;
      seen.set(entry.key, entry.touchedAtMs);
      const old = entry.touchedAtMs < cutoffMs;
      oldEverywhere.set(entry.key, (oldEverywhere.get(entry.key) ?? true) && old);
    }
    perStore.push(seen);
  }
  const candidates = [...oldEverywhere].filter(([, old]) => old).map(([key]) => key);
  const result: DeckBlobSweepResult = { listed, candidates: candidates.length, rowsScanned: 0, referenced: 0, deleted: 0, refused: 0 };
  if (candidates.length === 0) return result;

  // 3. Mark: every key any live row names. Started only after listing.
  const referenced = new Set<string>();
  for await (const row of opts.rows()) {
    result.rowsScanned += 1;
    collectBlobReferences(row, referenced);
  }
  // 3b. A moved slide list protects its slides: open every manifest named and
  // mark what it names. Any manifest that will not read → delete nothing.
  const readBlob = opts.readBlob ?? defaultBlobReader(opts.stores);
  const opened = new Set<string>();
  for (;;) {
    const next = [...referenced].filter((key) => isSlideListKey(key) && !opened.has(key));
    if (!next.length) break;
    for (const key of next) {
      opened.add(key);
      const body = await readBlob(key).catch(() => null);
      let parsed: unknown;
      try { parsed = body ? JSON.parse(body.toString('utf8')) : undefined; } catch { parsed = undefined; }
      if (!Array.isArray(parsed)) {
        result.referenced = referenced.size;
        result.manifestsRead = opened.size - 1;
        result.skipped = `slide-list manifest ${key} could not be read; refusing to delete while the slides it names are unknown`;
        log.warn({ key, candidates: candidates.length }, `deck blob sweep: ${result.skipped}`);
        return result;
      }
      collectBlobReferences(parsed, referenced);
    }
  }
  result.manifestsRead = opened.size;
  result.referenced = referenced.size;
  if (result.rowsScanned === 0) {
    // Old deck blobs and not one presentation row is far likelier to be a
    // scan of the wrong table (a prefix mismatch) than a store with no decks.
    result.skipped = 'no presentation rows found; refusing to treat every deck blob as unreferenced';
    log.warn({ candidates: candidates.length }, `deck blob sweep: ${result.skipped}`);
    return result;
  }

  // 4. Sweep: unreferenced candidates, conditionally, durable store first.
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
    if (removed) result.deleted += 1;
  }
  if (result.deleted || result.refused) log.info(result, 'deck blob sweep complete');
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
