// Where a rendered deck lands: a document of type `presentation` in the same
// `crdt-documents` table the rest of the Documents explorer reads, plus the
// bytes in the blob store.
//
// There was no binary revision anywhere in this service — every generative
// pipeline so far writes text into a document's Y.Doc as a collaborator, and a
// `.pptx` is not text. This is the smallest honest extension of what already
// exists rather than a new subsystem:
//
//   - the document row keeps the fields the Documents page and the work-graph
//     activity reader already look for (`title`, `type`, `outline`, `pageCount`,
//     `revisionPreviews`), so a deck shows up in both without either learning a
//     new shape;
//   - a `revisions[]` list is added to the row, each entry naming its bytes by
//     `asset: { key, contentType, sizeBytes }` in the blob store — the record is
//     a pointer, never the payload;
//   - `revisionSlides[revisionId]` names each slide and the SVG asset behind it,
//     which is what a per-slide preview route and the work-graph projection both
//     need to list `slides[]`.
//
// The bytes live in the existing blob store (`IMAGE_BLOB_STORE=fs` →
// VolumeImageBlobStore), which now stores assets as well as images.

import { createHash, randomUUID } from 'node:crypto';
import { GetCommand, PutCommand, TransactWriteCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { docClient as defaultDocClient, ddbClient as defaultDdbClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import {
  DdbImageBlobStore, IMAGE_BLOB_SIZE_CAP_BYTES,
  type AssetBlobStore, type BlobAsset,
} from '../pipeline/image/ImageBlobStore';
import { deckSources, type DeckSpec } from './DeckSpec';
import { distinctHeading } from './distinctHeading';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import log from '../lib/logger';
import { AppError } from '../middleware/error-handler';
import { PPTX_CONTENT_TYPE } from './renderPptx';
import { SVG_CONTENT_TYPE } from './renderSlideSvg';
import { ChunkedBlobStore, assertChunkedBlobFits } from './chunkedBlobStore';
import { PPTX_CONTENT_KEY, buildPptxOnce, pptxContentKey, recalledPptx, rememberPptx } from './pptxCache';
import {
  PRESENTATION_REVISIONS_TABLE, ensurePresentationRevisionsTable, getRevisionItem, mergeRevisions,
  queryAllRevisionItems, queryRevisionPage, revKey, revisionCountOf, summarizeRevision, versionOf,
  type HeadRevisionSummary, type RevisionItem,
} from './presentationRevisionItems';

export const DOCUMENTS_TABLE = tableName(process.env.DYNAMODB_DOCUMENTS_TABLE || 'crdt-documents');
export const PRESENTATION_DOCUMENT_TYPE = 'presentation';

/** One slide of a stored revision: its identity, its title and the SVG behind it. */
export interface StoredSlide {
  id: string;
  index: number;
  title: string;
  /** `<documentId>.<revisionId>.<slideId>` — what a preview route is handed. */
  previewHandle: string;
  asset: BlobAsset;
  /**
   * The slide's own markup, kept on the row beside the blob copy.
   *
   * A slide SVG is 1–2 KB, and a row outlives the blob volume: a pod
   * replacement took the volume with it and left rows naming files that no
   * longer existed, so every per-slide preview 404'd while the inlined cover
   * kept working. The row is the durable copy; the blob is still written, and
   * is what serves a revision whose inline markup was refused for size.
   *
   * Written only by revisions saved before the row diet (NFR #4). A revision
   * written since stores its markup in the blob store under a content key
   * (`sha256`), in the durable DDB copy as well as the volume, and the row
   * keeps only this record — still read here when present.
   */
  svg?: string;
  /** SHA-256 of the markup. Its blob key is `slideContentKey(sha256)`; unchanged slides share one blob. */
  sha256?: string;
}

/** Where a revision's spec went when it stopped being the head (content-addressed, durable). */
export interface StoredSpecRef extends BlobAsset {
  sha256: string;
}

/**
 * Where a revision's slide list went when it stopped being the head (NFR #36):
 * a content-addressed JSON manifest (`deck-slides-<hash>.json`) holding the
 * same `StoredSlide[]` the row used to carry. Read it back with
 * `resolveRevisionSlides`.
 */
export interface StoredSlidesRef extends BlobAsset {
  sha256: string;
}

/** What a revision stands on, as the deck itself declared it. */
export interface RevisionSource {
  documentId: string;
  title: string;
  kind: 'document' | 'transcript';
  anchor?: string;
}

/** One revision of a presentation: the `.pptx` and the slides rendered with it. */
export interface StoredRevision {
  id: string;
  label: string;
  createdAt: string;
  createdBy: string;
  slideCount: number;
  /** The `.pptx` itself. */
  asset: BlobAsset;
  /**
   * The slides rendered with it. Only the head keeps its list inline; once a
   * newer revision lands, the older one's list moves to the blob store and
   * `slidesRef` names it (NFR #36) — read either with `resolveRevisionSlides`.
   * Rows written before then carry the list inline on every revision, and
   * are read as they are until their next save moves the old lists out.
   */
  slides?: StoredSlide[];
  slidesRef?: StoredSlidesRef;
  /**
   * The spec this revision was rendered from. Stored so the editor can reopen a
   * revision and change it, rather than reconstructing a deck from its pictures.
   * Absent on revisions written before specs were kept.
   *
   * Only the head keeps it inline. When a newer revision lands, the older
   * one's spec moves to the blob store and `specRef` names it — read it back
   * with `resolveRevisionSpec`.
   */
  spec?: DeckSpec;
  specRef?: StoredSpecRef;
  /** Every document the spec cited — what the work graph reads as the inputs. */
  sources?: RevisionSource[];
  /**
   * The pipeline run that wrote this revision, when one did (a composer edit).
   * `requestId` is what makes the run's write idempotent: a retried write step
   * finds its own revision here instead of filing a second one.
   */
  producedBy?: RevisionProducer;
  /**
   * The spec's own title ("Audit E2E") at write time. Kept on every revision,
   * because only the head keeps its spec inline and readers that show it as a
   * subtitle (the editor, the run card, the work-graph pane) should not have to
   * fetch a spec blob for one line.
   */
  heading?: string;
}

/**
 * The deck's own heading, when it says something the document's name does not
 * (see `distinctHeading`).
 */
export function revisionHeading(revision: Pick<StoredRevision, 'heading' | 'spec'> | undefined, documentTitle: string | undefined): string | undefined {
  return distinctHeading(revision?.heading ?? revision?.spec?.title, documentTitle);
}

/** Which run, and which composer request, wrote a revision. */
export interface RevisionProducer {
  pipelineId: string;
  runId: string;
  requestId: string;
}

/**
 * What each citation IS, from what the producer knew — never from how the id is
 * spelled. A transcript id looks like any other document id (the seeded one is
 * `active-now-2026-09-21-planning-transcript`, and a document could be called
 * that too), so the step that read the material declares which ids were
 * transcripts and an id nobody declared is a document.
 */
export function revisionSources(spec: DeckSpec, transcriptIds: readonly string[] = []): RevisionSource[] {
  const transcripts = new Set(transcriptIds.filter((id) => typeof id === 'string' && id));
  return deckSources(spec).map((source) => ({
    documentId: source.documentId,
    title: source.title,
    kind: transcripts.has(source.documentId) ? 'transcript' : 'document',
    ...(source.anchor ? { anchor: source.anchor } : {}),
  }));
}

export interface PresentationDocumentRow {
  documentId: string;
  title?: string;
  type?: string;
  /** The owner: the only person, with `editors`, who may add a revision (documentEditAccess.ts). */
  createdBy?: string;
  /** Users the owner lets change the document. Absent = the owner alone. */
  editors?: string[];
  /** Users who may read it but not change it — named so a refusal can say so. */
  viewers?: string[];
  createdAt?: string;
  updatedAt?: string;
  outline?: Array<{ id: string; title: string }>;
  pageCount?: number;
  revisions?: StoredRevision[];
  revisionSlides?: Record<string, StoredSlide[]>;
  /**
   * One cover image per revision for the work-graph preview route. Older
   * revisions carry the markup (`body`); since the row diet a revision names
   * its cover slide (`slideId`) and the bytes come from the slide's blob.
   */
  revisionPreviews?: Record<string, { contentType: string; body?: string; encoding?: string; slideId?: string }>;
  /**
   * How many revisions exist (NFR #188). Present once a revision was written
   * as its own item in `presentation-revisions` (or the row was migrated);
   * absent, `revisions[]` is the whole list.
   */
  revisionCount?: number;
  /** The newest revision in one line, so a list chip needs no revision read. */
  headRevision?: HeadRevisionSummary;
}

/** One page of revisions, newest first. */
export interface RevisionPage {
  revisions: StoredRevision[];
  total: number;
  /** The id to pass as `before` for the next older page; null on the last. */
  nextCursor: string | null;
  /** `before` named no revision of this document. */
  missingCursor?: true;
}

export interface AppendRevisionInput {
  documentId: string;
  title: string;
  createdBy: string;
  /**
   * The `.pptx` bytes. Left out (a composer edit), nothing is rendered or
   * stored now: the revision names the content key its spec hashes to and the
   * first download builds it (`pptxCache.ts`). Requires `spec`.
   */
  pptx?: Buffer;
  /** One SVG per slide, in slide order, including the Sources slide. */
  slides: Array<{ id: string; title: string; svg: string }>;
  /** The spec the bytes were rendered from, kept so the revision can be edited. */
  spec?: DeckSpec;
  /**
   * What the revision stands on. Left out, it is derived from the spec's own
   * citations — which is what both callers do; pass it only when the sources
   * are known from somewhere other than the deck itself.
   */
  sources?: RevisionSource[];
  /**
   * Which of the cited ids are transcripts, as the step that read them knows.
   * Nothing is inferred from an id's spelling.
   */
  transcriptIds?: readonly string[];
  /** A name for this revision. Defaults to its id (`v3`). */
  label?: string;
  /**
   * The newest revision the caller knew about when it started editing — `null`
   * for "none yet". When given and the row's head has moved past it, the append
   * is refused with a `PresentationRevisionConflictError` naming the new head,
   * rather than filing the caller's spec on top of a revision it never saw.
   * Left out (a pipeline run that owns the document), a concurrent append just
   * pushes this one to the next id.
   */
  expectedHead?: string | null;
  /** The run that is writing this revision, recorded on it (see `StoredRevision.producedBy`). */
  producedBy?: RevisionProducer;
}

/** Someone else's revision landed first. Carries the head the caller did not see. */
export class PresentationRevisionConflictError extends Error {
  constructor(
    readonly documentId: string,
    readonly head: StoredRevision | null,
    readonly revisionCount: number,
  ) {
    super(head
      ? `${head.label || head.id} was saved while this one was being written`
      : 'the presentation changed while this revision was being written');
    this.name = 'PresentationRevisionConflictError';
  }
}

/**
 * How many times an unconditioned append re-reads and takes the next id.
 * Each lost race means another writer landed, so N racing writers need at
 * most N−1 retries; 30 covers the 20-at-once check with room (NFR #188).
 */
export const MAX_APPEND_ATTEMPTS = 30;

function conditionFailed(error: unknown): boolean {
  const name = (error as { name?: string })?.name;
  if (name === 'ConditionalCheckFailedException') return true;
  if (name !== 'TransactionCanceledException') return false;
  // Either half of the append transaction refused: the version exists, or the
  // head row's count moved — both mean another writer landed first. A
  // TransactionConflict is two transactions on the same item at once: same.
  const reasons = (error as { CancellationReasons?: Array<{ Code?: string }> }).CancellationReasons;
  if (!reasons?.length) return /ConditionalCheckFailed|TransactionConflict/.test(String((error as Error).message));
  return reasons.some((reason) => reason?.Code === 'ConditionalCheckFailed' || reason?.Code === 'TransactionConflict');
}

/** The head's id from the row alone: the summary, the array's last entry, or the count. */
function headIdOf(row: PresentationDocumentRow | null): string | null {
  if (!row) return null;
  if (row.headRevision?.id) return row.headRevision.id;
  if (row.revisionCount !== undefined) return row.revisionCount > 0 ? `v${row.revisionCount}` : null;
  return headOf(row)?.id ?? null;
}

function headOf(row: PresentationDocumentRow | null): StoredRevision | null {
  const revisions = row?.revisions ?? [];
  return revisions.length ? revisions[revisions.length - 1]! : null;
}

/** A revision as just written: the head, so its slide list is always inline. */
export type HeadRevision = StoredRevision & { slides: StoredSlide[] };

export interface AppendRevisionResult {
  documentId: string;
  revision: HeadRevision;
}

/** Stored bytes, and which of the places they could have been they came from. */
export interface SlidePreview {
  contentType: string;
  body: Buffer;
  /** The row itself, the configured blob store, or the durable copy of it. */
  from: 'inline' | 'blob' | 'mirror' | 'memory' | 'built' | 'joined-local' | 'joined-remote' | 'stored';
  /** The content hash, when the record carries one — an ETag that never changes for these bytes. */
  sha256?: string;
}

/** The document half of the store, so a test can hand the handler a fake. */
export interface PresentationDocumentStore {
  /** The row with EVERY revision in `revisions`, oldest first, wherever each is stored. */
  get(documentId: string): Promise<PresentationDocumentRow | null>;
  /** The head row alone — one GetItem; `revisions` holds only what the row itself still carries. */
  getRow?(documentId: string): Promise<PresentationDocumentRow | null>;
  /** One revision: its item, else the row's array. */
  getRevision?(documentId: string, revisionId: string, row?: PresentationDocumentRow | null): Promise<StoredRevision | null>;
  /** One page, newest first: a single Query once the row's revisions are items. */
  listRevisions?(documentId: string, opts: { limit: number; before?: string }, row?: PresentationDocumentRow | null): Promise<RevisionPage | null>;
  appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult>;
  /**
   * One slide's bytes: the row's own markup first, the blob store second.
   * Optional so a fake store can stay two methods; a caller that finds it
   * absent reads the blob the slide record names, which is what this does.
   */
  slidePreview?(documentId: string, revisionId: string, slideId: string): Promise<SlidePreview | null>;
  /**
   * A revision's `.pptx`: the configured store first, then the durable copy.
   * Optional for the same reason `slidePreview` is — a caller that finds it
   * absent reads the blob the revision names.
   */
  revisionFile?(documentId: string, revisionId: string, row?: PresentationDocumentRow | null): Promise<SlidePreview | null>;
  /**
   * The bytes behind a blob reference the row recorded (a spec that moved off
   * the row): the configured store first, then the durable copy.
   */
  readAsset?(asset: BlobAsset): Promise<Buffer | null>;
}

/**
 * How much inline slide markup one document row may hold.
 *
 * Kept for rows written before the row diet; the guard a write actually
 * applies is `MAX_ROW_BYTES` on the whole item (see `estimateItemBytes`).
 */
export const MAX_INLINE_SLIDE_BYTES = 300 * 1024;

/**
 * The most a presentation row may weigh, estimated the way DynamoDB counts an
 * item. DynamoDB refuses anything over 400 KB with a raw ValidationException
 * that says nothing about decks; 100 KB under it the save is refused here
 * instead, with a sentence a person can act on (Loop 26: was 380 KB, which
 * left no room for the estimate's error or for fields other writers add).
 * Every save already spills earlier revisions' specs and slide lists to blobs,
 * so a row this large is the head revision alone, or a pre-diet row not yet
 * stripped (scripts/backfill-revision-slides.ts).
 */
export const MAX_ROW_BYTES = 300 * 1024;
/** Past this, a save still lands but says so in the log, once per save. */
export const WARN_ROW_BYTES = 200 * 1024;

/** A row too big to write. 413, with the sizes in the message. */
export class PresentationRowTooLargeError extends AppError {
  constructor(readonly documentId: string, readonly estimatedBytes: number) {
    super(413,
      `presentation '${documentId}' would weigh about ${Math.ceil(estimatedBytes / 1024)} KB, over the ${MAX_ROW_BYTES / 1024} KB a document row may hold `
      + '(DynamoDB caps an item at 400 KB) — the deck\'s current revision is too large to keep inline. Shorten the speaker notes or split the deck.');
    this.name = 'PresentationRowTooLargeError';
  }
}

/**
 * The size DynamoDB charges an item: attribute names plus values, strings by
 * UTF-8 length, numbers by digit count, lists and maps with their small
 * per-element overhead. Within a few bytes of DynamoDB's own count on the
 * rows measured in NFR §8; an overestimate where it differs.
 */
export function estimateItemBytes(item: Record<string, unknown>): number {
  let total = 0;
  for (const [name, value] of Object.entries(item)) {
    if (value === undefined) continue;
    total += Buffer.byteLength(name, 'utf8') + valueBytes(value);
  }
  return total;
}

function valueBytes(value: unknown): number {
  if (value === null || typeof value === 'boolean') return 1;
  if (typeof value === 'string') return Buffer.byteLength(value, 'utf8');
  if (typeof value === 'number') return Math.ceil(String(Math.abs(value)).replace('.', '').length / 2) + 1;
  if (Buffer.isBuffer(value) || value instanceof Uint8Array) return value.byteLength;
  if (Array.isArray(value)) return 3 + value.reduce((sum: number, entry) => sum + 1 + (entry === undefined ? 0 : valueBytes(entry)), 0);
  if (typeof value === 'object') {
    let sum = 3;
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
      if (entry === undefined) continue;
      sum += 1 + Buffer.byteLength(key, 'utf8') + valueBytes(entry);
    }
    return sum;
  }
  return 0;
}

export function contentHash(body: Buffer | string): string {
  return createHash('sha256').update(body).digest('hex');
}

/**
 * A slide's blob key, from its content alone: a slide that did not change
 * between revisions (or decks) is one blob, written once.
 */
export function slideContentKey(sha256: string): string {
  return `deck-svg-${sha256.slice(0, 40)}.svg`;
}

export function specContentKey(sha256: string): string {
  return `deck-spec-${sha256.slice(0, 40)}.json`;
}

/** A moved slide list's blob key, from its content: an unchanged list is one blob. */
export function slideListContentKey(sha256: string): string {
  return `deck-slides-${sha256.slice(0, 40)}.json`;
}

/** True for a key `slideListContentKey` produced — what the blob sweeper opens to mark. */
export function isSlideListKey(key: unknown): key is string {
  return typeof key === 'string' && /^deck-slides-[A-Za-z0-9]+\.json$/.test(key);
}

/** The inline markup a row already holds, across every revision. */
export function inlineSlideBytes(row: PresentationDocumentRow | null): number {
  if (!row) return 0;
  let total = 0;
  const counted = new Set<StoredSlide>();
  for (const slide of [
    ...(row.revisions ?? []).flatMap((revision) => revision.slides ?? []),
    ...Object.values(row.revisionSlides ?? {}).flat(),
  ]) {
    if (!slide?.svg || counted.has(slide)) continue;
    counted.add(slide);
    total += Buffer.byteLength(slide.svg, 'utf8');
  }
  return total;
}

const HANDLE_SEPARATOR = '.';

/** The handle a preview route is given. Ids are already handle-safe (see SLIDE_ID_PATTERN). */
export function deckPreviewHandle(documentId: string, revisionId: string, slideId: string): string {
  return [documentId, revisionId, slideId].join(HANDLE_SEPARATOR);
}

/**
 * Splits a handle back into its three parts. The last two segments are the
 * revision and the slide — both are constrained to have no dots — so a
 * documentId containing dots survives the round trip.
 */
export function parseDeckPreviewHandle(handle: string): { documentId: string; revisionId: string; slideId: string } | null {
  const parts = handle.split(HANDLE_SEPARATOR);
  if (parts.length < 3) return null;
  const slideId = parts[parts.length - 1]!;
  const revisionId = parts[parts.length - 2]!;
  const documentId = parts.slice(0, parts.length - 2).join(HANDLE_SEPARATOR);
  if (!documentId || !revisionId || !slideId) return null;
  return { documentId, revisionId, slideId };
}

/** Blob keys are filenames on the volume store, so they carry no separators. */
export function blobKeySegment(value: string): string {
  return value.replace(/[^A-Za-z0-9_-]/g, '-').slice(0, 80);
}

/**
 * `attempt` makes the key unique to one write. Two saves racing for the same
 * `vN` both write their bytes before either commits the row; with a key made
 * of the revision id alone the loser overwrote the winner's `.pptx`, and the
 * row that won pointed at the bytes that lost. Readers only ever follow the
 * key the row recorded, so the suffix costs nothing to read.
 */
export function pptxBlobKey(documentId: string, revisionId: string, attempt?: string): string {
  return `deck-${blobKeySegment(documentId)}-${blobKeySegment(revisionId)}${attempt ? `-${blobKeySegment(attempt)}` : ''}.pptx`;
}

export function slideBlobKey(documentId: string, revisionId: string, slideId: string, attempt?: string): string {
  return `deck-${blobKeySegment(documentId)}-${blobKeySegment(revisionId)}${attempt ? `-${blobKeySegment(attempt)}` : ''}-${blobKeySegment(slideId)}.svg`;
}

export function nextRevisionId(row: PresentationDocumentRow | null): string {
  return `v${revisionCountOf(row) + 1}`;
}

export interface DdbPresentationDocumentStoreOptions {
  docClient?: DynamoDBDocumentClient;
  blobStore?: AssetBlobStore;
  /**
   * The second home for the `.pptx`. Defaults to the DDB blob store whenever
   * the primary is not already DDB-backed; pass `null` to write only the
   * primary (a deployment whose volume is durable does not need the copy).
   */
  mirror?: AssetBlobStore | null;
  now?: () => Date;
  /**
   * Where revisions are written, one item each (NFR #188). Defaults to
   * `presentation-revisions` for the process-wide client; a store handed its
   * own `docClient` (a test's fake table) keeps the single-row array unless it
   * names a table here. `null` = the array (also `PRESENTATION_REVISION_ITEMS=off`).
   */
  revisionTable?: string | null;
  /** For creating the revisions table on first use. */
  ddbClient?: DynamoDBClient;
}

export class DdbPresentationDocumentStore implements PresentationDocumentStore {
  private readonly docClient: DynamoDBDocumentClient;
  private readonly blobStore: AssetBlobStore;
  /**
   * Where the `.pptx` is ALSO written.
   *
   * The filesystem store is a hostPath under Tilt: it goes with the node, and
   * a pod replacement left every earlier deck's download 404ing against a row
   * that still named the file. The slides survive that because they are on the
   * row; the `.pptx` is too big for a row and small enough for a DDB item, so
   * it gets the same substrate every other pipeline record already lives on.
   */
  private readonly mirror: AssetBlobStore | null;
  private readonly now: () => Date;
  /** Null = revisions stay in the row's array (the pre-#188 layout). */
  private readonly revisionTable: string | null;
  private readonly ddbClient: DynamoDBClient | null;
  private tableReady: Promise<void> | null = null;

  constructor(opts: DdbPresentationDocumentStoreOptions = {}) {
    this.docClient = opts.docClient ?? defaultDocClient;
    this.revisionTable = process.env.PRESENTATION_REVISION_ITEMS === 'off'
      ? null
      : opts.revisionTable !== undefined ? opts.revisionTable : (opts.docClient ? null : PRESENTATION_REVISIONS_TABLE);
    this.ddbClient = opts.ddbClient ?? (opts.docClient ? null : defaultDdbClient);
    // NFR #61: a DynamoDB-backed store holds a body over one item's cap as
    // several items (chunkedBlobStore.ts), so a large `.pptx` keeps its
    // durable copy instead of failing the save.
    const primary = opts.blobStore ?? getAssetBlobStore();
    this.blobStore = primary instanceof DdbImageBlobStore ? new ChunkedBlobStore(primary) : primary;
    this.mirror = opts.mirror === undefined
      // Kept for good: the row now points at these blobs instead of carrying
      // the markup, so a 30-day TTL would expire a deck's slides under it.
      ? (primary instanceof DdbImageBlobStore ? null : new ChunkedBlobStore(new DdbImageBlobStore({ ttlDays: null })))
      // A raw DDB store passed in is chunked too: unwrapped it throws mid-save.
      : (opts.mirror instanceof DdbImageBlobStore ? new ChunkedBlobStore(opts.mirror) : opts.mirror);
    this.now = opts.now ?? (() => new Date());
    // Which store a deck's bytes land in, said out loud once. A process that
    // renders a deck without IMAGE_BLOB_DIR set writes somewhere the operator
    // never mounted, and the row it produces looks exactly the same.
    log.info({
      store: this.blobStore.constructor?.name ?? 'unknown',
      directory: (this.blobStore as { directory?: string }).directory ?? null,
      imageBlobStore: process.env.IMAGE_BLOB_STORE ?? null,
      imageBlobDir: process.env.IMAGE_BLOB_DIR ?? null,
      mirror: this.mirror ? this.mirror.constructor?.name ?? 'unknown' : null,
    }, 'presentation blobs will be written here');
  }

  /** The revisions table exists before the first write names it (idempotent, once per process). */
  private ready(): Promise<void> {
    if (!this.revisionTable || !this.ddbClient) return Promise.resolve();
    if (!this.tableReady) {
      this.tableReady = ensurePresentationRevisionsTable(this.ddbClient, this.revisionTable)
        .catch((error) => { this.tableReady = null; throw error; });
    }
    return this.tableReady;
  }

  /** True when this row's revisions (some or all) are items. */
  private hasItems(row: PresentationDocumentRow | null): boolean {
    return !!this.revisionTable && typeof row?.revisionCount === 'number';
  }

  async getRow(documentId: string): Promise<PresentationDocumentRow | null> {
    const result = await this.docClient.send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId }, ConsistentRead: true }));
    return (result.Item as PresentationDocumentRow | undefined) ?? null;
  }

  async get(documentId: string): Promise<PresentationDocumentRow | null> {
    const row = await this.getRow(documentId);
    if (!row || !this.hasItems(row)) return row;
    const items = await queryAllRevisionItems(this.docClient, documentId, this.revisionTable!);
    return { ...row, revisions: mergeRevisions(row.revisions, items) };
  }

  async getRevision(documentId: string, revisionId: string, rowIn?: PresentationDocumentRow | null): Promise<StoredRevision | null> {
    const row = rowIn === undefined ? await this.getRow(documentId) : rowIn;
    if (!row) return null;
    const version = versionOf(revisionId);
    if (this.hasItems(row) && version !== null) {
      const item = await getRevisionItem(this.docClient, documentId, version, this.revisionTable!);
      if (item) return item;
    }
    return row.revisions?.find((candidate) => candidate.id === revisionId) ?? null;
  }

  async listRevisions(documentId: string, opts: { limit: number; before?: string }, rowIn?: PresentationDocumentRow | null): Promise<RevisionPage | null> {
    const row = rowIn === undefined ? await this.getRow(documentId) : rowIn;
    if (!row) return null;
    const total = revisionCountOf(row);
    // Every revision an item (a new or migrated row): ONE Query, newest first.
    if (this.hasItems(row) && !row.revisions?.length) {
      const beforeVersion = opts.before === undefined ? undefined : versionOf(opts.before) ?? undefined;
      if (opts.before !== undefined && (beforeVersion === undefined || beforeVersion > total)) return { revisions: [], total, nextCursor: null, missingCursor: true };
      const page = await queryRevisionPage(this.docClient, documentId, { limit: opts.limit, ...(beforeVersion !== undefined ? { beforeVersion } : {}) }, this.revisionTable!);
      const last = page.revisions[page.revisions.length - 1];
      // Versions are 1..total with no gaps, so an older one exists exactly when
      // the page's oldest is past v1 (a LastEvaluatedKey on an exact last page would say otherwise).
      const lastVersion = versionOf(last?.id);
      return { revisions: page.revisions, total, nextCursor: last && (lastVersion === null ? page.more : lastVersion > 1) ? last.id : null };
    }
    // The array still holds some (not yet migrated): merge, then slice.
    const all = this.hasItems(row)
      ? mergeRevisions(row.revisions, await queryAllRevisionItems(this.docClient, documentId, this.revisionTable!))
      : row.revisions ?? [];
    const end = opts.before === undefined ? all.length : all.findIndex((revision) => revision.id === opts.before);
    if (end === -1) return { revisions: [], total: all.length, nextCursor: null, missingCursor: true };
    const start = Math.max(0, end - opts.limit);
    return { revisions: all.slice(start, end).reverse(), total: all.length, nextCursor: start > 0 ? all[start]!.id : null };
  }

  /**
   * Stores the bytes, then records the revision on the document — creating the
   * document row when the client has not already (a `/deck` run gets an empty
   * document from the client first, the way `/diagram` does, but a pipeline that
   * generates a deck unprompted may not).
   */
  async appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult> {
    await this.ready();
    for (let attempt = 1; ; attempt += 1) {
      const existing = this.revisionTable ? await this.getRow(input.documentId) : await this.get(input.documentId);
      if (input.expectedHead !== undefined && headIdOf(existing) !== input.expectedHead) {
        throw new PresentationRevisionConflictError(input.documentId, await this.headRevisionOf(existing), revisionCountOf(existing));
      }
      let written: AppendRevisionResult;
      try {
        written = this.revisionTable
          ? await this.writeRevisionItem(input, existing)
          : await this.writeRevision(input, existing);
      } catch (error) {
        if (!conditionFailed(error)) throw error;
        // Someone else's revision took this id between our read and our write.
        const current = this.revisionTable ? await this.getRow(input.documentId) : await this.get(input.documentId);
        log.info({ documentId: input.documentId, attempt, head: headIdOf(current) }, 'presentation revision raced another save');
        if (input.expectedHead !== undefined || attempt >= MAX_APPEND_ATTEMPTS) {
          throw new PresentationRevisionConflictError(input.documentId, await this.headRevisionOf(current), revisionCountOf(current));
        }
        // A short jittered pause: N writers retrying in lockstep collide again.
        await new Promise((resolve) => setTimeout(resolve, 5 + Math.floor(Math.random() * 25)));
        continue;
      }
      // NFR #15: every writer (a `/deck` run, a revise run, a save, an Undo)
      // tells the open presentation pages, over the Redis relay and MQ.
      // Loaded on first use: the relay module is not part of the store's own
      // load (tests that mock the logger import the store without it).
      const note = {
        documentId: written.documentId,
        revisionId: written.revision.id,
        slideCount: written.revision.slideCount,
        createdBy: written.revision.createdBy,
        createdAt: written.revision.createdAt,
      };
      void import('./deckReviseEvents.js')
        .then(({ publishDeckRevisionWritten }) => { publishDeckRevisionWritten(note); })
        .catch(() => { /* a signal never fails a write */ });
      return written;
    }
  }

  /** The newest revision in full (a conflict answer carries its spec for a rebase). */
  private async headRevisionOf(row: PresentationDocumentRow | null): Promise<StoredRevision | null> {
    const headId = headIdOf(row);
    if (!row || !headId) return null;
    return (await this.getRevision(row.documentId, headId, row).catch(() => null)) ?? headOf(row);
  }

  /**
   * One attempt, one item per revision (NFR #188): new blobs, then ONE
   * transaction — the revision item, only if its version does not exist, and
   * the head row, only if it still counts the revisions this attempt read. The
   * row no longer grows with each save; each revision keeps its own spec and
   * slide list inline on its own item.
   */
  private async writeRevisionItem(input: AppendRevisionInput, existing: PresentationDocumentRow | null): Promise<AppendRevisionResult> {
    const table = this.revisionTable!;
    const at = this.now().toISOString();
    const expected = revisionCountOf(existing);
    const version = expected + 1;
    const revisionId = `v${version}`;
    if (!input.pptx && !input.spec) throw new Error('appendRevision: a revision with no .pptx needs its spec');
    const pptxKey = input.spec ? pptxContentKey(input.spec) : pptxBlobKey(input.documentId, revisionId, randomUUID().slice(0, 8));
    const pptxMeta = { contentType: PPTX_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId}.pptx` };
    if (input.pptx && (this.mirror || this.blobStore instanceof ChunkedBlobStore)) {
      assertChunkedBlobFits(pptxKey, input.pptx.byteLength, 'rendered deck (.pptx)');
    }
    const pptxAsset: BlobAsset = { key: pptxKey, contentType: PPTX_CONTENT_TYPE, sizeBytes: input.pptx?.byteLength ?? 0 };

    // Slides the head already stored are the same blobs (content keys).
    const head = expected > 0 ? await this.headRevisionOf(existing) : null;
    const known = new Map<string, BlobAsset>();
    for (const slide of head?.slides ?? []) {
      if (slide?.sha256 && slide.asset?.key === slideContentKey(slide.sha256)) known.set(slide.sha256, slide.asset);
    }
    const pending = new Map<string, { key: string; body: Buffer; meta: { contentType: string; createdBy: string; label: string } }>();
    const slides: StoredSlide[] = input.slides.map((slide, index) => {
      const body = Buffer.from(slide.svg, 'utf8');
      const sha256 = contentHash(body);
      const asset = known.get(sha256) ?? { key: slideContentKey(sha256), contentType: SVG_CONTENT_TYPE, sizeBytes: body.byteLength };
      if (!known.has(sha256) && !pending.has(asset.key)) {
        pending.set(asset.key, { key: asset.key, body, meta: { contentType: SVG_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId} ${slide.id}` } });
      }
      return { id: slide.id, index: index + 1, title: slide.title, previewHandle: deckPreviewHandle(input.documentId, revisionId, slide.id), asset, sha256 };
    });

    const revision: HeadRevision = {
      id: revisionId,
      label: input.label?.trim() ? input.label.trim().slice(0, 80) : revisionId,
      createdAt: at,
      createdBy: input.createdBy,
      slideCount: slides.length,
      asset: pptxAsset,
      slides,
      ...(input.spec ? { spec: input.spec } : {}),
      ...(input.spec?.title?.trim() ? { heading: input.spec.title.trim().slice(0, 120) } : {}),
      ...(input.producedBy ? { producedBy: input.producedBy } : {}),
      ...(input.sources ?? (input.spec ? revisionSources(input.spec, input.transcriptIds) : undefined)
        ? { sources: input.sources ?? revisionSources(input.spec!, input.transcriptIds) }
        : {}),
    };
    const item: RevisionItem = { documentId: input.documentId, rev: revKey(version), version, revision };
    const outline = slides.map((slide) => ({ id: slide.id, title: slide.title }));
    const revisionPreviews = { ...(existing?.revisionPreviews ?? {}), ...firstSlidePreview(revisionId, slides) };
    const headRevision = summarizeRevision(revision);

    // Both items weighed before anything is stored.
    const itemBytes = estimateItemBytes(item as unknown as Record<string, unknown>);
    const rowBytes = estimateItemBytes({ ...(existing ?? {}), outline, pageCount: slides.length, revisionPreviews, revisionCount: version, headRevision, updatedAt: at });
    const estimated = Math.max(itemBytes, rowBytes);
    if (estimated > MAX_ROW_BYTES) {
      log.error({ documentId: input.documentId, itemBytes, rowBytes, maxBytes: MAX_ROW_BYTES }, 'presentation save refused: the revision or the row would be over the size guard');
      throw new PresentationRowTooLargeError(input.documentId, estimated);
    }
    if (estimated > WARN_ROW_BYTES) {
      log.warn({ documentId: input.documentId, itemBytes, rowBytes, maxBytes: MAX_ROW_BYTES }, 'presentation revision or row is approaching the size guard');
    }

    if (input.pptx) {
      if (this.mirror) await this.mirror.putBlob(pptxKey, input.pptx, pptxMeta);
      await this.blobStore.putBlob(pptxKey, input.pptx, pptxMeta);
      rememberPptx(pptxKey, input.pptx);
    }
    await Promise.all([...pending.values()].map(async (blob) => {
      if (this.mirror) await this.mirror.putBlob(blob.key, blob.body, blob.meta);
      await this.blobStore.putBlob(blob.key, blob.body, blob.meta);
    }));

    const putItem = {
      Put: {
        TableName: table,
        Item: item,
        ConditionExpression: 'attribute_not_exists(rev)',
      },
    };
    const headWrite = !existing
      ? {
        Put: {
          TableName: DOCUMENTS_TABLE,
          Item: {
            documentId: input.documentId,
            title: input.title,
            type: PRESENTATION_DOCUMENT_TYPE,
            status: 'draft',
            createdBy: input.createdBy,
            createdAt: at,
            updatedAt: at,
            outline,
            pageCount: slides.length,
            revisionPreviews,
            revisionCount: version,
            headRevision,
          },
          ConditionExpression: 'attribute_not_exists(documentId)',
        },
      }
      : {
        Update: {
          TableName: DOCUMENTS_TABLE,
          Key: { documentId: input.documentId },
          UpdateExpression: 'SET #type = :type, #title = :title, revisionPreviews = :revisionPreviews, outline = :outline, pageCount = :pageCount, updatedAt = :updatedAt, revisionCount = :count, headRevision = :head',
          // The CAS on the version: the count this attempt read, wherever it
          // was kept — `revisionCount`, or (a row not yet written this way)
          // the array's length, where a missing array counts as 0.
          ConditionExpression: 'attribute_exists(documentId) AND (revisionCount = :expected OR (attribute_not_exists(revisionCount) AND '
            + (expected === 0 ? '(attribute_not_exists(revisions) OR size(revisions) = :expected)))' : 'size(revisions) = :expected))'),
          ExpressionAttributeNames: { '#type': 'type', '#title': 'title' },
          ExpressionAttributeValues: {
            ':expected': expected,
            ':type': PRESENTATION_DOCUMENT_TYPE,
            ':title': existing.title || input.title,
            ':revisionPreviews': revisionPreviews,
            ':outline': outline,
            ':pageCount': slides.length,
            ':updatedAt': at,
            ':count': version,
            ':head': headRevision,
          },
        },
      };
    await this.docClient.send(new TransactWriteCommand({ TransactItems: [putItem, headWrite] }));
    return { documentId: input.documentId, revision };
  }

  /**
   * One attempt: bytes under keys unique to this attempt, then the row, whose
   * write is conditioned on the revision count this attempt read — so of two
   * saves that both computed `vN`, exactly one records it.
   */
  private async writeRevision(input: AppendRevisionInput, existing: PresentationDocumentRow | null): Promise<AppendRevisionResult> {
    const at = this.now().toISOString();
    const revisionId = nextRevisionId(existing);
    const writeId = randomUUID().slice(0, 8);

    // The file is content-addressed by its spec (NFR #178): the same spec is
    // the same blob, and a revision with no bytes yet (a composer edit) names
    // the key its first download will build. A spec-less save keeps the old
    // per-attempt key.
    if (!input.pptx && !input.spec) throw new Error('appendRevision: a revision with no .pptx needs its spec');
    const pptxKey = input.spec ? pptxContentKey(input.spec) : pptxBlobKey(input.documentId, revisionId, writeId);
    const pptxMeta = { contentType: PPTX_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId}.pptx` };
    // NFR #61: over one DynamoDB item the durable copy is chunked; over the
    // chunked limit the save is refused with a 413 before any blob is written.
    if (input.pptx && (this.mirror || this.blobStore instanceof ChunkedBlobStore)) {
      assertChunkedBlobFits(pptxKey, input.pptx.byteLength, 'rendered deck (.pptx)');
    }
    // What the store will record (every store returns exactly this). Planned
    // here and written only after the row-size guard below, so a refused save
    // leaves no `.pptx` behind (NFR #48). `sizeBytes: 0` = not built yet.
    const pptxAsset: BlobAsset = { key: pptxKey, contentType: PPTX_CONTENT_TYPE, sizeBytes: input.pptx?.byteLength ?? 0 };

    // Slides by content (NFR #4). A slide the deck already stored — an
    // unchanged slide of any earlier revision — is the same blob; only new
    // markup is written, and nothing but the record goes on the row.
    const known = new Map<string, BlobAsset>();
    for (const earlier of existing?.revisions ?? []) {
      for (const slide of earlier.slides ?? []) {
        if (slide?.sha256 && slide.asset?.key === slideContentKey(slide.sha256)) known.set(slide.sha256, slide.asset);
      }
    }
    const pending = new Map<string, { key: string; body: Buffer; meta: { contentType: string; createdBy: string; label: string } }>();
    const slides: StoredSlide[] = input.slides.map((slide, index) => {
      const body = Buffer.from(slide.svg, 'utf8');
      const sha256 = contentHash(body);
      const asset = known.get(sha256) ?? { key: slideContentKey(sha256), contentType: SVG_CONTENT_TYPE, sizeBytes: body.byteLength };
      if (!known.has(sha256) && !pending.has(asset.key)) {
        pending.set(asset.key, { key: asset.key, body, meta: { contentType: SVG_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId} ${slide.id}` } });
      }
      return {
        id: slide.id,
        index: index + 1,
        title: slide.title,
        previewHandle: deckPreviewHandle(input.documentId, revisionId, slide.id),
        asset,
        sha256,
      };
    });

    // Only the head keeps its spec and its slide list inline; every earlier
    // one moves each to a content-addressed blob the revision names (specs:
    // NFR #4, slide lists: NFR #36). Lossless — the blob is written (durably)
    // before the row stops carrying it. A row saved before either move is
    // migrated here, lazily, one save at a time; nothing rewrites rows in bulk.
    const earlier = (existing?.revisions ?? []).map((revision) => {
      let moved: StoredRevision = revision;
      if (moved.spec) {
        const body = Buffer.from(JSON.stringify(moved.spec), 'utf8');
        const sha256 = contentHash(body);
        const key = specContentKey(sha256);
        if (!pending.has(key)) pending.set(key, { key, body, meta: { contentType: 'application/json', createdBy: input.createdBy, label: `${input.title} ${revision.id} spec` } });
        const { spec: _spec, ...rest } = moved;
        moved = { ...rest, specRef: { key, contentType: 'application/json', sizeBytes: body.byteLength, sha256 } };
      }
      if (Array.isArray(moved.slides) && moved.slides.length) {
        const body = Buffer.from(JSON.stringify(moved.slides), 'utf8');
        const sha256 = contentHash(body);
        const key = slideListContentKey(sha256);
        if (!pending.has(key)) pending.set(key, { key, body, meta: { contentType: 'application/json', createdBy: input.createdBy, label: `${input.title} ${revision.id} slides` } });
        const { slides: _slides, ...rest } = moved;
        moved = { ...rest, slidesRef: { key, contentType: 'application/json', sizeBytes: body.byteLength, sha256 } };
      }
      return moved;
    });

    const revision: HeadRevision = {
      id: revisionId,
      label: input.label?.trim() ? input.label.trim().slice(0, 80) : revisionId,
      createdAt: at,
      createdBy: input.createdBy,
      slideCount: slides.length,
      asset: pptxAsset,
      slides,
      ...(input.spec ? { spec: input.spec } : {}),
      ...(input.spec?.title?.trim() ? { heading: input.spec.title.trim().slice(0, 120) } : {}),
      ...(input.producedBy ? { producedBy: input.producedBy } : {}),
      ...(input.sources ?? (input.spec ? revisionSources(input.spec, input.transcriptIds) : undefined)
        ? { sources: input.sources ?? revisionSources(input.spec!, input.transcriptIds) }
        : {}),
    };

    const outline = slides.map((slide) => ({ id: slide.id, title: slide.title }));
    const revisions = [...earlier, revision];
    const revisionPreviews = { ...(existing?.revisionPreviews ?? {}), ...firstSlidePreview(revisionId, slides) };

    // The whole item, as it will be written, weighed before anything is
    // stored: over the line, the save is refused with the reason rather than
    // left to DynamoDB's raw item-size error after the blobs are written.
    const estimated = estimateItemBytes({
      ...(existing ?? {}),
      documentId: input.documentId,
      title: existing?.title || input.title,
      type: PRESENTATION_DOCUMENT_TYPE,
      status: (existing as { status?: string } | null)?.status ?? 'draft',
      createdBy: existing?.createdBy ?? input.createdBy,
      createdAt: existing?.createdAt ?? at,
      updatedAt: at,
      outline,
      pageCount: slides.length,
      revisions,
      revisionPreviews,
    });
    if (estimated > MAX_ROW_BYTES) {
      log.error({ documentId: input.documentId, estimatedBytes: estimated, maxBytes: MAX_ROW_BYTES }, 'presentation save refused: the row would be over the size guard');
      throw new PresentationRowTooLargeError(input.documentId, estimated);
    }
    if (estimated > WARN_ROW_BYTES) {
      log.warn({ documentId: input.documentId, estimatedBytes: estimated, maxBytes: MAX_ROW_BYTES }, 'presentation row is approaching the size guard');
    }

    // The durable copy first: a revision recorded with bytes only on a volume
    // that may not come back is the failure this whole file keeps fixing.
    // The key is unique to this attempt; if the row write below loses a race
    // (or fails), nothing references it and the deck blob sweeper removes it
    // once it is past the grace period (src/deck/blobSweeper.ts).
    if (input.pptx) {
      if (this.mirror) await this.mirror.putBlob(pptxKey, input.pptx, pptxMeta);
      await this.blobStore.putBlob(pptxKey, input.pptx, pptxMeta);
      rememberPptx(pptxKey, input.pptx);
    }

    // New content only, durable copy first, in parallel.
    await Promise.all([...pending.values()].map(async (blob) => {
      if (this.mirror) await this.mirror.putBlob(blob.key, blob.body, blob.meta);
      await this.blobStore.putBlob(blob.key, blob.body, blob.meta);
    }));

    if (!existing) {
      await this.docClient.send(new PutCommand({
        TableName: DOCUMENTS_TABLE,
        Item: {
          documentId: input.documentId,
          title: input.title,
          type: PRESENTATION_DOCUMENT_TYPE,
          status: 'draft',
          createdBy: input.createdBy,
          createdAt: at,
          updatedAt: at,
          outline,
          pageCount: slides.length,
          revisions,
          revisionPreviews,
        },
        ConditionExpression: 'attribute_not_exists(documentId)',
      }));
      return { documentId: input.documentId, revision };
    }

    // Read-modify-write on the maps (DDB cannot SET a nested path whose parent
    // map may not exist), so the write only lands if the row still holds the
    // revisions this attempt read. A deck run and a person saving by hand — or
    // two people — can and do write the same document at once.
    await this.docClient.send(new UpdateCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId: input.documentId },
      UpdateExpression: [
        'SET #type = :type',
        '#title = :title',
        'revisions = :revisions',
        'revisionPreviews = :revisionPreviews',
        'outline = :outline',
        'pageCount = :pageCount',
        'updatedAt = :updatedAt',
      ].join(', '),
      // A row the client created empty has no `revisions` at all, and
      // size() of a missing attribute is false, not 0.
      ConditionExpression: existing.revisions?.length
        ? 'attribute_exists(documentId) AND size(revisions) = :expectedCount'
        : 'attribute_exists(documentId) AND (attribute_not_exists(revisions) OR size(revisions) = :expectedCount)',
      ExpressionAttributeNames: { '#type': 'type', '#title': 'title' },
      ExpressionAttributeValues: {
        ':expectedCount': existing.revisions?.length ?? 0,
        ':type': PRESENTATION_DOCUMENT_TYPE,
        ':title': existing.title || input.title,
        ':revisions': revisions,
        // `revisionSlides` is no longer written: it repeated `revisions[].slides`
        // (and, before the diet, every slide's markup a second time). Rows
        // that have it keep it; every reader takes either.
        ':revisionPreviews': revisionPreviews,
        ':outline': outline,
        ':pageCount': slides.length,
        ':updatedAt': at,
      },
    }));
    return { documentId: input.documentId, revision };
  }

  /**
   * The `.pptx` of one revision: the configured store first, the durable copy
   * second. Either is the same bytes under the same key; which one answers
   * depends only on whether the volume outlived the row.
   */
  async revisionFile(documentId: string, revisionId: string, rowIn?: PresentationDocumentRow | null): Promise<SlidePreview | null> {
    const row = rowIn ?? await (this.revisionTable ? this.getRow(documentId) : this.get(documentId)).catch(() => null);
    const revision = row ? await this.getRevision(documentId, revisionId, row).catch(() => null) : null;
    if (!revision?.asset?.key) return null;
    const key = revision.asset.key;
    const contentType = revision.asset.contentType || PPTX_CONTENT_TYPE;
    // Immutable bytes: a warm download is a memory read (NFR #178).
    const warm = recalledPptx(key);
    if (warm) return { contentType, body: warm, from: 'memory' };
    const primary = await this.blobStore.getBlob(key).catch(() => null);
    if (primary) { rememberPptx(key, primary.body); return { contentType: primary.asset.contentType || contentType, body: primary.body, from: 'blob' }; }
    const mirrored = this.mirror ? await this.mirror.getBlob(key).catch(() => null) : null;
    if (mirrored) {
      log.info({ documentId, revisionId, key }, 'presentation file served from the durable copy; the volume no longer has it');
      rememberPptx(key, mirrored.body);
      return { contentType: mirrored.asset.contentType || contentType, body: mirrored.body, from: 'mirror' };
    }
    // Not built yet (a composer edit), or its blob aged out: a content key is
    // rebuilt from the revision's own spec, once across replicas. A legacy
    // per-revision key is not — see pptxCache.ts.
    if (PPTX_CONTENT_KEY.test(key)) {
      const spec = await resolveRevisionSpec(this, revision);
      if (spec) {
        const meta = { contentType: PPTX_CONTENT_TYPE, createdBy: revision.createdBy, label: `${row?.title ?? 'presentation'} ${revisionId}.pptx` };
        const built = await buildPptxOnce(key, spec, {
          read: async (k) => (await this.blobStore.getBlob(k).catch(() => null))?.body
            ?? (this.mirror ? (await this.mirror.getBlob(k).catch(() => null))?.body ?? null : null),
          write: async (k, body) => {
            if (this.mirror || this.blobStore instanceof ChunkedBlobStore) assertChunkedBlobFits(k, body.byteLength, 'rendered deck (.pptx)');
            if (this.mirror) await this.mirror.putBlob(k, body, meta);
            await this.blobStore.putBlob(k, body, meta);
          },
        });
        return { contentType, body: built.body, from: built.source };
      }
    }
    log.warn({ documentId, revisionId, key }, 'a revision names a .pptx that neither store holds');
    return null;
  }

  /**
   * One slide's bytes: the row's own markup first, the blob the record names
   * second. The row is what survives a volume that does not, so it is read
   * first; the blob still answers for a revision written before inlining, or
   * one whose markup was too big to keep on the row.
   */
  async slidePreview(documentId: string, revisionId: string, slideId: string): Promise<SlidePreview | null> {
    const row = await (this.revisionTable ? this.getRow(documentId) : this.get(documentId)).catch(() => null);
    if (!row) return null;
    const revision = await this.getRevision(documentId, revisionId, row).catch(() => null);
    const recorded = [
      ...(revision ? (await resolveRevisionSlides(this, revision)) ?? [] : []),
      ...(row.revisionSlides?.[revisionId] ?? []),
    ];
    const slide = recorded.find((entry) => entry?.id === slideId);
    if (!slide) return null;
    const tag = slide.sha256 ? { sha256: slide.sha256 } : {};
    if (slide.svg) {
      return { contentType: slide.asset?.contentType || SVG_CONTENT_TYPE, body: Buffer.from(slide.svg, 'utf8'), from: 'inline', ...tag };
    }
    if (!slide.asset?.key) return null;
    const blob = await this.blobStore.getBlob(slide.asset.key).catch(() => null);
    if (blob) return { contentType: blob.asset.contentType || slide.asset.contentType, body: blob.body, from: 'blob', ...tag };
    const mirrored = this.mirror ? await this.mirror.getBlob(slide.asset.key).catch(() => null) : null;
    if (mirrored) return { contentType: mirrored.asset.contentType || slide.asset.contentType, body: mirrored.body, from: 'mirror', ...tag };
    log.warn({ documentId, revisionId, slideId, key: slide.asset.key }, 'a revision names slide bytes that are neither on the row nor in either blob store');
    return null;
  }

  async readAsset(asset: BlobAsset): Promise<Buffer | null> {
    if (!asset?.key) return null;
    const primary = await this.blobStore.getBlob(asset.key).catch(() => null);
    if (primary) return primary.body;
    const mirrored = this.mirror ? await this.mirror.getBlob(asset.key).catch(() => null) : null;
    return mirrored?.body ?? null;
  }
}

/**
 * The revision's cover image for the existing work-graph preview route, which
 * serves `revisionPreviews[revisionId]` — one image per revision. The first
 * slide is the deck's own cover, so that is what it gets; the per-slide previews
 * are served from the document routes by handle.
 */
function firstSlidePreview(
  revisionId: string,
  slides: Array<{ id: string }>,
): Record<string, { contentType: string; slideId: string }> {
  const first = slides[0];
  if (!first) return {};
  // A pointer at the cover slide, whose bytes are the slide's own blob — not a
  // third copy of its markup.
  return { [revisionId]: { contentType: SVG_CONTENT_TYPE, slideId: first.id } };
}

/**
 * A revision's spec wherever it is: inline on the head, or the blob its
 * `specRef` names once a newer revision took its place. Null when the
 * revision never kept one or the bytes are gone.
 */
export async function resolveRevisionSpec(store: PresentationDocumentStore, revision: StoredRevision): Promise<DeckSpec | null> {
  if (revision.spec) return revision.spec;
  if (!revision.specRef?.key) return null;
  const body = store.readAsset
    ? await store.readAsset(revision.specRef)
    : await getAssetBlobStore().getBlob(revision.specRef.key).then((blob) => blob?.body ?? null).catch(() => null);
  if (!body) {
    log.warn({ revisionId: revision.id, key: revision.specRef.key }, 'a revision names a spec neither blob store holds');
    return null;
  }
  try { return JSON.parse(body.toString('utf8')) as DeckSpec; } catch { return null; }
}

/**
 * A revision's slide records wherever they are: inline (the head, or any
 * revision of a row saved before NFR #36), or the manifest its `slidesRef`
 * names — the configured store first, then the durable copy. `[]` for a
 * revision that records neither; null only when a manifest is named but
 * cannot be read, so a caller can tell "no slides" from "slides unreadable".
 */
export async function resolveRevisionSlides(store: PresentationDocumentStore, revision: StoredRevision): Promise<StoredSlide[] | null> {
  if (Array.isArray(revision.slides) && (revision.slides.length || !revision.slidesRef?.key)) return revision.slides;
  if (!revision.slidesRef?.key) return [];
  const body = store.readAsset
    ? await store.readAsset(revision.slidesRef).catch(() => null)
    : await getAssetBlobStore().getBlob(revision.slidesRef.key).then((blob) => blob?.body ?? null).catch(() => null);
  if (!body) {
    log.warn({ revisionId: revision.id, key: revision.slidesRef.key }, 'a revision names a slide list neither blob store holds');
    return null;
  }
  try {
    const parsed = JSON.parse(body.toString('utf8')) as unknown;
    return Array.isArray(parsed) ? parsed as StoredSlide[] : null;
  } catch { return null; }
}

/**
 * The row with every revision's slide list in place (`slides` filled from its
 * manifest where one moved off the row; `[]` where it cannot be read). For
 * readers that list every revision's slides at once. Manifests are read in
 * parallel; the row itself is not changed.
 */
export async function withRevisionSlides(store: PresentationDocumentStore, row: PresentationDocumentRow): Promise<PresentationDocumentRow> {
  if (!row.revisions?.some((revision) => !Array.isArray(revision.slides) && revision.slidesRef?.key)) return row;
  const revisions = await Promise.all(row.revisions.map(async (revision) => (
    Array.isArray(revision.slides) ? revision : { ...revision, slides: (await resolveRevisionSlides(store, revision)) ?? [] }
  )));
  return { ...row, revisions };
}

let store: PresentationDocumentStore | null = null;

/** The process-wide store, built on first use like the vision services are. */
export function getPresentationDocumentStore(): PresentationDocumentStore {
  if (!store) store = new DdbPresentationDocumentStore();
  return store;
}

/** Tests and the bootstrap can substitute one. */
export function setPresentationDocumentStore(next: PresentationDocumentStore | null): void {
  store = next;
}
