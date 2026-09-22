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

import { GetCommand, PutCommand, UpdateCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { docClient as defaultDocClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import type { AssetBlobStore, BlobAsset } from '../pipeline/image/ImageBlobStore';
import { deckSources, type DeckSpec } from './DeckSpec';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import { PPTX_CONTENT_TYPE } from './renderPptx';
import { SVG_CONTENT_TYPE } from './renderSlideSvg';

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
  slides: StoredSlide[];
  /**
   * The spec this revision was rendered from. Stored so the editor can reopen a
   * revision and change it, rather than reconstructing a deck from its pictures.
   * Absent on revisions written before specs were kept.
   */
  spec?: DeckSpec;
  /** Every document the spec cited — what the work graph reads as the inputs. */
  sources?: RevisionSource[];
}

/** A transcript cites itself by a `transcript` id; everything else is a document. */
export function revisionSources(spec: DeckSpec): RevisionSource[] {
  return deckSources(spec).map((source) => ({
    documentId: source.documentId,
    title: source.title,
    kind: /^transcript(?:[:-]|$)/i.test(source.documentId) ? 'transcript' : 'document',
    ...(source.anchor ? { anchor: source.anchor } : {}),
  }));
}

export interface PresentationDocumentRow {
  documentId: string;
  title?: string;
  type?: string;
  createdBy?: string;
  createdAt?: string;
  updatedAt?: string;
  outline?: Array<{ id: string; title: string }>;
  pageCount?: number;
  revisions?: StoredRevision[];
  revisionSlides?: Record<string, StoredSlide[]>;
  revisionPreviews?: Record<string, { contentType: string; body: string; encoding?: string }>;
}

export interface AppendRevisionInput {
  documentId: string;
  title: string;
  createdBy: string;
  /** The `.pptx` bytes. */
  pptx: Buffer;
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
  /** A name for this revision. Defaults to its id (`v3`). */
  label?: string;
}

export interface AppendRevisionResult {
  documentId: string;
  revision: StoredRevision;
}

/** The document half of the store, so a test can hand the handler a fake. */
export interface PresentationDocumentStore {
  get(documentId: string): Promise<PresentationDocumentRow | null>;
  appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult>;
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

export function pptxBlobKey(documentId: string, revisionId: string): string {
  return `deck-${blobKeySegment(documentId)}-${blobKeySegment(revisionId)}.pptx`;
}

export function slideBlobKey(documentId: string, revisionId: string, slideId: string): string {
  return `deck-${blobKeySegment(documentId)}-${blobKeySegment(revisionId)}-${blobKeySegment(slideId)}.svg`;
}

export function nextRevisionId(row: PresentationDocumentRow | null): string {
  return `v${(row?.revisions?.length ?? 0) + 1}`;
}

export interface DdbPresentationDocumentStoreOptions {
  docClient?: DynamoDBDocumentClient;
  blobStore?: AssetBlobStore;
  now?: () => Date;
}

export class DdbPresentationDocumentStore implements PresentationDocumentStore {
  private readonly docClient: DynamoDBDocumentClient;
  private readonly blobStore: AssetBlobStore;
  private readonly now: () => Date;

  constructor(opts: DdbPresentationDocumentStoreOptions = {}) {
    this.docClient = opts.docClient ?? defaultDocClient;
    this.blobStore = opts.blobStore ?? getAssetBlobStore();
    this.now = opts.now ?? (() => new Date());
  }

  async get(documentId: string): Promise<PresentationDocumentRow | null> {
    const result = await this.docClient.send(new GetCommand({ TableName: DOCUMENTS_TABLE, Key: { documentId } }));
    return (result.Item as PresentationDocumentRow | undefined) ?? null;
  }

  /**
   * Stores the bytes, then records the revision on the document — creating the
   * document row when the client has not already (a `/deck` run gets an empty
   * document from the client first, the way `/diagram` does, but a pipeline that
   * generates a deck unprompted may not).
   */
  async appendRevision(input: AppendRevisionInput): Promise<AppendRevisionResult> {
    const existing = await this.get(input.documentId);
    const at = this.now().toISOString();
    const revisionId = nextRevisionId(existing);

    const pptxAsset = await this.blobStore.putBlob(
      pptxBlobKey(input.documentId, revisionId),
      input.pptx,
      { contentType: PPTX_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId}.pptx` },
    );

    const slides: StoredSlide[] = [];
    for (const [index, slide] of input.slides.entries()) {
      const asset = await this.blobStore.putBlob(
        slideBlobKey(input.documentId, revisionId, slide.id),
        Buffer.from(slide.svg, 'utf8'),
        { contentType: SVG_CONTENT_TYPE, createdBy: input.createdBy, label: `${input.title} ${revisionId} ${slide.id}` },
      );
      slides.push({
        id: slide.id,
        index: index + 1,
        title: slide.title,
        previewHandle: deckPreviewHandle(input.documentId, revisionId, slide.id),
        asset,
      });
    }

    const revision: StoredRevision = {
      id: revisionId,
      label: input.label?.trim() ? input.label.trim().slice(0, 80) : revisionId,
      createdAt: at,
      createdBy: input.createdBy,
      slideCount: slides.length,
      asset: pptxAsset,
      slides,
      ...(input.spec ? { spec: input.spec } : {}),
      ...(input.sources ?? (input.spec ? revisionSources(input.spec) : undefined)
        ? { sources: input.sources ?? revisionSources(input.spec!) }
        : {}),
    };

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
          outline: slides.map((slide) => ({ id: slide.id, title: slide.title })),
          pageCount: slides.length,
          revisions: [revision],
          revisionSlides: { [revisionId]: slides },
          revisionPreviews: firstSlidePreview(revisionId, input.slides),
        },
      }));
      return { documentId: input.documentId, revision };
    }

    // Read-modify-write on the maps: DDB cannot SET a nested path whose parent
    // map may not exist, and a document only ever has one deck run writing to it
    // at a time (the run owns the document it was handed).
    const revisions = [...(existing.revisions ?? []), revision];
    await this.docClient.send(new UpdateCommand({
      TableName: DOCUMENTS_TABLE,
      Key: { documentId: input.documentId },
      UpdateExpression: [
        'SET #type = :type',
        '#title = :title',
        'revisions = :revisions',
        'revisionSlides = :revisionSlides',
        'revisionPreviews = :revisionPreviews',
        'outline = :outline',
        'pageCount = :pageCount',
        'updatedAt = :updatedAt',
      ].join(', '),
      ExpressionAttributeNames: { '#type': 'type', '#title': 'title' },
      ExpressionAttributeValues: {
        ':type': PRESENTATION_DOCUMENT_TYPE,
        ':title': existing.title || input.title,
        ':revisions': revisions,
        ':revisionSlides': { ...(existing.revisionSlides ?? {}), [revisionId]: slides },
        ':revisionPreviews': { ...(existing.revisionPreviews ?? {}), ...firstSlidePreview(revisionId, input.slides) },
        ':outline': slides.map((slide) => ({ id: slide.id, title: slide.title })),
        ':pageCount': slides.length,
        ':updatedAt': at,
      },
    }));
    return { documentId: input.documentId, revision };
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
  slides: Array<{ svg: string }>,
): Record<string, { contentType: string; body: string; encoding: string }> {
  const first = slides[0];
  if (!first) return {};
  return { [revisionId]: { contentType: SVG_CONTENT_TYPE, body: first.svg, encoding: 'utf8' } };
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
