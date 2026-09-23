// The bytes of a presentation: the .pptx a person downloads, and the SVG of one
// slide a preview pane shows.
//
// `GET /api/work-graph/previews/:handle` already serves ONE preview per
// revision, and its handle (`<nodeId>.<revisionId>`) has no room for a slide. A
// deck has many slides, so these routes address them directly — by document,
// revision and slide id, which is what the generator recorded. The work-graph
// preview route can delegate to the same store when it learns per-slide handles;
// until then a caller that holds `<documentId>.<revisionId>.<slideId>` (what the
// `deck` step returns as `previewHandles`) can read every slide from here.
//
// Nothing is served that the document does not itself point at: the revision has
// to be listed on the row and the asset key has to be the one it recorded, so a
// guessed key reads nothing.

import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, AppError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import { PPTX_CONTENT_TYPE, renderPptx } from '../deck/renderPptx';
import { platformWorkGraphSourcePublisher } from '../work-graph/sources/platformPublisher';
import { workSourceAccountRepo } from '../repositories';
import { SVG_CONTENT_TYPE } from '../deck/renderSlideSvg';
import { previewDeckSpec } from '../deck/preview';
import { healCitationTitles } from '../deck/citationTitles';
import log from '../lib/logger';
import {
  getPresentationDocumentStore,
  PRESENTATION_DOCUMENT_TYPE,
  PresentationRevisionConflictError,
  type PresentationDocumentRow,
  type StoredRevision,
} from '../deck/presentationDocuments';

export const documentRevisionsRouter = Router({ mergeParams: true });

const ID = /^[A-Za-z0-9][A-Za-z0-9_.@:-]{0,200}$/;

/** A filename Content-Disposition can carry: ASCII, no quotes, no separators. */
export function safeFilename(title: string, revisionId: string, extension: string): string {
  const base = title
    .normalize('NFKD')
    .replace(/[^\x20-\x7E]/g, '')
    .replace(/[^A-Za-z0-9 _-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 80) || 'presentation';
  return `${base} ${revisionId}.${extension}`.replace(/ /g, '-');
}

async function loadRevision(documentId: string, revisionId: string): Promise<{ row: PresentationDocumentRow; revision: StoredRevision }> {
  if (!ID.test(documentId)) throw new ValidationError('documentId is invalid');
  if (!ID.test(revisionId)) throw new ValidationError('revisionId is invalid');
  const row = await getPresentationDocumentStore().get(documentId);
  if (!row) throw new NotFoundError('Document not found');
  const revision = (row.revisions ?? []).find((candidate) => candidate.id === revisionId);
  if (!revision) throw new NotFoundError('Revision not found');
  return { row, revision };
}

/**
 * GET /api/documents/:documentId/revisions
 * What revisions exist and what each one holds — no bytes.
 */
documentRevisionsRouter.get('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  if (!ID.test(documentId)) throw new ValidationError('documentId is invalid');
  const row = await getPresentationDocumentStore().get(documentId);
  if (!row) throw new NotFoundError('Document not found');
  res.status(200).json({
    documentId,
    title: row.title ?? 'Untitled',
    type: row.type ?? PRESENTATION_DOCUMENT_TYPE,
    revisions: (row.revisions ?? []).map((revision) => ({
      id: revision.id,
      label: revision.label,
      createdAt: revision.createdAt,
      createdBy: revision.createdBy,
      slideCount: revision.slideCount,
      contentType: revision.asset.contentType,
      sizeBytes: revision.asset.sizeBytes,
      hasSpec: !!revision.spec,
      ...(revision.sources ? { sources: revision.sources } : {}),
      slides: revision.slides.map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
    })),
  });
}));

/**
 * POST /api/documents/:documentId/revisions  { spec, label? }
 * Renders the spec and files it as the next revision. This is the only write in
 * this router, and the only place in the deck path that persists anything —
 * editing is previewing until someone saves.
 *
 * Authorization is the document's own: `/api/documents/:documentId` is behind
 * `requireDocumentPolicy` (app.ts), which resolves a POST to the `edit`
 * operation and refuses a locked document, so a viewer cannot write a revision.
 */
documentRevisionsRouter.post('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  if (!ID.test(documentId)) throw new ValidationError('documentId is invalid');
  const body = (req.body ?? {}) as { spec?: unknown; label?: unknown; title?: unknown; transcriptIds?: unknown; baseRevisionId?: unknown };
  if (body.spec === undefined) throw new ValidationError('spec is required');
  // The newest revision the editor knew about. Sent, a save that would land on
  // top of a revision the person never saw answers 409 instead; left out (an
  // older client), it appends as before, but still never shares an id.
  if (body.baseRevisionId !== undefined && body.baseRevisionId !== null
    && (typeof body.baseRevisionId !== 'string' || !ID.test(body.baseRevisionId))) {
    throw new ValidationError('baseRevisionId must be a revision id or null');
  }
  const expectedHead = body.baseRevisionId as string | null | undefined;
  const outcome = previewDeckSpec(body.spec);
  if (!outcome.ok) {
    res.status(outcome.status).json({ error: outcome.errors[0], errors: outcome.errors });
    return;
  }
  const label = typeof body.label === 'string' && body.label.trim() ? body.label.trim().slice(0, 80) : undefined;
  const title = typeof body.title === 'string' && body.title.trim() ? body.title.trim().slice(0, 120) : outcome.spec.title;
  const createdBy = req.user?.sub ?? 'unknown';
  // Which cited ids are transcripts, as the caller knows them. Unsaid means
  // document: nothing here reads an id's spelling for its kind.
  const transcriptIds = Array.isArray(body.transcriptIds)
    ? body.transcriptIds.filter((value): value is string => typeof value === 'string' && !!value.trim()).slice(0, 20)
    : [];

  const pptx = await renderPptx(outcome.spec);
  let revision: StoredRevision;
  try {
    ({ revision } = await getPresentationDocumentStore().appendRevision({
      documentId,
      title,
      createdBy,
      pptx,
      slides: outcome.preview.slides.map((slide) => ({ id: slide.id, title: slide.title, svg: slide.svg })),
      spec: outcome.spec,
      ...(transcriptIds.length ? { transcriptIds } : {}),
      ...(label ? { label } : {}),
      ...(expectedHead !== undefined ? { expectedHead } : {}),
    }));
  } catch (error) {
    if (!(error instanceof PresentationRevisionConflictError)) throw error;
    log.info({ documentId, expectedHead: expectedHead ?? null, head: error.head?.id ?? null, userId: createdBy }, 'deck revision refused: someone saved first');
    res.status(409).json({
      error: error.message,
      code: 'revision-conflict',
      revisionCount: error.revisionCount,
      head: error.head ? {
        id: error.head.id,
        label: error.head.label,
        createdAt: error.head.createdAt,
        createdBy: error.head.createdBy,
      } : null,
    });
    return;
  }
  log.info({ documentId, revisionId: revision.id, slides: revision.slideCount, userId: createdBy }, 'deck revision saved');
  // A revision saved by hand reaches the work graph the same way a rendered
  // one does — same source, same node — with no run to claim as its
  // provenance, because a person made it.
  if (req.user?.orgId) {
    await workSourceAccountRepo.ensureVerifiedPrincipal({
      source: 'document', organizationId: req.user.orgId, subject: createdBy,
    }).catch(() => undefined);
    const projected = await platformWorkGraphSourcePublisher.document({
      kind: 'document-revision',
      documentId,
      revisionId: revision.id,
      occurredAt: revision.createdAt,
      safeLabel: title,
    }, { organizationId: req.user.orgId, sourceSubject: createdBy }).catch(() => null);
    if (projected?.published) {
      log.info({ documentId, revisionId: revision.id, producedByRunId: null }, 'document lifecycle enqueued');
    } else {
      log.warn({ documentId, revisionId: revision.id, reason: projected?.reason ?? 'publish-failed' }, 'deck revision was not projected');
    }
  }
  res.status(201).json({
    documentId,
    revision: {
      id: revision.id,
      label: revision.label,
      createdAt: revision.createdAt,
      createdBy: revision.createdBy,
      slideCount: revision.slideCount,
      contentType: revision.asset.contentType,
      sizeBytes: revision.asset.sizeBytes,
      sources: revision.sources ?? [],
      slides: revision.slides.map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
    },
    warnings: outcome.preview.warnings,
  });
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/spec
 * The DeckSpec a revision was rendered from — what the editor reopens. A
 * revision written before specs were kept says so rather than answering with a
 * spec reconstructed from its pictures.
 */
documentRevisionsRouter.get('/:revisionId/spec', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { revision } = await loadRevision(String(req.params.documentId ?? ''), String(req.params.revisionId ?? ''));
  if (!revision.spec) {
    throw new AppError(409, `revision ${revision.id} was written before specs were stored, so it cannot be reopened for editing`);
  }
  // Specs drafted before f100e61 cite a transcript by its call's wire id; the
  // editor gets the human title, and its next save stores it.
  const healed = await healCitationTitles(revision.spec, revision.sources ?? []);
  res.status(200).json({
    revisionId: revision.id,
    label: revision.label,
    createdAt: revision.createdAt,
    createdBy: revision.createdBy,
    sources: healed.sources,
    spec: healed.spec,
  });
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/file
 * The presentation itself, as the file a person opens.
 */
documentRevisionsRouter.get('/:revisionId/file', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const { row, revision } = await loadRevision(documentId, String(req.params.revisionId ?? ''));
  // The configured store first, then the durable copy the store keeps for it —
  // a deck rendered before a pod replacement is still downloadable.
  const store = getPresentationDocumentStore();
  const stored = store.revisionFile
    ? await store.revisionFile(documentId, revision.id)
    : await getAssetBlobStore().getBlob(revision.asset.key)
      .then((blob) => (blob ? { contentType: blob.asset.contentType, body: blob.body } : null));
  if (!stored) throw new NotFoundError('Revision file not found');
  const filename = safeFilename(row.title ?? 'presentation', revision.id, 'pptx');
  res
    .status(200)
    .setHeader('content-type', stored.contentType || revision.asset.contentType || PPTX_CONTENT_TYPE)
    .setHeader('content-length', String(stored.body.byteLength))
    .setHeader('content-disposition', `attachment; filename="${filename}"`)
    .setHeader('cache-control', 'private, no-store')
    .setHeader('x-content-type-options', 'nosniff')
    .send(stored.body);
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/slides/:slideId.svg
 * One slide's preview. Served inert, the way the work-graph preview route serves
 * its own: no sniffing, no framing, nothing cached anywhere shared.
 */
documentRevisionsRouter.get('/:revisionId/slides/:slideId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const documentId = String(req.params.documentId ?? '');
  const slideId = String(req.params.slideId ?? '').replace(/\.svg$/i, '');
  const { revision } = await loadRevision(documentId, String(req.params.revisionId ?? ''));
  const slide = revision.slides.find((candidate) => candidate.id === slideId);
  if (!slide) throw new NotFoundError('Slide not found');
  // The row's own markup first, the blob second — a row outlives the volume it
  // points at, and a preview that 404s after a pod replacement is the defect
  // this order exists to prevent.
  const store = getPresentationDocumentStore();
  const stored = store.slidePreview
    ? await store.slidePreview(documentId, revision.id, slideId)
    : await getAssetBlobStore().getBlob(slide.asset.key)
      .then((blob) => (blob ? { contentType: blob.asset.contentType, body: blob.body } : null));
  if (!stored) throw new NotFoundError('Slide preview not found');
  res
    .status(200)
    .setHeader('content-type', stored.contentType || slide.asset.contentType || SVG_CONTENT_TYPE)
    .setHeader('content-length', String(stored.body.byteLength))
    .setHeader('cache-control', 'private, no-store')
    .setHeader('x-content-type-options', 'nosniff')
    .setHeader('content-disposition', 'inline')
    .setHeader('content-security-policy', "default-src 'none'; style-src 'unsafe-inline'; sandbox")
    .send(stored.body);
}));
