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
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import { PPTX_CONTENT_TYPE } from '../deck/renderPptx';
import { SVG_CONTENT_TYPE } from '../deck/renderSlideSvg';
import {
  getPresentationDocumentStore,
  PRESENTATION_DOCUMENT_TYPE,
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
      slides: revision.slides.map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
    })),
  });
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/file
 * The presentation itself, as the file a person opens.
 */
documentRevisionsRouter.get('/:revisionId/file', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { row, revision } = await loadRevision(String(req.params.documentId ?? ''), String(req.params.revisionId ?? ''));
  const stored = await getAssetBlobStore().getBlob(revision.asset.key);
  if (!stored) throw new NotFoundError('Revision file not found');
  const filename = safeFilename(row.title ?? 'presentation', revision.id, 'pptx');
  res
    .status(200)
    .setHeader('content-type', revision.asset.contentType || PPTX_CONTENT_TYPE)
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
  const slideId = String(req.params.slideId ?? '').replace(/\.svg$/i, '');
  const { revision } = await loadRevision(String(req.params.documentId ?? ''), String(req.params.revisionId ?? ''));
  const slide = revision.slides.find((candidate) => candidate.id === slideId);
  if (!slide) throw new NotFoundError('Slide not found');
  const stored = await getAssetBlobStore().getBlob(slide.asset.key);
  if (!stored) throw new NotFoundError('Slide preview not found');
  res
    .status(200)
    .setHeader('content-type', slide.asset.contentType || SVG_CONTENT_TYPE)
    .setHeader('content-length', String(stored.body.byteLength))
    .setHeader('cache-control', 'private, no-store')
    .setHeader('x-content-type-options', 'nosniff')
    .setHeader('content-disposition', 'inline')
    .setHeader('content-security-policy', "default-src 'none'; style-src 'unsafe-inline'; sandbox")
    .send(stored.body);
}));
