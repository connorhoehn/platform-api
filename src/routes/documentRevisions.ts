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

import { createHash } from 'crypto';
import { Router, type Request, type Response } from 'express';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, AppError, NotFoundError, ValidationError } from '../middleware/error-handler';
import { getAssetBlobStore } from '../pipeline/image/visionServices';
import { PPTX_CONTENT_TYPE, renderPptx } from '../deck/renderPptx';
import { platformWorkGraphSourcePublisher } from '../work-graph/sources/platformPublisher';
import { workSourceAccountRepo } from '../repositories';
import { SVG_CONTENT_TYPE } from '../deck/renderSlideSvg';
import { getSlideReadCache } from '../deck/slideReadCache';
import { previewDeckSpec } from '../deck/preview';
import { healCitationTitles } from '../deck/citationTitles';
import log from '../lib/logger';
import { canEditDocument, canReadDocument, documentRoleOf, editRefusal } from '../deck/documentEditAccess';
import {
  getPresentationDocumentStore,
  resolveRevisionSlides,
  resolveRevisionSpec,
  revisionHeading,
  withRevisionSlides,
  PRESENTATION_DOCUMENT_TYPE,
  PresentationRevisionConflictError,
  type PresentationDocumentRow,
  type StoredRevision,
  type StoredSlide,
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

/**
 * The reader check every GET here makes, once per request (NFR #135): the
 * `document:read` policy — owner, editor, viewer or a member of the document's
 * channel. A scoped document grant was already checked for `read` by the
 * document-policy middleware. A request with no bearer at all is 401 even under
 * SKIP_AUTH's `dev-user` fallback; a caller without read access gets the same
 * 404 as a document that does not exist, before any revision id is looked at.
 */
async function assertCanReadDocument(req: Request, row: PresentationDocumentRow, documentId: string): Promise<void> {
  if (req.documentGrant) return;
  if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
  const decision = await canReadDocument(row, req.user, documentId);
  if (decision.allow) return;
  log.warn({ documentId, userId: req.user?.sub ?? '', orgId: req.user?.orgId, route: req.path }, 'deck revision read refused: the caller may not read this document');
  throw new NotFoundError('Document not found');
}

async function loadRevision(req: Request, documentId: string, revisionId: string): Promise<{ row: PresentationDocumentRow; revision: StoredRevision }> {
  if (!ID.test(documentId)) throw new ValidationError('documentId is invalid');
  if (!ID.test(revisionId)) throw new ValidationError('revisionId is invalid');
  const row = await getPresentationDocumentStore().get(documentId);
  if (!row) throw new NotFoundError('Document not found');
  await assertCanReadDocument(req, row, documentId);
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
  const store = getPresentationDocumentStore();
  const stored = await store.get(documentId);
  if (!stored) throw new NotFoundError('Document not found');
  await assertCanReadDocument(req, stored, documentId);
  // Older revisions' slide lists live in manifests off the row (NFR #36).
  const row = await withRevisionSlides(store, stored);
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
      hasSpec: !!(revision.spec || revision.specRef),
      ...(revision.sources ? { sources: revision.sources } : {}),
      // A composer edit's revision names the run that wrote it.
      ...(revision.producedBy ? { producedBy: revision.producedBy } : {}),
      // The deck's own heading, only where it differs from the document's name.
      ...(revisionHeading(revision, row.title) ? { heading: revisionHeading(revision, row.title) } : {}),
      slides: (revision.slides ?? []).map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
    })),
  });
}));

/** How many decks one `row-summaries` read may ask about (the list's page of deck rows). */
export const MAX_ROW_SUMMARY_IDS = 24;

/**
 * GET /api/deck/row-summaries?ids=a,b,c
 * The documents list's `v4 · 12 slides` chip for a page of deck rows, in one
 * read. The list is org-wide while a deck's revisions answer only its readers
 * (NFR #135), so asking `/revisions` per row turned every deck the viewer may
 * not read into a 404 in the console. Here a deck that is missing or unreadable
 * is simply absent from the answer — the same silence for both, so nothing
 * about a hidden deck leaks — and the reader check runs once per id.
 */
export const deckRowSummariesRouter = Router();

deckRowSummariesRouter.get('/', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
  const raw = typeof req.query.ids === 'string' ? req.query.ids : '';
  const ids = [...new Set(raw.split(',').map((id) => id.trim()).filter(Boolean))];
  if (ids.length > MAX_ROW_SUMMARY_IDS) throw new ValidationError(`at most ${MAX_ROW_SUMMARY_IDS} ids`);
  if (ids.some((id) => !ID.test(id))) throw new ValidationError('ids are invalid');
  const store = getPresentationDocumentStore();
  const summaries: Record<string, { revisionLabel: string; slideCount: number }> = {};
  await Promise.all(ids.map(async (documentId) => {
    const row = await store.get(documentId);
    if (!row) return;
    const decision = await canReadDocument(row, req.user, documentId);
    if (!decision.allow) return;
    const head = (row.revisions ?? [])[(row.revisions ?? []).length - 1];
    if (!head?.label) return;
    summaries[documentId] = { revisionLabel: head.label, slideCount: Number(head.slideCount ?? 0) };
  }));
  res.status(200).json({ summaries });
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
  // Only the owner or an editor saves a revision. A scoped document grant was
  // already checked for 'edit' by the document-policy middleware.
  if (!req.documentGrant) {
    const existing = await getPresentationDocumentStore().get(documentId);
    const decision = await canEditDocument(existing, req.user, documentId);
    if (!decision.allow) {
      const userId = req.user?.sub ?? '';
      log.warn({ documentId, userId, role: documentRoleOf(existing, userId) }, 'deck revision refused: the caller may not edit this document');
      res.status(403).json({ error: editRefusal(existing, userId), code: 'forbidden', documentId });
      return;
    }
  }
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
      slides: (revision.slides ?? []).map((slide) => ({ id: slide.id, index: slide.index, title: slide.title, previewHandle: slide.previewHandle })),
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
  const { revision } = await loadRevision(req, String(req.params.documentId ?? ''), String(req.params.revisionId ?? ''));
  if (!revision.spec && !revision.specRef) {
    throw new AppError(409, `revision ${revision.id} was written before specs were stored, so it cannot be reopened for editing`);
  }
  // Inline on the head; an older revision's spec is in the blob store (NFR #4).
  const spec = await resolveRevisionSpec(getPresentationDocumentStore(), revision);
  if (!spec) throw new NotFoundError(`the spec of revision ${revision.id} is no longer stored`);
  // Specs drafted before f100e61 cite a transcript by its call's wire id; the
  // editor gets the human title, and its next save stores it.
  const healed = await healCitationTitles(spec, revision.sources ?? []);
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
  const started = Date.now();
  const { row, revision } = await loadRevision(req, documentId, String(req.params.revisionId ?? ''));
  const loadedAt = Date.now();
  // Memory, then the configured store, then the durable copy — a deck rendered
  // before a pod replacement is still downloadable — then, for a content key,
  // one build across replicas (NFR #171). The row loadRevision read is reused.
  const store = getPresentationDocumentStore();
  const stored = store.revisionFile
    ? await store.revisionFile(documentId, revision.id, row)
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
    .setHeader('x-pptx-source', 'from' in stored && stored.from ? String(stored.from) : 'blob')
    .setHeader('server-timing', `row;dur=${loadedAt - started}, file;dur=${Date.now() - loadedAt}`)
    .send(stored.body);
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/slides
 * Every slide of one revision in ONE answer (NFR #6). The per-slide route makes
 * a reader pay the auth check, the row read and the manifest resolve once per
 * slide — seven of each for a seven-slide deck, and the store's own
 * `slidePreview` reads the row again inside each. Here the row is read once, the
 * slide list resolved once, and each slide's markup taken from the row (or its
 * blob when it was too big to inline).
 *
 * The markup travels as JSON strings; the reader turns each into an
 * `image/svg+xml` blob shown through `<img>`, which runs nothing in it — the
 * same inertness the per-slide route gets from its CSP. A revision never
 * changes, so the answer is `private, immutable` and carries an ETag over the
 * slides' hashes; a revalidation still passes the auth check before its 304.
 */
documentRevisionsRouter.get('/:revisionId/slides', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const started = Date.now();
  const documentId = String(req.params.documentId ?? '');
  const { row, revision } = await loadRevision(req, documentId, String(req.params.revisionId ?? ''));
  const loadedAt = Date.now();
  const store = getPresentationDocumentStore();
  const recorded: StoredSlide[] = [
    ...((await resolveRevisionSlides(store, revision)) ?? []),
    ...(row.revisionSlides?.[revision.id] ?? []),
  ];
  const seen = new Set<string>();
  const recordedSlides = recorded.filter((slide) => {
    if (!slide?.id || seen.has(slide.id)) return false;
    seen.add(slide.id);
    return true;
  });
  if (recordedSlides.length === 0) throw new NotFoundError('Slide previews not found');
  const readBytes = async (slide: StoredSlide): Promise<Buffer | null> => {
    if (!slide.asset?.key) return null;
    try {
      if (store.readAsset) return await store.readAsset(slide.asset);
      return (await getAssetBlobStore().getBlob(slide.asset.key))?.body ?? null;
    } catch {
      return null;
    }
  };
  const slides = await Promise.all(recordedSlides.map(async (slide) => {
    const contentType = slide.asset?.contentType || SVG_CONTENT_TYPE;
    const svg = slide.svg ?? (await readBytes(slide))?.toString('utf8');
    return svg
      ? { id: slide.id, contentType, svg }
      : { id: slide.id, contentType, missing: true as const };
  }));
  const bytesAt = Date.now();
  const hashes = recordedSlides.map((slide) => slide.sha256 ?? '').join(',');
  if (recordedSlides.every((slide) => slide.sha256)) {
    const etag = `"${createHash('sha256').update(`${revision.id}:${hashes}`).digest('hex').slice(0, 40)}"`;
    res.setHeader('etag', etag);
    if (req.headers['if-none-match'] === etag) {
      res.status(304).setHeader('cache-control', 'private, max-age=31536000, immutable').end();
      return;
    }
  }
  log.info({ documentId, revisionId: revision.id, slides: slides.length, missing: slides.filter((slide) => 'missing' in slide).length, ms: bytesAt - started }, 'deck slide batch served (one authorization pass)');
  res
    .status(200)
    .setHeader('cache-control', 'private, max-age=31536000, immutable')
    .setHeader('x-content-type-options', 'nosniff')
    .setHeader('server-timing', `load;dur=${loadedAt - started}, bytes;dur=${bytesAt - loadedAt}`)
    .json({ documentId, revisionId: revision.id, slides });
}));

/**
 * GET /api/documents/:documentId/revisions/:revisionId/slides/:slideId.svg
 * One slide's preview. Served inert, the way the work-graph preview route serves
 * its own: no sniffing, no framing, nothing cached anywhere shared.
 */
documentRevisionsRouter.get('/:revisionId/slides/:slideId', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
  // NFR #166: the slide requests in flight together share one row read and
  // one read decision (nothing kept after); the slide list and bytes are kept
  // once read (immutable) — the per-request cost the batch route avoids.
  const started = Date.now();
  const documentId = String(req.params.documentId ?? '');
  const revisionId = String(req.params.revisionId ?? '');
  const slideId = String(req.params.slideId ?? '').replace(/\.svg$/i, '');
  if (!ID.test(documentId)) throw new ValidationError('documentId is invalid');
  if (!ID.test(revisionId)) throw new ValidationError('revisionId is invalid');
  const store = getPresentationDocumentStore();
  const cache = getSlideReadCache();
  const row = await cache.row(store, documentId);
  if (!row) throw new NotFoundError('Document not found');
  if (!req.documentGrant) {
    if (!req.headers.authorization?.startsWith('Bearer ')) throw new AppError(401, 'Authorization required');
    const allowed = await cache.canRead(`${req.user?.orgId ?? ''}\u0000${req.user?.sub ?? ''}\u0000${documentId}`,
      async () => (await canReadDocument(row, req.user, documentId)).allow);
    if (!allowed) {
      log.warn({ documentId, userId: req.user?.sub ?? '', orgId: req.user?.orgId, route: req.path }, 'deck revision read refused: the caller may not read this document');
      throw new NotFoundError('Document not found');
    }
  }
  const revision = (row.revisions ?? []).find((candidate) => candidate.id === revisionId);
  if (!revision) throw new NotFoundError('Revision not found');
  const loadedAt = Date.now();
  // Inline on the head; an older revision's list is a manifest in the blob
  // store (NFR #36); a legacy entry on the row last.
  const slide = ((await cache.slides(store, documentId, revision)) ?? []).find((candidate) => candidate?.id === slideId)
    ?? (row.revisionSlides?.[revision.id] ?? []).find((candidate) => candidate?.id === slideId);
  if (!slide) throw new NotFoundError('Slide not found');
  const cacheControl = 'private, max-age=31536000, immutable';
  // A revision's slide never changes: the URL names (document, revision,
  // slide), so the browser keeps it privately, and a revalidation is answered
  // by the content hash before any bytes are read.
  const recordedEtag = slide.sha256 ? `"${slide.sha256.slice(0, 40)}"` : undefined;
  if (recordedEtag && req.headers['if-none-match'] === recordedEtag) {
    res.status(304).setHeader('etag', recordedEtag).setHeader('cache-control', cacheControl)
      .setHeader('server-timing', `load;dur=${loadedAt - started}`).end();
    return;
  }
  // The row's own markup first, the blob (then its durable copy) second — a
  // row outlives the volume it points at.
  const body = await cache.slideBytes(`${documentId}\u0000${revision.id}\u0000${slideId}`, async () => {
    if (slide.svg) return Buffer.from(slide.svg, 'utf8');
    if (!slide.asset?.key) return null;
    try {
      if (store.readAsset) return await store.readAsset(slide.asset);
      return (await getAssetBlobStore().getBlob(slide.asset.key))?.body ?? null;
    } catch {
      return null;
    }
  });
  if (!body) {
    log.warn({ documentId, revisionId: revision.id, slideId, key: slide.asset?.key }, 'a revision names slide bytes that are neither on the row nor in either blob store');
    throw new NotFoundError('Slide preview not found');
  }
  const etag = recordedEtag ?? `"${createHash('sha256').update(body).digest('hex').slice(0, 40)}"`;
  res.setHeader('etag', etag);
  if (req.headers['if-none-match'] === etag) { res.status(304).setHeader('cache-control', cacheControl).end(); return; }
  res
    .status(200)
    .setHeader('content-type', slide.asset?.contentType || SVG_CONTENT_TYPE)
    .setHeader('content-length', String(body.byteLength))
    .setHeader('cache-control', cacheControl)
    .setHeader('x-content-type-options', 'nosniff')
    .setHeader('content-disposition', 'inline')
    .setHeader('content-security-policy', "default-src 'none'; style-src 'unsafe-inline'; sandbox")
    .setHeader('server-timing', `load;dur=${loadedAt - started}, bytes;dur=${Date.now() - loadedAt}`)
    .send(body);
}));
