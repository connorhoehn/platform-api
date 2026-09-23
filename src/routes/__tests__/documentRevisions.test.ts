// /api/documents/:documentId/revisions — the .pptx download and the per-slide
// SVG previews. Driven over raw http (same pattern as images.test.ts) because
// what these routes are judged on is their headers.

import express from 'express';
import http from 'http';
import type { AddressInfo } from 'net';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { errorHandler } from '../../middleware/error-handler';

const blobStore = new InMemoryImageBlobStore();
/**
 * The durable copy of the .pptx. It is the ONLY place the file exists in this
 * suite, which is the state a pod replacement leaves behind: the row and the
 * copy survive, the volume does not.
 */
const mirrorStore = new InMemoryImageBlobStore();

jest.mock('../../pipeline/image/visionServices', () => ({
  getAssetBlobStore: () => blobStore,
  getImageBlobStore: () => blobStore,
  getVisionResultRepo: jest.fn(),
  getVisionClient: jest.fn(),
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
  ddbClient: { send: jest.fn() },
}));

jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

// Import AFTER the mocks.
import { documentRevisionsRouter, safeFilename } from '../documentRevisions';
import {
  DdbPresentationDocumentStore,
  PresentationRevisionConflictError,
  setPresentationDocumentStore,
  type PresentationDocumentRow,
  type PresentationDocumentStore,
} from '../../deck/presentationDocuments';
import { PPTX_CONTENT_TYPE } from '../../deck/renderPptx';
import { sampleDeck } from '../../deck/__tests__/fixtures';
import { hidePptxNodeDetection } from '../../deck/__tests__/pptxTestEnv';
import { MAX_SLIDES, type DeckSpec } from '../../deck/DeckSpec';
import * as citationTitles from '../../deck/citationTitles';

const PPTX_KEY = 'deck-doc-1-v1.pptx';
const SVG_KEY = 'deck-doc-1-v1-cover.svg';
const SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 720"></svg>';

const row: PresentationDocumentRow = {
  documentId: 'doc-1',
  title: 'Q4 platform review',
  type: 'presentation',
  createdBy: 'dev-user',
  revisions: [{
    id: 'v1',
    label: 'v1',
    createdAt: '2026-09-21T10:00:00.000Z',
    createdBy: 'user-frank',
    slideCount: 2,
    heading: 'Platform audit',
    asset: { key: PPTX_KEY, contentType: PPTX_CONTENT_TYPE, sizeBytes: 9 },
    slides: [
      { id: 'cover', index: 1, title: 'Cover', previewHandle: 'doc-1.v1.cover', asset: { key: SVG_KEY, contentType: 'image/svg+xml', sizeBytes: SVG.length } },
      // Its blob is gone (the volume did not survive); its markup is on the row.
      { id: 'sources', index: 2, title: 'Sources', previewHandle: 'doc-1.v1.sources', asset: { key: 'missing.svg', contentType: 'image/svg+xml', sizeBytes: 0 }, svg: '<svg xmlns="http://www.w3.org/2000/svg">sources</svg>' },
      // Neither on the row nor in the store.
      { id: 'gone', index: 3, title: 'Gone', previewHandle: 'doc-1.v1.gone', asset: { key: 'gone.svg', contentType: 'image/svg+xml', sizeBytes: 0 } },
    ],
  }],
};

// NFR #36: v1's slide list moved to a manifest (only in the durable copy
// here); v2 is the head and keeps its list inline.
const MOVED_SLIDES = [{ id: 'cover', index: 1, title: 'Moved cover', previewHandle: 'doc-moved.v1.cover', asset: { key: SVG_KEY, contentType: 'image/svg+xml', sizeBytes: SVG.length } }];
const MOVED_KEY = 'deck-slides-0123456789abcdef0123456789abcdef01234567.json';
const movedRow: PresentationDocumentRow = {
  documentId: 'doc-moved',
  title: 'Moved',
  type: 'presentation',
  createdBy: 'dev-user',
  revisions: [
    { ...row.revisions![0]!, slides: undefined, slidesRef: { key: MOVED_KEY, contentType: 'application/json', sizeBytes: 1, sha256: '0123456789abcdef0123456789abcdef01234567' } },
    { ...row.revisions![0]!, id: 'v2', label: 'v2', slides: [{ ...MOVED_SLIDES[0]!, title: 'Head cover', previewHandle: 'doc-moved.v2.cover' }] },
  ],
};
const rowFor = (documentId: string) => (documentId === 'doc-1' ? row : documentId === 'doc-moved' ? movedRow : null);

const appended: Array<Record<string, unknown>> = [];

// The real store's own lookup (row first, blob second), over the same in-memory
// blob store the routes read — so the route test exercises the real preference
// rather than a fake that agrees with it.
const realStore = new DdbPresentationDocumentStore({
  docClient: { send: async () => ({}) } as never,
  blobStore,
  mirror: mirrorStore,
});
jest.spyOn(realStore, 'get').mockImplementation(async (documentId: string) => rowFor(documentId));

const store: PresentationDocumentStore = {
  get: async (documentId: string) => rowFor(documentId),
  readAsset: (asset) => realStore.readAsset(asset),
  slidePreview: (documentId, revisionId, slideId) => realStore.slidePreview(documentId, revisionId, slideId),
  revisionFile: (documentId, revisionId) => realStore.revisionFile(documentId, revisionId),
  appendRevision: async (input) => {
    appended.push(input as unknown as Record<string, unknown>);
    if (input.expectedHead !== undefined && input.expectedHead !== 'v1') {
      throw new PresentationRevisionConflictError(input.documentId, row.revisions![0]!, 1);
    }
    return {
      documentId: input.documentId,
      revision: {
        id: 'v2',
        label: input.label ?? 'v2',
        createdAt: '2026-09-21T11:00:00.000Z',
        createdBy: input.createdBy,
        slideCount: input.slides.length,
        asset: { key: 'deck-doc-1-v2.pptx', contentType: PPTX_CONTENT_TYPE, sizeBytes: input.pptx.byteLength },
        slides: input.slides.map((slide, index) => ({
          id: slide.id, index: index + 1, title: slide.title,
          previewHandle: `${input.documentId}.v2.${slide.id}`,
          asset: { key: `deck-${input.documentId}-v2-${slide.id}.svg`, contentType: 'image/svg+xml', sizeBytes: slide.svg.length },
        })),
        ...(input.spec ? { spec: input.spec, sources: [{ documentId: 'doc-sprint-review', title: 'Sprint review', kind: 'document' as const }] } : {}),
      },
    };
  },
};

function postJson(path: string, body: unknown, user?: string): Promise<{ status: number; body: Record<string, unknown> }> {
  const { port } = server.address() as AddressInfo;
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  const headers: Record<string, string> = { 'content-type': 'application/json', 'content-length': String(payload.length) };
  // A dev token: SKIP_AUTH decodes it (never verifies) to name who is asking.
  if (user) {
    const part = (value: unknown) => Buffer.from(JSON.stringify(value)).toString('base64url');
    headers.authorization = `Bearer ${part({ alg: 'none', typ: 'JWT' })}.${part({ sub: user })}.x`;
  }
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, method: 'POST', path,
      headers,
    }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8');
        resolve({ status: res.statusCode ?? 0, body: text ? JSON.parse(text) : {} });
      });
    });
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

function request(path: string, headers: Record<string, string> = {}): Promise<{ status: number; body: Buffer; headers: http.IncomingHttpHeaders }> {
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, method: 'GET', path, headers }, (res) => {
      const chunks: Buffer[] = [];
      res.on('data', (chunk: Buffer) => chunks.push(chunk));
      res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks), headers: res.headers }));
    });
    req.on('error', reject);
    req.end();
  });
}

let server: http.Server;
let restorePptx: () => void;
const originalSkipAuth = process.env.SKIP_AUTH;

beforeAll(async () => {
  process.env.SKIP_AUTH = 'true';
  setPresentationDocumentStore(store);
  await blobStore.putBlob(PPTX_KEY, Buffer.from('PKfake'), { contentType: PPTX_CONTENT_TYPE });
  await blobStore.putBlob(SVG_KEY, Buffer.from(SVG, 'utf8'), { contentType: 'image/svg+xml' });
  restorePptx = hidePptxNodeDetection();
  const app = express();
  app.use(express.json({ limit: '4mb' }));
  app.use('/api/documents/:documentId/revisions', documentRevisionsRouter);
  app.use(errorHandler);
  server = http.createServer(app);
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
});

afterAll(async () => {
  restorePptx();
  setPresentationDocumentStore(null);
  blobStore.reset();
  mirrorStore.reset();
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
  else process.env.SKIP_AUTH = originalSkipAuth;
  await new Promise<void>((resolve) => server.close(() => resolve()));
});

describe('GET /api/documents/:id/revisions', () => {
  it('lists the revisions and their slides, with no bytes', async () => {
    const res = await request('/api/documents/doc-1/revisions');
    expect(res.status).toBe(200);
    const body = JSON.parse(res.body.toString('utf8'));
    expect(body.revisions).toHaveLength(1);
    expect(body.revisions[0].slides.map((slide: { id: string }) => slide.id)).toEqual(['cover', 'sources', 'gone']);
    expect(body.revisions[0].slides[0].previewHandle).toBe('doc-1.v1.cover');
    // The deck's own title rides along as a subtitle where it differs from the name.
    expect(body.revisions[0].heading).toBe('Platform audit');
  });

  it('404s an unknown document', async () => {
    expect((await request('/api/documents/nope/revisions')).status).toBe(404);
  });

  it('lists an older revision’s slides from its manifest (NFR #36), read from the durable copy', async () => {
    await mirrorStore.putBlob(MOVED_KEY, Buffer.from(JSON.stringify(MOVED_SLIDES)), { contentType: 'application/json' });
    const res = await request('/api/documents/doc-moved/revisions');
    expect(res.status).toBe(200);
    const body = JSON.parse(res.body.toString('utf8'));
    expect(body.revisions.map((revision: { slides: Array<{ title: string }> }) => revision.slides.map((slide) => slide.title)))
      .toEqual([['Moved cover'], ['Head cover']]);
    const slide = await request('/api/documents/doc-moved/revisions/v1/slides/cover.svg');
    expect(slide.status).toBe(200);
    expect(slide.body.toString('utf8')).toBe(SVG);
    expect((await request('/api/documents/doc-moved/revisions/v1/slides/nope.svg')).status).toBe(404);
  });
});

describe('GET /api/documents/:id/revisions/:rev/file', () => {
  it('serves the .pptx from the durable copy when the volume no longer has it', async () => {
    const res = await request('/api/documents/doc-1/revisions/v1/file');
    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe(PPTX_CONTENT_TYPE);
    expect(res.headers['content-disposition']).toBe('attachment; filename="Q4-platform-review-v1.pptx"');
    expect(res.headers['cache-control']).toBe('private, no-store');
    expect(res.headers['x-content-type-options']).toBe('nosniff');
    expect(res.body.subarray(0, 2).toString('utf8')).toBe('PK');
  });

  it('404s a revision the document does not list', async () => {
    expect((await request('/api/documents/doc-1/revisions/v9/file')).status).toBe(404);
  });
});

describe('GET /api/documents/:id/revisions/:rev/slides/:slide.svg', () => {
  it('serves the slide as inert SVG', async () => {
    const res = await request('/api/documents/doc-1/revisions/v1/slides/cover.svg');
    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('image/svg+xml');
    expect(res.headers['content-disposition']).toBe('inline');
    expect(res.headers['content-security-policy']).toContain("default-src 'none'");
    expect(res.headers['x-content-type-options']).toBe('nosniff');
    expect(res.body.toString('utf8')).toBe(SVG);
  });

  it('answers to the slide id without the extension too', async () => {
    expect((await request('/api/documents/doc-1/revisions/v1/slides/cover')).status).toBe(200);
  });

  it('404s a slide the revision does not have', async () => {
    expect((await request('/api/documents/doc-1/revisions/v1/slides/nope.svg')).status).toBe(404);
  });

  it('serves the slide from the row when the blob file is gone', async () => {
    const res = await request('/api/documents/doc-1/revisions/v1/slides/sources.svg');
    expect(res.status).toBe(200);
    expect(res.headers['content-type']).toBe('image/svg+xml');
    expect(res.body.toString('utf8')).toContain('sources');
  });

  it('404s when the bytes are neither on the row nor in the store', async () => {
    expect((await request('/api/documents/doc-1/revisions/v1/slides/gone.svg')).status).toBe(404);
  });
});

describe('authorization', () => {
  it('refuses an unauthenticated request', async () => {
    delete process.env.SKIP_AUTH;
    try {
      const file = await request('/api/documents/doc-1/revisions/v1/file');
      const slide = await request('/api/documents/doc-1/revisions/v1/slides/cover.svg');
      const list = await request('/api/documents/doc-1/revisions');
      const spec = await request('/api/documents/doc-1/revisions/v1/spec');
      const write = await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck() });
      expect([file.status, slide.status, list.status, spec.status, write.status]).toEqual([401, 401, 401, 401, 401]);
    } finally {
      process.env.SKIP_AUTH = 'true';
    }
  });
});

describe('POST /api/documents/:id/revisions', () => {
  beforeEach(() => { appended.length = 0; });

  describe('who may save (NFR #41)', () => {
    beforeEach(() => { row.editors = ['dev-erin']; row.viewers = ['dev-frank']; });
    afterEach(() => { delete row.editors; delete row.viewers; });
    it.each([
      ['owner', 'dev-user', 201],
      ['editor', 'dev-erin', 201],
      ['viewer', 'dev-frank', 403],
      ['stranger', 'dev-mallory', 403],
    ])('%s → %i', async (_role, user, status) => {
      const res = await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck() }, user);
      expect(res.status).toBe(status);
      if (status === 403) {
        expect(res.body).toMatchObject({ code: 'forbidden', documentId: 'doc-1' });
        expect(appended).toHaveLength(0);
      } else {
        expect(appended).toHaveLength(1);
      }
    });
    it('a document that does not exist yet is the caller\'s to create', async () => {
      const res = await postJson('/api/documents/doc-new/revisions', { spec: sampleDeck() }, 'dev-mallory');
      expect(res.status).toBe(201);
    });
  });

  it('renders the spec and files it as the next revision', async () => {
    const res = await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck(), label: 'Draft for the sync' });
    expect(res.status).toBe(201);
    const revision = res.body.revision as Record<string, unknown>;
    expect(revision).toMatchObject({ id: 'v2', label: 'Draft for the sync', slideCount: 9, createdBy: 'dev-user' });
    expect((revision.slides as Array<{ previewHandle: string }>)[0]!.previewHandle).toBe('doc-1.v2.cover');
    expect(revision.sources).toEqual([{ documentId: 'doc-sprint-review', title: 'Sprint review', kind: 'document' }]);
    expect(Array.isArray(res.body.warnings)).toBe(true);

    // The bytes really were rendered, and the spec was kept with them.
    const stored = appended[0]!;
    expect((stored.pptx as Buffer).subarray(0, 2).toString('utf8')).toBe('PK');
    expect((stored.spec as DeckSpec).title).toBe('Q4 platform review');
    expect((stored.slides as Array<{ svg: string }>)).toHaveLength(9);
  }, 30_000);

  it('400s a spec with an uncited slide, and stores nothing', async () => {
    const spec = sampleDeck();
    spec.slides[0]!.sources = [];
    const res = await postJson('/api/documents/doc-1/revisions', { spec });
    expect(res.status).toBe(400);
    expect(appended).toHaveLength(0);
  });

  it('413s a spec past the slide cap', async () => {
    const slide = { id: 's', layout: 'bullets', title: 'T', bullets: ['b'], sources: [{ documentId: 'd', title: 'D' }] };
    const res = await postJson('/api/documents/doc-1/revisions', {
      spec: { title: 'Deck', slides: Array.from({ length: MAX_SLIDES + 1 }, (_v, index) => ({ ...slide, id: `s${index}` })) },
    });
    expect(res.status).toBe(413);
    expect(appended).toHaveLength(0);
  });

  it('400s when no spec is given', async () => {
    expect((await postJson('/api/documents/doc-1/revisions', {})).status).toBe(400);
  });

  it('passes the base the editor knew about through, and saves when it is still the head', async () => {
    const res = await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck(), baseRevisionId: 'v1' });
    expect(res.status).toBe(201);
    expect(appended[0]!.expectedHead).toBe('v1');
  }, 30_000);

  it('409s a save built on a revision that is no longer the head, naming the one that landed', async () => {
    const res = await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck(), baseRevisionId: null });
    expect(res.status).toBe(409);
    expect(res.body).toMatchObject({
      code: 'revision-conflict',
      revisionCount: 1,
      head: { id: 'v1', label: 'v1', createdBy: 'user-frank', createdAt: '2026-09-21T10:00:00.000Z' },
    });
  }, 30_000);

  it('400s a base that is not a revision id', async () => {
    expect((await postJson('/api/documents/doc-1/revisions', { spec: sampleDeck(), baseRevisionId: 7 })).status).toBe(400);
    expect(appended).toHaveLength(0);
  });
});

describe('GET /api/documents/:id/revisions/:rev/spec', () => {
  it('409s a revision written before specs were stored, rather than inventing one', async () => {
    const res = await request('/api/documents/doc-1/revisions/v1/spec');
    expect(res.status).toBe(409);
    expect(JSON.parse(res.body.toString('utf8')).error).toContain('cannot be reopened');
  });

  it('404s a revision the document does not list', async () => {
    expect((await request('/api/documents/doc-1/revisions/v9/spec')).status).toBe(404);
  });

  it('names a citation stored with a call wire id from its recording', async () => {
    const lookup = jest.spyOn(citationTitles, 'lookupTranscriptTitle')
      .mockImplementation(async (id: string) => (id === 'tr-planning' ? 'Call with Connor Hoehn & Frank Davis' : null));
    const spec = sampleDeck();
    spec.slides = spec.slides.map((slide) => ({ ...slide, sources: [{ documentId: 'tr-planning', title: 'dm:dev-connor:dev-frank' }] }));
    const revisions = row.revisions!;
    revisions.push({
      ...revisions[0]!,
      id: 'v-legacy',
      label: 'v-legacy',
      spec,
      sources: [{ documentId: 'tr-planning', title: 'dm:dev-connor:dev-frank', kind: 'document' }],
    });
    try {
      const res = await request('/api/documents/doc-1/revisions/v-legacy/spec');
      expect(res.status).toBe(200);
      const body = JSON.parse(res.body.toString('utf8')) as { spec: DeckSpec; sources: Array<{ title: string }> };
      expect(new Set(body.spec.slides.flatMap((slide) => slide.sources.map((source) => source.title))))
        .toEqual(new Set(['Call with Connor Hoehn & Frank Davis']));
      expect(body.sources.map((source) => source.title)).toEqual(['Call with Connor Hoehn & Frank Davis']);
    } finally {
      revisions.pop();
      lookup.mockRestore();
    }
  });
});

describe('safeFilename', () => {
  it('produces a filename a header can carry', () => {
    expect(safeFilename('Q4 "platform" review — 2026', 'v3', 'pptx')).toBe('Q4-platform-review-2026-v3.pptx');
    expect(safeFilename('   ', 'v1', 'pptx')).toBe('presentation-v1.pptx');
  });
});
