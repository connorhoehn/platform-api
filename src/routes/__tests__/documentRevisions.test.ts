// /api/documents/:documentId/revisions — the .pptx download and the per-slide
// SVG previews. Driven over raw http (same pattern as images.test.ts) because
// what these routes are judged on is their headers.

import express from 'express';
import http from 'http';
import type { AddressInfo } from 'net';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { errorHandler } from '../../middleware/error-handler';

const blobStore = new InMemoryImageBlobStore();

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
  setPresentationDocumentStore,
  type PresentationDocumentRow,
  type PresentationDocumentStore,
} from '../../deck/presentationDocuments';
import { PPTX_CONTENT_TYPE } from '../../deck/renderPptx';
import { sampleDeck } from '../../deck/__tests__/fixtures';
import { hidePptxNodeDetection } from '../../deck/__tests__/pptxTestEnv';
import { MAX_SLIDES, type DeckSpec } from '../../deck/DeckSpec';

const PPTX_KEY = 'deck-doc-1-v1.pptx';
const SVG_KEY = 'deck-doc-1-v1-cover.svg';
const SVG = '<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 1280 720"></svg>';

const row: PresentationDocumentRow = {
  documentId: 'doc-1',
  title: 'Q4 platform review',
  type: 'presentation',
  revisions: [{
    id: 'v1',
    label: 'v1',
    createdAt: '2026-09-21T10:00:00.000Z',
    createdBy: 'user-frank',
    slideCount: 2,
    asset: { key: PPTX_KEY, contentType: PPTX_CONTENT_TYPE, sizeBytes: 9 },
    slides: [
      { id: 'cover', index: 1, title: 'Cover', previewHandle: 'doc-1.v1.cover', asset: { key: SVG_KEY, contentType: 'image/svg+xml', sizeBytes: SVG.length } },
      { id: 'sources', index: 2, title: 'Sources', previewHandle: 'doc-1.v1.sources', asset: { key: 'missing.svg', contentType: 'image/svg+xml', sizeBytes: 0 } },
    ],
  }],
};

const appended: Array<Record<string, unknown>> = [];

const store: PresentationDocumentStore = {
  get: async (documentId: string) => (documentId === 'doc-1' ? row : null),
  appendRevision: async (input) => {
    appended.push(input as unknown as Record<string, unknown>);
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

function postJson(path: string, body: unknown): Promise<{ status: number; body: Record<string, unknown> }> {
  const { port } = server.address() as AddressInfo;
  const payload = Buffer.from(JSON.stringify(body), 'utf8');
  return new Promise((resolve, reject) => {
    const req = http.request({
      host: '127.0.0.1', port, method: 'POST', path,
      headers: { 'content-type': 'application/json', 'content-length': String(payload.length) },
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
    expect(body.revisions[0].slides.map((slide: { id: string }) => slide.id)).toEqual(['cover', 'sources']);
    expect(body.revisions[0].slides[0].previewHandle).toBe('doc-1.v1.cover');
  });

  it('404s an unknown document', async () => {
    expect((await request('/api/documents/nope/revisions')).status).toBe(404);
  });
});

describe('GET /api/documents/:id/revisions/:rev/file', () => {
  it('serves the .pptx with the PowerPoint content type', async () => {
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

  it('404s when the revision points at bytes the store does not hold', async () => {
    expect((await request('/api/documents/doc-1/revisions/v1/slides/sources.svg')).status).toBe(404);
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
});

describe('safeFilename', () => {
  it('produces a filename a header can carry', () => {
    expect(safeFilename('Q4 "platform" review — 2026', 'v3', 'pptx')).toBe('Q4-platform-review-2026-v3.pptx');
    expect(safeFilename('   ', 'v1', 'pptx')).toBe('presentation-v1.pptx');
  });
});
