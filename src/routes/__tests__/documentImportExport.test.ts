// Integration tests for /api/documents/:documentId/(export|import) routes.
//
// Mocks docClient, repositories, and importer/exporter services at module
// level so we exercise validation, format dispatch, and error paths without
// touching DDB.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockDocClientSend = jest.fn();
jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: mockDocClientSend },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (n: string) => n,
}));

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

const mockGetSections = jest.fn();
const mockGetComments = jest.fn();
const mockGetReviews = jest.fn();
const mockGetItems = jest.fn();

jest.mock('../../repositories', () => ({
  documentSectionRepo: { getSectionsForDocument: mockGetSections },
  documentCommentRepo: { getCommentsForDocument: mockGetComments },
  sectionReviewRepo: { getReviewsForDocument: mockGetReviews },
  sectionItemRepo: { getItemsForDocument: mockGetItems },
}));

const mockBuildJson = jest.fn();
const mockBuildMd = jest.fn();
jest.mock('../../services/document-exporter', () => ({
  buildJsonExport: (...args: unknown[]) => mockBuildJson(...args),
  buildMarkdownExport: (...args: unknown[]) => mockBuildMd(...args),
}));

const mockParseJson = jest.fn();
const mockParseMd = jest.fn();
const mockApplyImport = jest.fn();
jest.mock('../../services/document-importer', () => ({
  parseJsonImport: (...args: unknown[]) => mockParseJson(...args),
  parseMarkdownSections: (...args: unknown[]) => mockParseMd(...args),
  applyImport: (...args: unknown[]) => mockApplyImport(...args),
}));

import { documentImportExportRouter } from '../documentImportExport';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  headers: Record<string, unknown>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(n: string, v: unknown): MockRes;
  getHeader(n: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    headers: {},
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader(n, v) { this.headers[n] = v; return this; },
    getHeader(n) { return this.headers[n]; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    for (const [k, v] of params.entries()) query[k] = v;
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query,
    body: opts.body,
    user: { sub: opts.userId ?? 'tester' },
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/documents/:documentId', documentImportExportRouter);
  app.use(errorHandler);
  return app;
}

const DOC_ITEM = {
  documentId: 'doc-1',
  title: 'Test Doc',
  type: 'custom',
  status: 'draft',
  createdBy: 'user-1',
  createdAt: '2026-01-01T00:00:00Z',
  updatedAt: '2026-01-01T00:00:00Z',
};

function mockDocExists() {
  mockDocClientSend.mockResolvedValue({ Item: DOC_ITEM });
}

function mockDocNotFound() {
  mockDocClientSend.mockResolvedValue({ Item: undefined });
}

beforeEach(() => {
  mockDocClientSend.mockReset();
  mockGetSections.mockReset();
  mockGetComments.mockReset();
  mockGetReviews.mockReset();
  mockGetItems.mockReset();
  mockBuildJson.mockReset();
  mockBuildMd.mockReset();
  mockParseJson.mockReset();
  mockParseMd.mockReset();
  mockApplyImport.mockReset();
});

describe('GET /api/documents/:documentId/export', () => {
  it('returns 200 JSON export by default', async () => {
    mockDocExists();
    mockGetSections.mockResolvedValue([]);
    mockGetComments.mockResolvedValue({ items: [] });
    mockGetReviews.mockResolvedValue([]);
    mockGetItems.mockResolvedValue([]);
    mockBuildJson.mockReturnValue({ document: {} });

    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/export');
    expect(res.statusCode).toBe(200);
    expect(mockBuildJson).toHaveBeenCalledTimes(1);
  });

  it('returns 200 JSON export with format=json', async () => {
    mockDocExists();
    mockGetSections.mockResolvedValue([]);
    mockGetComments.mockResolvedValue({ items: [] });
    mockGetReviews.mockResolvedValue([]);
    mockGetItems.mockResolvedValue([]);
    mockBuildJson.mockReturnValue({ document: {} });

    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/export?format=json');
    expect(res.statusCode).toBe(200);
    expect(mockBuildJson).toHaveBeenCalledTimes(1);
  });

  it('returns 200 markdown export with format=md', async () => {
    mockDocExists();
    mockGetSections.mockResolvedValue([]);
    mockGetComments.mockResolvedValue({ items: [] });
    mockGetReviews.mockResolvedValue([]);
    mockGetItems.mockResolvedValue([]);
    mockBuildMd.mockReturnValue('# Test Doc\n');

    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/export?format=md');
    expect(res.statusCode).toBe(200);
    expect(res.body).toBe('# Test Doc\n');
    expect(res.headers['Content-Type']).toBe('text/markdown; charset=utf-8');
    expect(mockBuildMd).toHaveBeenCalledTimes(1);
  });

  it('rejects invalid format with 400', async () => {
    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/export?format=csv');
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when document not found', async () => {
    mockDocNotFound();
    const res = await run(buildApp(), 'GET', '/api/documents/doc-1/export?format=json');
    expect(res.statusCode).toBe(404);
  });
});

describe('POST /api/documents/:documentId/import', () => {
  it('returns 201 on successful markdown import', async () => {
    mockDocExists();
    mockParseMd.mockReturnValue([{ title: 'S1', items: [] }]);
    mockApplyImport.mockResolvedValue({ sectionsCreated: 1, itemsCreated: 0 });

    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'markdown', content: '## S1\n' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as { sectionsCreated: number }).sectionsCreated).toBe(1);
    expect(mockParseMd).toHaveBeenCalledWith('## S1\n');
  });

  it('returns 201 on successful JSON import', async () => {
    mockDocExists();
    mockParseJson.mockReturnValue([{ title: 'S1', items: [] }]);
    mockApplyImport.mockResolvedValue({ sectionsCreated: 1, itemsCreated: 2 });

    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'json', content: '{"document":{"sections":[]}}' },
    });
    expect(res.statusCode).toBe(201);
    expect((res.body as { itemsCreated: number }).itemsCreated).toBe(2);
    expect(mockParseJson).toHaveBeenCalledTimes(1);
  });

  it('rejects missing format with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { content: 'hello' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects invalid format with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'csv', content: 'data' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects missing content with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'markdown' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects empty content with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'markdown', content: '   ' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 404 when document not found', async () => {
    mockDocNotFound();
    const res = await run(buildApp(), 'POST', '/api/documents/doc-1/import', {
      body: { format: 'markdown', content: '## Section\n' },
    });
    expect(res.statusCode).toBe(404);
  });
});
