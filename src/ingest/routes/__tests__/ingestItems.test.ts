// social-api/src/ingest/routes/__tests__/ingestItems.test.ts
//
// Integration tests for ingestItemsRouter: list, get, retry, and skip endpoints.

import express, { type NextFunction, type Request, type Response } from 'express';
import type { IngestItem } from '../../types';

// ─── in-memory repo fake ───────────────────────────────────────────────────

const store: Map<string, IngestItem> = new Map();

jest.mock('../../repositories', () => ({
    ingestItemRepo: {
        getById: jest.fn(async (id: string) => store.get(id) ?? null),
        listBySource: jest.fn(async (sourceId: string, status?: string) => {
            const items = Array.from(store.values()).filter((i) => i.sourceId === sourceId);
            return status ? items.filter((i) => i.status === status) : items;
        }),
        listByStatus: jest.fn(async (status: string, limit: number) =>
            Array.from(store.values()).filter((i) => i.status === status).slice(0, limit),
        ),
        updateStatus: jest.fn(async (id: string, status: string, patch?: Partial<IngestItem>) => {
            const item = store.get(id);
            if (!item) return null;
            const updated = { ...item, status, ...(patch ?? {}) } as IngestItem;
            store.set(id, updated);
            return updated;
        }),
    },
}));

jest.mock('../../../lib/logger', () => ({
    withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

import { ingestItemsRouter } from '../ingestItems';
import { errorHandler } from '../../../middleware/error-handler';

// ─── test rig ─────────────────────────────────────────────────────────────

interface MockRes {
    statusCode: number;
    body: unknown;
    ended: boolean;
    finished: Promise<void>;
    status(code: number): MockRes;
    json(body: unknown): MockRes;
    setHeader(name: string, value: unknown): MockRes;
    getHeader(name: string): unknown;
    end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
    let resolveFinished!: () => void;
    const finished = new Promise<void>((r) => { resolveFinished = r; });
    const headers: Record<string, unknown> = {};
    const r: MockRes = {
        statusCode: 200, body: undefined, ended: false, finished,
        status(code) { this.statusCode = code; return this; },
        json(body) { this.body = body; this.ended = true; resolveFinished(); return this; },
        setHeader(name, value) { headers[String(name)] = value; return this; },
        getHeader(name) { return headers[String(name)]; },
        end(..._args) { this.ended = true; resolveFinished(); return this; },
    };
    return r;
}

async function run(
    app: express.Express,
    method: string,
    url: string,
    body?: unknown,
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
        body: body ?? {},
        params: {},
        user: { sub: 'tester' },
        ip: '127.0.0.1',
    } as unknown as Request;
    await new Promise<void>((resolve, reject) => {
        const finalHandler = (err?: unknown): void => {
            if (err) { reject(err); return; }
            if (!res.ended) { res.status(404).json({ error: 'not found' }); }
            resolve();
        };
        (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
            req, res as unknown as Response, finalHandler,
        );
        res.finished.then(() => resolve()).catch(reject);
    });
    return res;
}

function buildApp(): express.Express {
    const app = express();
    app.use(express.json());
    app.use('/api/ingest/items', ingestItemsRouter);
    app.use(errorHandler);
    return app;
}

function makeItem(overrides: Partial<IngestItem> = {}): IngestItem {
    return {
        ingestId: `ing-${Math.random().toString(36).slice(2)}`,
        sourceId: 'src-1',
        filePath: '/docs/file.json',
        fileHash: 'abc',
        mimeType: 'application/json',
        fileSize: 100,
        status: 'queued',
        mappedTypeId: null,
        mappingConfidence: null,
        mappingMethod: null,
        documentId: null,
        pipelineRunId: null,
        error: null,
        attempts: 0,
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

beforeEach(() => {
    store.clear();
    jest.clearAllMocks();
});

// ─── GET / — list ─────────────────────────────────────────────────────────

describe('GET /api/ingest/items', () => {
    test('returns empty items array when store is empty', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/items');
        expect(res.statusCode).toBe(200);
        expect((res.body as { items: unknown[] }).items).toEqual([]);
    });

    test('?sourceId filter returns only matching items', async () => {
        const app = buildApp();
        const a = makeItem({ sourceId: 'src-a', status: 'queued' });
        const b = makeItem({ sourceId: 'src-b', status: 'queued' });
        store.set(a.ingestId, a);
        store.set(b.ingestId, b);
        const res = await run(app, 'GET', '/api/ingest/items?sourceId=src-a');
        const items = (res.body as { items: IngestItem[] }).items;
        expect(items).toHaveLength(1);
        expect(items[0].sourceId).toBe('src-a');
    });

    test('?status filter returns only matching items', async () => {
        const app = buildApp();
        const a = makeItem({ status: 'failed' });
        const b = makeItem({ status: 'done' });
        store.set(a.ingestId, a);
        store.set(b.ingestId, b);
        const res = await run(app, 'GET', '/api/ingest/items?status=failed');
        const items = (res.body as { items: IngestItem[] }).items;
        expect(items.every((i) => i.status === 'failed')).toBe(true);
    });
});

// ─── GET /:id — detail ────────────────────────────────────────────────────

describe('GET /api/ingest/items/:id', () => {
    test('200 with item when found', async () => {
        const app = buildApp();
        const item = makeItem();
        store.set(item.ingestId, item);
        const res = await run(app, 'GET', `/api/ingest/items/${item.ingestId}`);
        expect(res.statusCode).toBe(200);
        expect((res.body as IngestItem).ingestId).toBe(item.ingestId);
    });

    test('404 on unknown id', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/items/ghost');
        expect(res.statusCode).toBe(404);
    });
});

// ─── POST /:id/retry ──────────────────────────────────────────────────────

describe('POST /api/ingest/items/:id/retry', () => {
    test('requeues a failed item and increments attempts', async () => {
        const app = buildApp();
        const item = makeItem({ status: 'failed', attempts: 2 });
        store.set(item.ingestId, item);
        const res = await run(app, 'POST', `/api/ingest/items/${item.ingestId}/retry`);
        expect(res.statusCode).toBe(200);
        const body = res.body as { status: string; attempts: number };
        expect(body.status).toBe('queued');
        expect(body.attempts).toBe(3);
    });

    test('400 when item is not in failed status', async () => {
        const app = buildApp();
        const item = makeItem({ status: 'done' });
        store.set(item.ingestId, item);
        const res = await run(app, 'POST', `/api/ingest/items/${item.ingestId}/retry`);
        expect(res.statusCode).toBe(400);
    });

    test('404 when item not found', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/items/ghost/retry');
        expect(res.statusCode).toBe(404);
    });
});

// ─── POST /:id/skip ───────────────────────────────────────────────────────

describe('POST /api/ingest/items/:id/skip', () => {
    test('marks item as done with skipped flag', async () => {
        const app = buildApp();
        const item = makeItem({ status: 'queued' });
        store.set(item.ingestId, item);
        const res = await run(app, 'POST', `/api/ingest/items/${item.ingestId}/skip`);
        expect(res.statusCode).toBe(200);
        const body = res.body as { status: string; skipped: boolean };
        expect(body.status).toBe('done');
        expect(body.skipped).toBe(true);
    });

    test('skip works on any status (not just failed)', async () => {
        const app = buildApp();
        const item = makeItem({ status: 'mapping' });
        store.set(item.ingestId, item);
        const res = await run(app, 'POST', `/api/ingest/items/${item.ingestId}/skip`);
        expect(res.statusCode).toBe(200);
    });

    test('404 when item not found', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/items/ghost/skip');
        expect(res.statusCode).toBe(404);
    });
});
