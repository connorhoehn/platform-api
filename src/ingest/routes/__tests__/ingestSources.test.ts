// social-api/src/ingest/routes/__tests__/ingestSources.test.ts
//
// Integration tests for ingestSourcesRouter: CRUD endpoints and lifecycle
// actions (sync, pause, resume, stats).

import express, { type NextFunction, type Request, type Response } from 'express';
import type { IngestSource } from '../../types';

// ─── in-memory repo fake ───────────────────────────────────────────────────

const store: Map<string, IngestSource> = new Map();

jest.mock('../../repositories', () => ({
    ingestSourceRepo: {
        putSource: jest.fn(async (s: IngestSource) => { store.set(s.sourceId, s); }),
        listAll: jest.fn(async () => Array.from(store.values())),
        getById: jest.fn(async (id: string) => store.get(id) ?? null),
        updateSource: jest.fn(async (id: string, patch: Partial<IngestSource>) => {
            const existing = store.get(id);
            if (!existing) return null;
            const updated = { ...existing, ...patch } as IngestSource;
            store.set(id, updated);
            return updated;
        }),
        deleteSource: jest.fn(async (id: string) => { store.delete(id); }),
    },
}));

// ─── engine-singleton fake ────────────────────────────────────────────────

let _mockEngine: { triggerSync: jest.Mock } | null = null;

jest.mock('../../engine-singleton', () => ({
    getIngestEngine: jest.fn(() => _mockEngine),
}));

// ─── logger stub ──────────────────────────────────────────────────────────

jest.mock('../../../lib/logger', () => ({
    withContext: jest.fn(() => ({
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
    })),
}));

import { ingestSourcesRouter } from '../ingestSources';
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
            if (!res.ended) { res.status(404).json({ error: 'route not found' }); }
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
    app.use('/api/ingest/sources', ingestSourcesRouter);
    app.use(errorHandler);
    return app;
}

beforeEach(() => {
    store.clear();
    _mockEngine = null;
    jest.clearAllMocks();
});

// ─── helpers ──────────────────────────────────────────────────────────────

function validBody(overrides: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        name: 'My Source',
        type: 'filesystem',
        config: { rootPath: '/data' },
        ...overrides,
    };
}

async function createSource(app: express.Express, body: Record<string, unknown> = validBody()): Promise<IngestSource> {
    const res = await run(app, 'POST', '/api/ingest/sources', body);
    return res.body as IngestSource;
}

// ─── POST / — create ──────────────────────────────────────────────────────

describe('POST /api/ingest/sources', () => {
    test('201 with the new source on valid payload', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources', validBody());
        expect(res.statusCode).toBe(201);
        const body = res.body as IngestSource;
        expect(body.sourceId).toBeTruthy();
        expect(body.name).toBe('My Source');
        expect(body.type).toBe('filesystem');
        expect(body.status).toBe('active');
    });

    test('400 when name is missing', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources', { type: 'filesystem', config: {} });
        expect(res.statusCode).toBe(400);
    });

    test('400 when type is missing', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources', { name: 'X', config: {} });
        expect(res.statusCode).toBe(400);
    });

    test('400 when config is missing', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources', { name: 'X', type: 'filesystem' });
        expect(res.statusCode).toBe(400);
    });

    test('400 when config is an array', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources', { name: 'X', type: 'filesystem', config: [] });
        expect(res.statusCode).toBe(400);
    });

    test('assigns null defaults for optional fields', async () => {
        const app = buildApp();
        const res2 = await run(app, 'POST', '/api/ingest/sources', validBody());
        const b = res2.body as IngestSource;
        expect(b.syncSchedule).toBeNull();
        expect(b.defaultPipelineId).toBeNull();
        expect(b.lastSyncAt).toBeNull();
        expect(b.mappingRules).toEqual([]);
    });
});

// ─── GET / — list ─────────────────────────────────────────────────────────

describe('GET /api/ingest/sources', () => {
    test('returns empty sources array when none exist', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/sources');
        expect(res.statusCode).toBe(200);
        expect((res.body as { sources: unknown[] }).sources).toEqual([]);
    });

    test('returns all created sources', async () => {
        const app = buildApp();
        await createSource(app, validBody({ name: 'A' }));
        await createSource(app, validBody({ name: 'B' }));
        const res = await run(app, 'GET', '/api/ingest/sources');
        expect((res.body as { sources: unknown[] }).sources).toHaveLength(2);
    });
});

// ─── GET /:id — fetch one ─────────────────────────────────────────────────

describe('GET /api/ingest/sources/:id', () => {
    test('200 with the source on existing id', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'GET', `/api/ingest/sources/${created.sourceId}`);
        expect(res.statusCode).toBe(200);
        expect((res.body as IngestSource).sourceId).toBe(created.sourceId);
    });

    test('404 on unknown id', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/sources/does-not-exist');
        expect(res.statusCode).toBe(404);
    });
});

// ─── PUT /:id — update ────────────────────────────────────────────────────

describe('PUT /api/ingest/sources/:id', () => {
    test('updates name and returns updated source', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'PUT', `/api/ingest/sources/${created.sourceId}`, { name: 'Renamed' });
        expect(res.statusCode).toBe(200);
        expect((res.body as IngestSource).name).toBe('Renamed');
    });

    test('updates config', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'PUT', `/api/ingest/sources/${created.sourceId}`, {
            config: { rootPath: '/new' },
        });
        expect(res.statusCode).toBe(200);
        expect((res.body as IngestSource).config).toEqual({ rootPath: '/new' });
    });

    test('404 when source not found', async () => {
        const app = buildApp();
        const res = await run(app, 'PUT', '/api/ingest/sources/ghost', { name: 'X' });
        expect(res.statusCode).toBe(404);
    });
});

// ─── DELETE /:id ──────────────────────────────────────────────────────────

describe('DELETE /api/ingest/sources/:id', () => {
    test('204 on successful delete', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'DELETE', `/api/ingest/sources/${created.sourceId}`);
        expect(res.statusCode).toBe(204);
    });

    test('404 on unknown id', async () => {
        const app = buildApp();
        const res = await run(app, 'DELETE', '/api/ingest/sources/ghost');
        expect(res.statusCode).toBe(404);
    });

    test('source is no longer retrievable after delete', async () => {
        const app = buildApp();
        const created = await createSource(app);
        await run(app, 'DELETE', `/api/ingest/sources/${created.sourceId}`);
        const res = await run(app, 'GET', `/api/ingest/sources/${created.sourceId}`);
        expect(res.statusCode).toBe(404);
    });
});

// ─── POST /:id/sync ───────────────────────────────────────────────────────

describe('POST /api/ingest/sources/:id/sync', () => {
    test('202 with sync_triggered when engine is available', async () => {
        const app = buildApp();
        const created = await createSource(app);
        _mockEngine = { triggerSync: jest.fn().mockResolvedValue(undefined) };
        const res = await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/sync`);
        expect(res.statusCode).toBe(202);
        expect((res.body as { status: string }).status).toBe('sync_triggered');
    });

    test('503 when engine not available', async () => {
        const app = buildApp();
        const created = await createSource(app);
        _mockEngine = null;
        const res = await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/sync`);
        expect(res.statusCode).toBe(503);
    });

    test('404 when source not found', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources/ghost/sync');
        expect(res.statusCode).toBe(404);
    });
});

// ─── POST /:id/pause ──────────────────────────────────────────────────────

describe('POST /api/ingest/sources/:id/pause', () => {
    test('200 with status:paused', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/pause`);
        expect(res.statusCode).toBe(200);
        expect((res.body as { status: string }).status).toBe('paused');
    });

    test('404 when source not found', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources/ghost/pause');
        expect(res.statusCode).toBe(404);
    });
});

// ─── POST /:id/resume ─────────────────────────────────────────────────────

describe('POST /api/ingest/sources/:id/resume', () => {
    test('200 with status:active', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/resume`);
        expect(res.statusCode).toBe(200);
        expect((res.body as { status: string }).status).toBe('active');
    });

    test('resume after pause restores active status', async () => {
        const app = buildApp();
        const created = await createSource(app);
        await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/pause`);
        const res = await run(app, 'POST', `/api/ingest/sources/${created.sourceId}/resume`);
        expect((res.body as { status: string }).status).toBe('active');
    });

    test('404 when source not found', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/sources/ghost/resume');
        expect(res.statusCode).toBe(404);
    });
});

// ─── GET /:id/stats ───────────────────────────────────────────────────────

describe('GET /api/ingest/sources/:id/stats', () => {
    test('returns stats shape for existing source', async () => {
        const app = buildApp();
        const created = await createSource(app);
        const res = await run(app, 'GET', `/api/ingest/sources/${created.sourceId}/stats`);
        expect(res.statusCode).toBe(200);
        const body = res.body as Record<string, unknown>;
        expect(body.sourceId).toBe(created.sourceId);
        expect(body).toHaveProperty('lastSyncAt');
        expect(body).toHaveProperty('lastSyncStats');
        expect(body).toHaveProperty('status');
    });

    test('404 when source not found', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/sources/ghost/stats');
        expect(res.statusCode).toBe(404);
    });
});
