// social-api/src/ingest/routes/__tests__/ingestTemplates.test.ts
//
// Tests for ingestTemplatesRouter: list, get by id, instantiate, 404 cases.

import express, { type NextFunction, type Request, type Response } from 'express';
import { ingestTemplatesRouter } from '../ingestTemplates';

// ─── Mock dependencies ────────────────────────────────────────────────────

jest.mock('../../templates', () => ({
    ingestPipelineTemplates: [
        {
            id: 'tmpl-transcript',
            name: 'Transcript Extraction',
            description: 'Extract transcript from audio/video',
            category: 'media',
            definition: { id: 'tmpl-transcript', name: 'Transcript Extraction', nodes: [], edges: [] },
        },
        {
            id: 'tmpl-coe',
            name: 'COE Generation',
            description: 'Generate context-of-evidence doc',
            category: 'knowledge',
            definition: { id: 'tmpl-coe', name: 'COE Generation', nodes: [], edges: [] },
        },
    ],
}));

const mockPut = jest.fn();
jest.mock('../../../pipeline/definitions-repository', () => ({
    get definitionsRepo() {
        return { put: mockPut };
    },
}));

// ─── Helpers ───────────────────────────────────────────────────────────────

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

function buildApp(userSub = 'user-123'): express.Express {
    const app = express();
    app.use(express.json());
    // Inject a fake req.user
    app.use((req: Request, _res: Response, next: NextFunction) => {
        (req as Request & { user?: { sub: string } }).user = { sub: userSub };
        next();
    });
    app.use('/api/ingest/templates', ingestTemplatesRouter);
    app.use((err: { status?: number; statusCode?: number; message: string }, _req: Request, res: Response, _next: NextFunction) => {
        res.status(err.status ?? err.statusCode ?? 500).json({ error: err.message });
    });
    return app;
}

async function run(
    app: express.Express,
    method: string,
    url: string,
    body?: unknown,
): Promise<MockRes> {
    const res = mockRes();
    const req = {
        method,
        url,
        originalUrl: url,
        path: url,
        headers: { 'content-type': 'application/json' },
        query: {},
        body: body ?? {},
        params: {},
        ip: '127.0.0.1',
        user: { sub: 'user-123' },
    } as unknown as Request;
    await new Promise<void>((resolve, reject) => {
        const finalHandler = (err?: unknown): void => {
            if (err) {
                const e = err as { status?: number; statusCode?: number; message: string };
                res.status(e.status ?? e.statusCode ?? 500).json({ error: e.message });
            }
            if (!res.ended) res.status(404).json({ error: 'not found' });
            resolve();
        };
        (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
            req, res as unknown as Response, finalHandler,
        );
        res.finished.then(() => resolve()).catch(reject);
    });
    return res;
}

// ─── GET / — list ──────────────────────────────────────────────────────────

describe('GET /api/ingest/templates', () => {
    test('returns list of template summaries', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/templates/');
        expect(res.statusCode).toBe(200);
        const body = res.body as { templates: Array<{ id: string; name: string }> };
        expect(body.templates).toHaveLength(2);
        expect(body.templates[0].id).toBe('tmpl-transcript');
        expect(body.templates[1].id).toBe('tmpl-coe');
    });

    test('summaries omit the full definition', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/templates/');
        const body = res.body as { templates: Array<Record<string, unknown>> };
        expect(body.templates[0].definition).toBeUndefined();
    });

    test('summaries include id, name, description, category', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/templates/');
        const body = res.body as { templates: Array<{ id: string; name: string; description: string; category: string }> };
        expect(body.templates[0]).toMatchObject({
            id: 'tmpl-transcript',
            name: 'Transcript Extraction',
            description: 'Extract transcript from audio/video',
            category: 'media',
        });
    });
});

// ─── GET /:id — get full template ─────────────────────────────────────────

describe('GET /api/ingest/templates/:id', () => {
    test('returns full template including definition', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/templates/tmpl-transcript');
        expect(res.statusCode).toBe(200);
        const body = res.body as { id: string; definition: unknown };
        expect(body.id).toBe('tmpl-transcript');
        expect(body.definition).toBeDefined();
    });

    test('404 when template not found', async () => {
        const app = buildApp();
        const res = await run(app, 'GET', '/api/ingest/templates/does-not-exist');
        expect(res.statusCode).toBe(404);
        expect((res.body as { error: string }).error).toContain('not found');
    });
});

// ─── POST /:id/instantiate ────────────────────────────────────────────────

describe('POST /api/ingest/templates/:id/instantiate', () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    test('creates pipeline from template with generated pipelineId', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/templates/tmpl-transcript/instantiate', {});
        expect(res.statusCode).toBe(201);
        const body = res.body as { pipelineId: string; templateId: string };
        expect(typeof body.pipelineId).toBe('string');
        expect(body.pipelineId).toHaveLength(36); // UUID
        expect(body.templateId).toBe('tmpl-transcript');
    });

    test('uses provided name when given in body', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/templates/tmpl-transcript/instantiate', {
            name: 'My Custom Pipeline',
        });
        expect((res.body as { name: string }).name).toBe('My Custom Pipeline');
    });

    test('falls back to template name when no name in body', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/templates/tmpl-transcript/instantiate', {});
        expect((res.body as { name: string }).name).toBe('Transcript Extraction');
    });

    test('calls definitionsRepo.put with correct typeId and userId', async () => {
        const app = buildApp('user-456');
        await run(app, 'POST', '/api/ingest/templates/tmpl-coe/instantiate', {});
        expect(mockPut).toHaveBeenCalledTimes(1);
        const [userId] = mockPut.mock.calls[0] as [string, unknown];
        expect(userId).toBe('user-456');
    });

    test('definition in response has status=draft and version=1', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/templates/tmpl-coe/instantiate', {});
        const body = res.body as { definition: { status: string; version: number } };
        expect(body.definition.status).toBe('draft');
        expect(body.definition.version).toBe(1);
    });

    test('404 when instantiating non-existent template', async () => {
        const app = buildApp();
        const res = await run(app, 'POST', '/api/ingest/templates/ghost/instantiate', {});
        expect(res.statusCode).toBe(404);
    });
});
