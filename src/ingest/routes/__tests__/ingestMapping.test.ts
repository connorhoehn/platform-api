// social-api/src/ingest/routes/__tests__/ingestMapping.test.ts
//
// Integration tests for ingestMappingRouter: POST /preview dry-run endpoint.
// matchByRules is not mocked — it's a pure function exercised directly.

import express, { type NextFunction, type Request, type Response } from 'express';
import type { MappingResult } from '../../types';

jest.mock('../../../lib/logger', () => ({
    withContext: jest.fn(() => ({ info: jest.fn(), warn: jest.fn(), error: jest.fn() })),
}));

import { ingestMappingRouter } from '../ingestMapping';
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

async function run(app: express.Express, body: unknown): Promise<MockRes> {
    const res = mockRes();
    const req = {
        method: 'POST',
        url: '/api/ingest/mapping/preview',
        originalUrl: '/api/ingest/mapping/preview',
        path: '/api/ingest/mapping/preview',
        headers: { 'content-type': 'application/json' },
        query: {},
        body,
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
    app.use('/api/ingest/mapping', ingestMappingRouter);
    app.use(errorHandler);
    return app;
}

function pdfFile() {
    return { path: '/docs/report.pdf', name: 'report.pdf', hash: 'abc', size: 1024, mimeType: 'application/pdf', lastModified: '2026-01-01T00:00:00Z' };
}

function pdfRule(targetTypeId = 'type-pdf') {
    return { ruleId: 'r1', condition: { field: 'extension', op: 'eq', value: 'pdf' }, targetTypeId, fieldMapping: { title: 'f-title' }, priority: 10 };
}

// ─── validation ───────────────────────────────────────────────────────────

describe('POST /preview — validation', () => {
    test('400 when file is missing', async () => {
        const app = buildApp();
        const res = await run(app, { rules: [] });
        expect(res.statusCode).toBe(400);
    });

    test('400 when file is an array', async () => {
        const app = buildApp();
        const res = await run(app, { file: [], rules: [] });
        expect(res.statusCode).toBe(400);
    });

    test('400 when rules is missing', async () => {
        const app = buildApp();
        const res = await run(app, { file: pdfFile() });
        expect(res.statusCode).toBe(400);
    });

    test('400 when rules is not an array', async () => {
        const app = buildApp();
        const res = await run(app, { file: pdfFile(), rules: 'not-array' });
        expect(res.statusCode).toBe(400);
    });

    test('400 when file lacks path', async () => {
        const app = buildApp();
        const res = await run(app, { file: { name: 'f.pdf' }, rules: [] });
        expect(res.statusCode).toBe(400);
    });

    test('400 when file lacks name', async () => {
        const app = buildApp();
        const res = await run(app, { file: { path: '/f.pdf' }, rules: [] });
        expect(res.statusCode).toBe(400);
    });
});

// ─── matching ─────────────────────────────────────────────────────────────

describe('POST /preview — matching', () => {
    test('result is null when no rules match', async () => {
        const app = buildApp();
        const res = await run(app, { file: pdfFile(), rules: [] });
        expect(res.statusCode).toBe(200);
        expect((res.body as { result: unknown }).result).toBeNull();
    });

    test('returns MappingResult when rule matches', async () => {
        const app = buildApp();
        const res = await run(app, { file: pdfFile(), rules: [pdfRule()] });
        expect(res.statusCode).toBe(200);
        const result = (res.body as { result: MappingResult }).result;
        expect(result).not.toBeNull();
        expect(result.targetTypeId).toBe('type-pdf');
        expect(result.method).toBe('rule');
        expect(result.ruleId).toBe('r1');
    });

    test('rule with non-matching condition returns null result', async () => {
        const app = buildApp();
        const csvRule = { ...pdfRule(), condition: { field: 'extension', op: 'eq', value: 'csv' } };
        const res = await run(app, { file: pdfFile(), rules: [csvRule] });
        expect((res.body as { result: unknown }).result).toBeNull();
    });

    test('best-priority rule wins when multiple match', async () => {
        const app = buildApp();
        const hiPri = { ...pdfRule('type-hi'), ruleId: 'hi', priority: 1 };
        const loPri = { ...pdfRule('type-lo'), ruleId: 'lo', priority: 5 };
        const res = await run(app, { file: pdfFile(), rules: [loPri, hiPri] });
        const result = (res.body as { result: MappingResult }).result;
        expect(result.targetTypeId).toBe('type-hi');
    });
});
