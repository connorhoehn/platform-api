// social-api/src/ingest/routes/__tests__/ingestWebhooks.test.ts
//
// Tests for createIngestWebhookRoutes: source-not-found, signature validation,
// Drive/S3/generic payload parsing, sync trigger vs dedup, and error handling.

import express, { type NextFunction, type Request, type Response } from 'express';
import * as crypto from 'crypto';
import { createIngestWebhookRoutes } from '../ingestWebhooks';
import type { IngestSource } from '../../types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
    return {
        sourceId: 'src-1',
        name: 'Test Source',
        type: 'webhook',
        config: {},
        syncSchedule: null,
        mappingRules: [],
        defaultPipelineId: null,
        status: 'active',
        lastSyncAt: null,
        lastSyncStats: null,
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeRepo(source: IngestSource | null = makeSource()) {
    return { get: jest.fn(async () => source) };
}

function makeHandle(deduped = false) {
    return { syncProducer: { send: jest.fn(async () => ({ deduped })) } };
}

function buildApp(repoSource: IngestSource | null = makeSource(), deduped = false) {
    const app = express();
    app.use(express.json());
    app.use(
        '/api/ingest/webhooks',
        createIngestWebhookRoutes({
            sourceRepository: makeRepo(repoSource) as never,
            ingestHandle: makeHandle(deduped) as never,
        }),
    );
    return app;
}

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
    url: string,
    body: unknown,
    extraHeaders: Record<string, string> = {},
): Promise<MockRes> {
    const res = mockRes();
    const req = {
        method: 'POST',
        url,
        originalUrl: url,
        path: url,
        headers: { 'content-type': 'application/json', ...extraHeaders },
        query: {},
        body,
        params: {},
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

// ─── source not found ─────────────────────────────────────────────────────

describe('POST /:sourceId — source not found', () => {
    test('404 when source does not exist', async () => {
        const app = buildApp(null);
        const res = await run(app, '/api/ingest/webhooks/ghost', {});
        expect(res.statusCode).toBe(404);
        expect((res.body as { error: string }).error).toContain('not found');
    });
});

// ─── signature validation ─────────────────────────────────────────────────

describe('POST /:sourceId — signature validation', () => {
    test('passes through when no x-webhook-signature header', async () => {
        const source = makeSource({ type: 'local', config: {} });
        const app = buildApp(source);
        const res = await run(app, '/api/ingest/webhooks/src-1', { files: [{ path: '/a.txt', action: 'modified' }] });
        expect(res.statusCode).toBe(200);
    });

    test('403 when signature is invalid', async () => {
        const source = makeSource({ config: { webhookSecret: 'my-secret' } });
        const app = buildApp(source);
        const res = await run(app, '/api/ingest/webhooks/src-1', {}, { 'x-webhook-signature': 'bad-sig' });
        expect(res.statusCode).toBe(403);
    });

    test('passes with correct HMAC-SHA256 signature', async () => {
        const secret = 'correct-secret';
        const payload = { files: [{ path: '/file.txt', action: 'modified' }] };
        const payloadString = JSON.stringify(payload);
        const expected = 'sha256=' + crypto.createHmac('sha256', secret).update(payloadString).digest('hex');
        const source = makeSource({ config: { webhookSecret: secret } });
        const app = buildApp(source);
        const res = await run(app, '/api/ingest/webhooks/src-1', payload, { 'x-webhook-signature': expected });
        expect(res.statusCode).toBe(200);
    });

    test('no-secret source: signature header present but skips real validation', async () => {
        const source = makeSource({ config: {} });
        const app = buildApp(source);
        // Any signature is accepted when no secret is configured
        const res = await run(app, '/api/ingest/webhooks/src-1', { files: [] }, { 'x-webhook-signature': 'any' });
        expect(res.statusCode).toBe(200);
    });
});

// ─── no changes ───────────────────────────────────────────────────────────

describe('POST /:sourceId — no changes', () => {
    test('returns no_changes when payload produces no files', async () => {
        const app = buildApp(makeSource({ type: 'local' }));
        const res = await run(app, '/api/ingest/webhooks/src-1', {});
        expect(res.statusCode).toBe(200);
        expect((res.body as { status: string }).status).toBe('no_changes');
    });

    test('returns no_changes for empty files array', async () => {
        const app = buildApp(makeSource({ type: 'local' }));
        const res = await run(app, '/api/ingest/webhooks/src-1', { files: [] });
        expect((res.body as { status: string }).status).toBe('no_changes');
    });
});

// ─── payload parsing ──────────────────────────────────────────────────────

describe('POST /:sourceId — Google Drive payload', () => {
    test('parses drive changes and triggers sync', async () => {
        const source = makeSource({ type: 'google-drive', config: {} });
        const app = buildApp(source);
        const payload = {
            kind: 'drive#change',
            changes: [
                { fileId: 'f1', file: { name: 'doc.pdf', mimeType: 'application/pdf' }, removed: false },
                { fileId: 'f2', file: { name: 'old.txt', mimeType: 'text/plain' }, removed: true },
            ],
        };
        const res = await run(app, '/api/ingest/webhooks/src-1', payload);
        expect(res.statusCode).toBe(200);
        const body = res.body as { files: Array<{ path: string; action: string }> };
        expect(body.files).toHaveLength(2);
        expect(body.files[0]).toEqual({ path: 'doc.pdf', action: 'modified' });
        expect(body.files[1]).toEqual({ path: 'old.txt', action: 'deleted' });
    });
});

describe('POST /:sourceId — S3 event payload', () => {
    test('parses S3 PutObject record', async () => {
        const source = makeSource({ type: 's3', config: {} });
        const app = buildApp(source);
        const payload = {
            Records: [
                { eventName: 'ObjectCreated:Put', s3: { bucket: { name: 'my-bucket' }, object: { key: 'docs/report.pdf', eTag: 'abc' } } },
            ],
        };
        const res = await run(app, '/api/ingest/webhooks/src-1', payload);
        const body = res.body as { files: Array<{ path: string; action: string }> };
        expect(body.files[0]).toEqual({ path: 'docs/report.pdf', action: 'modified' });
    });

    test('parses S3 Delete record', async () => {
        const source = makeSource({ type: 's3', config: {} });
        const app = buildApp(source);
        const payload = {
            Records: [
                { eventName: 'ObjectRemoved:Delete', s3: { bucket: { name: 'b' }, object: { key: 'old.txt', eTag: 'x' } } },
            ],
        };
        const res = await run(app, '/api/ingest/webhooks/src-1', payload);
        const body = res.body as { files: Array<{ path: string; action: string }> };
        expect(body.files[0].action).toBe('deleted');
    });
});

describe('POST /:sourceId — generic payload', () => {
    test('uses files array from generic payload', async () => {
        const app = buildApp(makeSource({ type: 'local', config: {} }));
        const payload = { files: [{ path: '/report.csv', action: 'modified' }] };
        const res = await run(app, '/api/ingest/webhooks/src-1', payload);
        const body = res.body as { files: Array<{ path: string }> };
        expect(body.files[0].path).toBe('/report.csv');
    });
});

// ─── sync trigger / dedup ─────────────────────────────────────────────────

describe('POST /:sourceId — sync trigger', () => {
    test('returns sync_triggered when not deduped', async () => {
        const source = makeSource({ type: 'local', config: {} });
        const app = buildApp(source, false);
        const res = await run(app, '/api/ingest/webhooks/src-1', { files: [{ path: '/a.txt', action: 'modified' }] });
        expect((res.body as { status: string }).status).toBe('sync_triggered');
    });

    test('returns sync_in_progress when deduped', async () => {
        const source = makeSource({ type: 'local', config: {} });
        const app = buildApp(source, true);
        const res = await run(app, '/api/ingest/webhooks/src-1', { files: [{ path: '/a.txt', action: 'modified' }] });
        expect((res.body as { status: string }).status).toBe('sync_in_progress');
    });

    test('includes filesChanged count in response', async () => {
        const source = makeSource({ type: 'local', config: {} });
        const app = buildApp(source, false);
        const payload = { files: [{ path: '/a.txt', action: 'modified' }, { path: '/b.txt', action: 'modified' }] };
        const res = await run(app, '/api/ingest/webhooks/src-1', payload);
        expect((res.body as { filesChanged: number }).filesChanged).toBe(2);
    });
});
