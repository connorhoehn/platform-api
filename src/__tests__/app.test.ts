// social-api/src/__tests__/app.test.ts
//
// Tests for createApp middleware: CORS headers, OPTIONS preflight,
// /internal/metrics endpoint. Uses raw http (no supertest dep).

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';
import { renderPrometheusText } from '../observability/metrics';
import { shouldParseJsonBody } from '../app';

// ─── helpers ───────────────────────────────────────────────────────────────

interface RawResponse {
    status: number;
    headers: Record<string, string | string[] | undefined>;
    body: string;
}

function rawRequest(
    server: http.Server,
    method: string,
    path: string,
    reqHeaders: Record<string, string> = {},
): Promise<RawResponse> {
    const { port } = server.address() as AddressInfo;
    return new Promise((resolve, reject) => {
        const req = http.request(
            { host: '127.0.0.1', port, method, path, headers: reqHeaders },
            (res) => {
                let body = '';
                res.on('data', (chunk: Buffer) => { body += chunk.toString('utf8'); });
                res.on('end', () => {
                    resolve({
                        status: res.statusCode ?? 0,
                        headers: res.headers as Record<string, string | string[] | undefined>,
                        body,
                    });
                });
                res.on('error', reject);
            },
        );
        req.on('error', reject);
        req.end();
    });
}

// Build a minimal express app with just the CORS middleware from app.ts
// (inlined here so we don't drag in the full routes/index transitive deps).
function buildCorsApp(): express.Application {
    const app = express();
    app.use((req, res, next) => {
        const origin = req.headers.origin;
        if (origin) res.header('Access-Control-Allow-Origin', origin);
        res.header('Vary', 'Origin');
        res.header('Access-Control-Allow-Credentials', 'true');
        res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
        res.header('Access-Control-Allow-Headers', 'Content-Type,Authorization');
        if (req.method === 'OPTIONS') { res.sendStatus(204); return; }
        next();
    });
    app.get('/ping', (_req, res) => res.json({ ok: true }));
    return app;
}

// Build a minimal app with just the /internal/metrics route.
function buildMetricsApp(): express.Application {
    const app = express();
    app.get('/internal/metrics', (_req, res) => {
        res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8');
        res.status(200).send(renderPrometheusText());
    });
    return app;
}

// ─── CORS ──────────────────────────────────────────────────────────────────

describe('CORS middleware', () => {
    let server: http.Server;
    beforeAll(async () => {
        server = http.createServer(buildCorsApp());
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    });
    afterAll(async () => {
        await new Promise<void>((resolve) => server.close(() => resolve()));
    });

    test('echoes Origin in Access-Control-Allow-Origin', async () => {
        const res = await rawRequest(server, 'GET', '/ping', { origin: 'http://localhost:3000' });
        expect(res.headers['access-control-allow-origin']).toBe('http://localhost:3000');
    });

    test('sets Vary: Origin', async () => {
        const res = await rawRequest(server, 'GET', '/ping', { origin: 'http://example.com' });
        expect(String(res.headers['vary'])).toContain('Origin');
    });

    test('sets Access-Control-Allow-Credentials: true', async () => {
        const res = await rawRequest(server, 'GET', '/ping', { origin: 'http://localhost:3000' });
        expect(res.headers['access-control-allow-credentials']).toBe('true');
    });

    test('OPTIONS preflight returns 204', async () => {
        const res = await rawRequest(server, 'OPTIONS', '/ping', { origin: 'http://localhost:3000' });
        expect(res.status).toBe(204);
    });

    test('OPTIONS response includes Allow-Methods with POST', async () => {
        const res = await rawRequest(server, 'OPTIONS', '/ping', { origin: 'http://localhost:3000' });
        expect(String(res.headers['access-control-allow-methods'])).toContain('POST');
    });

    test('no CORS header when Origin is absent', async () => {
        const res = await rawRequest(server, 'GET', '/ping', {});
        expect(res.headers['access-control-allow-origin']).toBeUndefined();
    });
});

// ─── /internal/metrics ─────────────────────────────────────────────────────

describe('/internal/metrics endpoint', () => {
    let server: http.Server;
    beforeAll(async () => {
        server = http.createServer(buildMetricsApp());
        await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    });
    afterAll(async () => {
        await new Promise<void>((resolve) => server.close(() => resolve()));
    });

    test('returns 200', async () => {
        const res = await rawRequest(server, 'GET', '/internal/metrics');
        expect(res.status).toBe(200);
    });

    test('Content-Type includes text/plain', async () => {
        const res = await rawRequest(server, 'GET', '/internal/metrics');
        expect(String(res.headers['content-type'])).toContain('text/plain');
    });

    test('body contains known metric names', async () => {
        const res = await rawRequest(server, 'GET', '/internal/metrics');
        expect(res.body).toContain('wsg_active_connections');
        expect(res.body).toContain('pipeline_triggers_total');
    });
});

// ─── global JSON parser scoping ────────────────────────────────────────────
//
// HMAC-verified webhook receivers mount their own `express.raw` and need the
// exact signed bytes. body-parser sets `req._body` once ANY parser runs, so a
// router-level raw parser no-ops if the global JSON parser got there first —
// the handler then sees a parsed object, `Buffer.isBuffer(req.body)` false,
// and computes its HMAC over zero bytes.
//
// /hooks/lvs was excluded; /hooks/pipeline was NOT, so every
// `Content-Type: application/json` pipeline webhook silently lost its payload
// and could never verify a signature (the live pod logged `bodySize: 0` for a
// 15-byte POST). These assert the real exported predicate, not a copy.

describe('shouldParseJsonBody', () => {
    test('skips pipeline webhooks so the raw body survives for HMAC', () => {
        expect(
            shouldParseJsonBody('/hooks/pipeline/weekly-digest', 'application/json'),
        ).toBe(false);
    });

    test('skips LVS webhooks', () => {
        expect(shouldParseJsonBody('/hooks/lvs/recording', 'application/json')).toBe(false);
    });

    test('parses ordinary JSON API traffic', () => {
        expect(shouldParseJsonBody('/api/pipelines/defs/p1', 'application/json')).toBe(true);
        expect(
            shouldParseJsonBody('/api/pipelines/defs/p1', 'application/json; charset=utf-8'),
        ).toBe(true);
    });

    test('leaves non-JSON content types alone', () => {
        expect(shouldParseJsonBody('/api/anything', 'text/plain')).toBe(false);
        expect(shouldParseJsonBody('/api/anything', '')).toBe(false);
    });
});
