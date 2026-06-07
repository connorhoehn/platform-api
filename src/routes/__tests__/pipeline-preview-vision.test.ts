// src/routes/__tests__/pipeline-preview-vision.test.ts
//
// 6 cases:
//   1. 400 missing imageUrl
//   2. 200 caption-only success
//   3. 200 with structured schema (extracted field present)
//   4. 413 image too large
//   5. content-policy refusal → 200 with empty caption + failureReason
//   6. throttled → 429

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';
import { errorHandler } from '../../middleware/error-handler';
import type { Request, Response, NextFunction } from 'express';

// ── Suppress Redis in tests ───────────────────────────────────────────────
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

// ── Skip Cognito JWT ──────────────────────────────────────────────────────
jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => {
    (_req as Request & { user?: { sub: string } }).user = { sub: 'test-vision-user' };
    next();
  },
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

// ── Mock vision client factory ────────────────────────────────────────────
const mockAnalyze = jest.fn();
jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: jest.fn(),
  getVisionResultRepo: jest.fn(),
  getVisionClient: () => ({ analyze: mockAnalyze }),
  __resetVisionServicesForTests: jest.fn(),
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
  ddbClient: { send: jest.fn() },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

// ── Tiny valid PNG bytes ──────────────────────────────────────────────────
const SMALL_PNG = Buffer.from(
  'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==',
  'base64',
);

// ── Mock global fetch ─────────────────────────────────────────────────────
function installFetchMock(body: Buffer, status = 200, contentType = 'image/png') {
  (global.fetch as jest.Mock | undefined);
  // We need to intercept the `fetch` call inside the route. The route uses
  // the Node global fetch. Jest sets up globalThis.fetch — we can replace it.
  (globalThis as unknown as { fetch: jest.Mock }).fetch = jest.fn().mockResolvedValue({
    ok: status >= 200 && status < 300,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    headers: {
      get: (header: string) => {
        if (header === 'content-type') return contentType;
        if (header === 'content-length') return String(body.length);
        return null;
      },
    },
    arrayBuffer: async () => {
      return body.buffer.slice(body.byteOffset, body.byteOffset + body.byteLength) as ArrayBuffer;
    },
  });
}

// Import AFTER mocks
import { pipelinePreviewVisionRouter } from '../pipeline-preview-vision';

// ── Express test app + HTTP server helpers ────────────────────────────────

function buildServer() {
  const app = express();
  app.use(express.json({ limit: '8kb' }));
  app.use('/api/pipeline/preview/vision', pipelinePreviewVisionRouter);
  app.use(errorHandler);
  return http.createServer(app);
}

function jsonRequest(
  server: http.Server,
  body: unknown,
  userId = 'test-vision-user',
): Promise<{ status: number; body: unknown }> {
  const payload = JSON.stringify(body);
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method: 'POST',
        path: '/api/pipeline/preview/vision',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(Buffer.byteLength(payload)),
          'X-Test-User': userId,
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => {
          let parsed: unknown;
          try { parsed = JSON.parse(Buffer.concat(chunks).toString('utf8')); } catch { parsed = null; }
          resolve({ status: res.statusCode ?? 0, body: parsed });
        });
      },
    );
    req.on('error', reject);
    req.write(payload);
    req.end();
  });
}

// ── Tests ─────────────────────────────────────────────────────────────────

describe('POST /api/pipeline/preview/vision', () => {
  let server: http.Server;

  beforeEach((done) => {
    _resetRateLimitMemFallback();
    mockAnalyze.mockReset();
    // Default successful caption response
    mockAnalyze.mockResolvedValue({
      output: { caption: 'A test image.', extracted: null },
      usage: { input_tokens: 100, output_tokens: 50 },
      rawText: 'A test image.',
      latencyMs: 123,
      cacheHit: false,
    });
    installFetchMock(SMALL_PNG);
    server = buildServer();
    server.listen(0, '127.0.0.1', done);
  });

  afterEach((done) => {
    server.close(done);
  });

  it('400 when imageUrl is missing', async () => {
    const { status, body } = await jsonRequest(server, {
      userPrompt: 'Describe this.',
    });
    expect(status).toBe(400);
    expect((body as { error: string }).error).toMatch(/imageUrl/);
  });

  it('200 caption-only success', async () => {
    const { status, body } = await jsonRequest(server, {
      imageUrl: 'https://example.com/image.png',
      userPrompt: 'Describe this image.',
    });
    expect(status).toBe(200);
    const b = body as { caption: string; usage: { inputTokens: number }; latencyMs: number; costUsd: number };
    expect(b.caption).toBe('A test image.');
    expect(b.usage.inputTokens).toBe(100);
    expect(typeof b.costUsd).toBe('number');
    expect(typeof b.latencyMs).toBe('number');
  });

  it('200 with structured schema — extracted field present', async () => {
    mockAnalyze.mockResolvedValueOnce({
      output: { caption: '', extracted: { label: 'cat', confidence: 0.92 } },
      usage: { input_tokens: 120, output_tokens: 30 },
      rawText: '{"label":"cat","confidence":0.92}',
      latencyMs: 200,
      cacheHit: false,
    });

    const { status, body } = await jsonRequest(server, {
      imageUrl: 'https://example.com/photo.jpg',
      userPrompt: 'Classify this image.',
      outputSchema: {
        type: 'object',
        properties: { label: { type: 'string' }, confidence: { type: 'number' } },
        required: ['label', 'confidence'],
      },
    });

    expect(status).toBe(200);
    const b = body as { extracted: { label: string; confidence: number } };
    expect(b.extracted).toEqual({ label: 'cat', confidence: 0.92 });
  });

  it('413 when image response exceeds 5 MB', async () => {
    const OVER_5MB = Buffer.alloc(5 * 1024 * 1024 + 1, 0xff);
    installFetchMock(OVER_5MB);

    const { status } = await jsonRequest(server, {
      imageUrl: 'https://example.com/huge.png',
      userPrompt: 'Describe this.',
    });
    expect(status).toBe(413);
  });

  it('200 with empty caption + failureReason on content-policy refusal', async () => {
    mockAnalyze.mockResolvedValueOnce({
      output: null,
      usage: { input_tokens: 80, output_tokens: 0 },
      rawText: '',
      latencyMs: 50,
      cacheHit: false,
      failureReason: 'refusal',
    });

    const { status, body } = await jsonRequest(server, {
      imageUrl: 'https://example.com/image.png',
      userPrompt: 'Describe this.',
    });
    expect(status).toBe(200);
    const b = body as { caption: string; failureReason: string };
    expect(b.caption).toBe('');
    expect(b.failureReason).toBe('refusal');
  });

  it('429 when rate limit is exceeded', async () => {
    // Rate limit is 10 calls/min. Fire 11 requests sequentially to ensure
    // the in-memory token bucket depletes.
    const LIMIT = 10;
    const statuses: number[] = [];
    for (let i = 0; i < LIMIT + 1; i++) {
      const { status } = await jsonRequest(server, {
        imageUrl: 'https://example.com/image.png',
        userPrompt: 'Describe this.',
      });
      statuses.push(status);
    }
    expect(statuses).toContain(429);
  });
});
