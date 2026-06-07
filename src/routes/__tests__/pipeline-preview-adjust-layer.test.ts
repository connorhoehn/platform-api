// src/routes/__tests__/pipeline-preview-adjust-layer.test.ts
//
// 6 cases:
//   1. 400 missing imageId
//   2. 404 unknown image
//   3. 200 single-layer success
//   4. 200 multi-layer success
//   5. 413 image too large
//   6. 400 malformed layer kind

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';
import sharp from 'sharp';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';
import { errorHandler } from '../../middleware/error-handler';
import type { Request, Response, NextFunction } from 'express';

// ── Suppress real Redis lookups in tests ──────────────────────────────────
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

// ── Shared in-memory blob store ──────────────────────────────────────────
const blobStore = new InMemoryImageBlobStore();

jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: jest.fn(() => blobStore),
  getVisionResultRepo: jest.fn(),
  getVisionClient: jest.fn(),
  __resetVisionServicesForTests: jest.fn(),
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
  ddbClient: { send: jest.fn() },
}));

jest.mock('../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));

// Skip Cognito JWT validation in tests.
jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => {
    (_req as Request & { user?: { sub: string } }).user = { sub: 'test-user' };
    next();
  },
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

// Import AFTER mocks
import { pipelinePreviewAdjustLayerRouter } from '../pipeline-preview-adjust-layer';

// ── Express test app + HTTP server helpers ────────────────────────────────

function buildServer() {
  const app = express();
  app.use(express.json({ limit: '4kb' }));
  app.use('/api/pipeline/preview/adjust-layer', pipelinePreviewAdjustLayerRouter);
  app.use(errorHandler);
  const server = http.createServer(app);
  return server;
}

function jsonRequest(
  server: http.Server,
  body: unknown,
): Promise<{ status: number; body: unknown }> {
  const payload = JSON.stringify(body);
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method: 'POST',
        path: '/api/pipeline/preview/adjust-layer',
        headers: {
          'Content-Type': 'application/json',
          'Content-Length': String(Buffer.byteLength(payload)),
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

// ── Test fixtures ─────────────────────────────────────────────────────────

async function makeTinyPng(): Promise<Buffer> {
  return sharp({
    create: { width: 4, height: 4, channels: 3, background: { r: 128, g: 64, b: 32 } },
  }).png().toBuffer();
}

// ── Tests ─────────────────────────────────────────────────────────────────

describe('POST /api/pipeline/preview/adjust-layer', () => {
  let server: http.Server;
  let knownImageId: string;

  beforeAll(async () => {
    const png = await makeTinyPng();
    knownImageId = 'test-image-id-' + Date.now();
    await blobStore.put(knownImageId, png, {
      mimeType: 'image/png',
      sizeBytes: png.length,
      width: 4,
      height: 4,
      uploadedAt: new Date().toISOString(),
    });
  });

  beforeEach((done) => {
    _resetRateLimitMemFallback();
    server = buildServer();
    server.listen(0, '127.0.0.1', done);
  });

  afterEach((done) => {
    server.close(done);
  });

  it('400 when imageId is missing', async () => {
    const { status, body } = await jsonRequest(server, { layers: [] });
    expect(status).toBe(400);
    expect((body as { error: string }).error).toMatch(/imageId/);
  });

  it('404 when image is unknown', async () => {
    const { status } = await jsonRequest(server, {
      imageId: 'does-not-exist-xyz',
      layers: [],
    });
    expect(status).toBe(404);
  });

  it('200 single-layer success', async () => {
    const { status, body } = await jsonRequest(server, {
      imageId: knownImageId,
      layers: [{ kind: 'brightness', value: 1.2 }],
    });
    expect(status).toBe(200);
    const b = body as { dataUrl: string; width: number; height: number; mimeType: string };
    expect(b.dataUrl).toMatch(/^data:image\/png;base64,/);
    expect(b.width).toBeGreaterThan(0);
    expect(b.height).toBeGreaterThan(0);
    expect(b.mimeType).toBe('image/png');
  });

  it('200 multi-layer success', async () => {
    const { status, body } = await jsonRequest(server, {
      imageId: knownImageId,
      layers: [
        { kind: 'brightness', value: 1.1 },
        { kind: 'contrast', value: 1.05 },
        { kind: 'saturation', value: 0.8 },
        { kind: 'hue', degrees: 30 },
        { kind: 'blur', radiusPx: 1 },
      ],
    });
    expect(status).toBe(200);
    const b = body as { dataUrl: string };
    expect(b.dataUrl).toMatch(/^data:image\/png;base64,/);
  });

  it('413 when stored image exceeds 5 MB', async () => {
    // Override getImageBlobStore to return an oversized image for this test.
    const { getImageBlobStore } = require('../../pipeline/image/visionServices') as {
      getImageBlobStore: jest.Mock;
    };
    const OVER_5MB = Buffer.alloc(5 * 1024 * 1024 + 1, 0xab);
    getImageBlobStore.mockReturnValueOnce({
      get: jest.fn().mockResolvedValue({
        body: OVER_5MB,
        meta: { mimeType: 'image/png', sizeBytes: OVER_5MB.length, width: 1, height: 1, uploadedAt: '' },
      }),
      put: jest.fn(),
      getMetadata: jest.fn(),
    });

    const { status } = await jsonRequest(server, {
      imageId: 'oversize-image',
      layers: [],
    });
    expect(status).toBe(413);
  });

  it('400 when layer has unknown kind', async () => {
    const { status, body } = await jsonRequest(server, {
      imageId: knownImageId,
      layers: [{ kind: 'invert', value: 1 }],
    });
    expect(status).toBe(400);
    expect((body as { error: string }).error).toMatch(/kind/i);
  });
});
