// platform-api/src/routes/__tests__/images.test.ts
//
// Integration tests for /api/images. We:
//   - mock visionServices.getImageBlobStore() to return an InMemoryImageBlobStore
//   - generate tiny valid JPEG/PNG bytes via sharp (no fixture files)
//   - drive the express app over raw http (same pattern as lvsWebhooks.test.ts)
//
// What we cover:
//   - 201 happy path with Content-Type: image/jpeg → imageId returned, body round-trips
//   - 400 on unsupported Content-Type
//   - 400 on empty body
//   - 400 on corrupted bytes (sharp throws)
//   - 404 GET when imageId missing
//   - GET metadata returns mimeType/size/dimensions

// Loop 29c: image access has its own suite (imageAccess.test.ts).
jest.mock('../../pipeline/image/imageAccess', () => {
  const actual = jest.requireActual('../../pipeline/image/imageAccess');
  return { ...actual, requireImageAccess: () => Object.assign((_q: unknown, _s: unknown, n: () => void) => n(), { accessPolicy: 'image:uploader' }) };
});

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';
import sharp from 'sharp';
import { InMemoryImageBlobStore } from '../../pipeline/image/ImageBlobStore';
import { errorHandler } from '../../middleware/error-handler';

const blobStore = new InMemoryImageBlobStore();

jest.mock('../../pipeline/image/visionServices', () => ({
  getImageBlobStore: () => blobStore,
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

// Import AFTER mocks
import { imagesRouter } from '../images';

// ---------------------------------------------------------------------------
// Raw http helpers (mirror lvsWebhooks.test.ts)
// ---------------------------------------------------------------------------

function rawRequest(
  server: http.Server,
  method: 'POST' | 'GET',
  path: string,
  body: Buffer | null,
  headers: Record<string, string> = {},
): Promise<{ status: number; body: Buffer; headers: http.IncomingHttpHeaders }> {
  const { port } = server.address() as AddressInfo;
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        host: '127.0.0.1',
        port,
        method,
        path,
        headers: {
          ...(body ? { 'Content-Length': String(body.length) } : {}),
          ...headers,
        },
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on('data', (c: Buffer) => chunks.push(c));
        res.on('end', () => resolve({ status: res.statusCode ?? 0, body: Buffer.concat(chunks), headers: res.headers }));
      },
    );
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

async function makeTinyJpeg(): Promise<Buffer> {
  return sharp({
    create: {
      width: 100,
      height: 100,
      channels: 3,
      background: { r: 255, g: 0, b: 0 },
    },
  })
    .jpeg({ quality: 85 })
    .toBuffer();
}

async function makeTinyPng(): Promise<Buffer> {
  return sharp({
    create: {
      width: 50,
      height: 50,
      channels: 4,
      background: { r: 0, g: 255, b: 0, alpha: 1 },
    },
  })
    .png()
    .toBuffer();
}

// ---------------------------------------------------------------------------
// Setup
// ---------------------------------------------------------------------------

let server: http.Server;

beforeAll(async () => {
  const app = express();
  // Attach a stub req.user so the routes resolve userId.
  app.use((req, _res, next) => {
    (req as unknown as { user: { sub: string } }).user = { sub: 'tester' };
    next();
  });
  app.use('/api/images', imagesRouter);
  app.use(errorHandler);
  server = http.createServer(app);
  await new Promise<void>((r) => server.listen(0, '127.0.0.1', r));
});

afterAll(async () => {
  await new Promise<void>((r) => server.close(() => r()));
});

beforeEach(() => {
  blobStore.reset();
});

// ---------------------------------------------------------------------------
// POST /api/images
// ---------------------------------------------------------------------------

describe('POST /api/images', () => {
  it('returns 201 + imageId on a valid JPEG upload', async () => {
    const jpeg = await makeTinyJpeg();
    const res = await rawRequest(server, 'POST', '/api/images', jpeg, {
      'Content-Type': 'image/jpeg',
    });
    expect(res.status).toBe(201);
    const body = JSON.parse(res.body.toString()) as { imageId: string; mimeType: string; sizeBytes: number; width: number; height: number };
    expect(body.imageId).toMatch(/^[0-9a-f-]{36}$/);
    expect(body.mimeType).toBe('image/jpeg');
    expect(body.width).toBe(100);
    expect(body.height).toBe(100);
    expect(body.sizeBytes).toBeGreaterThan(0);
  });

  it('persists to the blob store + the bytes round-trip via GET', async () => {
    const jpeg = await makeTinyJpeg();
    const post = await rawRequest(server, 'POST', '/api/images', jpeg, {
      'Content-Type': 'image/jpeg',
    });
    const { imageId } = JSON.parse(post.body.toString()) as { imageId: string };

    const get = await rawRequest(server, 'GET', `/api/images/${imageId}`, null);
    expect(get.status).toBe(200);
    expect(get.headers['content-type']).toBe('image/jpeg');
    // Sharp re-encoded so byte-equality is wrong — but the body must
    // start with the JPEG magic header.
    expect(get.body[0]).toBe(0xff);
    expect(get.body[1]).toBe(0xd8);
  });

  it('accepts PNG uploads', async () => {
    const png = await makeTinyPng();
    const res = await rawRequest(server, 'POST', '/api/images', png, {
      'Content-Type': 'image/png',
    });
    expect(res.status).toBe(201);
    const body = JSON.parse(res.body.toString()) as { mimeType: string; width: number };
    expect(body.mimeType).toBe('image/png');
    expect(body.width).toBe(50);
  });

  it('rejects unsupported Content-Type with 400', async () => {
    const jpeg = await makeTinyJpeg();
    const res = await rawRequest(server, 'POST', '/api/images', jpeg, {
      'Content-Type': 'image/gif',
    });
    expect(res.status).toBe(400);
  });

  it('rejects missing Content-Type with 400', async () => {
    const jpeg = await makeTinyJpeg();
    const res = await rawRequest(server, 'POST', '/api/images', jpeg, {
      // express-raw uses { type: () => true } so it'll parse but route
      // explicitly checks Content-Type whitelist after.
      'Content-Type': '',
    });
    expect(res.status).toBe(400);
  });

  it('rejects corrupted image bytes with 400 (sharp decode error)', async () => {
    const garbage = Buffer.from('this is not a jpeg, no matter how hard you try');
    const res = await rawRequest(server, 'POST', '/api/images', garbage, {
      'Content-Type': 'image/jpeg',
    });
    expect(res.status).toBe(400);
  });

  it('rejects empty body with 400', async () => {
    const res = await rawRequest(server, 'POST', '/api/images', Buffer.alloc(0), {
      'Content-Type': 'image/jpeg',
      'Content-Length': '0',
    });
    expect(res.status).toBe(400);
  });
});

// ---------------------------------------------------------------------------
// GET /api/images/:imageId
// ---------------------------------------------------------------------------

describe('GET /api/images/:imageId', () => {
  it('returns 404 when imageId is unknown', async () => {
    const res = await rawRequest(server, 'GET', '/api/images/img_nope', null);
    expect(res.status).toBe(404);
  });

  it('returns 200 + the raw bytes for a stored image', async () => {
    const jpeg = await makeTinyJpeg();
    const post = await rawRequest(server, 'POST', '/api/images', jpeg, {
      'Content-Type': 'image/jpeg',
    });
    const { imageId, sizeBytes } = JSON.parse(post.body.toString()) as { imageId: string; sizeBytes: number };
    const get = await rawRequest(server, 'GET', `/api/images/${imageId}`, null);
    expect(get.status).toBe(200);
    expect(get.body.byteLength).toBe(sizeBytes);
  });
});

// ---------------------------------------------------------------------------
// GET /api/images/:imageId/metadata
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// POST /api/images/from-url
// ---------------------------------------------------------------------------

describe('POST /api/images/from-url', () => {
  const realFetch = global.fetch;
  const realAllow = process.env.ALLOW_INTERNAL_IMAGE_FETCH;

  beforeAll(() => {
    process.env.ALLOW_INTERNAL_IMAGE_FETCH = 'true';
  });
  afterAll(() => {
    if (realAllow === undefined) delete process.env.ALLOW_INTERNAL_IMAGE_FETCH;
    else process.env.ALLOW_INTERNAL_IMAGE_FETCH = realAllow;
    global.fetch = realFetch;
  });

  afterEach(() => {
    global.fetch = realFetch;
  });

  it('returns 400 when body has no url field', async () => {
    const res = await rawRequest(server, 'POST', '/api/images/from-url', Buffer.from('{}'), {
      'Content-Type': 'application/json',
    });
    expect(res.status).toBe(400);
  });

  it('returns 400 for an invalid URL string', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'not a url' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('returns 400 for non-http protocols', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'file:///etc/passwd' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('fetches the URL, processes, and returns 201 + imageId', async () => {
    const jpeg = await makeTinyJpeg();
    global.fetch = jest.fn().mockResolvedValueOnce(
      new Response(jpeg, {
        status: 200,
        headers: { 'content-type': 'image/jpeg', 'content-length': String(jpeg.byteLength) },
      }),
    ) as unknown as typeof fetch;

    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/cat.jpg' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(201);
    const body = JSON.parse(res.body.toString()) as { imageId: string; mimeType: string };
    expect(body.imageId).toMatch(/^[0-9a-f-]{36}$/);
    expect(body.mimeType).toBe('image/jpeg');
  });

  it('returns 400 when the URL response has an unsupported Content-Type', async () => {
    global.fetch = jest.fn().mockResolvedValueOnce(
      new Response('not an image', {
        status: 200,
        headers: { 'content-type': 'text/html' },
      }),
    ) as unknown as typeof fetch;

    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/page' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('returns 400 when the URL is unreachable (fetch throws)', async () => {
    global.fetch = jest.fn().mockRejectedValueOnce(new Error('ENOTFOUND')) as unknown as typeof fetch;
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://nowhere.invalid/cat.jpg' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('returns 400 when the upstream returns non-2xx', async () => {
    global.fetch = jest.fn().mockResolvedValueOnce(
      new Response('not found', { status: 404, statusText: 'Not Found' }),
    ) as unknown as typeof fetch;
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/missing.jpg' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });
});

describe('POST /api/images/from-url — SSRF defense', () => {
  const realAllow = process.env.ALLOW_INTERNAL_IMAGE_FETCH;
  beforeAll(() => {
    delete process.env.ALLOW_INTERNAL_IMAGE_FETCH;
  });
  afterAll(() => {
    if (realAllow !== undefined) process.env.ALLOW_INTERNAL_IMAGE_FETCH = realAllow;
  });

  it('refuses to fetch from 127.0.0.1', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'http://127.0.0.1:8000/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('refuses to fetch from 10.0.0.0/8', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'http://10.0.0.5/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('refuses to fetch from *.internal', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'http://something.internal/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('refuses to fetch from IPv6 loopback ([::1])', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'http://[::1]:8000/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('refuses to fetch from IPv6 link-local (fe80::1)', async () => {
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'http://[fe80::1]/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it.each(['::ffff:10.0.0.1', '::ffff:a00:1', '::ffff:127.0.0.1', '::ffff:169.254.169.254'])('refuses IPv4-mapped IPv6 %s before any fetch', async hostname => {
    const fetchSpy = jest.spyOn(global, 'fetch').mockRejectedValue(new Error('private URL must not reach fetch'));
    try {
      const res = await rawRequest(
        server,
        'POST',
        '/api/images/from-url',
        Buffer.from(JSON.stringify({ url: `http://[${hostname}]/img` })),
        { 'Content-Type': 'application/json' },
      );
      expect(res.status).toBe(400);
      expect(fetchSpy).not.toHaveBeenCalled();
    } finally { fetchSpy.mockRestore(); }
  });

  it('rejects a 302 redirect that lands on a private address', async () => {
    // Redirect chain: public URL → private URL. Native fetch with
    // redirect:'follow' would silently follow. Our ssrfSafeFetch
    // re-validates at every hop.
    global.fetch = jest.fn()
      .mockResolvedValueOnce(
        new Response(null, { status: 302, headers: { location: 'http://10.0.0.5/img' } }),
      ) as unknown as typeof fetch;
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
    expect(res.body.toString()).toMatch(/internal address/);
  });

  it('rejects a 302 redirect to a non-http(s) URL', async () => {
    global.fetch = jest.fn()
      .mockResolvedValueOnce(
        new Response(null, { status: 302, headers: { location: 'file:///etc/passwd' } }),
      ) as unknown as typeof fetch;
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
  });

  it('rejects after exceeding the max-redirect hop count', async () => {
    // Chain 7 redirects (one more than our max of 5 hops past the
    // initial = 6 total fetches). The 7th should error.
    const redir = (target: string) => new Response(null, { status: 302, headers: { location: target } });
    global.fetch = jest.fn()
      .mockResolvedValueOnce(redir('https://a.example.com/img'))
      .mockResolvedValueOnce(redir('https://b.example.com/img'))
      .mockResolvedValueOnce(redir('https://c.example.com/img'))
      .mockResolvedValueOnce(redir('https://d.example.com/img'))
      .mockResolvedValueOnce(redir('https://e.example.com/img'))
      .mockResolvedValueOnce(redir('https://f.example.com/img'))
      .mockResolvedValueOnce(redir('https://g.example.com/img')) as unknown as typeof fetch;
    const res = await rawRequest(
      server,
      'POST',
      '/api/images/from-url',
      Buffer.from(JSON.stringify({ url: 'https://example.com/img' })),
      { 'Content-Type': 'application/json' },
    );
    expect(res.status).toBe(400);
    expect(res.body.toString()).toMatch(/too many redirects/);
  });
});

describe('GET /api/images/:imageId/metadata', () => {
  it('returns 404 when imageId is unknown', async () => {
    const res = await rawRequest(server, 'GET', '/api/images/img_nope/metadata', null);
    expect(res.status).toBe(404);
  });

  it('returns the metadata fields without the body bytes', async () => {
    const jpeg = await makeTinyJpeg();
    const post = await rawRequest(server, 'POST', '/api/images', jpeg, {
      'Content-Type': 'image/jpeg',
    });
    const { imageId } = JSON.parse(post.body.toString()) as { imageId: string };
    const meta = await rawRequest(server, 'GET', `/api/images/${imageId}/metadata`, null);
    expect(meta.status).toBe(200);
    const body = JSON.parse(meta.body.toString()) as { imageId: string; mimeType: string; width: number; height: number; sizeBytes: number; uploadedAt: string };
    expect(body.imageId).toBe(imageId);
    expect(body.mimeType).toBe('image/jpeg');
    expect(body.width).toBe(100);
    expect(body.height).toBe(100);
    expect(typeof body.uploadedAt).toBe('string');
  });
});
