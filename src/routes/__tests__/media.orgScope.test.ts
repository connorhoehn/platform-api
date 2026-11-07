// The media catalog is per organisation (security review §8, platform-api
// Medium: any authenticated user from any org could read it, and one user
// could fill it with 40 MB batches). These run the REAL repository over an
// in-memory stand-in for DynamoDB, so the keying and filtering are what is
// under test, not a mock's say-so.
import express, { type Request, type Response, type NextFunction } from 'express';

type Item = Record<string, unknown>;
const items: Item[] = [];
const fakeDoc = {
  send: async (cmd: { input: Record<string, unknown> }) => {
    const input = cmd.input as {
      Key?: { fileId: string };
      IndexName?: string;
      ExpressionAttributeValues: Record<string, unknown>;
      RequestItems?: unknown;
    };
    if (input.Key) {
      const key = input.Key;
      return { Item: items.find((i) => i.fileId === key.fileId) };
    }
    if (input.IndexName === 'time-index') {
      return { Items: items.filter((i) => i.bucket === input.ExpressionAttributeValues[':b']) };
    }
    if (input.IndexName === 'burst-index') {
      const v = input.ExpressionAttributeValues;
      return { Items: items.filter((i) => i.burstId === v[':g'] && (!v[':o'] || i.orgId === v[':o'])) };
    }
    if (input.RequestItems) return { UnprocessedItems: {} };
    throw new Error(`unexpected command ${JSON.stringify(input)}`);
  },
};

jest.mock('../../repositories', () => {
  const { MediaCatalogRepository } = jest.requireActual('../../repositories/MediaCatalogRepository');
  return { mediaCatalogRepo: new MediaCatalogRepository(fakeDoc) };
});
const mockStart = jest.fn(async () => 'run-1');
jest.mock('../../media/mediaCatalogPipeline', () => ({ startMediaCatalogRun: (...a: unknown[]) => mockStart(...(a as [])) }));

import { mediaRouter, MEDIA_BATCHES_PER_MINUTE } from '../media';
import { errorHandler } from '../../middleware/error-handler';
import { _resetRateLimitMemFallback } from '../../middleware/rateLimit';

const SHA = 'a'.repeat(64);

interface Body { items: Array<Record<string, unknown>>; frames: unknown[] }

function rowFor(orgId: string, sha: string, burstId = 'burst-aaaa'): Item {
  return { fileId: `${orgId}#${sha}`, contentId: sha, orgId, bucket: orgId, burstId, path: `/${orgId}/a.jpg`, shotAt: '2026-01-01T00:00:00Z' };
}

async function call(method: string, url: string, user: Record<string, unknown> | undefined, body?: unknown) {
  const app = express();
  app.use('/api/media', mediaRouter);
  app.use(errorHandler);
  return new Promise<{ status: number; body: Body }>((resolve, reject) => {
    const out = { status: 200, body: undefined as unknown };
    const res = {
      statusCode: 200,
      status(c: number) { out.status = c; this.statusCode = c; return this; },
      json(b: unknown) { out.body = b; resolve(out as never); return this; },
      setHeader() { return this; },
      getHeader() { return undefined; },
      set() { return this; },
      end() { resolve(out as never); return this; },
    };
    const req = { method, url, originalUrl: url, path: url, headers: {}, query: {}, body, user, ip: '127.0.0.1' };
    (app as unknown as (q: Request, s: Response, n: NextFunction) => void)(req as never, res as never, (err?: unknown) => {
      if (err) reject(err); else resolve({ status: 404, body: { items: [], frames: [] } });
    });
  });
}

const userA = { sub: 'alice', orgId: 'org-a' };
const userB = { sub: 'bob', orgId: 'org-b' };

beforeEach(() => {
  items.length = 0;
  items.push(rowFor('org-a', SHA));
  // A row written before the catalog was keyed by org: nobody's.
  items.push({ fileId: 'b'.repeat(64), bucket: 'all', burstId: 'burst-aaaa', path: '/legacy.jpg' });
  mockStart.mockClear();
  _resetRateLimitMemFallback();
});

describe('media catalog — org scope', () => {
  it('a user from org B cannot list org A\'s media', async () => {
    const res = await call('GET', '/api/media/catalog', userB);
    expect(res.status).toBe(200);
    expect(res.body.items).toEqual([]);
  });

  it('org A sees its own rows, with the content hash as the fileId', async () => {
    const res = await call('GET', '/api/media/catalog', userA);
    expect(res.body.items).toHaveLength(1);
    expect(res.body.items[0]).toMatchObject({ fileId: SHA, orgId: 'org-a' });
  });

  it('org B cannot read org A\'s file or burst by id', async () => {
    expect((await call('GET', `/api/media/catalog/files/${SHA}`, userB)).status).toBe(404);
    expect((await call('GET', '/api/media/catalog/bursts/burst-aaaa', userB)).status).toBe(404);
    expect((await call('GET', `/api/media/catalog/files/${SHA}`, userA)).status).toBe(200);
    expect((await call('GET', '/api/media/catalog/bursts/burst-aaaa', userA)).body.frames).toHaveLength(1);
  });

  it('a caller with no organisation is refused', async () => {
    expect((await call('GET', '/api/media/catalog', { sub: 'nobody' })).status).toBe(403);
  });

  it('a batch is started in the caller\'s org', async () => {
    const batch = { batchId: 'b1', root: '/v', files: [{ fileId: SHA, path: '/v/a.jpg', modifiedAt: '2026-09-14T12:00:00.000Z' }] };
    const res = await call('POST', '/api/media/catalog/batches', userB, batch);
    expect(res.status).toBe(202);
    expect(mockStart).toHaveBeenCalledWith(expect.objectContaining({ batchId: 'b1' }), 'bob', 'org-b');
  });

  it('caps batches per user per minute', async () => {
    const batch = { batchId: 'b1', root: '/v', files: [{ fileId: SHA, path: '/v/a.jpg', modifiedAt: '2026-09-14T12:00:00.000Z' }] };
    for (let i = 0; i < MEDIA_BATCHES_PER_MINUTE; i++) {
      expect((await call('POST', '/api/media/catalog/batches', userA, batch)).status).toBe(202);
    }
    expect((await call('POST', '/api/media/catalog/batches', userA, batch)).status).toBe(429);
    // Another user has their own allowance.
    expect((await call('POST', '/api/media/catalog/batches', userB, batch)).status).toBe(202);
  });
});
