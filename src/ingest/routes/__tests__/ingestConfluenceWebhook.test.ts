// social-api/src/ingest/routes/__tests__/ingestConfluenceWebhook.test.ts
//
// Tests for createConfluenceWebhookRoutes — covers happy roundtrip
// (webhook payload → normalized IngestEvent + sync trigger) plus failure
// modes (unknown source, malformed payload, page_removed bypass).

import express, { type NextFunction, type Request, type Response } from 'express';
import * as crypto from 'crypto';
import {
  createConfluenceWebhookRoutes,
  normalizeConfluencePayload,
} from '../ingestConfluenceWebhook';
import type { IngestEvent, IngestSource } from '../../types';

// ─── helpers (cribbed from ingestWebhooks.test.ts) ────────────────────────

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-confl',
    name: 'Acme Engineering Wiki',
    type: 'confluence',
    config: {
      baseUrl: 'https://acme.atlassian.net/wiki',
      email: 'agent@acme.com',
      apiToken: 'tok',
      spaceKey: 'ENG',
    },
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
    url, originalUrl: url, path: url,
    headers: { 'content-type': 'application/json', ...extraHeaders },
    query: {}, body, params: {}, ip: '127.0.0.1',
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

interface BuildAppOpts {
  source?: IngestSource | null;
  deduped?: boolean;
  publishedEvents?: IngestEvent[];
}

function buildApp(opts: BuildAppOpts = {}) {
  const published = opts.publishedEvents ?? [];
  const source = opts.source === undefined ? makeSource() : opts.source;
  const triggerPageRefetch = jest.fn(async () => ({ deduped: opts.deduped ?? false }));
  const app = express();
  app.use(express.json());
  app.use(
    '/api/ingest/webhook/confluence',
    createConfluenceWebhookRoutes({
      sourceRepository: { get: async () => source },
      syncProducer: { triggerPageRefetch },
      eventBus: { publish: (e) => { published.push(e); } },
      now: () => new Date('2026-05-06T12:00:00Z'),
    }),
  );
  return { app, triggerPageRefetch, published };
}

// ─── tests ────────────────────────────────────────────────────────────────

describe('normalizeConfluencePayload', () => {
  test('parses classic page_updated payload', () => {
    const out = normalizeConfluencePayload({
      event: 'page_updated',
      page: { id: 12345, title: 'Spec', version: { number: 7 } },
    });
    expect(out).toEqual({
      pageId: '12345',
      title: 'Spec',
      versionNumber: 7,
      eventType: 'page_updated',
    });
  });

  test('parses connect-style eventType + content payload', () => {
    const out = normalizeConfluencePayload({
      eventType: 'page_updated',
      content: { id: '99', title: 'X', version: { number: 2 } },
    });
    expect(out?.pageId).toBe('99');
    expect(out?.eventType).toBe('page_updated');
  });

  test('flags removals as page_removed', () => {
    const out = normalizeConfluencePayload({
      event: 'page_removed',
      page: { id: 'x' },
    });
    expect(out?.eventType).toBe('page_removed');
  });

  test('returns null on unparseable payload', () => {
    expect(normalizeConfluencePayload(null)).toBeNull();
    expect(normalizeConfluencePayload({})).toBeNull();
    expect(normalizeConfluencePayload({ event: 'x' })).toBeNull();
  });
});

describe('POST /:sourceId — happy roundtrip', () => {
  test('webhook payload publishes IngestEvent and triggers refetch', async () => {
    const { app, triggerPageRefetch, published } = buildApp();
    const payload = {
      event: 'page_updated',
      page: { id: '12345', title: 'Spec', version: { number: 9 } },
    };
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', payload);

    expect(res.statusCode).toBe(200);
    expect((res.body as { status: string }).status).toBe('sync_triggered');
    expect((res.body as { pageId: string }).pageId).toBe('12345');

    expect(triggerPageRefetch).toHaveBeenCalledWith({ sourceId: 'src-confl', pageId: '12345' });

    // Normalized IngestEvent was published
    expect(published).toHaveLength(1);
    expect(published[0].type).toBe('sync.file.fetched');
    if (published[0].type === 'sync.file.fetched') {
      expect(published[0].sourceId).toBe('src-confl');
      expect(published[0].filePath).toBe('12345');
      expect(published[0].timestamp).toBe('2026-05-06T12:00:00.000Z');
    }
  });

  test('returns sync_in_progress when producer reports deduped', async () => {
    const { app } = buildApp({ deduped: true });
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', {
      event: 'page_updated',
      page: { id: '12345' },
    });
    expect((res.body as { status: string }).status).toBe('sync_in_progress');
  });
});

describe('POST /:sourceId — error/edge cases', () => {
  test('404 when source does not exist', async () => {
    const { app } = buildApp({ source: null });
    const res = await run(app, '/api/ingest/webhook/confluence/ghost', {
      event: 'page_updated', page: { id: '1' },
    });
    expect(res.statusCode).toBe(404);
  });

  test('400 on unrecognized payload', async () => {
    const { app } = buildApp();
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', { foo: 'bar' });
    expect(res.statusCode).toBe(400);
  });

  test('page_removed does not trigger a re-fetch', async () => {
    const { app, triggerPageRefetch, published } = buildApp();
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', {
      event: 'page_removed',
      page: { id: '777' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { status: string }).status).toBe('page_removed');
    expect(triggerPageRefetch).not.toHaveBeenCalled();
    // Audit IngestEvent still emitted so the operator UI sees the deletion.
    expect(published).toHaveLength(1);
  });
});

describe('POST /:sourceId — HMAC verification', () => {
  function sign(secret: string, body: unknown): string {
    return 'sha256=' + crypto.createHmac('sha256', secret).update(JSON.stringify(body)).digest('hex');
  }

  test('passes through when source has no webhookSecret', async () => {
    const { app } = buildApp({ source: makeSource({ config: { spaceKey: 'ENG' } }) });
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', {
      event: 'page_updated', page: { id: '1' },
    });
    expect(res.statusCode).toBe(200);
  });

  test('403 when secret configured and signature missing', async () => {
    const { app } = buildApp({
      source: makeSource({ config: { spaceKey: 'ENG', webhookSecret: 'shh' } }),
    });
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', {
      event: 'page_updated', page: { id: '1' },
    });
    expect(res.statusCode).toBe(403);
    expect((res.body as { error: string }).error).toBe('invalid webhook signature');
  });

  test('403 when signature is wrong', async () => {
    const { app } = buildApp({
      source: makeSource({ config: { spaceKey: 'ENG', webhookSecret: 'shh' } }),
    });
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl',
      { event: 'page_updated', page: { id: '1' } },
      { 'x-webhook-signature': 'sha256=bad' },
    );
    expect(res.statusCode).toBe(403);
  });

  test('200 with matching HMAC-SHA256 signature', async () => {
    const secret = 'correct-horse';
    const { app } = buildApp({
      source: makeSource({ config: { spaceKey: 'ENG', webhookSecret: secret } }),
    });
    const payload = { event: 'page_updated', page: { id: '42' } };
    const res = await run(app, '/api/ingest/webhook/confluence/src-confl', payload, {
      'x-webhook-signature': sign(secret, payload),
    });
    expect(res.statusCode).toBe(200);
  });
});
