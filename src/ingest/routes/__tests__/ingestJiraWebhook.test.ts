// social-api/src/ingest/routes/__tests__/ingestJiraWebhook.test.ts
//
// Tests for createJiraWebhookRoutes: source-not-found, non-jira source guard,
// payload → IngestEvent mapping (issue + comment), sync-trigger dedup.

import express, { type NextFunction, type Request, type Response } from 'express';
import {
  createJiraWebhookRoutes,
  mapJiraPayload,
  changeToIngestEvent,
} from '../ingestJiraWebhook';
import type { IngestEvent, IngestSource } from '../../types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeJiraSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-jira',
    name: 'Jira ENG',
    type: 'jira',
    config: { projectKey: 'ENG' },
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

function makeRepo(source: IngestSource | null = makeJiraSource()) {
  return { get: jest.fn(async () => source) };
}

function makeProducer(deduped = false) {
  return { send: jest.fn(async () => ({ deduped })) };
}

function makeBus() {
  const events: IngestEvent[] = [];
  return { events, emit: (e: IngestEvent) => { events.push(e); } };
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
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'POST',
    url,
    originalUrl: url,
    path: url,
    headers: { 'content-type': 'application/json' },
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

function buildApp(opts: {
  source?: IngestSource | null;
  deduped?: boolean;
  bus?: ReturnType<typeof makeBus>;
} = {}) {
  const source = opts.source === undefined ? makeJiraSource() : opts.source;
  const app = express();
  app.use(express.json());
  const router = createJiraWebhookRoutes({
    sourceRepository: makeRepo(source) as never,
    syncProducer: makeProducer(opts.deduped) as never,
    eventBus: opts.bus,
  });
  app.use('/api/ingest/webhook', router);
  return app;
}

// ─── mapJiraPayload (pure-fn tests) ───────────────────────────────────────

describe('mapJiraPayload — payload → JiraChange list', () => {
  test('maps jira:issue_updated to a single issue change', () => {
    const out = mapJiraPayload(
      {
        webhookEvent: 'jira:issue_updated',
        issue: { id: '1', key: 'ENG-1', fields: { project: { key: 'ENG' } } },
      },
      'ENG',
    );
    expect(out).toEqual([
      { kind: 'issue', path: 'ENG/ENG-1', issueKey: 'ENG-1', webhookEvent: 'jira:issue_updated' },
    ]);
  });

  test('maps comment_created to a comment change with commentId', () => {
    const out = mapJiraPayload(
      {
        webhookEvent: 'comment_created',
        issue: { id: '1', key: 'ENG-1', fields: { project: { key: 'ENG' } } },
        comment: { id: 'c42', body: 'hi' },
      },
      'ENG',
    );
    expect(out).toEqual([
      {
        kind: 'comment',
        path: 'ENG/ENG-1/comment/c42',
        issueKey: 'ENG-1',
        commentId: 'c42',
        webhookEvent: 'comment_created',
      },
    ]);
  });

  test('drops events from a different project key', () => {
    const out = mapJiraPayload(
      {
        webhookEvent: 'jira:issue_updated',
        issue: { id: '1', key: 'OPS-9', fields: { project: { key: 'OPS' } } },
      },
      'ENG',
    );
    expect(out).toEqual([]);
  });

  test('returns [] on malformed body', () => {
    expect(mapJiraPayload(null, 'ENG')).toEqual([]);
    expect(mapJiraPayload({}, 'ENG')).toEqual([]);
    expect(mapJiraPayload({ webhookEvent: 'jira:issue_updated' }, 'ENG')).toEqual([]);
  });
});

// ─── changeToIngestEvent ──────────────────────────────────────────────────

describe('changeToIngestEvent', () => {
  test('lifts an issue change into an item.status event', () => {
    const evt = changeToIngestEvent('src-1', {
      kind: 'issue',
      path: 'ENG/ENG-7',
      issueKey: 'ENG-7',
      webhookEvent: 'jira:issue_updated',
    });
    expect(evt.type).toBe('item.status');
    if (evt.type === 'item.status') {
      expect(evt.ingestId).toBe('jira:src-1:ENG/ENG-7');
      expect(evt.status).toBe('queued');
      expect(evt.detail).toContain('issue');
    }
  });
});

// ─── route — source lookup ────────────────────────────────────────────────

describe('POST /jira/:sourceId — source lookup', () => {
  test('404 when source does not exist', async () => {
    const app = buildApp({ source: null });
    const res = await run(app, '/api/ingest/webhook/jira/ghost', {});
    expect(res.statusCode).toBe(404);
  });

  test('400 when source exists but is not type=jira', async () => {
    const app = buildApp({ source: makeJiraSource({ type: 's3' }) });
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', {});
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toContain('not a jira source');
  });

  test('400 when source missing projectKey config', async () => {
    const app = buildApp({ source: makeJiraSource({ config: {} }) });
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', {});
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toContain('projectKey');
  });
});

// ─── route — payload roundtrip ────────────────────────────────────────────

describe('POST /jira/:sourceId — payload roundtrip', () => {
  test('issue_updated webhook → IngestEvent emitted + sync triggered', async () => {
    const bus = makeBus();
    const app = buildApp({ bus });

    const payload = {
      webhookEvent: 'jira:issue_updated',
      issue: { id: '10000', key: 'ENG-123', fields: { project: { key: 'ENG' } } },
    };
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', payload);

    expect(res.statusCode).toBe(200);
    expect((res.body as { status: string }).status).toBe('sync_triggered');
    expect((res.body as { changes: unknown[] }).changes).toHaveLength(1);

    expect(bus.events).toHaveLength(1);
    expect(bus.events[0].type).toBe('item.status');
    if (bus.events[0].type === 'item.status') {
      expect(bus.events[0].ingestId).toBe('jira:src-jira:ENG/ENG-123');
      expect(bus.events[0].status).toBe('queued');
    }
  });

  test('comment_created webhook → comment IngestEvent', async () => {
    const bus = makeBus();
    const app = buildApp({ bus });

    const payload = {
      webhookEvent: 'comment_created',
      issue: { id: '10000', key: 'ENG-9', fields: { project: { key: 'ENG' } } },
      comment: { id: 'c1', body: 'hi' },
    };
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', payload);

    expect(res.statusCode).toBe(200);
    const body = res.body as { changes: Array<{ path: string; kind: string }> };
    expect(body.changes[0]).toEqual(expect.objectContaining({
      kind: 'comment', path: 'ENG/ENG-9/comment/c1',
    }));
    expect(bus.events).toHaveLength(1);
    if (bus.events[0].type === 'item.status') {
      expect(bus.events[0].ingestId).toBe('jira:src-jira:ENG/ENG-9/comment/c1');
    }
  });

  test('returns no_changes when payload has no extractable changes', async () => {
    const bus = makeBus();
    const app = buildApp({ bus });
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', {});
    expect(res.statusCode).toBe(200);
    expect((res.body as { status: string }).status).toBe('no_changes');
    expect(bus.events).toHaveLength(0);
  });

  test('returns sync_in_progress when sync is already deduped', async () => {
    const app = buildApp({ deduped: true });
    const payload = {
      webhookEvent: 'jira:issue_updated',
      issue: { id: '1', key: 'ENG-1', fields: { project: { key: 'ENG' } } },
    };
    const res = await run(app, '/api/ingest/webhook/jira/src-jira', payload);
    expect((res.body as { status: string }).status).toBe('sync_in_progress');
  });
});
