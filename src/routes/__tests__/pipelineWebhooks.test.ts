// Tests for pipelineWebhooks router.
//
// Covers:
//   - 202 on valid path with expected response shape (accepted, webhookPath, at)
//   - 400 on invalid path (whitespace, slashes, too long, empty alts)
//   - Header pass-through forwards only `x-*` headers (uppercase + lowercase
//     X-Foo are both captured; Authorization / Cookie / Content-Type are not)
//   - Empty body is tolerated (defaults to {})

import express, { type NextFunction, type Request, type Response } from 'express';

// Mock the structured logger so the route's `log.info` / `log.error` calls
// land on jest.fn-backed methods rather than going through pino (which
// writes to process.stdout, defeating both noise-suppression and the
// `expect(...).toHaveBeenCalled()` assertions further down).
//
// Singleton stub: every `withContext(...)` call returns the SAME object so
// the test-side handles below stay valid across the module-load `withContext`
// invocation in pipelineWebhooks.ts. The factory can't reference outer
// variables (jest hoists `jest.mock` above all imports), so we hang the
// stub off `globalThis` and re-grab it after the import settles.
jest.mock('../../lib/logger', () => {
  // Self-referential pino-shaped stub: every contextual helper (`child`,
  // `withContext`) returns the SAME object so call-site chains route
  // through the same jest.fn instances we assert on. We hang `withContext`
  // off the default export too, matching the in-flight call pattern in
  // pipelineDefinitions.ts (`logger.withContext(...)`) — without it, the
  // import chain pipelineWebhooks → pipelineDefinitions throws on load.
  const stub: Record<string, jest.Mock> = {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
    child: jest.fn(),
    withContext: jest.fn(),
  };
  stub.child.mockReturnValue(stub);
  stub.withContext.mockReturnValue(stub);
  (globalThis as unknown as { __mockLog: typeof stub }).__mockLog = stub;
  return {
    __esModule: true,
    default: stub,
    withContext: jest.fn(() => stub),
  };
});

// Mock the audit repository so the route's fire-and-forget `auditRepo.record`
// resolves immediately without touching DynamoDB. Individual tests can
// override the resolved value or inspect call args via `mockedAuditRecord`.
jest.mock('../../pipeline/audit-repository', () => ({
  auditRepo: {
    record: jest.fn(() => Promise.resolve()),
  },
}));

// ---------------------------------------------------------------------------
// Mock `definitionsRepo` so the PUT route doesn't reach for real DynamoDB.
//
// Wave-3 of the pipeline-definitions persistence migration removed the
// synchronous in-memory mirror. The webhook router now reads from
// `pipelineDefinitionsCache`, a Scan-backed snapshot fed by
// `definitionsRepo.listAll()`. The signature tests below drive a real PUT
// through `pipelineDefinitionsRouter` (see `seedViaPut`) — the route's
// `pokeCacheAfterWrite()` hook then refreshes the cache, which calls
// `listAll()` on this mock, and the webhook handler picks up the secret.
// Without this mock every PUT in this suite would attempt a DynamoDB
// roundtrip during the unit run.
//
// Per-suite in-memory Map keyed by `userId|pipelineId` — preserves the
// original write-then-read-back semantics. Stashed on globalThis (jest
// hoists `jest.mock` above imports, so the factory can't close over outer
// locals); the test body re-grabs the handle below to reset between cases.
// ---------------------------------------------------------------------------
jest.mock('../../pipeline/definitions-repository', () => {
  const store = new Map<string, unknown>();
  const k = (userId: string, pipelineId: string): string =>
    `${userId}|${pipelineId}`;
  const repo = {
    get: jest.fn(async (userId: string, pipelineId: string) => {
      return store.get(k(userId, pipelineId)) ?? null;
    }),
    list: jest.fn(async (userId: string) => {
      const out: unknown[] = [];
      for (const [key, value] of store.entries()) {
        if (key.startsWith(`${userId}|`)) out.push(value);
      }
      return out;
    }),
    listAll: jest.fn(async () => {
      // Cross-user enumeration powering the Scan-backed cache.
      return Array.from(store.values());
    }),
    put: jest.fn(async (userId: string, def: { id: string }) => {
      store.set(k(userId, def.id), def);
    }),
    delete: jest.fn(async (userId: string, pipelineId: string) => {
      store.delete(k(userId, pipelineId));
    }),
    __resetForTests: () => store.clear(),
  };
  (
    globalThis as unknown as { __mockDefinitionsRepo: typeof repo }
  ).__mockDefinitionsRepo = repo;
  return { definitionsRepo: repo };
});

import { pipelineWebhooksRouter, pickSafeHeaders } from '../pipelineWebhooks';
import { pipelineDefinitionsRouter } from '../pipelineDefinitions';
import { pipelineDefinitionsCache } from '../../pipeline/definitions-cache';
import {
  setPipelineBridge,
  type PipelineBridge,
} from '../pipelineTriggers';
import {
  computeSignature,
  generateWebhookSecret,
  SIGNATURE_HEADER,
} from '../../lib/webhookSignature';
import { auditRepo } from '../../pipeline/audit-repository';

const mockLog = (globalThis as unknown as {
  __mockLog: {
    info: jest.Mock;
    warn: jest.Mock;
    error: jest.Mock;
    debug: jest.Mock;
    child: jest.Mock;
  };
}).__mockLog;
const mockedAuditRecord = auditRepo.record as jest.Mock;

// Phase-4 fix: the webhook handler now forwards into the PipelineModule
// bridge instead of silently 202'ing. Tests that exercise the happy path
// install a minimal stub bridge whose `trigger` returns a deterministic
// runId; tests that exercise the "subsystem unavailable" branch leave the
// bridge unset (or null it out explicitly).
function makeStubBridge(
  trigger: PipelineBridge['trigger'] = async () => ({ runId: 'stub-run-id' }),
): PipelineBridge {
  return {
    trigger,
    getRun: () => null,
    getHistory: () => [],
    resolveApproval: () => undefined,
  };
}

// ---------------------------------------------------------------------------
// Test rig (mirrors pipelineTriggers.test.ts) — hand-rolled mock req/res so we
// don't depend on supertest, which isn't in the social-api dep tree.
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => {
    resolveFinished = r;
  });
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    finished,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      this.ended = true;
      resolveFinished();
      return this;
    },
    setHeader(name, value) {
      this.headers[String(name)] = value;
      return this;
    },
    getHeader(name) {
      return this.headers[String(name)];
    },
    end(..._args) {
      this.ended = true;
      resolveFinished();
      return this;
    },
  };
  return r;
}

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; headers?: Record<string, string> } = {},
): Promise<MockRes> {
  const res = mockRes();
  const headers: Record<string, string> = {};
  for (const [k, v] of Object.entries(opts.headers ?? {})) {
    headers[k.toLowerCase()] = v;
  }
  const [pathname] = url.split('?', 2);
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: pathname,
    headers,
    query: {},
    body: opts.body,
  } as unknown as Request;

  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) {
        reject(err);
        return;
      }
      if (!res.ended) {
        res.status(404).json({ error: 'route not found' });
      }
      resolve();
    };
    (app as unknown as (
      req: Request,
      res: Response,
      next: NextFunction,
    ) => void)(req, res as unknown as Response, finalHandler);
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/hooks/pipeline', pipelineWebhooksRouter);
  return app;
}

// Reset logger + audit mocks between tests so per-test assertions don't
// see leftover calls from earlier cases. The structured logger replaces
// the old `console.log` noise — it's silenced by virtue of being a
// jest.fn (no real I/O), so the assertions below just check call counts.
beforeEach(() => {
  mockLog.info.mockClear();
  mockLog.warn.mockClear();
  mockLog.error.mockClear();
  mockLog.debug.mockClear();
  mockedAuditRecord.mockClear();
  mockedAuditRecord.mockImplementation(() => Promise.resolve());
  // Install a stub bridge so the handler's new "must forward" path resolves
  // a runId. Individual tests that need to exercise the 503/null-bridge
  // branch override this with `setPipelineBridge(null)` directly.
  setPipelineBridge(makeStubBridge());
});
afterEach(() => {
  setPipelineBridge(null);
});

// ---------------------------------------------------------------------------
// POST /hooks/pipeline/:path
// ---------------------------------------------------------------------------

describe('POST /hooks/pipeline/:path', () => {
  test('202 with shape { accepted, runId, webhookPath, at } on valid path', async () => {
    // Install a bridge whose trigger returns a known runId so we can assert
    // the new forward-and-respond contract end-to-end.
    setPipelineBridge(
      makeStubBridge(async () => ({ runId: 'run-weekly-digest-1' })),
    );
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/weekly-digest', {
      body: { ping: 'pong' },
    });
    expect(res.statusCode).toBe(202);
    const body = res.body as {
      accepted: boolean;
      runId: string;
      webhookPath: string;
      at: string;
    };
    expect(body.accepted).toBe(true);
    expect(body.runId).toBe('run-weekly-digest-1');
    expect(body.webhookPath).toBe('weekly-digest');
    expect(typeof body.at).toBe('string');
    // ISO 8601 date string (cheap shape check).
    expect(body.at).toMatch(/^\d{4}-\d{2}-\d{2}T/);
  });

  test('503 when pipeline bridge is unavailable (no silent 202)', async () => {
    // Wave-2 audit: external systems lying about success is worse than
    // failing loud. With no bridge wired, the route MUST surface 503.
    setPipelineBridge(null);
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/no-bridge', {
      body: { hello: 'world' },
    });
    expect(res.statusCode).toBe(503);
    const body = res.body as { accepted: boolean; error: string };
    expect(body.accepted).toBe(false);
    expect(body.error).toMatch(/unavailable/i);
  });

  test('500 when bridge.trigger throws — does not silently 202', async () => {
    setPipelineBridge(
      makeStubBridge(async () => {
        throw new Error('downstream pipeline-module exploded');
      }),
    );
    // The route logs the failure via the structured logger; the mocked
    // `log.error` jest.fn captures the call without writing to stdout.
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/throws', {
      body: { x: 1 },
    });
    expect(res.statusCode).toBe(500);
    const body = res.body as { accepted: boolean; error: string };
    expect(body.accepted).toBe(false);
    expect(body.error).toMatch(/exploded/);
    expect(mockLog.error).toHaveBeenCalled();
  });

  test('forwards parsed body + headers + path to bridge.trigger as triggerPayload', async () => {
    const calls: Array<Parameters<NonNullable<PipelineBridge['trigger']>>[0]> =
      [];
    setPipelineBridge(
      makeStubBridge(async (args) => {
        calls.push(args);
        return { runId: 'forwarded-run' };
      }),
    );

    const app = buildApp();
    // Pass body as a Buffer so the handler's `Buffer.isBuffer(req.body)`
    // branch runs (mirrors the signature tests — `express.raw` is bypassed
    // by the test rig but the handler tolerates either shape).
    const rawBody = Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8');
    const res = await run(app, 'POST', '/hooks/pipeline/forwarded', {
      body: rawBody,
      headers: { 'X-Custom': 'yes' },
    });

    expect(res.statusCode).toBe(202);
    expect(calls).toHaveLength(1);
    const args = calls[0];
    // pipelineId falls back to the webhook path when no definition is
    // bound (we didn't seed a pipeline def for this path).
    expect(args.pipelineId).toBe('forwarded');
    expect(args.triggeredBy.userId).toBe('webhook:forwarded');
    const tp = args.triggerPayload as {
      webhookPath: string;
      body: { hello: string };
      headers: Record<string, string>;
    };
    expect(tp.webhookPath).toBe('forwarded');
    expect(tp.body).toEqual({ hello: 'world' });
    // The test rig lowercases header names on inbound (mirrors how Node's
    // http parser presents them); pickSafeHeaders preserves whatever case
    // it received.
    expect(tp.headers['x-custom']).toBe('yes');
  });

  test('accepts alphanumeric, underscore, hyphen', async () => {
    const app = buildApp();
    for (const path of ['abc', 'a_b', 'a-b', 'A1_b-2', 'x'.repeat(64)]) {
      const res = await run(app, 'POST', `/hooks/pipeline/${path}`, {
        body: {},
      });
      expect(res.statusCode).toBe(202);
    }
  });

  test('400 on invalid path: special chars, too long, empty', async () => {
    const app = buildApp();

    // 65 chars — past the cap.
    const tooLong = await run(
      app,
      'POST',
      `/hooks/pipeline/${'x'.repeat(65)}`,
      { body: {} },
    );
    expect(tooLong.statusCode).toBe(400);
    expect((tooLong.body as { error: string }).error).toMatch(/invalid path/);

    // dot is not allowed.
    const dot = await run(app, 'POST', '/hooks/pipeline/has.dot', { body: {} });
    expect(dot.statusCode).toBe(400);

    // slash inside a segment isn't routable as `:path` anyway, but a percent-
    // encoded one is and should be rejected.
    const pct = await run(app, 'POST', '/hooks/pipeline/has%2Fslash', {
      body: {},
    });
    expect(pct.statusCode).toBe(400);
  });

  test('forwards only x-* headers; strips Authorization / Cookie / Content-Type (helper)', () => {
    // Direct assertion against the pure helper — the route uses this function
    // verbatim, so the route's behavior is covered transitively.
    const safe = pickSafeHeaders({
      'X-Custom': 'yes',
      'x-other': 'also',
      'X-Forwarded-For': '10.0.0.1',
      Authorization: 'Bearer secret',
      Cookie: 'session=abc',
      'Content-Type': 'application/json',
      // Multi-value headers (express delivers as arrays) are dropped — we
      // only forward simple string values to keep the pipeline context flat.
      'x-multi': ['a', 'b'],
      // Undefined / null entries from upstream parsers are tolerated.
      'x-undef': undefined,
    });

    expect(safe).toEqual({
      'X-Custom': 'yes',
      'x-other': 'also',
      'X-Forwarded-For': '10.0.0.1',
    });
  });

  test('route handler runs the header filter without crashing on mixed inputs', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/h1', {
      body: { ok: true },
      headers: {
        'X-Custom': 'yes',
        'x-other': 'also',
        Authorization: 'Bearer secret',
        Cookie: 'session=abc',
        'Content-Type': 'application/json',
      },
    });
    expect(res.statusCode).toBe(202);
    expect((res.body as { webhookPath: string }).webhookPath).toBe('h1');
    // Structured `pipeline-webhook received` log line landed on the
    // mocked logger.
    expect(mockLog.info).toHaveBeenCalled();
  });

  test('tolerates an empty body and still returns 202', async () => {
    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/empty', {
      body: undefined,
    });
    expect(res.statusCode).toBe(202);
    expect((res.body as { accepted: boolean }).accepted).toBe(true);
  });
});

// ---------------------------------------------------------------------------
// HMAC-SHA256 signature verification (X-Pipeline-Signature-256)
//
// We seed the pipelineDefinitionsCache with a webhook trigger binding that carries
// a `webhookSecret`. The route reads the secret out of the store and rejects
// any POST whose signature header doesn't match HMAC-SHA256(secret, body).
// Pipelines without a configured secret keep the Phase-1 unsigned-OK
// behavior so dev fixtures don't break.
//
// `req.body` is passed as a Buffer so the upstream `express.raw` middleware
// leaves it untouched (`typeis.hasBody(req)` is false without a
// content-length header → middleware skips → handler sees the Buffer).
// ---------------------------------------------------------------------------

/**
 * Drive a real PUT through `pipelineDefinitionsRouter` so the same upsert
 * code path the production app uses installs the binding (and persists the
 * provided secret). Keeps the test honest about how secrets actually get
 * into the store.
 */
async function seedViaPut(opts: {
  webhookPath: string;
  webhookSecret?: string;
}): Promise<{ secret: string }> {
  const app = express();
  app.use(express.json());
  // Inject a mock auth context — the upsert handler reads `req.user!.sub`.
  app.use((req, _res, next) => {
    (req as unknown as { user: { sub: string } }).user = { sub: 'test-user' };
    next();
  });
  app.use('/defs', pipelineDefinitionsRouter);

  const pipelineId = `pipe-${opts.webhookPath}`;
  const secret = opts.webhookSecret ?? generateWebhookSecret();
  const def = {
    id: pipelineId,
    name: 'sig test',
    version: 1,
    status: 'draft',
    nodes: [],
    edges: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString(),
    createdBy: 'test-user',
    triggerBinding: {
      event: 'webhook',
      webhookPath: opts.webhookPath,
      webhookSecret: secret,
    },
  };

  await run(app, 'PUT', `/defs/${pipelineId}`, { body: def });
  // The PUT handler's `pokeCacheAfterWrite()` is fire-and-forget, so
  // `await` an explicit refresh here to make the test deterministic —
  // the webhook handler that runs next reads from this cache.
  await pipelineDefinitionsCache.refresh();
  return { secret };
}

describe('POST /hooks/pipeline/:path — HMAC-SHA256 signature', () => {
  beforeEach(async () => {
    // Wave-3: clear the mocked `definitionsRepo`'s in-memory backing store
    // and reset the cache snapshot so each test starts from a clean slate.
    (
      globalThis as unknown as {
        __mockDefinitionsRepo: { __resetForTests: () => void };
      }
    ).__mockDefinitionsRepo.__resetForTests();
    pipelineDefinitionsCache.__setSnapshotForTests([]);
    // Top-level beforeEach already clears mockLog/mockedAuditRecord; the
    // signature suite reuses those mocks rather than re-spying on console.
  });

  test('valid signature → 202', async () => {
    const { secret } = await seedViaPut({ webhookPath: 'signed-ok' });
    const rawBody = Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8');
    const sig = computeSignature(secret, rawBody);

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/signed-ok', {
      body: rawBody,
      headers: { [SIGNATURE_HEADER]: sig },
    });

    expect(res.statusCode).toBe(202);
    expect((res.body as { accepted: boolean }).accepted).toBe(true);
  });

  test('invalid signature → 401 with problem-details body', async () => {
    await seedViaPut({
      webhookPath: 'signed-bad',
      webhookSecret: 'a'.repeat(64),
    });
    const rawBody = Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8');
    // Compute signature with the WRONG secret.
    const wrongSig = computeSignature('b'.repeat(64), rawBody);

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/signed-bad', {
      body: rawBody,
      headers: { [SIGNATURE_HEADER]: wrongSig },
    });

    expect(res.statusCode).toBe(401);
    const body = res.body as {
      type: string;
      title: string;
      status: number;
      detail: string;
    };
    expect(body.title).toBe('Invalid webhook signature');
    expect(body.status).toBe(401);
    expect(body.detail).toMatch(/did not match/i);
    // Structured error log so observability can alert on it.
    expect(mockLog.error).toHaveBeenCalled();
  });

  test('missing signature header but secret configured → 401', async () => {
    await seedViaPut({ webhookPath: 'signed-missing' });
    const rawBody = Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8');

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/signed-missing', {
      body: rawBody,
      // no signature header
    });

    expect(res.statusCode).toBe(401);
    const body = res.body as { detail: string };
    expect(body.detail).toMatch(/required/i);
    expect(mockLog.error).toHaveBeenCalled();
  });

  test('no secret configured on def → 202 (legacy unsigned mode)', async () => {
    // Note: no seedViaPut call — store has no def for this path.
    const rawBody = Buffer.from(JSON.stringify({ hello: 'world' }), 'utf8');

    const app = buildApp();
    const res = await run(app, 'POST', '/hooks/pipeline/unsigned-legacy', {
      body: rawBody,
      // No signature header — and that's fine because no secret is set.
    });

    expect(res.statusCode).toBe(202);
    expect((res.body as { accepted: boolean }).accepted).toBe(true);
    expect(mockLog.error).not.toHaveBeenCalled();
  });

  test('upsert mints a webhookSecret server-side when none is provided', async () => {
    // Drive a PUT with `triggerBinding.event: 'webhook'` but no secret —
    // the pipelineDefinitions handler should mint one before storing.
    const app = express();
    app.use(express.json());
    app.use((req, _res, next) => {
      (req as unknown as { user: { sub: string } }).user = { sub: 'test-user' };
      next();
    });
    app.use('/defs', pipelineDefinitionsRouter);

    const pipelineId = 'pipe-mint';
    const def = {
      id: pipelineId,
      name: 'mint test',
      version: 1,
      status: 'draft',
      nodes: [],
      edges: [],
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      createdBy: 'test-user',
      triggerBinding: {
        event: 'webhook',
        webhookPath: 'mint-me',
        // webhookSecret intentionally omitted
      },
    };

    const res = await run(app, 'PUT', `/defs/${pipelineId}`, { body: def });
    expect(res.statusCode).toBe(200);
    const echoed = res.body as {
      triggerBinding: { webhookSecret?: string };
    };
    expect(typeof echoed.triggerBinding.webhookSecret).toBe('string');
    // 32 random bytes → 64 lowercase hex chars.
    expect(echoed.triggerBinding.webhookSecret).toMatch(/^[0-9a-f]{64}$/);
  });
});
