// social-api/src/routes/__tests__/pipelineBreakpointPatch.test.ts
//
// Unit tests for the hub#2226 edit-and-continue routes:
//   POST   /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch
//   DELETE /api/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch/:patchId

import express, { type NextFunction, type Request, type Response } from 'express';

jest.mock('../../lib/redis-client', () => ({
  getRedisClient: async () => null,
}));

import { errorHandler } from '../../middleware/error-handler';
import { setPipelineBridge, type PipelineBridge } from '../pipelineTriggers';
import { pipelineBreakpointPatchRouter } from '../pipelineBreakpointPatch';

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes;
  getHeader(_n: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const r: MockRes = {
    statusCode: 200, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(b) { this.body = b; this.ended = true; resolveFinished(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolveFinished(); return this; },
  };
  return r;
}

async function dispatch(
  app: express.Express,
  method: string,
  url: string,
  opts: { body?: unknown; userId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  const [path, qs] = url.split('?', 2);
  const query: Record<string, string> = {};
  if (qs) {
    for (const part of qs.split('&')) {
      const [k, v] = part.split('=', 2);
      if (k) query[decodeURIComponent(k)] = v ? decodeURIComponent(v) : '';
    }
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path,
    headers: { 'content-type': 'application/json' },
    query,
    body: opts.body ?? {},
    user: { sub: opts.userId ?? 'user-1' },
    params: {},
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use((req, _res, next) => {
    if (!(req as Request & { user?: unknown }).user) {
      (req as Request & { user?: { sub: string } }).user = { sub: 'user-1' };
    }
    next();
  });
  app.use(
    '/pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch',
    pipelineBreakpointPatchRouter,
  );
  app.use(errorHandler);
  return app;
}

const app = buildApp();

afterEach(() => {
  setPipelineBridge(null);
});

// ---------------------------------------------------------------------------
// POST /patch
// ---------------------------------------------------------------------------

describe('POST /pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch', () => {
  const pausedSnap = {
    runId: 'run-1',
    pipelineId: 'p1',
    status: 'paused_at_breakpoint',
    pausedAtNodeId: 'step-1',
  };

  function buildBridge(overrides: Partial<PipelineBridge> = {}): PipelineBridge {
    const patchPausedContext: PipelineBridge['patchPausedContext'] = jest.fn(
      async () => ({
        patchId: 'patch-abc',
        before: {},
        after: { foo: 'bar' },
        validation: { ok: true as const },
      }),
    );
    const revertContextPatch: PipelineBridge['revertContextPatch'] = jest.fn(
      async () => {},
    );
    return {
      getRun: async () => pausedSnap,
      getHistory: async () => [],
      resolveApproval: async () => {},
      patchPausedContext,
      revertContextPatch,
      ...overrides,
    };
  }

  test('200 + PatchResult on the happy path', async () => {
    const bridge = buildBridge();
    setPipelineBridge(bridge);

    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: { foo: 'bar' } } } },
    );

    expect(res.statusCode).toBe(200);
    const body = res.body as { patchId: string; validation: { ok: boolean } };
    expect(body.patchId).toBe('patch-abc');
    expect(body.validation).toEqual({ ok: true });
    expect(bridge.patchPausedContext).toHaveBeenCalledWith(
      'run-1',
      'step-1',
      { mode: 'merge', values: { foo: 'bar' } },
      'user-1',
    );
  });

  test('honours appliedBy when present in body', async () => {
    const bridge = buildBridge();
    setPipelineBridge(bridge);

    await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      {
        body: {
          patch: { mode: 'merge', values: { x: 1 } },
          appliedBy: 'operator@example.com',
        },
      },
    );

    expect(bridge.patchPausedContext).toHaveBeenCalledWith(
      'run-1',
      'step-1',
      { mode: 'merge', values: { x: 1 } },
      'operator@example.com',
    );
  });

  test('400 when body.patch is missing', async () => {
    setPipelineBridge(buildBridge());
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: {} },
    );
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/patch/);
  });

  test('400 when body.patch.mode is unknown', async () => {
    setPipelineBridge(buildBridge());
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'wat', values: {} } } },
    );
    expect(res.statusCode).toBe(400);
  });

  test('404 when run is unknown', async () => {
    setPipelineBridge(buildBridge({ getRun: async () => null }));
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/missing/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(404);
  });

  test('409 when run is not paused', async () => {
    setPipelineBridge(
      buildBridge({
        getRun: async () => ({ ...pausedSnap, status: 'running' }),
      }),
    );
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(409);
    expect((res.body as { error: string }).error).toMatch(/not paused/);
  });

  test('409 when stepId does not match pausedAtNodeId', async () => {
    setPipelineBridge(buildBridge());
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-OTHER/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(409);
    expect((res.body as { error: string }).error).toMatch(/paused at/);
  });

  test('422 when validator rejects (validation.ok === false)', async () => {
    const bridge = buildBridge({
      patchPausedContext: jest.fn(async () => ({
        patchId: 'patch-x',
        before: { foo: 1 },
        after: { foo: 1 },
        validation: { ok: false, reason: 'foo must be positive' },
      })),
    });
    setPipelineBridge(bridge);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: { foo: -1 } } } },
    );
    expect(res.statusCode).toBe(422);
    const body = res.body as { error: string; result: { validation: { reason: string } } };
    expect(body.error).toMatch(/foo must be positive/);
    expect(body.result.validation.reason).toBe('foo must be positive');
  });

  test('503 when bridge is not wired', async () => {
    setPipelineBridge(null);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(503);
  });

  test('503 when bridge has no patchPausedContext method', async () => {
    setPipelineBridge({
      getRun: async () => pausedSnap,
      getHistory: async () => [],
      resolveApproval: async () => {},
      // patchPausedContext intentionally absent
    });
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(503);
  });

  test('409 when executor throws InvalidRunStateError (name-based mapping)', async () => {
    class InvalidRunStateError extends Error {
      override name = 'InvalidRunStateError';
    }
    const bridge = buildBridge({
      patchPausedContext: jest.fn(async () => {
        throw new InvalidRunStateError('not paused');
      }),
    });
    setPipelineBridge(bridge);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'merge', values: {} } } },
    );
    expect(res.statusCode).toBe(409);
  });

  test('422 when executor throws PatchValidationError (name-based mapping)', async () => {
    class PatchValidationError extends Error {
      override name = 'PatchValidationError';
    }
    const bridge = buildBridge({
      patchPausedContext: jest.fn(async () => {
        throw new PatchValidationError('jsonPatch not supported');
      }),
    });
    setPipelineBridge(bridge);
    const res = await dispatch(
      app,
      'POST',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch',
      { body: { patch: { mode: 'jsonPatch', ops: [] } } },
    );
    expect(res.statusCode).toBe(422);
  });
});

// ---------------------------------------------------------------------------
// DELETE /patch/:patchId
// ---------------------------------------------------------------------------

describe('DELETE /pipelines/:pipelineId/runs/:runId/breakpoint/:stepId/patch/:patchId', () => {
  test('204 on successful revert', async () => {
    const revertContextPatch = jest.fn(async () => {});
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      revertContextPatch,
    });
    const res = await dispatch(
      app,
      'DELETE',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch/patch-1',
    );
    expect(res.statusCode).toBe(204);
    expect(revertContextPatch).toHaveBeenCalledWith('run-1', 'step-1', 'patch-1');
  });

  test('503 when bridge is not wired', async () => {
    setPipelineBridge(null);
    const res = await dispatch(
      app,
      'DELETE',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch/patch-1',
    );
    expect(res.statusCode).toBe(503);
  });

  test('503 when bridge has no revertContextPatch method', async () => {
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
    });
    const res = await dispatch(
      app,
      'DELETE',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch/patch-1',
    );
    expect(res.statusCode).toBe(503);
  });

  test('409 when executor throws InvalidRunStateError (run already resumed)', async () => {
    class InvalidRunStateError extends Error {
      override name = 'InvalidRunStateError';
    }
    setPipelineBridge({
      getRun: async () => null,
      getHistory: async () => [],
      resolveApproval: async () => {},
      revertContextPatch: jest.fn(async () => {
        throw new InvalidRunStateError('run resumed past pausedAtNodeId');
      }),
    });
    const res = await dispatch(
      app,
      'DELETE',
      '/pipelines/p1/runs/run-1/breakpoint/step-1/patch/patch-1',
    );
    expect(res.statusCode).toBe(409);
  });
});
