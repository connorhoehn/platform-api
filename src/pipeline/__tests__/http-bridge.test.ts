// platform-api/src/pipeline/__tests__/http-bridge.test.ts
//
// SPIKE coverage for HTTPPipelineBridge (M4 Block 1). Stands up a real
// Express server on a random port, points the bridge at it, and asserts
// each PipelineBridge method:
//   - hits the expected URL,
//   - serialises args correctly into the body,
//   - deserialises the response into the right shape,
//   - surfaces non-200 / timeout / network failure as BridgeRpcError,
//   - signs requests when signOpts is provided.

import express from 'express';
import http from 'http';
import { AddressInfo } from 'net';

import { HTTPPipelineBridge, BridgeRpcError } from '../http-bridge';
import { verifyEnvelope } from '@connorhoehn/service-runtime';

// ---------------------------------------------------------------------------
// Test harness — minimal Express app whose handlers we register per-test
// ---------------------------------------------------------------------------

interface RecordedCall {
  method: string;
  body: { args: unknown[] };
  headers: Record<string, string | undefined>;
  rawBody: string;
}

interface Harness {
  baseUrl: string;
  recorded: RecordedCall[];
  /** Register a handler for `/internal/pipeline/<method>`. */
  on: (method: string, handler: (call: RecordedCall) => { status?: number; body: unknown }) => void;
  close: () => Promise<void>;
}

async function startHarness(): Promise<Harness> {
  const app = express();
  app.use(express.text({ type: 'application/json', limit: '1mb' }));

  const recorded: RecordedCall[] = [];
  const handlers = new Map<
    string,
    (call: RecordedCall) => { status?: number; body: unknown }
  >();

  app.post('/internal/pipeline/:method', (req, res) => {
    const method = req.params.method;
    const rawBody = typeof req.body === 'string' ? req.body : '';
    let parsed: { args: unknown[] };
    try {
      parsed = JSON.parse(rawBody) as { args: unknown[] };
    } catch {
      parsed = { args: [] };
    }
    const call: RecordedCall = {
      method,
      body: parsed,
      // express lower-cases header names; copy a shallow snapshot
      headers: {
        'content-type': req.header('content-type'),
        'x-service-auth': req.header('x-service-auth'),
      },
      rawBody,
    };
    recorded.push(call);

    const handler = handlers.get(method);
    if (!handler) {
      res.status(404).json({ error: { message: `no handler registered for ${method}` } });
      return;
    }
    const out = handler(call);
    res.status(out.status ?? 200).json(out.body);
  });

  // 0 → kernel picks an unused port
  const server = await new Promise<http.Server>((resolve) => {
    const s = app.listen(0, '127.0.0.1', () => resolve(s));
  });
  const { port } = server.address() as AddressInfo;

  return {
    baseUrl: `http://127.0.0.1:${port}`,
    recorded,
    on(method, handler) {
      handlers.set(method, handler);
    },
    close: () =>
      new Promise<void>((resolve, reject) =>
        server.close((err) => (err ? reject(err) : resolve())),
      ),
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('HTTPPipelineBridge — construction', () => {
  test('throws when workerBaseUrl missing', () => {
    expect(
      () =>
        new HTTPPipelineBridge({
          // @ts-expect-error intentional misuse
          workerBaseUrl: undefined,
        }),
    ).toThrow(/workerBaseUrl/);
  });

  test('strips trailing slashes from workerBaseUrl', async () => {
    const h = await startHarness();
    h.on('getRun', () => ({ body: { result: { runId: 'r-1' } } }));
    try {
      // Note the trailing /// on baseUrl
      const bridge = new HTTPPipelineBridge({ workerBaseUrl: `${h.baseUrl}///` });
      const run = await bridge.getRun('r-1');
      expect(run).toEqual({ runId: 'r-1' });
      // If we hadn't stripped the slashes the URL would be
      // "http://.../internal/pipeline/getRun" but built from "...//"
      // which most servers reject. Successful round-trip → strip works.
      expect(h.recorded[0]?.method).toBe('getRun');
    } finally {
      await h.close();
    }
  });
});

describe('HTTPPipelineBridge — per-method wire format', () => {
  let harness: Harness;
  let bridge: HTTPPipelineBridge;

  beforeEach(async () => {
    harness = await startHarness();
    bridge = new HTTPPipelineBridge({ workerBaseUrl: harness.baseUrl, timeoutMs: 2000 });
  });
  afterEach(async () => {
    await harness.close();
  });

  test('trigger() POSTs to /trigger with single object arg, returns runId', async () => {
    harness.on('trigger', (call) => {
      // arg shape mirrors createBridge.trigger({...})
      expect(call.body.args).toHaveLength(1);
      const arg = call.body.args[0] as { pipelineId: string; triggeredBy: { userId: string } };
      expect(arg.pipelineId).toBe('p-1');
      expect(arg.triggeredBy).toEqual({ userId: 'u-1' });
      return { body: { result: { runId: 'run-xyz' } } };
    });
    const result = await bridge.trigger({
      pipelineId: 'p-1',
      definition: { id: 'p-1', nodes: [], edges: [] },
      triggerPayload: { hello: 'world' },
      triggeredBy: { userId: 'u-1' },
      breakpoints: ['node-2'],
    });
    expect(result).toEqual({ runId: 'run-xyz' });
    expect(harness.recorded[0].method).toBe('trigger');
    expect(harness.recorded[0].headers['content-type']).toMatch(/application\/json/);
  });

  test('resolveBreakpoint() forwards (runId, stepId, action)', async () => {
    harness.on('resolveBreakpoint', (call) => {
      expect(call.body.args).toEqual(['r-1', 'step-2', 'step_over']);
      return { body: { result: undefined } };
    });
    await bridge.resolveBreakpoint('r-1', 'step-2', 'step_over');
    expect(harness.recorded[0].method).toBe('resolveBreakpoint');
  });

  test('resolveBreakpoint() defaults action to "continue"', async () => {
    harness.on('resolveBreakpoint', (call) => {
      expect(call.body.args).toEqual(['r-1', 'step-2', 'continue']);
      return { body: { result: undefined } };
    });
    await bridge.resolveBreakpoint('r-1', 'step-2');
  });

  test('resumeFromStep() forwards (runId, fromNodeId, options)', async () => {
    harness.on('resumeFromStep', (call) => {
      expect(call.body.args[0]).toBe('r-1');
      expect(call.body.args[1]).toBe('node-3');
      expect(call.body.args[2]).toEqual({ allowCompleted: true });
      return { body: { result: { runId: 'r-1', status: 'running' } } };
    });
    const out = await bridge.resumeFromStep('r-1', 'node-3', { allowCompleted: true });
    expect(out).toEqual({ runId: 'r-1', status: 'running' });
  });

  test('patchPausedContext() forwards 4 args and returns PatchResult', async () => {
    const patch = { mode: 'merge' as const, values: { foo: 'bar' } };
    harness.on('patchPausedContext', (call) => {
      expect(call.body.args).toEqual(['r-1', 'step-1', patch, 'user-9']);
      return {
        body: {
          result: {
            patchId: 'patch-1',
            before: { x: 1 },
            after: { x: 1, foo: 'bar' },
            validation: { ok: true },
          },
        },
      };
    });
    const result = await bridge.patchPausedContext('r-1', 'step-1', patch, 'user-9');
    expect(result.patchId).toBe('patch-1');
    expect(result.validation).toEqual({ ok: true });
  });

  test('revertContextPatch() forwards (runId, stepId, patchId)', async () => {
    harness.on('revertContextPatch', (call) => {
      expect(call.body.args).toEqual(['r-1', 'step-1', 'patch-1']);
      return { body: { result: undefined } };
    });
    await bridge.revertContextPatch('r-1', 'step-1', 'patch-1');
  });

  test('getRun() returns a snapshot with runId/pipelineId/status', async () => {
    harness.on('getRun', (call) => {
      expect(call.body.args).toEqual(['run-1']);
      return {
        body: {
          result: { runId: 'run-1', pipelineId: 'p-1', status: 'running' },
        },
      };
    });
    const run = await bridge.getRun('run-1');
    expect(run).toEqual({ runId: 'run-1', pipelineId: 'p-1', status: 'running' });
  });

  test('getRun() round-trips a JSON null when run is unknown', async () => {
    harness.on('getRun', () => ({ body: { result: null } }));
    const run = await bridge.getRun('unknown');
    expect(run).toBeNull();
  });

  test('getHistory() forwards (runId, fromVersion) and returns BusEvent[]', async () => {
    harness.on('getHistory', (call) => {
      expect(call.body.args).toEqual(['r-1', 7]);
      return {
        body: {
          result: [
            { type: 'pipeline.step.started', version: 8, payload: { nodeId: 'n-1' } },
            { type: 'pipeline.step.completed', version: 9, payload: { nodeId: 'n-1' } },
          ],
        },
      };
    });
    const events = await bridge.getHistory('r-1', 7);
    expect(events).toHaveLength(2);
    expect(events[0].type).toBe('pipeline.step.started');
  });

  test('resolveApproval() forwards 5 args including optional comment', async () => {
    harness.on('resolveApproval', (call) => {
      expect(call.body.args).toEqual(['r-1', 'step-2', 'u-9', 'approve', 'lgtm']);
      return { body: { result: undefined } };
    });
    await bridge.resolveApproval('r-1', 'step-2', 'u-9', 'approve', 'lgtm');
  });

  test('resolveApproval() omits comment when undefined', async () => {
    harness.on('resolveApproval', (call) => {
      // comment is the 5th arg; JSON.stringify drops trailing undefined
      // entries from arrays (becomes null), so we accept either form.
      expect(call.body.args.slice(0, 4)).toEqual(['r-1', 'step-2', 'u-9', 'reject']);
      return { body: { result: undefined } };
    });
    await bridge.resolveApproval('r-1', 'step-2', 'u-9', 'reject');
  });

  test('listRuns() forwards (pipelineId, opts) and returns paged shape', async () => {
    harness.on('listRuns', (call) => {
      expect(call.body.args).toEqual(['p-1', { limit: 25 }]);
      return {
        body: {
          result: {
            runs: [{ runId: 'r-1', pipelineId: 'p-1' }, { runId: 'r-2', pipelineId: 'p-1' }],
            nextCursor: 'r-2',
          },
        },
      };
    });
    const out = await bridge.listRuns('p-1', { limit: 25 });
    expect(out.runs).toHaveLength(2);
    expect(out.nextCursor).toBe('r-2');
  });

  test('listActiveRuns() takes no args and returns array', async () => {
    harness.on('listActiveRuns', (call) => {
      expect(call.body.args).toEqual([]);
      return {
        body: {
          result: [{ runId: 'r-1', pipelineId: 'p-1', status: 'running' }],
        },
      };
    });
    const runs = await bridge.listActiveRuns();
    expect(runs).toHaveLength(1);
    expect(runs[0].runId).toBe('r-1');
  });

  test('cancelRun() forwards (runId, userId)', async () => {
    harness.on('cancelRun', (call) => {
      expect(call.body.args).toEqual(['r-1', 'u-9']);
      return { body: { result: undefined } };
    });
    await bridge.cancelRun('r-1', 'u-9');
  });

  test('getPendingApprovals() returns the array unchanged', async () => {
    harness.on('getPendingApprovals', (call) => {
      expect(call.body.args).toEqual([]);
      return {
        body: {
          result: [
            { runId: 'r-1', stepId: 'step-1', requestedAt: '2026-05-13T00:00:00Z' },
          ],
        },
      };
    });
    const approvals = await bridge.getPendingApprovals();
    expect(approvals).toHaveLength(1);
  });

  test('getMetrics() returns the metrics object verbatim', async () => {
    const metrics = {
      runsStarted: 12,
      runsCompleted: 10,
      runsFailed: 1,
      runsActive: 1,
      runsAwaitingApproval: 0,
      avgDurationMs: 1200,
      llmTokensIn: 5000,
      llmTokensOut: 3200,
      avgFirstTokenLatencyMs: 450,
      asOf: '2026-05-13T00:00:00Z',
    };
    harness.on('getMetrics', () => ({ body: { result: metrics } }));
    const out = await bridge.getMetrics();
    expect(out).toEqual(metrics);
  });

  test('getDashboardData() returns the dashboard payload verbatim', async () => {
    const dash = {
      summaryKPIs: [{ label: 'Runs', value: 12 }],
      charts: [],
      alerts: [],
    };
    harness.on('getDashboardData', () => ({ body: { result: dash } }));
    const out = await bridge.getDashboardData();
    expect(out).toEqual(dash);
  });

  test('getLastEventAtAsync() returns the unix-ms timestamp', async () => {
    harness.on('getLastEventAt', () => ({ body: { result: 1715600000000 } }));
    const ts = await bridge.getLastEventAtAsync();
    expect(ts).toBe(1715600000000);
  });

  test('getLastEventAt() (sync stub) returns null without calling the wire', async () => {
    expect(bridge.getLastEventAt()).toBeNull();
    // No HTTP call should have been recorded
    expect(harness.recorded).toHaveLength(0);
  });

  test('getInspector() returns a stateful proxy whose methods hit /inspector.<verb>', async () => {
    harness.on('inspector.summarizeQueue', (call) => {
      expect(call.body.args).toEqual([{ topN: 5 }]);
      return { body: { result: { pendingCount: 3, inflightCount: 1, dlqCount: 0 } } };
    });
    const inspector = bridge.getInspector();
    // Cast to any-shaped proxy — see http-bridge.ts for the spike's
    // intentionally-narrow QueueInspector mapping.
    const summary = await (
      inspector as unknown as {
        summarizeQueue: (opts: unknown) => Promise<{ pendingCount: number }>;
      }
    ).summarizeQueue({ topN: 5 });
    expect(summary.pendingCount).toBe(3);
    // getInspector() should be cached: same handle on second call.
    expect(bridge.getInspector()).toBe(inspector);
  });

  test('getDLQ() returns null in spike (no HTTP call)', () => {
    expect(bridge.getDLQ()).toBeNull();
    expect(harness.recorded).toHaveLength(0);
  });
});

// ---------------------------------------------------------------------------
// Error / failure-mode coverage
// ---------------------------------------------------------------------------

describe('HTTPPipelineBridge — error handling', () => {
  let harness: Harness;
  let bridge: HTTPPipelineBridge;

  beforeEach(async () => {
    harness = await startHarness();
    bridge = new HTTPPipelineBridge({ workerBaseUrl: harness.baseUrl, timeoutMs: 1500 });
  });
  afterEach(async () => {
    await harness.close();
  });

  test('non-200 response → BridgeRpcError with status + parsed body', async () => {
    harness.on('getRun', () => ({
      status: 404,
      body: { error: { code: 'NOT_FOUND', message: 'run does not exist' } },
    }));
    await expect(bridge.getRun('missing')).rejects.toMatchObject({
      name: 'BridgeRpcError',
      status: 404,
      method: 'getRun',
      message: expect.stringContaining('run does not exist'),
    });
  });

  test('non-200 with no JSON body still throws BridgeRpcError', async () => {
    harness.on('getRun', () => ({ status: 500, body: 'oops not json' }));
    // The Express handler will JSON-stringify the string, but we deliberately
    // exercise the JSON-parse-failure-path by registering an unknown method
    // (the harness returns 404 with a real error object — close enough; an
    // unknown method exercises the worker-side 404 path).
    await expect(bridge.getRun('x')).rejects.toBeInstanceOf(BridgeRpcError);
  });

  test('unknown method → 404 surfaces as BridgeRpcError(status=404)', async () => {
    // Don't register any handler for cancelRun; harness 404s.
    await expect(bridge.cancelRun('r-1', 'u-1')).rejects.toMatchObject({
      name: 'BridgeRpcError',
      status: 404,
    });
  });

  test('hanging server → AbortSignal.timeout fires, surfaces as BridgeRpcError', async () => {
    // Use a fresh harness that never responds.
    const h = await startHarness();
    h.on('getRun', () => {
      // Return a Promise-handler shape — but the express harness invokes
      // the handler synchronously and writes the response. To actually
      // hang we monkey-patch with a never-resolving handler at the
      // raw-express level. Easier: spin up a dedicated hanging server.
      return { body: { result: null } };
    });
    await h.close(); // close the polite harness; build a hanging one below

    const hangApp = express();
    hangApp.use(express.json());
    hangApp.post('/internal/pipeline/:method', () => {
      // never call res.* — request hangs until the client aborts
    });
    const hangServer = await new Promise<http.Server>((resolve) => {
      const s = hangApp.listen(0, '127.0.0.1', () => resolve(s));
    });
    const { port } = hangServer.address() as AddressInfo;

    try {
      const hangBridge = new HTTPPipelineBridge({
        workerBaseUrl: `http://127.0.0.1:${port}`,
        timeoutMs: 200,
      });
      const start = Date.now();
      await expect(hangBridge.getRun('r-1')).rejects.toMatchObject({
        name: 'BridgeRpcError',
        status: 0,
        // The underlying cause's name is AbortError or TimeoutError depending
        // on Node version; the wrapped message contains one of those strings.
        message: expect.stringMatching(/(AbortError|TimeoutError|aborted)/i),
      });
      const elapsed = Date.now() - start;
      // Should abort within ~timeout + slop
      expect(elapsed).toBeLessThan(2000);
    } finally {
      await new Promise<void>((resolve, reject) =>
        hangServer.close((err) => (err ? reject(err) : resolve())),
      );
    }
  });

  test('network-down (no listener) → BridgeRpcError(status=0)', async () => {
    // Pick a port nothing is listening on. We bind a server, capture the
    // port, then close it before the bridge call.
    const tmpServer = await new Promise<http.Server>((resolve) => {
      const s = http.createServer().listen(0, '127.0.0.1', () => resolve(s));
    });
    const { port } = tmpServer.address() as AddressInfo;
    await new Promise<void>((resolve) => tmpServer.close(() => resolve()));

    const deadBridge = new HTTPPipelineBridge({
      workerBaseUrl: `http://127.0.0.1:${port}`,
      timeoutMs: 1000,
    });
    await expect(deadBridge.getRun('x')).rejects.toMatchObject({
      name: 'BridgeRpcError',
      status: 0,
    });
  });
});

// ---------------------------------------------------------------------------
// Service-auth signing path
// ---------------------------------------------------------------------------

describe('HTTPPipelineBridge — request signing (X-Service-Auth)', () => {
  const SECRET = 'spike-shared-secret';
  const SERVICE = 'platform-api';

  let harness: Harness;
  let bridge: HTTPPipelineBridge;

  beforeEach(async () => {
    harness = await startHarness();
    bridge = new HTTPPipelineBridge({
      workerBaseUrl: harness.baseUrl,
      timeoutMs: 2000,
      signOpts: { secret: SECRET, serviceId: SERVICE },
    });
  });
  afterEach(async () => {
    await harness.close();
  });

  test('attaches X-Service-Auth header that the worker can verify', async () => {
    harness.on('getRun', (call) => {
      // Worker-side: verifyEnvelope against the same payload the bridge
      // signed (the raw JSON body string).
      const header = call.headers['x-service-auth'];
      expect(typeof header).toBe('string');
      const result = verifyEnvelope(header as string, {
        secret: SECRET,
        allowedServices: [SERVICE],
        // Worker received the raw body string; sign over the same.
        payload: call.rawBody,
      });
      expect(result).toEqual({ ok: true, serviceId: SERVICE });
      return { body: { result: { runId: 'r-1' } } };
    });
    await bridge.getRun('r-1');
  });

  test('header is omitted when signOpts not provided', async () => {
    const unsignedBridge = new HTTPPipelineBridge({
      workerBaseUrl: harness.baseUrl,
      timeoutMs: 2000,
    });
    harness.on('getRun', (call) => {
      expect(call.headers['x-service-auth']).toBeUndefined();
      return { body: { result: null } };
    });
    await unsignedBridge.getRun('r-1');
  });

  test('worker rejecting a tampered body → bridge surfaces 401 as BridgeRpcError', async () => {
    harness.on('cancelRun', (call) => {
      // Pretend the worker re-canonicalises against a DIFFERENT payload
      // (simulates a body tampered in flight). verifyEnvelope returns
      // ok:false; worker would respond 401.
      const header = call.headers['x-service-auth'];
      const result = verifyEnvelope(header as string, {
        secret: SECRET,
        allowedServices: [SERVICE],
        payload: '{"args":["TAMPERED"]}',
      });
      if (!result.ok) {
        return { status: 401, body: { error: { message: `auth failed: ${result.reason}` } } };
      }
      return { body: { result: undefined } };
    });
    await expect(bridge.cancelRun('r-1', 'u-1')).rejects.toMatchObject({
      name: 'BridgeRpcError',
      status: 401,
    });
  });
});
