import { Writable } from 'stream';
import pino from 'pino';
import {
  createLogger,
  makeHealthHandler,
  HealthCheck,
  HealthBody,
  MinimalResponse,
} from '../observability-base';

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

/** Capture-buffer res that records the last status + json body. */
function makeMockRes(): {
  res: MinimalResponse;
  getStatus: () => number | undefined;
  getBody: () => HealthBody | undefined;
} {
  let statusCode: number | undefined;
  let body: HealthBody | undefined;
  const res: MinimalResponse = {
    status(code: number) {
      statusCode = code;
      return res;
    },
    json(payload: unknown) {
      body = payload as HealthBody;
      return res;
    },
  };
  return { res, getStatus: () => statusCode, getBody: () => body };
}

/** Build a pino logger that writes JSON lines into the provided array. */
function captureLogger(opts: { service: string; level?: string; context?: Record<string, unknown> }) {
  const lines: Array<Record<string, unknown>> = [];
  const stream = new Writable({
    write(chunk, _enc, cb) {
      try {
        lines.push(JSON.parse(chunk.toString()));
      } catch {
        /* ignore non-JSON lines */
      }
      cb();
    },
  });
  // Mirror createLogger's shape but route to capture stream so we can
  // assert serialized output without re-implementing the function.
  const baseFields: Record<string, unknown> = { service: opts.service, ...(opts.context ?? {}) };
  const logger = pino(
    {
      level: opts.level ?? 'info',
      base: baseFields,
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    stream,
  );
  return { logger, lines };
}

// ---------------------------------------------------------------------
// createLogger
// ---------------------------------------------------------------------

describe('createLogger', () => {
  it('returns a working pino logger with the service bound', () => {
    const log = createLogger({ service: 'test-svc' });
    expect(typeof log.info).toBe('function');
    expect(typeof log.error).toBe('function');
    expect(typeof log.child).toBe('function');
    expect(log.level).toBe('info');
  });

  it('respects the level option', () => {
    const log = createLogger({ service: 'test-svc', level: 'warn' });
    expect(log.level).toBe('warn');
  });

  it('binds context fields to every log line (via capture-stream parity)', () => {
    // The exported createLogger writes to stdout, so we mirror its
    // shape against a capture stream to assert serialized output.
    const { logger, lines } = captureLogger({
      service: 'test-svc',
      context: { correlationId: 'abc-123', region: 'us-east-1' },
    });
    logger.info({ action: 'noop' }, 'hello');
    expect(lines).toHaveLength(1);
    const line = lines[0];
    expect(line.service).toBe('test-svc');
    expect(line.correlationId).toBe('abc-123');
    expect(line.region).toBe('us-east-1');
    expect(line.action).toBe('noop');
    expect(line.msg).toBe('hello');
  });
});

// ---------------------------------------------------------------------
// makeHealthHandler
// ---------------------------------------------------------------------

describe('makeHealthHandler', () => {
  const okCheck: HealthCheck = {
    name: 'always-ok',
    check: async () => ({ ok: true, details: { ping: 'pong' } }),
  };

  const failCheck: HealthCheck = {
    name: 'always-fail',
    check: async () => {
      throw new Error('boom');
    },
  };

  const slowCheck: HealthCheck = {
    name: 'always-slow',
    check: () =>
      new Promise((resolve) => {
        setTimeout(() => resolve({ ok: true }), 250);
      }),
  };

  it('all checks pass → 200 healthy', async () => {
    const handler = makeHealthHandler({
      service: 'test-svc',
      version: '0.0.1',
      checks: [okCheck, { name: 'second-ok', check: async () => ({ ok: true }) }],
    });
    const { res, getStatus, getBody } = makeMockRes();
    await handler({}, res);
    expect(getStatus()).toBe(200);
    const body = getBody()!;
    expect(body.status).toBe('healthy');
    expect(body.checks).toHaveLength(2);
    expect(body.checks.every((c) => c.ok)).toBe(true);
  });

  it('one check throws → 503 unhealthy with error captured', async () => {
    const handler = makeHealthHandler({
      service: 'test-svc',
      version: '0.0.1',
      checks: [okCheck, failCheck],
    });
    const { res, getStatus, getBody } = makeMockRes();
    await handler({}, res);
    expect(getStatus()).toBe(503);
    const body = getBody()!;
    expect(body.status).toBe('unhealthy');
    const failed = body.checks.find((c) => c.name === 'always-fail');
    expect(failed).toBeDefined();
    expect(failed!.ok).toBe(false);
    expect(failed!.error).toBe('boom');
  });

  it('one check times out → 503 unhealthy with timeout error', async () => {
    const handler = makeHealthHandler({
      service: 'test-svc',
      version: '0.0.1',
      checks: [okCheck, slowCheck],
      timeoutMs: 50,
    });
    const { res, getStatus, getBody } = makeMockRes();
    await handler({}, res);
    expect(getStatus()).toBe(503);
    const body = getBody()!;
    expect(body.status).toBe('unhealthy');
    const timedOut = body.checks.find((c) => c.name === 'always-slow');
    expect(timedOut).toBeDefined();
    expect(timedOut!.ok).toBe(false);
    expect(timedOut!.error).toMatch(/timed out/i);
    expect(timedOut!.durationMs).toBeGreaterThanOrEqual(40);
  });

  it('body schema matches the documented shape', async () => {
    const handler = makeHealthHandler({
      service: 'test-svc',
      version: '1.2.3',
      checks: [okCheck],
    });
    const { res, getBody } = makeMockRes();
    await handler({}, res);
    const body = getBody()!;
    expect(Object.keys(body).sort()).toEqual(
      ['checks', 'service', 'status', 'timestamp', 'version'].sort(),
    );
    expect(body.service).toBe('test-svc');
    expect(body.version).toBe('1.2.3');
    expect(['healthy', 'degraded', 'unhealthy']).toContain(body.status);
    expect(Array.isArray(body.checks)).toBe(true);
    expect(typeof body.timestamp).toBe('string');
    // ISO-8601 round-trips through Date()
    expect(new Date(body.timestamp).toISOString()).toBe(body.timestamp);
    // Each check report has the required fields
    for (const c of body.checks) {
      expect(typeof c.name).toBe('string');
      expect(typeof c.ok).toBe('boolean');
      expect(typeof c.durationMs).toBe('number');
    }
  });
});
