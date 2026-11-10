// /internal/metrics and /internal/bus-publish-timing (whose ?reset=1 clears
// the counters) were open to anyone who could reach the port. They now need a
// loopback socket or a request-bound service-auth envelope.
import type { Request, Response } from 'express';
import { signServiceRequest, SERVICE_AUTH_HEADER } from '@connorhoehn/service-runtime';
import { isLoopbackAddress, requireLoopbackOrServiceAuth, INTERNAL_OPS_AUDIENCE } from '../internalOpsAccess';

const SECRET = 'test-secret-'.padEnd(40, 'x');

function run(opts: { remote: string; url?: string; method?: string; header?: string; env?: Record<string, string> }) {
  const headers: Record<string, string> = {};
  if (opts.header) headers[SERVICE_AUTH_HEADER.toLowerCase()] = opts.header;
  const req = {
    method: opts.method ?? 'GET',
    originalUrl: opts.url ?? '/internal/metrics',
    socket: { remoteAddress: opts.remote },
    // A forwarded-for header claiming loopback must not count.
    headers: { 'x-forwarded-for': '127.0.0.1', ...headers },
    header: (name: string) => headers[name.toLowerCase()],
  } as unknown as Request;
  let status = 0;
  let body: unknown;
  const res = {
    status(c: number) { status = c; return this; },
    json(b: unknown) { body = b; return this; },
  } as unknown as Response;
  const next = jest.fn();
  requireLoopbackOrServiceAuth((opts.env ?? {}) as NodeJS.ProcessEnv)(req, res, next);
  return { passed: next.mock.calls.length === 1, status, body };
}

const sign = (target: string, serviceId = 'orchestrator', audience = INTERNAL_OPS_AUDIENCE, secret = SECRET) =>
  signServiceRequest({ method: 'GET', target, body: '' }, { secret, serviceId, audience });

describe('internal ops access', () => {
  it('recognises loopback socket addresses only', () => {
    for (const a of ['127.0.0.1', '127.1.2.3', '::1', '::ffff:127.0.0.1']) expect(isLoopbackAddress(a)).toBe(true);
    for (const a of ['10.0.0.5', '::ffff:10.0.0.5', '0.0.0.0', '', undefined]) expect(isLoopbackAddress(a)).toBe(false);
  });

  it('serves a loopback caller with no credentials', () => {
    expect(run({ remote: '127.0.0.1' }).passed).toBe(true);
    expect(run({ remote: '::1', url: '/internal/bus-publish-timing?reset=1' }).passed).toBe(true);
  });

  it('refuses a network caller without credentials, even with X-Forwarded-For: 127.0.0.1', () => {
    const r = run({ remote: '10.1.2.3', env: { SERVICE_AUTH_SECRET: SECRET } });
    expect(r.passed).toBe(false);
    expect(r.status).toBe(401);
    expect(r.body).toEqual({ error: 'SERVICE_AUTH_REQUIRED' });
    expect(run({ remote: '10.1.2.3', url: '/internal/bus-publish-timing?reset=1' }).passed).toBe(false);
  });

  it('serves a network caller with a valid envelope for this exact request', () => {
    const env = { SERVICE_AUTH_SECRET: SECRET };
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics'), env }).passed).toBe(true);
    const reset = '/internal/bus-publish-timing?reset=1';
    expect(run({ remote: '10.1.2.3', url: reset, header: sign(reset), env }).passed).toBe(true);
  });

  it('refuses an envelope for another path, audience, service or secret, and any envelope with no secret configured', () => {
    const env = { SERVICE_AUTH_SECRET: SECRET };
    // Signed for the read, replayed on the reset.
    expect(run({ remote: '10.1.2.3', url: '/internal/bus-publish-timing?reset=1', header: sign('/internal/bus-publish-timing'), env }).passed).toBe(false);
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics', 'orchestrator', 'message-queue'), env }).passed).toBe(false);
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics', 'stranger'), env }).passed).toBe(false);
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics', 'orchestrator', INTERNAL_OPS_AUDIENCE, 'other-secret'.padEnd(40, 'y')), env }).passed).toBe(false);
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics') }).passed).toBe(false);
    // SERVICE_AUTH_ALLOWED_SERVICES narrows the list.
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics'), env: { ...env, SERVICE_AUTH_ALLOWED_SERVICES: 'scraper' } }).passed).toBe(false);
    expect(run({ remote: '10.1.2.3', header: sign('/internal/metrics', 'scraper'), env: { ...env, SERVICE_AUTH_ALLOWED_SERVICES: 'scraper' } }).passed).toBe(true);
  });

  it('is mounted on both internal ops routes in createApp', () => {
    // eslint-disable-next-line @typescript-eslint/no-var-requires
    const src = require('fs').readFileSync(require('path').join(__dirname, '../../app.ts'), 'utf8') as string;
    expect(src).toMatch(/app\.get\('\/internal\/metrics', internalOps,/);
    expect(src).toMatch(/app\.get\('\/internal\/bus-publish-timing', internalOps,/);
  });
});
