// NFR #72: a gateway (WebSocket) trigger names the person in a signed body.
// The gateway signs the body OBJECT (canonical sorted-key JSON), as every
// verifyEnvelope receiver re-canonicalises what it parsed.
import type { Request, Response, NextFunction } from 'express';
import { signEnvelope, SERVICE_AUTH_HEADER } from '@connorhoehn/service-runtime';
import { serviceOnBehalfOf } from '../serviceOnBehalfOf';
import { requireAuth } from '../auth';

const SECRET = 'test-secret';

function res() {
  const r = { statusCode: 200, body: undefined as unknown,
    status(c: number) { r.statusCode = c; return r; }, json(b: unknown) { r.body = b; return r; } };
  return r;
}

function req(body: Record<string, unknown>, opts: { serviceId?: string; secret?: string; sign?: boolean; tamper?: (b: Record<string, unknown>) => void } = {}): Request {
  const bodyStr = JSON.stringify(body);
  const headers: Record<string, string> = {};
  if (opts.sign !== false) {
    headers[SERVICE_AUTH_HEADER.toLowerCase()] = signEnvelope(body, { secret: opts.secret ?? SECRET, serviceId: opts.serviceId ?? 'realtime-examples' });
  }
  const parsed = JSON.parse(bodyStr);
  opts.tamper?.(parsed);
  return {
    method: 'POST', body: parsed, originalUrl: '/api/pipelines/p1/runs', baseUrl: '/api/pipelines/p1/runs', path: '/',
    headers,
    header: (name: string) => headers[name.toLowerCase()],
  } as unknown as Request;
}

describe('serviceOnBehalfOf (NFR #72)', () => {
  const env = { ...process.env };
  beforeEach(() => { process.env.SERVICE_AUTH_SECRET = SECRET; process.env.SKIP_AUTH = 'true'; process.env.NODE_ENV = 'test'; });
  afterAll(() => { process.env = env; });

  it('a signed gateway trigger runs as the named person and org, and requireAuth keeps it', async () => {
    const r = req({ triggerPayload: { documentId: 'audit-x' }, onBehalfOf: { userId: 'connor', orgId: 'dev-org' } });
    const next = jest.fn();
    serviceOnBehalfOf(r, res() as unknown as Response, next as NextFunction);
    expect(next).toHaveBeenCalledTimes(1);
    expect(r.user).toEqual({ sub: 'connor', orgId: 'dev-org' });
    expect(r.serviceActor).toBe('realtime-examples');
    expect(r.body).toEqual({ triggerPayload: { documentId: 'audit-x' } });
    const next2 = jest.fn();
    await requireAuth(r, res() as unknown as Response, next2 as NextFunction);
    expect(next2).toHaveBeenCalledTimes(1);
    expect(r.user).toEqual({ sub: 'connor', orgId: 'dev-org' });
  });

  it('an org-less person stays org-less (never a default org)', () => {
    const r = req({ onBehalfOf: { userId: 'connor', orgId: null } });
    serviceOnBehalfOf(r, res() as unknown as Response, jest.fn() as NextFunction);
    expect(r.user).toEqual({ sub: 'connor' });
  });

  it('refuses a tampered identity (signature covers onBehalfOf)', () => {
    const r = req({ onBehalfOf: { userId: 'connor', orgId: 'dev-org' } }, { tamper: (b) => { (b.onBehalfOf as Record<string, string>).userId = 'frank'; } });
    const out = res(); const next = jest.fn();
    serviceOnBehalfOf(r, out as unknown as Response, next as NextFunction);
    expect(next).not.toHaveBeenCalled();
    expect(out.statusCode).toBe(401);
    expect(r.user).toBeUndefined();
  });

  it('refuses an unsigned claim, a wrong secret and a service that is not the gateway', () => {
    for (const opts of [{ sign: false }, { secret: 'nope' }, { serviceId: 'someone-else' }]) {
      const r = req({ onBehalfOf: { userId: 'connor' } }, opts);
      const out = res(); const next = jest.fn();
      serviceOnBehalfOf(r, out as unknown as Response, next as NextFunction);
      expect(next).not.toHaveBeenCalled();
      expect(out.statusCode).toBe(401);
    }
  });

  it('refuses a malformed id', () => {
    const r = req({ onBehalfOf: { userId: '../../x y' } });
    const out = res();
    serviceOnBehalfOf(r, out as unknown as Response, jest.fn() as NextFunction);
    expect(out.statusCode).toBe(400);
  });

  it('a request without onBehalfOf passes through untouched (requireAuth decides)', () => {
    const r = req({ triggerPayload: {} }, { sign: false });
    const next = jest.fn();
    serviceOnBehalfOf(r, res() as unknown as Response, next as NextFunction);
    expect(next).toHaveBeenCalledTimes(1);
    expect(r.user).toBeUndefined();
  });
});
