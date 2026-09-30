// The OrgIQ realtime token verifier (lib/orgiqRealtimeToken.ts) and its wiring
// into requireAuth / optionalAuth. Real jsonwebtoken here — auth.test.ts mocks
// the module, so the HS256 path is exercised in this file instead.
import jwt from 'jsonwebtoken';
import type { NextFunction, Request, Response } from 'express';
import { isOrgiqCandidate, verifyOrgiqToken } from '../orgiqRealtimeToken';
import { requireAuth, optionalAuth } from '../../middleware/auth';

const SECRET = 'orgiq-test-secret';
const claims = {
  sub: 'u-vera', displayName: 'Vera Green', given_name: 'Vera', family_name: 'Green',
  email: 'vera@example.com', org: 'orgiq', tenants: ['orgiq'], role: 'org-leader', tokenKind: 'orgiq-hs256',
};
function mint(over: Record<string, unknown> = {}, opts: jwt.SignOptions = {}, secret = SECRET): string {
  const base: jwt.SignOptions = { algorithm: 'HS256', issuer: 'orgiq-middleware', audience: 'realtime' };
  if (!('exp' in over)) base.expiresIn = 600;
  return jwt.sign({ ...claims, ...over }, secret, { ...base, ...opts });
}

const saved = { ...process.env };
beforeEach(() => {
  process.env.REALTIME_JWT_SECRET = SECRET;
  delete process.env.REALTIME_JWT_ISSUER;
  delete process.env.REALTIME_JWT_AUDIENCE;
  delete process.env.SKIP_AUTH;
});
afterAll(() => { process.env = saved; });

describe('verifyOrgiqToken', () => {
  it('maps sub / email / org / displayName onto the principal', () => {
    expect(verifyOrgiqToken(mint())).toEqual({
      sub: 'u-vera', email: 'vera@example.com', orgId: 'orgiq', tenants: ['orgiq'], displayName: 'Vera Green', authKind: 'orgiq-hs256',
    });
  });
  it('carries the tenants claim (strings only, trimmed, deduped); none → no tenants', () => {
    expect(verifyOrgiqToken(mint({ tenants: ['org-a', ' social ', 'org-a', 7, ''] })).tenants).toEqual(['org-a', 'social']);
    expect(verifyOrgiqToken(mint({ tenants: undefined }))).not.toHaveProperty('tenants');
    expect(verifyOrgiqToken(mint({ tenants: 'social' }))).not.toHaveProperty('tenants');
  });
  it.each([
    ['a wrong secret', () => mint({}, {}, 'forged')],
    ['an expired token', () => mint({ exp: Math.floor(Date.now() / 1000) - 5 })],
    ['a wrong audience', () => mint({}, { audience: 'other' })],
    ['a wrong issuer', () => mint({}, { issuer: 'someone-else' })],
    ['a non-orgiq tokenKind', () => mint({ tokenKind: 'cognito' })],
    ['no org', () => mint({ org: '' })],
  ])('refuses %s', (_label, make) => {
    expect(() => verifyOrgiqToken(make())).toThrow();
  });
  it('refuses alg none even with a matching payload', () => {
    const none = jwt.sign({ ...claims }, '', { algorithm: 'none', issuer: 'orgiq-middleware', audience: 'realtime' });
    expect(() => verifyOrgiqToken(none)).toThrow();
  });
  it('honours REALTIME_JWT_ISSUER / REALTIME_JWT_AUDIENCE overrides', () => {
    process.env.REALTIME_JWT_ISSUER = 'iss-2';
    process.env.REALTIME_JWT_AUDIENCE = 'aud-2';
    expect(() => verifyOrgiqToken(mint())).toThrow();
    expect(verifyOrgiqToken(mint({}, { issuer: 'iss-2', audience: 'aud-2' })).sub).toBe('u-vera');
  });
});

describe('isOrgiqCandidate', () => {
  it('is off without REALTIME_JWT_SECRET', () => {
    delete process.env.REALTIME_JWT_SECRET;
    expect(isOrgiqCandidate(mint())).toBe(false);
  });
  it('routes HS256 only — an RS256 (Cognito-shaped) header is not ours', () => {
    expect(isOrgiqCandidate(mint())).toBe(true);
    const rsHeader = Buffer.from(JSON.stringify({ alg: 'RS256', kid: 'k1' })).toString('base64url');
    expect(isOrgiqCandidate(`${rsHeader}.e30.sig`)).toBe(false);
    expect(isOrgiqCandidate('not-a-jwt')).toBe(false);
  });
});

function reqWith(bearer?: string): Request {
  return { method: 'POST', baseUrl: '/api/video', path: '/sessions', headers: bearer ? { authorization: `Bearer ${bearer}` } : {} } as unknown as Request;
}
function res() {
  const r = { statusCode: 200, body: undefined as unknown, status(c: number) { r.statusCode = c; return r; }, json(b: unknown) { r.body = b; return r; } };
  return r;
}

describe('requireAuth with an OrgIQ token', () => {
  it('accepts a valid token and sets the principal (no SKIP_AUTH)', async () => {
    const req = reqWith(mint());
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(req, res() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toMatchObject({ sub: 'u-vera', orgId: 'orgiq', authKind: 'orgiq-hs256' });
  });
  it('401s a forged token', async () => {
    const r = res();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(reqWith(mint({}, {}, 'forged')), r as unknown as Response, next);
    expect(r.statusCode).toBe(401);
    expect(r.body).toEqual({ error: 'Invalid token' });
    expect(next).not.toHaveBeenCalled();
  });
  it('401s an expired token as "Token expired"', async () => {
    const r = res();
    await requireAuth(reqWith(mint({ exp: Math.floor(Date.now() / 1000) - 5 })), r as unknown as Response, jest.fn() as unknown as NextFunction);
    expect(r.statusCode).toBe(401);
    expect(r.body).toEqual({ error: 'Token expired' });
  });
  it('verifies even under SKIP_AUTH: a forged OrgIQ token is not decoded into a user', async () => {
    process.env.SKIP_AUTH = 'true';
    const r = res();
    const req = reqWith(mint({}, {}, 'forged'));
    await requireAuth(req, r as unknown as Response, jest.fn() as unknown as NextFunction);
    expect(r.statusCode).toBe(401);
    expect(req.user).toBeUndefined();
  });
});

describe('optionalAuth with an OrgIQ token', () => {
  it('sets the principal for a valid token', async () => {
    const req = reqWith(mint());
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, res() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user?.sub).toBe('u-vera');
  });
  it('proceeds with no principal for a forged token', async () => {
    const req = reqWith(mint({}, {}, 'forged'));
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, res() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toBeUndefined();
  });
});
