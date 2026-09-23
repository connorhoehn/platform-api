// Unit tests for auth middleware.
//
// Mocks jsonwebtoken and jwks-rsa at module level. Tests SKIP_AUTH
// bypass, missing/malformed headers, token expiry, and the
// optionalAuth variant.

import type { Request, Response, NextFunction } from 'express';

const mockDecode = jest.fn();
const mockVerify = jest.fn();
const mockGetSigningKey = jest.fn();

jest.mock('jsonwebtoken', () => ({
  decode: (...args: unknown[]) => mockDecode(...args),
  verify: (...args: unknown[]) => mockVerify(...args),
}));

jest.mock('jwks-rsa', () => () => ({
  getSigningKey: (...args: unknown[]) => mockGetSigningKey(...args),
}));

process.env.COGNITO_REGION = 'us-east-1';
process.env.COGNITO_USER_POOL_ID = 'us-east-1_TestPool';

import { requireAuth, optionalAuth, requirePrincipal } from '../auth';

interface TestRes {
  statusCode: number;
  body: unknown;
  status(code: number): TestRes;
  json(body: unknown): TestRes;
}

function mockRes(): TestRes {
  const r: TestRes = {
    statusCode: 200,
    body: undefined,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; return this; },
  };
  return r;
}

function mockReq(authHeader?: string): Request {
  return {
    headers: authHeader ? { authorization: authHeader } : {},
  } as unknown as Request;
}

const originalSkip = process.env.SKIP_AUTH;

beforeEach(() => {
  mockDecode.mockReset();
  mockVerify.mockReset();
  mockGetSigningKey.mockReset();
  delete process.env.SKIP_AUTH;
});

afterAll(() => {
  if (originalSkip !== undefined) process.env.SKIP_AUTH = originalSkip;
  else delete process.env.SKIP_AUTH;
});

describe('requireAuth', () => {
  it('bypasses auth when SKIP_AUTH=true', async () => {
    process.env.SKIP_AUTH = 'true';
    const req = mockReq();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toEqual({ sub: 'dev-user', email: 'dev@local' });
  });

  it('returns 401 when no authorization header', async () => {
    const res = mockRes();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(mockReq(), res as unknown as Response, next);
    expect(res.statusCode).toBe(401);
    expect(next).not.toHaveBeenCalled();
  });

  it('returns 401 when authorization is not Bearer', async () => {
    const res = mockRes();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(mockReq('Basic abc'), res as unknown as Response, next);
    expect(res.statusCode).toBe(401);
  });

  it('returns 401 when token cannot be decoded', async () => {
    mockDecode.mockReturnValue(null);
    const res = mockRes();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(mockReq('Bearer bad-token'), res as unknown as Response, next);
    expect(res.statusCode).toBe(401);
  });

  it('returns 401 with "Token expired" on TokenExpiredError', async () => {
    mockDecode.mockReturnValue({ header: { kid: 'kid-1' } });
    mockGetSigningKey.mockResolvedValue({ getPublicKey: () => 'pk' });
    const err = new Error('jwt expired');
    err.name = 'TokenExpiredError';
    mockVerify.mockImplementation(() => { throw err; });

    const res = mockRes();
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(mockReq('Bearer expired-token'), res as unknown as Response, next);
    expect(res.statusCode).toBe(401);
    expect((res.body as { error: string }).error).toBe('Token expired');
  });

  it('sets req.user on valid token', async () => {
    mockDecode.mockReturnValue({ header: { kid: 'kid-1' } });
    mockGetSigningKey.mockResolvedValue({ getPublicKey: () => 'pk' });
    mockVerify.mockReturnValue({ sub: 'user-42', email: 'user@example.com' });

    const req = mockReq('Bearer valid-token');
    const next = jest.fn() as unknown as NextFunction;
    await requireAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toEqual({ sub: 'user-42', email: 'user@example.com' });
  });
});

describe('optionalAuth', () => {
  it('bypasses auth when SKIP_AUTH=true', async () => {
    process.env.SKIP_AUTH = 'true';
    const req = mockReq();
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toEqual({ sub: 'dev-user', email: 'dev@local' });
  });

  it('calls next without user when no auth header', async () => {
    const req = mockReq();
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toBeUndefined();
  });

  it('calls next without user on invalid token', async () => {
    mockDecode.mockReturnValue({ header: { kid: 'kid-1' } });
    mockGetSigningKey.mockResolvedValue({ getPublicKey: () => 'pk' });
    mockVerify.mockImplementation(() => { throw new Error('bad'); });

    const req = mockReq('Bearer bad-token');
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toBeUndefined();
  });

  it('sets req.user on valid token', async () => {
    mockDecode.mockReturnValue({ header: { kid: 'kid-1' } });
    mockGetSigningKey.mockResolvedValue({ getPublicKey: () => 'pk' });
    mockVerify.mockReturnValue({ sub: 'user-42', email: 'a@b.com' });

    const req = mockReq('Bearer ok-token');
    const next = jest.fn() as unknown as NextFunction;
    await optionalAuth(req, mockRes() as unknown as Response, next);
    expect(next).toHaveBeenCalled();
    expect(req.user).toEqual({ sub: 'user-42', email: 'a@b.com' });
  });
});

describe('requirePrincipal (no silent dev-user)', () => {
  it('refuses a request that has no principal', () => {
    const res = mockRes();
    const next = jest.fn();
    requirePrincipal({ method: 'GET', baseUrl: '/api/v2', path: '/pipelines/x/runs' } as unknown as Request, res as unknown as Response, next as NextFunction);
    expect(next).not.toHaveBeenCalled();
    expect(res.statusCode).toBe(401);
  });

  it('passes a request requireAuth attributed', () => {
    const res = mockRes();
    const next = jest.fn();
    requirePrincipal({ user: { sub: 'dev-connor' } } as unknown as Request, res as unknown as Response, next as NextFunction);
    expect(next).toHaveBeenCalled();
  });

  it('without SKIP_AUTH an anonymous request is 401, never dev-user', async () => {
    const prev = process.env.SKIP_AUTH;
    delete process.env.SKIP_AUTH;
    const req = { headers: {}, method: 'GET', path: '/pipelines/defs/x' } as unknown as Request;
    const res = mockRes();
    const next = jest.fn();
    await requireAuth(req, res as unknown as Response, next as NextFunction);
    expect(res.statusCode).toBe(401);
    expect((req as Request & { user?: unknown }).user).toBeUndefined();
    process.env.SKIP_AUTH = prev;
  });

  it('SKIP_AUTH in production still refuses (the fallback is dev-only)', async () => {
    const prevSkip = process.env.SKIP_AUTH;
    const prevEnv = process.env.NODE_ENV;
    process.env.SKIP_AUTH = 'true';
    process.env.NODE_ENV = 'production';
    const req = { headers: {}, method: 'GET', path: '/pipelines/defs/x' } as unknown as Request;
    const res = mockRes();
    await requireAuth(req, res as unknown as Response, jest.fn() as NextFunction);
    expect(res.statusCode).toBe(401);
    process.env.SKIP_AUTH = prevSkip;
    process.env.NODE_ENV = prevEnv;
  });
});
