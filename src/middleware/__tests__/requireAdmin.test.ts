// requireAdmin: constant-time comparison, no SKIP_AUTH bypass (security
// review §8, platform-api Low).
import type { Request, Response, NextFunction } from 'express';
import { requireAdmin } from '../requireAdmin';

function call(authorization?: string): { status: number; next: jest.Mock } {
  const req = { path: '/admin/x', header: (n: string) => (n.toLowerCase() === 'authorization' ? authorization : undefined) } as unknown as Request;
  const out = { status: 200 };
  const res = {
    status(code: number) { out.status = code; return this; },
    json() { return this; },
  } as unknown as Response;
  const next = jest.fn();
  requireAdmin(req, res, next as unknown as NextFunction);
  return { status: out.status, next };
}

const saved = { ...process.env };
afterEach(() => {
  for (const k of ['ADMIN_TOKEN', 'VIDEO_ADMIN_TOKEN', 'SKIP_AUTH']) {
    if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k];
  }
});

describe('requireAdmin', () => {
  beforeEach(() => { process.env.ADMIN_TOKEN = 'correct-horse-battery'; delete process.env.VIDEO_ADMIN_TOKEN; });

  it('passes the configured token', () => {
    expect(call('Bearer correct-horse-battery').next).toHaveBeenCalled();
  });

  it('a wrong-length token is 401, not a thrown comparison', () => {
    const r = call('Bearer short');
    expect(r.status).toBe(401);
    expect(r.next).not.toHaveBeenCalled();
  });

  it('a same-length wrong token is 401', () => {
    const r = call('Bearer correct-horse-batterx');
    expect(r.status).toBe(401);
    expect(r.next).not.toHaveBeenCalled();
  });

  it('a missing header is 401', () => {
    expect(call(undefined).status).toBe(401);
  });

  it('no configured token refuses (503) even with SKIP_AUTH=true', () => {
    delete process.env.ADMIN_TOKEN;
    process.env.SKIP_AUTH = 'true';
    const r = call(undefined);
    expect(r.status).toBe(503);
    expect(r.next).not.toHaveBeenCalled();
  });
});
