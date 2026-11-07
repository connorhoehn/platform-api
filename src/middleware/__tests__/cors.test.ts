// CORS answers only allowlisted origins (security review §7, platform-api Low:
// it echoed any Origin with credentials).
import type { Request, Response, NextFunction } from 'express';
import { corsMiddleware, allowedOrigins } from '../cors';

function call(origin: string | undefined, env: NodeJS.ProcessEnv, method = 'GET') {
  const headers: Record<string, string> = {};
  let status = 0;
  const req = { method, headers: origin ? { origin } : {} } as unknown as Request;
  const res = {
    header(k: string, v: string) { headers[k] = v; return this; },
    sendStatus(code: number) { status = code; return this; },
  } as unknown as Response;
  const next = jest.fn();
  corsMiddleware(env)(req, res, next as unknown as NextFunction);
  return { headers, status, next };
}

describe('corsMiddleware', () => {
  const env = { CORS_ALLOWED_ORIGINS: 'https://app.example.com, https://admin.example.com' } as NodeJS.ProcessEnv;

  it('echoes an allowlisted origin with credentials', () => {
    const r = call('https://admin.example.com', env);
    expect(r.headers['Access-Control-Allow-Origin']).toBe('https://admin.example.com');
    expect(r.headers['Access-Control-Allow-Credentials']).toBe('true');
    expect(r.next).toHaveBeenCalled();
  });

  it('does not echo any other origin', () => {
    const r = call('https://evil.example', env);
    expect(r.headers['Access-Control-Allow-Origin']).toBeUndefined();
    expect(r.headers['Access-Control-Allow-Credentials']).toBeUndefined();
    expect(r.headers['Vary']).toBe('Origin');
  });

  it('a preflight from a refused origin gets no CORS grant', () => {
    const r = call('https://evil.example', env, 'OPTIONS');
    expect(r.status).toBe(204);
    expect(r.headers['Access-Control-Allow-Origin']).toBeUndefined();
  });

  it('no list configured: no origin is allowed', () => {
    expect(call('http://localhost:5173', {} as NodeJS.ProcessEnv).headers['Access-Control-Allow-Origin']).toBeUndefined();
  });

  it('insecure dev mode without a list allows loopback dev servers only', () => {
    const dev = { INSECURE_DEV_MODE: 'true', NODE_ENV: 'development' } as NodeJS.ProcessEnv;
    expect(call('http://localhost:5173', dev).headers['Access-Control-Allow-Origin']).toBe('http://localhost:5173');
    expect(call('http://127.0.0.1:3000', dev).headers['Access-Control-Allow-Origin']).toBe('http://127.0.0.1:3000');
    expect(call('http://localhost.evil.example', dev).headers['Access-Control-Allow-Origin']).toBeUndefined();
  });

  it('parses the list', () => {
    expect(allowedOrigins(env)).toEqual(['https://app.example.com', 'https://admin.example.com']);
  });
});
