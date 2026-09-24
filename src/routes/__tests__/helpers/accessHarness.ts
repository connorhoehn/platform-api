// Loop 29c: one request helper for the access tests — drives an Express app
// in-process (no supertest, no socket) as a named user, or anonymously.
import type { Application, NextFunction, Request, Response } from 'express';

export interface MockRes { statusCode: number; body: unknown; ended: boolean; finished: Promise<void>; status(c: number): MockRes; json(b: unknown): MockRes; send(b?: unknown): MockRes; setHeader(): MockRes; set(): MockRes; type(): MockRes; getHeader(): unknown; end(): MockRes }

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  return {
    statusCode: 200, body: undefined, ended: false, finished,
    status(c) { this.statusCode = c; return this; },
    json(b) { this.body = b; this.ended = true; resolve(); return this; },
    send(b) { this.body = b; this.ended = true; resolve(); return this; },
    setHeader() { return this; }, set() { return this; }, type() { return this; }, getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
}

export async function call(
  app: Application,
  method: string,
  url: string,
  opts: { userId?: string; anonymous?: boolean; body?: unknown; orgId?: string } = {},
): Promise<MockRes> {
  const res = mockRes();
  const headers: Record<string, string> = { 'content-type': 'application/json' };
  if (!opts.anonymous) headers.authorization = 'Bearer t';
  const [path, qs] = url.split('?');
  const query = Object.fromEntries(new URLSearchParams(qs ?? ''));
  const req = {
    method, url, originalUrl: url, path, headers, query, body: opts.body ?? {},
    user: opts.anonymous ? undefined : { sub: opts.userId ?? 'dev-owner', ...(opts.orgId ? { orgId: opts.orgId } : {}) },
    params: {}, get: (h: string) => headers[h.toLowerCase()], header: (h: string) => headers[h.toLowerCase()],
  } as unknown as Request;
  await new Promise<void>((done, reject) => {
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(req, res as unknown as Response, (err?: unknown) => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      done();
    });
    res.finished.then(() => done()).catch(reject);
  });
  return res;
}
