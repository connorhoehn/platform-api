// platform-api/src/middleware/internalOpsAccess.ts
//
// Who may read the operational endpoints (`/internal/metrics`,
// `/internal/bus-publish-timing`, whose `?reset=1` also clears the counters).
// They used to be open to anyone who could reach the port (security
// verification, 9 November 2026). Now a request is served when either:
//
//   - it arrives on a loopback socket (a sidecar scraper, `curl` on the box).
//     The socket's own address is used, never X-Forwarded-For, so a request
//     relayed by a load balancer is not loopback; or
//   - it carries a request-bound service-auth envelope (X-Service-Auth,
//     audience `platform-api`, signed over the method and exact path and
//     query) from a service in SERVICE_AUTH_ALLOWED_SERVICES, verified with
//     SERVICE_AUTH_SECRET.
//
// Anything else gets 401. With no SERVICE_AUTH_SECRET only loopback works.

import type { NextFunction, Request, Response } from 'express';
import { SERVICE_AUTH_HEADER, verifyServiceRequest } from '@connorhoehn/service-runtime';

export const INTERNAL_OPS_AUDIENCE = 'platform-api';
const DEFAULT_ALLOWED_SERVICES = ['orchestrator', 'orgiq-middleware'];

export function isLoopbackAddress(address: string | undefined | null): boolean {
  if (!address) return false;
  const a = address.trim().toLowerCase();
  if (a === '::1') return true;
  const v4 = a.startsWith('::ffff:') ? a.slice('::ffff:'.length) : a;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(v4);
}

function allowedServices(env: NodeJS.ProcessEnv): string[] {
  const raw = env.SERVICE_AUTH_ALLOWED_SERVICES?.trim();
  if (!raw) return DEFAULT_ALLOWED_SERVICES;
  return raw.split(',').map((s) => s.trim()).filter(Boolean);
}

export function requireLoopbackOrServiceAuth(env: NodeJS.ProcessEnv = process.env) {
  return (req: Request, res: Response, next: NextFunction): void => {
    if (isLoopbackAddress(req.socket?.remoteAddress)) {
      next();
      return;
    }
    const secret = env.SERVICE_AUTH_SECRET?.trim();
    const header = req.header(SERVICE_AUTH_HEADER);
    if (secret && header) {
      const result = verifyServiceRequest(
        header,
        { method: req.method, target: req.originalUrl, body: '' },
        { secret, audience: INTERNAL_OPS_AUDIENCE, allowedServices: allowedServices(env), ttlSec: 30, toleranceSec: 5 },
      );
      if (result.ok) {
        next();
        return;
      }
    }
    res.status(401).json({ error: 'SERVICE_AUTH_REQUIRED' });
  };
}
