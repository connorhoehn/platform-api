// platform-api/src/middleware/cors.ts
//
// CORS from an allowlist. It used to echo whatever Origin arrived, with
// credentials, so any site could make credentialed calls and read the answer
// (security review, 6 November 2026).
//
//   CORS_ALLOWED_ORIGINS  comma-separated exact origins
//                         (`https://app.example.com,https://admin.example.com`).
//   unset                 no cross-origin access at all — except in insecure
//                         dev mode (lib/insecureDevMode.ts), where loopback dev
//                         servers (`http://localhost:<port>`,
//                         `http://127.0.0.1:<port>`) are allowed.
//
// A refused origin gets no CORS headers; the browser then blocks the read.

import type { Request, Response, NextFunction, RequestHandler } from 'express';
import { insecureDevMode } from '../lib/insecureDevMode';

const LOOPBACK_ORIGIN = /^http:\/\/(localhost|127\.0\.0\.1)(:\d{1,5})?$/;

export function allowedOrigins(env: NodeJS.ProcessEnv = process.env): string[] {
  return (env.CORS_ALLOWED_ORIGINS ?? '').split(',').map((o) => o.trim()).filter(Boolean);
}

export function isAllowedOrigin(origin: string, env: NodeJS.ProcessEnv = process.env): boolean {
  const list = allowedOrigins(env);
  if (list.length > 0) return list.includes(origin);
  return insecureDevMode(env) && LOOPBACK_ORIGIN.test(origin);
}

/** `env` is read per request when omitted, so config changes apply without a rebuild. */
export function corsMiddleware(env?: NodeJS.ProcessEnv): RequestHandler {
  return (req: Request, res: Response, next: NextFunction): void => {
    const origin = req.headers.origin;
    res.header('Vary', 'Origin');
    if (typeof origin === 'string' && isAllowedOrigin(origin, env ?? process.env)) {
      res.header('Access-Control-Allow-Origin', origin);
      res.header('Access-Control-Allow-Credentials', 'true');
      res.header('Access-Control-Allow-Methods', 'GET,POST,PUT,PATCH,DELETE,OPTIONS');
      res.header('Access-Control-Allow-Headers', 'Content-Type,Authorization,Idempotency-Key');
    }
    if (req.method === 'OPTIONS') { res.sendStatus(204); return; }
    next();
  };
}
