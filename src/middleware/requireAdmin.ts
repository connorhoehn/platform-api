// requireAdmin — simple bearer-token gate for operator/admin endpoints.
//
// platform-api doesn't carry a Cognito "admin role" claim yet, so admin
// routes (like /api/admin/recordings/health) are gated by a shared
// secret instead: callers must send `Authorization: Bearer <ADMIN_TOKEN>`
// where ADMIN_TOKEN matches the env var.
//
// Fails closed: no ADMIN_TOKEN configured is a 503 in every mode (SKIP_AUTH
// used to wave admin calls through), a missing or wrong bearer is a 401, and
// the comparison is constant-time over fixed-length digests so neither the
// token's content nor its length leaks through timing.
//
// VIDEO_ADMIN_TOKEN is honoured as a fallback so a single shared admin
// secret can gate both this surface and the LVS pod-to-pod calls.

import { createHash, timingSafeEqual } from 'node:crypto';
import type { Request, Response, NextFunction } from 'express';
import log from '../lib/logger';

function getConfiguredToken(): string | undefined {
  const t = process.env.ADMIN_TOKEN || process.env.VIDEO_ADMIN_TOKEN;
  return t && t.length > 0 ? t : undefined;
}

/** Constant-time equality for two strings of any length. */
export function tokensMatch(provided: string, expected: string): boolean {
  const a = createHash('sha256').update(provided, 'utf8').digest();
  const b = createHash('sha256').update(expected, 'utf8').digest();
  return timingSafeEqual(a, b);
}

export function requireAdmin(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const configured = getConfiguredToken();
  if (!configured) {
    log.warn(
      { path: req.path },
      '[requireAdmin] no ADMIN_TOKEN configured — refusing admin call',
    );
    res.status(503).json({ error: 'admin endpoint not configured' });
    return;
  }

  const header = req.header('authorization') ?? '';
  if (!header.toLowerCase().startsWith('bearer ')) {
    res.status(401).json({ error: 'admin authorization required' });
    return;
  }
  const provided = header.slice(7).trim();
  if (!provided || !tokensMatch(provided, configured)) {
    res.status(401).json({ error: 'admin authorization required' });
    return;
  }
  next();
}
