// requireAdmin — simple bearer-token gate for operator/admin endpoints.
//
// platform-api doesn't carry a Cognito "admin role" claim yet, so admin
// routes (like /api/admin/recordings/health) are gated by a shared
// secret instead: callers must send `Authorization: Bearer <ADMIN_TOKEN>`
// where ADMIN_TOKEN matches the env var.
//
// When `ADMIN_TOKEN` is unset, in dev mode (`SKIP_AUTH=true`) we let the
// request through; in any other case we 503 so we fail closed if the
// secret was accidentally not provisioned.
//
// VIDEO_ADMIN_TOKEN is honoured as a fallback so a single shared admin
// secret can gate both this surface and the LVS pod-to-pod calls.

import type { Request, Response, NextFunction } from 'express';
import log from '../lib/logger';

function getConfiguredToken(): string | undefined {
  const t = process.env.ADMIN_TOKEN || process.env.VIDEO_ADMIN_TOKEN;
  return t && t.length > 0 ? t : undefined;
}

export function requireAdmin(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  const configured = getConfiguredToken();
  if (!configured) {
    if (process.env.SKIP_AUTH === 'true') {
      // Dev convenience — no token configured, allow through so Tilt
      // demos can hit admin endpoints without provisioning a secret.
      next();
      return;
    }
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
  if (provided !== configured) {
    res.status(403).json({ error: 'admin forbidden' });
    return;
  }
  next();
}
