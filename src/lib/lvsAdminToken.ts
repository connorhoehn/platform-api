// platform-api/src/lib/lvsAdminToken.ts
//
// The bearer platform-api presents to live-video-streaming's admin surface.
//
// Every caller used to fall back to the literal 'admin' when no token was
// configured — a guessable credential that LVS treats as a role=admin wildcard
// (security review, 6 November 2026). Now an unset token means NO
// Authorization header: LVS refuses the call, which is the failure we want.

import log from './logger';

type TokenVar = 'VIDEO_ADMIN_TOKEN' | 'LVS_ADMIN_TOKEN';

let warned = false;

/** The configured LVS admin token, in the given precedence; undefined when unset. */
export function lvsAdminToken(order: readonly TokenVar[] = ['VIDEO_ADMIN_TOKEN', 'LVS_ADMIN_TOKEN']): string | undefined {
  for (const name of order) {
    const v = process.env[name]?.trim();
    if (v) return v;
  }
  if (!warned) {
    warned = true;
    log.error('[lvs] no VIDEO_ADMIN_TOKEN / LVS_ADMIN_TOKEN configured — admin calls to LVS go unauthenticated and will be refused');
  }
  return undefined;
}

/** `{ Authorization: 'Bearer <token>' }`, or `{}` when no token is configured. */
export function lvsAdminAuthHeader(order?: readonly TokenVar[]): Record<string, string> {
  const token = lvsAdminToken(order);
  return token ? { Authorization: `Bearer ${token}` } : {};
}
