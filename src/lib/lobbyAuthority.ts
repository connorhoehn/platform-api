// platform-api/src/lib/lobbyAuthority.ts
//
// Per-resource lobby admission for OrgIQ realtime tokens (fail-closed defence
// in depth). `orgiqLobbyGuard` (lib/lobbyAccess.ts) knows a lobby's tenant
// prefix and DM ids, and nothing else: it admits `orgiq:initiative:<any id>`,
// `assessment:team:<any id>`, a private `social:ch:<slug>` or a name that is
// no lobby family at all to anyone holding that tenant. Whether this person
// may use THAT initiative, team, run or channel is business authority only
// the OrgIQ BFF holds (its gateway `lobbyGuard` + live directory reads).
//
// So, with LOBBY_AUTHORITY_REQUIRED=true, every lobby decision for an OrgIQ
// token also asks the BFF — POST LOBBY_AUTHORITY_URL with the caller's own
// bearer and `{ lobbyName }` — and admits only on HTTP 200 `{ allowed: true }`.
// Anything else (403, 401 for an expired token, a 404 from a BFF without the
// route, 5xx, timeout, malformed body, no URL configured) refuses. The BFF
// answers with the same fresh call admission a `join` frame gets on its /ws.
//
// Why a callback and not lobby grants carried in the token: the BFF's token is
// one page-wide credential shared with the gateway socket (a changed token
// reconnects it), and the call library asks for it with no lobby argument, so
// a per-lobby grant could only be minted by rebuilding the client's token
// plumbing; the callback needs no client change and is fresher (no grant can
// outlive a revocation by its TTL). See docs/lobby-authority.md.
//
// Rollout (two independently deployable halves): the BFF route ships first
// and is inert; then platform-api turns LOBBY_AUTHORITY_REQUIRED on with
// LOBBY_AUTHORITY_URL. Off (the default) keeps today's behaviour exactly.

import { withContext } from './logger';

const log = withContext({ component: 'lobby-authority' });

export const LOBBY_AUTHORITY_TIMEOUT_MS = 3_000;

export function lobbyAuthorityRequired(env: NodeJS.ProcessEnv = process.env): boolean {
  return (env.LOBBY_AUTHORITY_REQUIRED ?? '').trim().toLowerCase() === 'true';
}

export function lobbyAuthorityUrl(env: NodeJS.ProcessEnv = process.env): string {
  return (env.LOBBY_AUTHORITY_URL ?? '').trim();
}

/** The tenant prefixes whose lobbies only the BFF admits (comma list). */
export function lobbyAuthorityTenants(env: NodeJS.ProcessEnv = process.env): string[] {
  const raw = (env.LOBBY_AUTHORITY_TENANTS ?? '').trim();
  return (raw ? raw.split(',') : ['orgiq', 'assessment', 'social', 'pipelines']).map((t) => t.trim()).filter(Boolean);
}

let warnedNoUrl = false;

/**
 * Does the BFF admit this bearer to `lobbyName` now? Never throws: every
 * failure is a refusal.
 */
export async function askLobbyAuthority(
  bearer: string,
  lobbyName: string,
  opts: { url?: string; fetchImpl?: typeof fetch; timeoutMs?: number; documentSourceId?: string } = {},
): Promise<boolean> {
  const url = opts.url ?? lobbyAuthorityUrl();
  if (!url) {
    if (!warnedNoUrl) {
      warnedNoUrl = true;
      log.error('LOBBY_AUTHORITY_REQUIRED is on but LOBBY_AUTHORITY_URL is unset — every OrgIQ call lobby is refused');
    }
    return false;
  }
  if (!bearer || !lobbyName) return false;
  const doFetch = opts.fetchImpl ?? fetch;
  try {
    const res = await doFetch(url, {
      method: 'POST',
      redirect: 'error',
      headers: { authorization: `Bearer ${bearer}`, 'content-type': 'application/json', accept: 'application/json' },
      body: JSON.stringify({ lobbyName, ...(opts.documentSourceId !== undefined ? { documentSourceId: opts.documentSourceId } : {}) }),
      signal: AbortSignal.timeout(opts.timeoutMs ?? LOBBY_AUTHORITY_TIMEOUT_MS),
    });
    if (res.status !== 200) {
      if (res.status !== 403) log.warn({ status: res.status, lobbyName }, 'lobby authority refused (non-403)');
      return false;
    }
    // The source-specific recording callback is still a tiny yes/no proof.
    // Bound it before JSON parsing rather than trusting Content-Length.
    if (!res.body) return false;
    const reader = res.body.getReader(); const chunks: Uint8Array[] = []; let size = 0;
    try {
      for (;;) {
        const part = await reader.read(); if (part.done) break;
        size += part.value.byteLength;
        if (size > 8192) { await reader.cancel(); return false; }
        chunks.push(part.value);
      }
    } finally { reader.releaseLock(); }
    const body = JSON.parse(Buffer.concat(chunks).toString('utf8')) as { allowed?: unknown } | null;
    return body?.allowed === true;
  } catch (err) {
    log.warn({ lobbyName, reason: err instanceof Error ? err.message : String(err) }, 'lobby authority unreachable — refused');
    return false;
  }
}
