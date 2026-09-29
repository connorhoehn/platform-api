// platform-api/src/lib/orgiqRealtimeToken.ts
//
// The OrgIQ realtime token: an HS256 JWT the OrgIQ middleware (the BFF in
// aws-agentcore, `middleware/src/lib/realtime-token.ts`) mints from its signed
// session cookie and hands the browser for calls. It is NOT a Cognito id_token
// — OrgIQ's demo/picker sign-ins have none — so `requireAuth`/`optionalAuth`
// verify it on their own path, beside the Cognito JWKS path, when
// `REALTIME_JWT_SECRET` is set:
//
//   alg      HS256 only (the header decides which path a token takes; a
//            Cognito token is RS256 with a `kid` and never reaches this code)
//   secret   REALTIME_JWT_SECRET (the same Secrets Manager value the
//            middleware signs with: `orgiq-realtime-jwt-secret`)
//   iss/aud  REALTIME_JWT_ISSUER / REALTIME_JWT_AUDIENCE, defaulting to what
//            the middleware mints: 'orgiq-middleware' / 'realtime'
//   exp      enforced (the middleware mints ten-minute tokens)
//   claims   tokenKind === 'orgiq-hs256', a non-empty `sub` and `org`
//
// Unset secret → the path is off and an HS256 token falls through to the
// Cognito verifier, which refuses it (no `kid`). Nothing about the Cognito
// path changes.

import jwt from 'jsonwebtoken';

export const ORGIQ_TOKEN_KIND = 'orgiq-hs256';
export const ORGIQ_DEFAULT_ISSUER = 'orgiq-middleware';
export const ORGIQ_DEFAULT_AUDIENCE = 'realtime';

export interface OrgiqPrincipal {
  sub: string;
  email?: string;
  /** The token's `org` — the OrgIQ tenant the user is acting in. */
  orgId: string;
  displayName?: string;
  authKind: typeof ORGIQ_TOKEN_KIND;
}

function secret(): string {
  return (process.env.REALTIME_JWT_SECRET ?? '').trim();
}

/** Is the OrgIQ verifier configured at all? */
export function orgiqTokensEnabled(): boolean {
  return secret().length > 0;
}

/**
 * Does this bearer belong on the OrgIQ path? True when the verifier is
 * configured and the JWT header says HS256. Parsed by hand (no library
 * decode): the header is the only thing that routes, and a malformed one is
 * simply "not ours".
 */
export function isOrgiqCandidate(token: string): boolean {
  if (!orgiqTokensEnabled()) return false;
  const head = token.split('.')[0];
  if (!head) return false;
  try {
    const header = JSON.parse(Buffer.from(head, 'base64url').toString('utf8')) as { alg?: unknown };
    return header.alg === 'HS256';
  } catch {
    return false;
  }
}

/** Verify an OrgIQ realtime token. Throws (TokenExpiredError included) on anything else. */
export function verifyOrgiqToken(token: string): OrgiqPrincipal {
  const s = secret();
  if (!s) throw new Error('REALTIME_JWT_SECRET is not configured');
  const payload = jwt.verify(token, s, {
    algorithms: ['HS256'],
    issuer: (process.env.REALTIME_JWT_ISSUER ?? '').trim() || ORGIQ_DEFAULT_ISSUER,
    audience: (process.env.REALTIME_JWT_AUDIENCE ?? '').trim() || ORGIQ_DEFAULT_AUDIENCE,
  });
  if (typeof payload === 'string') throw new Error('not an orgiq realtime token');
  if (payload.tokenKind !== ORGIQ_TOKEN_KIND) throw new Error('not an orgiq realtime token');
  if (typeof payload.sub !== 'string' || !payload.sub) throw new Error('token has no subject');
  if (typeof payload.org !== 'string' || !payload.org.trim()) throw new Error('token has no org');
  return {
    sub: payload.sub,
    ...(typeof payload.email === 'string' && payload.email ? { email: payload.email } : {}),
    orgId: payload.org.trim(),
    ...(typeof payload.displayName === 'string' && payload.displayName ? { displayName: payload.displayName } : {}),
    authKind: ORGIQ_TOKEN_KIND,
  };
}
