import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { readOrgClaim } from '../lib/org-claim';
import { withContext } from '../lib/logger';

// The SKIP_AUTH dev fallback: a request with no usable bearer runs as
// `dev-user` with no organization. That identity owns nothing and projects
// nowhere (NFR #72 hid behind it), so every use is logged — at most once a
// minute per route, with a count of the ones in between.
const authLog = withContext({ component: 'auth' });
const DEV_FALLBACK_LOG_MS = 60_000;
const devFallbackSeen = new Map<string, { at: number; suppressed: number }>();
export function devUserFallback(req: Request): { sub: string; email: string } {
  const route = `${req.method} ${(req.baseUrl || '') + (req.path || '')}`.replace(/\/[0-9a-f-]{16,}/gi, '/:id');
  const now = Date.now();
  const seen = devFallbackSeen.get(route);
  if (!seen || now - seen.at >= DEV_FALLBACK_LOG_MS) {
    authLog.warn({ route, suppressed: seen?.suppressed ?? 0 }, 'SKIP_AUTH dev fallback: no bearer, request runs as dev-user with no organization');
    devFallbackSeen.set(route, { at: now, suppressed: 0 });
  } else {
    seen.suppressed += 1;
  }
  return { sub: 'dev-user', email: 'dev@local' };
}

/**
 * Refuses a request that reached a handler with no principal. `requireAuth`
 * always sets one (a verified Cognito user, or — only with SKIP_AUTH=true
 * outside production — the logged `dev-user` fallback), so this fires only
 * when a route is mounted outside it. Handlers behind it read `req.user.sub`
 * directly instead of defaulting to `'dev-user'` silently.
 */
export function requirePrincipal(req: Request, res: Response, next: NextFunction): void {
  if (req.user?.sub) { next(); return; }
  authLog.warn({ route: `${req.method} ${(req.baseUrl || '') + (req.path || '')}` }, 'request reached a handler with no principal; refused');
  res.status(401).json({ error: 'Authorization required' });
}

const region = process.env.COGNITO_REGION!;
const userPoolId = process.env.COGNITO_USER_POOL_ID!;
const issuer = `https://cognito-idp.${region}.amazonaws.com/${userPoolId}`;

const client = jwksClient({
  jwksUri: `https://cognito-idp.${region}.amazonaws.com/${userPoolId}/.well-known/jwks.json`,
  cache: true,
  cacheMaxAge: 3600000,
  rateLimit: true,
  jwksRequestsPerMinute: 10,
});

async function getPublicKey(kid: string): Promise<string> {
  const key = await client.getSigningKey(kid);
  return key.getPublicKey();
}

export async function requireAuth(
  req: Request,
  res: Response,
  next: NextFunction
): Promise<void> {
  // This value is attached only after the document policy verifies signature,
  // current session epoch, tenant, document and operation on this request.
  if (req.documentGrant) { next(); return; }
  // A gateway call already verified by serviceOnBehalfOf (NFR #72).
  if (req.serviceActor && req.user) { next(); return; }
  // Skip auth in local development. We still try to extract the user
  // identity from the bearer when present — the FE's useAuth ships a
  // signed dev token whose `sub` is the actual dev userId (e.g. dev-bob
  // when running with `?as=bob`). Falling back to 'dev-user' for every
  // request broke per-user fan-outs (every Bob/Alice/Carol call ended
  // up keyed under 'dev-user' instead of their real identity, so
  // listByUser returned zero matches for new recordings).
  if (process.env.SKIP_AUTH === 'true' && process.env.NODE_ENV !== 'production') {
    const authHeader = req.headers.authorization;
    if (authHeader?.startsWith('Bearer ')) {
      const token = authHeader.slice(7);
      try {
        const decoded = jwt.decode(token) as jwt.JwtPayload | null;
        if (decoded && typeof decoded.sub === 'string' && decoded.sub.length > 0) {
          req.user = { sub: decoded.sub, email: (decoded.email as string | undefined) ?? 'dev@local', orgId: readOrgClaim(decoded) };
          next();
          return;
        }
      } catch {
        /* fall through to generic dev-user */
      }
    }
    req.user = devUserFallback(req);
    next();
    return;
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    res.status(401).json({ error: 'Authorization required' });
    return;
  }
  const token = authHeader.slice(7);
  try {
    const decoded = jwt.decode(token, { complete: true });
    if (!decoded || typeof decoded === 'string' || !decoded.header.kid) {
      res.status(401).json({ error: 'Invalid token' });
      return;
    }
    const publicKey = await getPublicKey(decoded.header.kid);
    const verified = jwt.verify(token, publicKey, {
      algorithms: ['RS256'],
      issuer,
    }) as jwt.JwtPayload;
    req.user = { sub: verified.sub!, email: verified.email as string | undefined, orgId: readOrgClaim(verified) };
    next();
  } catch (err) {
    const message = err instanceof Error && err.name === 'TokenExpiredError'
      ? 'Token expired'
      : 'Invalid token';
    res.status(401).json({ error: message });
  }
}

/**
 * Optional auth — sets req.user if a valid token is present, but allows
 * unauthenticated requests through (for sendBeacon cleanup calls).
 */
export async function optionalAuth(
  req: Request,
  _res: Response,
  next: NextFunction
): Promise<void> {
  // This value is attached only after the document policy verifies signature,
  // current session epoch, tenant, document and operation on this request.
  if (req.documentGrant) { next(); return; }
  if (process.env.SKIP_AUTH === 'true' && process.env.NODE_ENV !== 'production') {
    const authHeader = req.headers.authorization;
    if (authHeader?.startsWith('Bearer ')) {
      const token = authHeader.slice(7);
      try {
        const decoded = jwt.decode(token) as jwt.JwtPayload | null;
        if (decoded && typeof decoded.sub === 'string' && decoded.sub.length > 0) {
          req.user = { sub: decoded.sub, email: (decoded.email as string | undefined) ?? 'dev@local', orgId: readOrgClaim(decoded) };
          next();
          return;
        }
      } catch {
        /* fall through */
      }
    }
    req.user = devUserFallback(req);
    next();
    return;
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || !authHeader.startsWith('Bearer ')) {
    next();
    return;
  }
  const token = authHeader.slice(7);
  try {
    const decoded = jwt.decode(token, { complete: true });
    if (decoded && typeof decoded !== 'string' && decoded.header.kid) {
      const publicKey = await getPublicKey(decoded.header.kid);
      const verified = jwt.verify(token, publicKey, {
        algorithms: ['RS256'],
        issuer,
      }) as jwt.JwtPayload;
      req.user = { sub: verified.sub!, email: verified.email as string | undefined, orgId: readOrgClaim(verified) };
    }
  } catch {
    // Token invalid — proceed without user
  }
  next();
}
