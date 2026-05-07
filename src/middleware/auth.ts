import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';

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
  // Skip auth in local development
  if (process.env.SKIP_AUTH === 'true') {
    req.user = { sub: 'dev-user', email: 'dev@local' };
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
    req.user = { sub: verified.sub!, email: verified.email as string | undefined };
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
  if (process.env.SKIP_AUTH === 'true') {
    req.user = { sub: 'dev-user', email: 'dev@local' };
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
      req.user = { sub: verified.sub!, email: verified.email as string | undefined };
    }
  } catch {
    // Token invalid — proceed without user
  }
  next();
}
