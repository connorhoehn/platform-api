import type { RequestHandler } from 'express';

/**
 * Response headers for every reply. Hand-set rather than helmet: the set is
 * small and this service serves JSON only, so the CSP is the locked-down
 * `default-src 'none'` (it also covers Express's HTML 404 page).
 * HSTS is sent only over TLS: `req.secure`, or `x-forwarded-proto: https`
 * when `TRUST_PROXY` is on.
 */
export function securityHeaders(): RequestHandler {
  return (req, res, next) => {
    res.removeHeader('X-Powered-By');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'strict-origin-when-cross-origin');
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('Content-Security-Policy', "default-src 'none'; frame-ancestors 'none'");
    res.setHeader('Permissions-Policy', 'camera=(), microphone=(), geolocation=()');
    res.setHeader('Cross-Origin-Resource-Policy', 'same-site');
    const trusted = /^(1|true|yes)$/i.test(process.env.TRUST_PROXY ?? '');
    const forwardedHttps = trusted && String(req.headers['x-forwarded-proto'] ?? '').split(',')[0].trim() === 'https';
    if (req.secure || forwardedHttps) {
      res.setHeader('Strict-Transport-Security', 'max-age=15552000; includeSubDomains');
    }
    next();
  };
}
