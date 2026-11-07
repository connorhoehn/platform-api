// platform-api/src/lib/lvsServiceAuth.ts
//
// The credential platform-api presents on LVS's session-lifecycle routes
// (`POST /api/sessions`, `/:arn/join`, `/:arn/end`, `/:arn/refresh-token`) —
// the ones LVS gates with `cognito-auth.js`.
//
// Before: platform-api forwarded the CALLER's bearer, so LVS verified a user
// token (and a Cognito token could mint a publisher token under any body userId).
//
// Now: with `VIDEO_SERVICE_TOKEN` set, platform-api calls LVS as itself —
// `Authorization: Bearer <VIDEO_SERVICE_TOKEN>`, which LVS accepts when its
// `LVS_SERVICE_TOKEN` holds the same value (constant-time compare) — and the
// user bearer never leaves platform-api. LVS takes identity from the request
// body (`creatorId`, `userId`), which platform-api fills from its own verified
// `req.user`, never from the client.
//
// Unset → fail closed: LVS (3a36786) accepts only its LVS_SERVICE_TOKEN on
// these routes, so forwarding a user bearer can never succeed. Every call
// refuses with a 503 naming the variable BEFORE any HTTP request is made.

import { AppError } from '../middleware/error-handler';

/** Is platform-api configured to call LVS as a service? */
export function lvsServiceTokenConfigured(): boolean {
  return (process.env.VIDEO_SERVICE_TOKEN ?? '').trim().length > 0;
}

/**
 * The Authorization header for an LVS session-lifecycle call. Throws a 503
 * AppError when VIDEO_SERVICE_TOKEN is unset or blank: there is no default
 * and no fallback to the caller's bearer.
 */
export function lvsSessionAuthHeader(): string {
  const service = (process.env.VIDEO_SERVICE_TOKEN ?? '').trim();
  if (!service) {
    throw new AppError(503, 'VIDEO_SERVICE_TOKEN is not set: platform-api cannot call the video service session routes (LVS needs the same value as LVS_SERVICE_TOKEN)');
  }
  return `Bearer ${service}`;
}
