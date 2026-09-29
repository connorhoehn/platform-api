// platform-api/src/lib/lvsServiceAuth.ts
//
// The credential platform-api presents on LVS's session-lifecycle routes
// (`POST /api/sessions`, `/:arn/join`, `/:arn/end`, `/:arn/refresh-token`) —
// the ones LVS gates with `cognito-auth.js`.
//
// Before: platform-api forwarded the CALLER's bearer, so LVS had to verify the
// same user token platform-api had just verified. That ties LVS to one
// identity provider and hands every user token to a second service; an OrgIQ
// realtime token (HS256, lib/orgiqRealtimeToken.ts) is not a Cognito token,
// so LVS refused it outright.
//
// Now: with `VIDEO_SERVICE_TOKEN` set, platform-api calls LVS as itself —
// `Authorization: Bearer <VIDEO_SERVICE_TOKEN>`, which LVS accepts when its
// `LVS_SERVICE_TOKEN` holds the same value (constant-time compare) — and the
// user bearer never leaves platform-api. LVS takes identity from the request
// body (`creatorId`, `userId`), which platform-api fills from its own verified
// `req.user`, never from the client.
//
// Unset → the legacy forward of the caller's bearer (realtime-examples' Tilt
// and CDK still run that way), logged once so the choice is visible.

import { withContext } from './logger';

const log = withContext({ component: 'lvs-service-auth' });
let warnedLegacy = false;

/** Is platform-api configured to call LVS as a service? */
export function lvsServiceTokenConfigured(): boolean {
  return (process.env.VIDEO_SERVICE_TOKEN ?? '').trim().length > 0;
}

/**
 * The Authorization header for an LVS session-lifecycle call: the service
 * token when configured, otherwise the caller's own header (legacy; may be
 * undefined, e.g. an unauthenticated sendBeacon /end).
 */
export function lvsSessionAuthHeader(callerAuthHeader: string | undefined): string | undefined {
  const service = (process.env.VIDEO_SERVICE_TOKEN ?? '').trim();
  if (service) return `Bearer ${service}`;
  if (!warnedLegacy) {
    warnedLegacy = true;
    log.warn('VIDEO_SERVICE_TOKEN is unset — forwarding the caller\'s bearer to LVS session routes (legacy). Set it (and LVS_SERVICE_TOKEN on the SFU) so no user token reaches the SFU.');
  }
  return callerAuthHeader;
}
