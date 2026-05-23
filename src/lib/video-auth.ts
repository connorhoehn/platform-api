/**
 * Video Auth — configuration for calling the live-video-streaming service.
 *
 * live-video-streaming uses per-channel streamKey as Bearer auth for
 * participant token minting. Channel creation is unauthenticated in local dev.
 *
 * Env vars:
 *   VIDEO_API_URL — base URL for the live-video-streaming service
 */

/** Base URL for the live-video-streaming service — used server-to-server
 *  from platform-api to LVS. In-cluster this is `http://lvs-sfu:3001`
 *  (Kubernetes service DNS), which is NOT browser-reachable. */
export const VIDEO_API_URL = process.env.VIDEO_API_URL ?? '';

/** Browser-reachable base URL for LVS — used in any URL platform-api
 *  hands back to the FE (e.g. recording playlist URLs). In Tilt local
 *  dev this is `http://localhost:3901` (the host-side port-forward to
 *  the lvs-sfu service). Falls back to VIDEO_API_URL when unset, which
 *  works only if VIDEO_API_URL itself happens to be browser-reachable
 *  (e.g. an ingress hostname in prod). */
export const VIDEO_PUBLIC_URL = process.env.VIDEO_PUBLIC_URL ?? process.env.VIDEO_API_URL ?? '';
