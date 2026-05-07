/**
 * Video Auth — configuration for calling the live-video-streaming service.
 *
 * live-video-streaming uses per-channel streamKey as Bearer auth for
 * participant token minting. Channel creation is unauthenticated in local dev.
 *
 * Env vars:
 *   VIDEO_API_URL — base URL for the live-video-streaming service
 */

/** Base URL for the live-video-streaming service. */
export const VIDEO_API_URL = process.env.VIDEO_API_URL ?? '';
