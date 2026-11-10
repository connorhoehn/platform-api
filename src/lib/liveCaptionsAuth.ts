// platform-api/src/lib/liveCaptionsAuth.ts
//
// The credential platform-api presents to the live-captions sidecar
// (live-video-streaming/services/live-captions). The sidecar requires
// `X-Live-Captions-Secret: <LIVE_CAPTIONS_SHARED_SECRET>` on every POST
// (`/pcm`, `/dictate/*`) and answers 401 without it; `GET /healthz` is open.
//
// platform-api reads the same value from its own `LIVE_CAPTIONS_SHARED_SECRET`.
// Unset → no header, and one warning on first use: the sidecar accepts that
// only when it runs with INSECURE_DEV_MODE=true and no secret of its own.
// The secret itself is never logged.

import log from './logger';

export const LIVE_CAPTIONS_SECRET_HEADER = 'X-Live-Captions-Secret';

let warned = false;

/** `{ 'X-Live-Captions-Secret': <secret> }`, or `{}` when no secret is configured. */
export function liveCaptionsAuthHeaders(): Record<string, string> {
  const secret = (process.env.LIVE_CAPTIONS_SHARED_SECRET ?? '').trim();
  if (secret) return { [LIVE_CAPTIONS_SECRET_HEADER]: secret };
  if (!warned) {
    warned = true;
    log.warn(
      { event: 'live_captions.secret_unset' },
      '[live-captions] LIVE_CAPTIONS_SHARED_SECRET is not set: sidecar POSTs go without X-Live-Captions-Secret, which only a sidecar in INSECURE_DEV_MODE with no secret accepts (set it to the sidecar\'s LIVE_CAPTIONS_SHARED_SECRET)',
    );
  }
  return {};
}

/** Test seam: the warning is once per process. */
export function __resetLiveCaptionsAuthWarningForTests(): void {
  warned = false;
}
