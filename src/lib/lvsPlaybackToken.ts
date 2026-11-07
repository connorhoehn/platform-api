// Playback tokens for LVS recording media. Since LVS 3a36786 the recording
// segment, composed MP4, transcript and thumbnail GETs need a credential for
// the channel; a browser <video src> / <img src> can only send it as
// `?token=<playback token>`. The token is minted by LVS
// (POST /api/channels/:arn/playback-tokens, admin bearer) and is scoped to
// one channel arn. It is a bearer secret: never log a URL that carries it.

import { VIDEO_API_URL } from './video-auth';
import { callSfu } from './sfu-breaker';
import { lvsAdminAuthHeader } from './lvsAdminToken';
import log from './logger';

export interface LvsPlaybackToken { token: string; expiresAt: string }

/** Mint a playback token for one channel arn; null (and a warning without the token) when LVS cannot. */
export async function mintLvsPlaybackToken(channelArn: string, ttlSeconds = 3600): Promise<LvsPlaybackToken | null> {
  try {
    const res = await callSfu(`${VIDEO_API_URL}/api/channels/${encodeURIComponent(channelArn)}/playback-tokens`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...lvsAdminAuthHeader(['VIDEO_ADMIN_TOKEN']) },
      body: JSON.stringify({ ttlSeconds }),
    });
    if (!res.ok) {
      log.warn({ status: res.status, channelArn }, '[lvs] playback token mint refused — media URLs go out without a token');
      return null;
    }
    const body = (await res.json()) as { token?: string; expiresAt?: string };
    return body.token ? { token: body.token, expiresAt: body.expiresAt ?? '' } : null;
  } catch (e: any) {
    log.warn({ err: e?.message ?? String(e), channelArn }, '[lvs] playback token mint failed — media URLs go out without a token');
    return null;
  }
}

/** Append `?token=` (or `&token=`) to an LVS media URL; the url unchanged when there is no token. */
export function withPlaybackToken<T extends string | null | undefined>(url: T, token?: string | null): T {
  if (!url || !token) return url;
  return `${url}${url.includes('?') ? '&' : '?'}token=${encodeURIComponent(token)}` as T;
}
