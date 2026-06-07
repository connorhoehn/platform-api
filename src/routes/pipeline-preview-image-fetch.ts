// platform-api/src/routes/pipeline-preview-image-fetch.ts
//
// Shared SSRF-safe image fetcher for the preview routes
// (`pipeline-preview-adjust-layer`, `pipeline-preview-vision`). Mirrors the
// guard logic in routes/images.ts (`from-url`) — same hostname rules,
// redirect cap, MIME allow-list, and per-call size cap.
//
// Kept local to the `routes/` directory because images.ts does not yet
// export its SSRF helpers; if/when a generic SSRF utility lands in
// `src/lib/`, both call sites should switch over.

import { ValidationError } from '../middleware/error-handler';

const URL_FETCH_TIMEOUT_MS = 5000;
const MAX_REDIRECTS = 5;
const ALLOWED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

const ALLOW_INTERNAL = process.env['ALLOW_INTERNAL_IMAGE_FETCH'] === 'true';

export function isPrivateOrLocalHost(hostname: string): boolean {
  let lower = hostname.toLowerCase();
  if (lower.startsWith('[') && lower.endsWith(']')) lower = lower.slice(1, -1);
  if (lower === 'localhost' || lower === '0.0.0.0') return true;
  if (lower === '127.0.0.1' || lower.startsWith('127.')) return true;
  if (/^10\./.test(lower)) return true;
  if (/^192\.168\./.test(lower)) return true;
  if (/^172\.(1[6-9]|2[0-9]|3[0-1])\./.test(lower)) return true;
  if (/^169\.254\./.test(lower)) return true;
  if (lower === '::1' || lower === '0:0:0:0:0:0:0:1') return true;
  if (lower.startsWith('fe80:') || lower.startsWith('fe80::')) return true;
  if (/^fc[0-9a-f]{2}:/.test(lower) || /^fd[0-9a-f]{2}:/.test(lower)) return true;
  const v4Mapped = /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/.exec(lower);
  if (v4Mapped) return isPrivateOrLocalHost(v4Mapped[1]!);
  if (lower.endsWith('.local') || lower.endsWith('.internal')) return true;
  return false;
}

async function ssrfSafeFetch(
  initialUrl: URL,
  signal: AbortSignal,
): Promise<{ response: Response; finalUrl: URL }> {
  let current = initialUrl;
  for (let hop = 0; hop <= MAX_REDIRECTS; hop++) {
    if (!ALLOW_INTERNAL && isPrivateOrLocalHost(current.hostname)) {
      throw new ValidationError(`refusing to fetch from internal address '${current.hostname}'`);
    }
    if (current.protocol !== 'http:' && current.protocol !== 'https:') {
      throw new ValidationError(`non-http(s) URL: '${current.toString()}'`);
    }
    const resp = await fetch(current.toString(), {
      method: 'GET',
      redirect: 'manual',
      signal,
      headers: { 'User-Agent': 'platform-api/preview-fetch' },
    });
    if (resp.status >= 300 && resp.status < 400) {
      const loc = resp.headers.get('location');
      if (!loc) throw new ValidationError(`${resp.status} redirect with no Location header`);
      try {
        current = new URL(loc, current);
      } catch {
        throw new ValidationError(`invalid Location on redirect: '${loc}'`);
      }
      continue;
    }
    return { response: resp, finalUrl: current };
  }
  throw new ValidationError(`too many redirects (>${MAX_REDIRECTS})`);
}

/** Helper to throw a 413 with a recognizable `.status` field. */
function throw413(msg: string): never {
  const err = new Error(msg) as Error & { status: number };
  err.status = 413;
  throw err;
}

/**
 * Fetch image bytes from a URL with SSRF protection. Validates MIME type
 * and size cap. Caller passes their own cap so each preview route can keep
 * its existing ceiling (currently both 5 MB, but kept as a parameter so a
 * future route can dial it down without forking this helper).
 */
export async function fetchPreviewImageFromUrl(
  rawUrl: string,
  sizeCapBytes: number,
): Promise<{ body: Buffer; mimeType: string }> {
  let parsed: URL;
  try {
    parsed = new URL(rawUrl.trim());
  } catch {
    throw new ValidationError(`invalid imageUrl: '${rawUrl}'`);
  }
  if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
    throw new ValidationError(`imageUrl protocol must be http or https`);
  }
  if (!ALLOW_INTERNAL && isPrivateOrLocalHost(parsed.hostname)) {
    throw new ValidationError(`refusing to fetch from internal address '${parsed.hostname}'`);
  }

  const ctrl = new AbortController();
  const timer = setTimeout(() => ctrl.abort(), URL_FETCH_TIMEOUT_MS);
  let response: Response;

  try {
    const fetched = await ssrfSafeFetch(parsed, ctrl.signal);
    response = fetched.response;
  } catch (err) {
    if (err instanceof ValidationError) throw err;
    throw new ValidationError(`could not fetch imageUrl: ${(err as Error).message}`);
  } finally {
    clearTimeout(timer);
  }

  if (!response.ok) {
    throw new ValidationError(`imageUrl returned ${response.status} ${response.statusText}`);
  }

  const ct = (response.headers.get('content-type') ?? '').split(';')[0]?.trim().toLowerCase() ?? '';
  if (!ALLOWED_MIME.has(ct)) {
    throw new ValidationError(
      `imageUrl returned unsupported Content-Type '${ct}' — must be image/jpeg, image/png, or image/webp`,
    );
  }

  const contentLength = response.headers.get('content-length');
  if (contentLength) {
    const len = parseInt(contentLength, 10);
    if (Number.isFinite(len) && len > sizeCapBytes) {
      throw413(`imageUrl response is ${len} bytes; cap is ${sizeCapBytes}`);
    }
  }

  const arrayBuf = await response.arrayBuffer();
  if (arrayBuf.byteLength === 0) throw new ValidationError('imageUrl returned empty body');
  if (arrayBuf.byteLength > sizeCapBytes) {
    throw413(`imageUrl response is ${arrayBuf.byteLength} bytes; cap is ${sizeCapBytes}`);
  }

  return { body: Buffer.from(arrayBuf), mimeType: ct };
}
