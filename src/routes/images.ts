// platform-api/src/routes/images.ts
//
// HTTP surface for the image:vision-analyze step's data plane.
//
// Endpoints:
//   POST /api/images
//     Body: raw image bytes (up to 5 MB before resize)
//     Content-Type: image/jpeg | image/png | image/webp
//     Server-side: re-encode + EXIF strip + dimension extract via Sharp;
//                  reject if final bytes exceed IMAGE_BLOB_SIZE_CAP_BYTES.
//     Response: { imageId, mimeType, width, height, sizeBytes }
//
//   GET /api/images/:imageId
//     Returns raw bytes with Content-Type from metadata.
//     404 when not found, 500 on store failure.
//
//   GET /api/images/:imageId/metadata
//     Returns { imageId, mimeType, sizeBytes, width, height, uploadedAt }
//     Cheaper than the body fetch — for canvas thumbnails / listings.
//
// Mounted from src/routes/index.ts under `/api/images`. Auth is
// applied by the `requireAuth` middleware in app.ts (mounted before
// the `/api/*` router) — no per-route auth logic here.
//
// Code parity with platform-api/src/routes/judgments.ts.

import { insecureDevMode } from '../lib/insecureDevMode';
import { Router, raw, json as jsonBody } from 'express';
import { randomUUID } from 'crypto';
import sharp from 'sharp';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import { getImageBlobStore } from '../pipeline/image/visionServices';
import { IMAGE_BLOB_SIZE_CAP_BYTES } from '../pipeline/image/ImageBlobStore';
import type { ImageMeta } from '../pipeline/image/types';
import { requireImageAccess } from '../pipeline/image/imageAccess';

const log = withContext({ route: 'images' });

/** Raw-body cap BEFORE server-side resize. Larger than the storage cap
 *  to give the resize step room to bring a 4MP phone photo down. */
const RAW_UPLOAD_CAP_BYTES = 5 * 1024 * 1024;

/** Long-edge cap for server-side resize. Matches Anthropic's
 *  `width * height / 750` token-cost sweet spot. */
const RESIZE_LONG_EDGE = 1568;

/** MIME types we accept on the upload route. */
const ALLOWED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

interface UploadResponse {
  imageId: string;
  mimeType: string;
  sizeBytes: number;
  width: number;
  height: number;
}

interface MetadataResponse extends ImageMeta {
  imageId: string;
}

export const imagesRouter = Router({ mergeParams: true });

// ---------------------------------------------------------------------------
// Shared: process + persist a raw image buffer. Extracted from POST / so
// the URL-fetch path goes through the exact same Sharp pipeline + storage
// flow (EXIF strip, polyglot defang, dimension extract, size cap).
//
// Returns the constructed UploadResponse on success. Caller is responsible
// for surfacing 4xx/5xx — this helper throws ValidationError for bad input
// and Error for storage failures.
// ---------------------------------------------------------------------------

interface ProcessOpts {
  userId: string;
  /** Free-form context for the log line (where the bytes came from). */
  source: 'upload' | 'url';
  /** When source='url', the URL the bytes were fetched from. */
  sourceUrl?: string;
}

async function processAndPersistImage(
  body: Buffer,
  contentType: string,
  opts: ProcessOpts,
): Promise<UploadResponse> {
  const reqLog = log.child({
    userId: opts.userId,
    source: opts.source,
    contentType,
    inputBytes: body.byteLength,
    ...(opts.sourceUrl ? { sourceUrl: opts.sourceUrl } : {}),
  });

  let processed: Buffer;
  let processedInfo: sharp.OutputInfo;
  try {
    // Single-pass: toBuffer({ resolveWithObject: true }) returns both
    // the encoded bytes AND the output dimensions in one Sharp decode.
    // Previously we re-decoded the output with sharp(processed).metadata()
    // — a second full decode for no gain.
    const pipeline = sharp(body, { limitInputPixels: 25_000_000 })
      .rotate()
      .resize({
        width: RESIZE_LONG_EDGE,
        height: RESIZE_LONG_EDGE,
        fit: 'inside',
        withoutEnlargement: true,
      });
    let result: { data: Buffer; info: sharp.OutputInfo };
    if (contentType === 'image/jpeg') {
      result = await pipeline.jpeg({ quality: 85, mozjpeg: true }).toBuffer({ resolveWithObject: true });
    } else if (contentType === 'image/png') {
      result = await pipeline.png({ compressionLevel: 8 }).toBuffer({ resolveWithObject: true });
    } else {
      result = await pipeline.webp({ quality: 85 }).toBuffer({ resolveWithObject: true });
    }
    processed = result.data;
    processedInfo = result.info;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    reqLog.warn({ err: msg }, 'image decode/re-encode failed');
    throw new ValidationError(`could not decode image: ${msg}`);
  }

  if (processed.byteLength > IMAGE_BLOB_SIZE_CAP_BYTES) {
    reqLog.warn(
      { processedBytes: processed.byteLength, cap: IMAGE_BLOB_SIZE_CAP_BYTES },
      'image exceeds storage cap after resize',
    );
    const err = new Error(
      `image still exceeds storage cap (${IMAGE_BLOB_SIZE_CAP_BYTES} bytes) after server-side resize. Pre-compress on the client.`,
    );
    (err as Error & { status?: number }).status = 413;
    throw err;
  }

  const imageId = randomUUID();
  const meta: ImageMeta = {
    mimeType: contentType,
    sizeBytes: processed.byteLength,
    width: processedInfo.width ?? 0,
    height: processedInfo.height ?? 0,
    uploadedAt: new Date().toISOString(),
    uploadedBy: opts.userId,
  };

  await getImageBlobStore().put(imageId, processed, meta);

  reqLog.info(
    { imageId, outputBytes: processed.byteLength, width: meta.width, height: meta.height },
    'image stored',
  );
  return {
    imageId,
    mimeType: meta.mimeType,
    sizeBytes: meta.sizeBytes,
    width: meta.width,
    height: meta.height,
  };
}

// ---------------------------------------------------------------------------
// POST /api/images — raw-body upload
// ---------------------------------------------------------------------------

imagesRouter.post(
  '/',
  raw({ type: () => true, limit: RAW_UPLOAD_CAP_BYTES }),
  asyncHandler<
    Record<string, never>,
    UploadResponse | { error: string }
  >(async (req, res) => {
    const userId = req.user?.sub ?? 'anonymous';
    const contentType = (req.headers['content-type'] ?? '').split(';')[0]?.trim().toLowerCase();
    if (!contentType || !ALLOWED_MIME.has(contentType)) {
      throw new ValidationError(`Content-Type must be one of ${[...ALLOWED_MIME].join(', ')} — got '${contentType ?? '(none)'}'`);
    }
    const body = req.body;
    if (!Buffer.isBuffer(body) || body.byteLength === 0) {
      throw new ValidationError('request body is empty or not a Buffer (use Content-Type: image/* + raw bytes)');
    }

    try {
      const result = await processAndPersistImage(body, contentType, { userId, source: 'upload' });
      res.status(201).json(result);
    } catch (err) {
      const status = (err as Error & { status?: number }).status;
      const msg = err instanceof Error ? err.message : String(err);
      if (status === 413) {
        res.status(413).json({ error: msg });
        return;
      }
      throw err;
    }
  }),
);

// ---------------------------------------------------------------------------
// POST /api/images/from-url — server-side fetch by URL
//
// Body: { url: string }
//
// Server fetches the URL with a 5s timeout, validates the response
// Content-Type against the same ALLOWED_MIME whitelist, caps the body
// at RAW_UPLOAD_CAP_BYTES, then runs the same Sharp pipeline as the
// raw-upload route. Returns the same UploadResponse shape so callers
// can treat both paths identically.
//
// Network safety: we do NOT auto-follow to private/loopback addresses
// (basic SSRF mitigation). When VITE_PLATFORM_API_URL is configured to
// trust a specific egress proxy, set ALLOW_INTERNAL_IMAGE_FETCH=true
// to bypass — used in tests + dev. Production should keep it false.
// ---------------------------------------------------------------------------

const ALLOW_INTERNAL_IMAGE_FETCH = process.env['ALLOW_INTERNAL_IMAGE_FETCH'] === 'true' && insecureDevMode();
const URL_FETCH_TIMEOUT_MS = 5000;

/**
 * Reject hostnames that resolve to private / loopback / link-local /
 * cloud-metadata addresses. Covers IPv4 + IPv6 + IPv4-mapped IPv6.
 *
 * Hardened after audit wave 9: previous version missed IPv6 link-local
 * (fe80::/10), ULA (fc00::/7), and IPv4-mapped IPv6 (::ffff:10.0.0.1).
 * Strip IPv6 brackets up front since Node URL.hostname preserves them.
 */
function isPrivateOrLocalHost(hostname: string): boolean {
  let lower = hostname.toLowerCase();
  if (lower.startsWith('[') && lower.endsWith(']')) {
    lower = lower.slice(1, -1);
  }
  // Common short forms
  if (lower === 'localhost' || lower === '0.0.0.0') return true;
  // IPv4 loopback + RFC1918 + link-local
  if (lower === '127.0.0.1' || lower.startsWith('127.')) return true;
  if (/^10\./.test(lower)) return true;
  if (/^192\.168\./.test(lower)) return true;
  if (/^172\.(1[6-9]|2[0-9]|3[0-1])\./.test(lower)) return true;
  if (/^169\.254\./.test(lower)) return true;
  // IPv6 loopback + link-local + ULA
  if (lower === '::1' || lower === '0:0:0:0:0:0:0:1') return true;
  if (lower.startsWith('fe80:') || lower.startsWith('fe80::')) return true;
  if (/^fc[0-9a-f]{2}:/.test(lower) || /^fd[0-9a-f]{2}:/.test(lower)) return true;
  // IPv4-mapped IPv6 — re-check embedded IPv4 against same private ranges.
  const v4Mapped = /^::ffff:(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})$/.exec(lower);
  if (v4Mapped) return isPrivateOrLocalHost(v4Mapped[1]!);
  // WHATWG URL canonicalizes ::ffff:10.0.0.1 to ::ffff:a00:1 before
  // hostname reaches this guard. Decode that form before checking IPv4.
  const v4MappedHex = /^::ffff:([0-9a-f]{1,4}):([0-9a-f]{1,4})$/.exec(lower);
  if (v4MappedHex) {
    const high = parseInt(v4MappedHex[1]!, 16), low = parseInt(v4MappedHex[2]!, 16);
    return isPrivateOrLocalHost([high >>> 8, high & 255, low >>> 8, low & 255].join('.'));
  }
  // .local / .internal mDNS + intra-cluster
  if (lower.endsWith('.local') || lower.endsWith('.internal')) return true;
  return false;
}

/**
 * Bounded redirect-following fetch with re-validation at every hop.
 * The native `fetch(..., { redirect: 'follow' })` would happily follow
 * a 301 from a public URL to a private one — bypasses our SSRF check.
 *
 * We follow up to MAX_REDIRECTS hops; at each hop we re-run the
 * private-host check + protocol whitelist. Returns the final Response
 * along with the resolved final URL (for logging).
 */
async function ssrfSafeFetch(
  initialUrl: URL,
  opts: { signal: AbortSignal; allowInternal: boolean; maxRedirects?: number; userAgent?: string },
): Promise<{ response: Response; finalUrl: URL }> {
  const maxRedirects = opts.maxRedirects ?? 5;
  let currentUrl = initialUrl;
  for (let hop = 0; hop <= maxRedirects; hop += 1) {
    if (!opts.allowInternal && isPrivateOrLocalHost(currentUrl.hostname)) {
      throw new ValidationError(
        `refusing to fetch from internal address '${currentUrl.hostname}'${hop > 0 ? ` (reached via redirect from ${initialUrl.toString()})` : ''}`,
      );
    }
    if (currentUrl.protocol !== 'http:' && currentUrl.protocol !== 'https:') {
      throw new ValidationError(`redirect to non-http(s) URL '${currentUrl.toString()}'`);
    }
    const resp = await fetch(currentUrl.toString(), {
      method: 'GET',
      redirect: 'manual',
      signal: opts.signal,
      headers: { 'User-Agent': opts.userAgent ?? 'platform-api/image-fetch' },
    });
    if (resp.status >= 300 && resp.status < 400) {
      const loc = resp.headers.get('location');
      if (!loc) {
        throw new ValidationError(`${resp.status} redirect with no Location header`);
      }
      let next: URL;
      try {
        next = new URL(loc, currentUrl);
      } catch {
        throw new ValidationError(`invalid Location header on redirect: '${loc}'`);
      }
      currentUrl = next;
      continue;
    }
    return { response: resp, finalUrl: currentUrl };
  }
  throw new ValidationError(`too many redirects (>${maxRedirects})`);
}

imagesRouter.post(
  '/from-url',
  jsonBody({ limit: '4kb' }),
  asyncHandler<
    Record<string, never>,
    UploadResponse | { error: string }
  >(async (req, res) => {
    const userId = req.user?.sub ?? 'anonymous';
    const body = req.body as { url?: unknown } | undefined;
    const rawUrl = body?.url;
    if (typeof rawUrl !== 'string' || rawUrl.trim().length === 0) {
      throw new ValidationError('body must be { "url": string }');
    }

    let parsed: URL;
    try {
      parsed = new URL(rawUrl.trim());
    } catch {
      throw new ValidationError(`invalid URL: '${rawUrl}'`);
    }
    if (parsed.protocol !== 'https:' && parsed.protocol !== 'http:') {
      throw new ValidationError(`URL protocol must be http or https — got '${parsed.protocol}'`);
    }
    if (!ALLOW_INTERNAL_IMAGE_FETCH && isPrivateOrLocalHost(parsed.hostname)) {
      throw new ValidationError(`refusing to fetch from internal address '${parsed.hostname}' (set ALLOW_INTERNAL_IMAGE_FETCH=true to override)`);
    }

    const reqLog = log.child({ userId, sourceUrl: parsed.toString() });
    reqLog.info('fetching image by URL');

    const ctrl = new AbortController();
    const timer = setTimeout(() => ctrl.abort(), URL_FETCH_TIMEOUT_MS);
    let response: Response;
    let finalUrl: URL;
    try {
      const fetched = await ssrfSafeFetch(parsed, {
        signal: ctrl.signal,
        allowInternal: ALLOW_INTERNAL_IMAGE_FETCH,
      });
      response = fetched.response;
      finalUrl = fetched.finalUrl;
    } catch (err) {
      if (err instanceof ValidationError) throw err;
      const msg = err instanceof Error ? err.message : String(err);
      reqLog.warn({ err: msg }, 'image URL fetch failed');
      throw new ValidationError(`could not fetch URL: ${msg}`);
    } finally {
      clearTimeout(timer);
    }

    if (!response.ok) {
      throw new ValidationError(`URL returned ${response.status} ${response.statusText}`);
    }
    if (finalUrl.toString() !== parsed.toString()) {
      reqLog.info({ finalUrl: finalUrl.toString() }, 'image URL followed redirects');
    }

    const respMime = (response.headers.get('content-type') ?? '').split(';')[0]?.trim().toLowerCase() ?? '';
    if (!ALLOWED_MIME.has(respMime)) {
      throw new ValidationError(`URL returned unsupported Content-Type '${respMime}' — must be one of ${[...ALLOWED_MIME].join(', ')}`);
    }

    const contentLengthHeader = response.headers.get('content-length');
    if (contentLengthHeader) {
      const declared = Number.parseInt(contentLengthHeader, 10);
      if (Number.isFinite(declared) && declared > RAW_UPLOAD_CAP_BYTES) {
        throw new ValidationError(`URL response is ${declared}B; cap is ${RAW_UPLOAD_CAP_BYTES}B`);
      }
    }

    const arrayBuf = await response.arrayBuffer();
    if (arrayBuf.byteLength === 0) {
      throw new ValidationError('URL returned empty body');
    }
    if (arrayBuf.byteLength > RAW_UPLOAD_CAP_BYTES) {
      throw new ValidationError(`URL response is ${arrayBuf.byteLength}B; cap is ${RAW_UPLOAD_CAP_BYTES}B`);
    }
    const buf = Buffer.from(arrayBuf);

    try {
      const result = await processAndPersistImage(buf, respMime, {
        userId,
        source: 'url',
        sourceUrl: parsed.toString(),
      });
      res.status(201).json(result);
    } catch (err) {
      const status = (err as Error & { status?: number }).status;
      const msg = err instanceof Error ? err.message : String(err);
      if (status === 413) {
        res.status(413).json({ error: msg });
        return;
      }
      throw err;
    }
  }),
);

// ---------------------------------------------------------------------------
// GET /api/images/:imageId — raw bytes
// ---------------------------------------------------------------------------

imagesRouter.get(
  '/:imageId',
  // Loop 29c (NFR #174): an uploaded image is its uploader's (404 otherwise).
  requireImageAccess(),
  asyncHandler<
    { imageId: string },
    Buffer | { error: string }
  >(async (req, res) => {
    const { imageId } = req.params;
    if (!imageId) {
      throw new ValidationError('imageId is required');
    }
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, imageId });

    const row = await getImageBlobStore().get(imageId);
    if (!row) {
      reqLog.info('image not found');
      res.status(404).json({ error: 'image not found' });
      return;
    }

    res.status(200)
      .setHeader('Content-Type', row.meta.mimeType)
      .setHeader('Content-Length', String(row.meta.sizeBytes))
      .setHeader('Cache-Control', 'private, max-age=3600')
      .send(row.body);
  }),
);

// ---------------------------------------------------------------------------
// GET /api/images/:imageId/metadata — JSON metadata only
// ---------------------------------------------------------------------------

imagesRouter.get(
  '/:imageId/metadata',
  // Loop 29c (NFR #174): an uploaded image is its uploader's (404 otherwise).
  requireImageAccess(),
  asyncHandler<
    { imageId: string },
    MetadataResponse | { error: string }
  >(async (req, res) => {
    const { imageId } = req.params;
    if (!imageId) {
      throw new ValidationError('imageId is required');
    }
    const userId = req.user?.sub ?? 'anonymous';
    const reqLog = log.child({ userId, imageId });

    const meta = await getImageBlobStore().getMetadata(imageId);
    if (!meta) {
      reqLog.info('image metadata not found');
      res.status(404).json({ error: 'image not found' });
      return;
    }

    res.status(200).json({ imageId, ...meta });
  }),
);
