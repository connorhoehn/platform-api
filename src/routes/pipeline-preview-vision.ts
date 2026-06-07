// platform-api/src/routes/pipeline-preview-vision.ts
//
// POST /api/pipeline/preview/vision
//
// Bench preview for the vision prompt editor. Calls VisionClient.analyze()
// with caller-supplied prompt / schema against a caller-supplied image URL.
//
// Differences from the full VisionStepHandler path:
//   - No VisionResultRepository persistence — result is in-response only.
//   - No pipeline:image:vision:* event emission.
//   - Image is fetched server-side via the same SSRF-safe path used by
//     /api/images/from-url (reuse the same isPrivateOrLocalHost guard).
//   - Cost is computed and returned so the bench panel can show a running
//     tally, but no audit log is written.
//   - Rate limit: 10 calls/min per authed user (vision calls are expensive;
//     client-side submit button guards the happy path).
//
// Auth: requireAuth.

import { Router } from 'express';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { createRateLimiter } from '../middleware/rateLimit';
import { requireAuth } from '../middleware/auth';
import { getImageBlobStore, getVisionClient } from '../pipeline/image/visionServices';
import { computeVisionCost } from '../pipeline/image/computeVisionCost';
import { randomUUID } from 'node:crypto';
import type { ImageMeta } from '../pipeline/image/types';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Hard cap on the raw image bytes we'll pass to the vision client. */
const INPUT_SIZE_CAP_BYTES = 5 * 1024 * 1024;

const DEFAULT_MODEL = 'claude-sonnet-4-6';
const DEFAULT_MAX_OUTPUT_TOKENS = 2048;

const URL_FETCH_TIMEOUT_MS = 5000;
const MAX_REDIRECTS = 5;
const ALLOWED_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

// ---------------------------------------------------------------------------
// Rate limiter — 10 calls/min per authed user.
// ---------------------------------------------------------------------------

const previewVisionRateLimit = createRateLimiter({
  capacity: 10,
  refillRate: 10,
  refillIntervalMs: 60_000,
  scope: 'pipeline:preview:vision',
  key: (req) => req.user?.sub ?? req.ip ?? 'anon',
});

// ---------------------------------------------------------------------------
// SSRF-safe fetch helpers (mirror of routes/images.ts — same logic, no
// shared helper yet since images.ts is not exported as a module).
// ---------------------------------------------------------------------------

function isPrivateOrLocalHost(hostname: string): boolean {
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

const ALLOW_INTERNAL = process.env['ALLOW_INTERNAL_IMAGE_FETCH'] === 'true';

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
      headers: { 'User-Agent': 'platform-api/vision-preview' },
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

/**
 * Fetch image bytes from a URL with SSRF protection. Validates MIME type
 * and size cap. Returns { body, mimeType }.
 */
async function fetchImageFromUrl(
  rawUrl: string,
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
  let finalUrl: URL;

  try {
    const fetched = await ssrfSafeFetch(parsed, ctrl.signal);
    response = fetched.response;
    finalUrl = fetched.finalUrl;
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
    if (Number.isFinite(len) && len > INPUT_SIZE_CAP_BYTES) {
      res413(`imageUrl response is ${len} bytes; cap is ${INPUT_SIZE_CAP_BYTES}`);
    }
  }

  const arrayBuf = await response.arrayBuffer();
  if (arrayBuf.byteLength === 0) throw new ValidationError('imageUrl returned empty body');
  if (arrayBuf.byteLength > INPUT_SIZE_CAP_BYTES) {
    res413(`imageUrl response is ${arrayBuf.byteLength} bytes; cap is ${INPUT_SIZE_CAP_BYTES}`);
  }

  return { body: Buffer.from(arrayBuf), mimeType: ct };
}

/** Helper to throw a 413 ValidationError with a recognizable 413 status. */
function res413(msg: string): never {
  const err = new Error(msg) as Error & { status: number };
  err.status = 413;
  throw err;
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

interface PreviewVisionRequestBody {
  imageUrl?: string;
  systemPrompt?: string;
  userPrompt?: string;
  outputSchema?: Record<string, unknown>;
  model?: string;
  maxOutputTokens?: number;
}

interface PreviewVisionResponse {
  caption: string;
  extracted?: unknown;
  usage: {
    inputTokens: number;
    outputTokens: number;
    cacheReadTokens?: number;
    cacheCreationTokens?: number;
  };
  latencyMs: number;
  costUsd: number;
  failureReason?: string;
}

export const pipelinePreviewVisionRouter = Router({ mergeParams: true });

pipelinePreviewVisionRouter.post(
  '/',
  requireAuth,
  previewVisionRateLimit,
  asyncHandler<
    Record<string, never>,
    PreviewVisionResponse | { error: string }
  >(async (req, res) => {
    const body = req.body as PreviewVisionRequestBody | undefined;

    // ── Validate request body ─────────────────────────────────────────────
    const {
      imageUrl,
      systemPrompt,
      userPrompt,
      outputSchema,
      model = DEFAULT_MODEL,
      maxOutputTokens = DEFAULT_MAX_OUTPUT_TOKENS,
    } = body ?? {};

    if (!imageUrl || typeof imageUrl !== 'string' || imageUrl.trim().length === 0) {
      throw new ValidationError('imageUrl is required');
    }
    if (typeof userPrompt !== 'string' || userPrompt.trim().length === 0) {
      throw new ValidationError('userPrompt is required');
    }
    if (outputSchema !== undefined && (typeof outputSchema !== 'object' || Array.isArray(outputSchema))) {
      throw new ValidationError('outputSchema must be a JSON object');
    }

    // ── Fetch image ───────────────────────────────────────────────────────
    let imageBytes: Buffer;
    let imageMimeType: string;

    try {
      const fetched = await fetchImageFromUrl(imageUrl.trim());
      imageBytes = fetched.body;
      imageMimeType = fetched.mimeType;
    } catch (err) {
      const status = (err as Error & { status?: number }).status;
      const msg = err instanceof Error ? err.message : String(err);
      if (status === 413) {
        res.status(413).json({ error: msg });
        return;
      }
      if (err instanceof ValidationError) throw err;
      throw new ValidationError(`image fetch error: ${msg}`);
    }

    // ── Store the image transiently so the vision client can reference it ─
    // The AnthropicVisionClient takes imageBytes directly; no store write needed.
    // ─────────────────────────────────────────────────────────────────────

    // ── Call vision client ────────────────────────────────────────────────
    const client = getVisionClient();
    const t0 = Date.now();
    const result = await client.analyze({
      model: (model as string) || DEFAULT_MODEL,
      systemPrompt: (systemPrompt ?? '').trim() || 'You analyze images and respond with concise, accurate observations.',
      userPrompt: userPrompt.trim(),
      imageBytes,
      imageMimeType,
      outputSchema: outputSchema,
      maxOutputTokens: typeof maxOutputTokens === 'number' ? maxOutputTokens : DEFAULT_MAX_OUTPUT_TOKENS,
    });
    const latencyMs = Date.now() - t0;

    const effectiveModel = (model as string) || DEFAULT_MODEL;
    const costUsd = computeVisionCost(effectiveModel, result.usage);

    // ── Content-policy refusal — still 200 ───────────────────────────────
    if (result.failureReason === 'refusal') {
      res.status(200).json({
        caption: '',
        usage: {
          inputTokens: result.usage.input_tokens,
          outputTokens: result.usage.output_tokens,
          ...(result.usage.cache_read_input_tokens ? { cacheReadTokens: result.usage.cache_read_input_tokens } : {}),
          ...(result.usage.cache_creation_input_tokens ? { cacheCreationTokens: result.usage.cache_creation_input_tokens } : {}),
        },
        latencyMs,
        costUsd,
        failureReason: 'refusal',
      });
      return;
    }

    // ── Other failures ────────────────────────────────────────────────────
    if (!result.output) {
      res.status(200).json({
        caption: '',
        usage: {
          inputTokens: result.usage.input_tokens,
          outputTokens: result.usage.output_tokens,
        },
        latencyMs,
        costUsd,
        failureReason: result.failureReason ?? 'unknown',
      });
      return;
    }

    // ── Success ───────────────────────────────────────────────────────────
    const response: PreviewVisionResponse = {
      caption: result.output.caption ?? '',
      ...(result.output.extracted !== null ? { extracted: result.output.extracted } : {}),
      usage: {
        inputTokens: result.usage.input_tokens,
        outputTokens: result.usage.output_tokens,
        ...(result.usage.cache_read_input_tokens ? { cacheReadTokens: result.usage.cache_read_input_tokens } : {}),
        ...(result.usage.cache_creation_input_tokens ? { cacheCreationTokens: result.usage.cache_creation_input_tokens } : {}),
      },
      latencyMs,
      costUsd,
    };

    res.status(200).json(response);
  }),
);
