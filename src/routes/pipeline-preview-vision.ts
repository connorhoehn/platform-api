// platform-api/src/routes/pipeline-preview-vision.ts
//
// POST /api/pipeline/preview/vision
//
// Bench preview for the vision prompt editor. Calls VisionClient.analyze()
// with caller-supplied prompt / schema against a stored image (preferred)
// or a caller-supplied URL (legacy fallback).
//
// Differences from the full VisionStepHandler path:
//   - No VisionResultRepository persistence — result is in-response only.
//   - No pipeline:image:vision:* event emission.
//   - Image input: caller supplies either `imageId` (preferred — server-side
//     blob-store lookup, works with signed/IP-locked URLs that platform-api
//     itself can't dereference) or `imageUrl` (legacy fallback — fetched
//     via the same SSRF-safe path used by /api/images/from-url).
//   - Cost is computed and returned so the bench panel can show a running
//     tally, but no audit log is written.
//   - Rate limit: 10 calls/min per authed user (vision calls are expensive;
//     client-side submit button guards the happy path).
//
// Auth: requireAuth.

import { Router } from 'express';
import { asyncHandler, NotFoundError, ValidationError } from '../middleware/error-handler';
import { createRateLimiter } from '../middleware/rateLimit';
import { requireAuth } from '../middleware/auth';
import { getImageBlobStore, getVisionClient } from '../pipeline/image/visionServices';
import { computeVisionCost } from '../pipeline/image/computeVisionCost';
import { fetchPreviewImageFromUrl } from './pipeline-preview-image-fetch';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/** Hard cap on the raw image bytes we'll pass to the vision client. */
const INPUT_SIZE_CAP_BYTES = 5 * 1024 * 1024;

const DEFAULT_MODEL = 'claude-sonnet-4-6';
const DEFAULT_MAX_OUTPUT_TOKENS = 2048;

/** MIME types the vision client accepts. Mirrors the URL-fetch allow-list. */
const ALLOWED_BLOB_MIME = new Set(['image/jpeg', 'image/png', 'image/webp']);

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
// Router
// ---------------------------------------------------------------------------

interface PreviewVisionRequestBody {
  /**
   * Preferred input — server-side blob-store lookup. Works even when the
   * canonical URL is signed / IP-locked and unreachable by platform-api.
   */
  imageId?: string;
  /**
   * Legacy fallback — fetched via the shared SSRF-safe path. Ignored when
   * `imageId` is also present.
   */
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
      imageId,
      imageUrl,
      systemPrompt,
      userPrompt,
      outputSchema,
      model = DEFAULT_MODEL,
      maxOutputTokens = DEFAULT_MAX_OUTPUT_TOKENS,
    } = body ?? {};

    const hasImageId = typeof imageId === 'string' && imageId.length > 0;
    const hasImageUrl =
      typeof imageUrl === 'string' && imageUrl.trim().length > 0;
    if (!hasImageId && !hasImageUrl) {
      throw new ValidationError('imageId or imageUrl is required');
    }
    if (typeof userPrompt !== 'string' || userPrompt.trim().length === 0) {
      throw new ValidationError('userPrompt is required');
    }
    if (outputSchema !== undefined && (typeof outputSchema !== 'object' || Array.isArray(outputSchema))) {
      throw new ValidationError('outputSchema must be a JSON object');
    }

    // ── Resolve image bytes ───────────────────────────────────────────────
    // Prefer imageId (blob-store lookup) — works with signed/IP-locked URLs
    // platform-api itself can't reach. Fall back to imageUrl via the shared
    // SSRF-safe fetch helper when only the URL is supplied.
    let imageBytes: Buffer;
    let imageMimeType: string;

    if (hasImageId) {
      const store = getImageBlobStore();
      const stored = await store.get(imageId as string);
      if (!stored) {
        throw new NotFoundError(`image '${imageId}' not found`);
      }
      if (stored.body.byteLength > INPUT_SIZE_CAP_BYTES) {
        res.status(413).json({
          error: `source image exceeds ${INPUT_SIZE_CAP_BYTES} byte preview limit`,
        });
        return;
      }
      const storedMime = (stored.meta.mimeType ?? '').toLowerCase();
      if (!ALLOWED_BLOB_MIME.has(storedMime)) {
        throw new ValidationError(
          `stored image MIME '${storedMime}' is not supported — must be image/jpeg, image/png, or image/webp`,
        );
      }
      imageBytes = stored.body;
      imageMimeType = storedMime;
    } else {
      try {
        const fetched = await fetchPreviewImageFromUrl(
          (imageUrl as string).trim(),
          INPUT_SIZE_CAP_BYTES,
        );
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
    }

    // ── The AnthropicVisionClient takes imageBytes directly; no store write. ─

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
