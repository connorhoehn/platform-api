// platform-api/src/pipeline/image/AnthropicVisionClient.ts
//
// SDK-based vision client. Posts a base64 image content block via
// `@anthropic-ai/sdk`'s `messages.stream` and streams text deltas via
// `.on('text', ...)`. Replaces the CLI-spawn transport in `VisionClient.ts`
// for production use — see VisionClient.ts header for the rationale of why
// the CLI path exists at all (Bedrock OAuth fallback).
//
// === Why the SDK path ===
// Fan-out scatter pipelines spawn N concurrent vision calls (one per
// detected crop). The CLI path's per-call subprocess overhead (~2-4 s cold
// start, plus tempfile + filesystem permission churn) compounds badly:
// N=4 arms = ~8-16 s of pure transport overhead before any token is
// emitted. The SDK keeps everything in-process and parallel-friendly.
//
// === Behavioral parity with VisionClient ===
//   - Non-throwing: always resolves to VisionClientCallResult.
//   - Same `onToken(delta)` callback shape; VisionStepHandler's
//     `pipeline:image:vision:token` event emission is unchanged.
//   - `outputSchema` triggers the same "JSON only" system-prompt stanza
//     via `buildVisionSystemPrompt`. Same `extractJsonObject` post-parse
//     and ONE corrective retry on malformed JSON.
//   - Refusal detection on `stop_reason: 'refusal'`.
//   - `cacheHit` flag from `cache_read_input_tokens > 0`.
//
// === Behavioral deltas ===
//   - `maxOutputTokens` is now actually honored (the CLI ignored it).
//   - Tempfile lifecycle is gone; image bytes go directly as base64.
//   - Cache telemetry uses the beta prompt-caching API automatically when
//     ANTHROPIC_BETA_PROMPT_CACHING=1; otherwise cache fields stay zero
//     and `cacheHit` is always false. Schema/caption modes are unchanged.
//
// === Cancellation ===
// VisionAnalyzeInput.abortSignal is forwarded to the SDK stream via the
// `signal` option; the SDK aborts the in-flight HTTP request and the
// async iterator returns. We finalize with `failureReason:'aborted'`.

import Anthropic from '@anthropic-ai/sdk';
import type {
  MessageStreamParams,
  MessageParam,
  ImageBlockParam,
  TextBlockParam,
} from '@anthropic-ai/sdk/resources/messages';
import type { AnthropicVisionUsage, VisionAnalyzeInput } from './types';
import {
  buildVisionSystemPrompt,
  extractJsonObject,
  type VisionClientCallResult,
  type VisionClientLike,
  type VisionRawOutput,
  type VisionTokenCallback,
} from './VisionClient';

/**
 * MIME types the Anthropic API accepts for image content blocks. GIF is
 * supported by the API but we don't emit it from the upload pipeline
 * (routes/images.ts re-encodes to JPEG/PNG/WebP).
 */
const SUPPORTED_MEDIA_TYPES = new Set<ImageBlockParam['source']['media_type']>([
  'image/jpeg',
  'image/png',
  'image/webp',
  'image/gif',
]);

/**
 * Coerce an arbitrary mime string to the SDK's typed media-type union.
 * Returns null when the type isn't supported (handler surfaces the
 * failure as `pipeline:image:vision:failed`).
 */
function coerceMediaType(
  mime: string,
): ImageBlockParam['source']['media_type'] | null {
  return SUPPORTED_MEDIA_TYPES.has(
    mime as ImageBlockParam['source']['media_type'],
  )
    ? (mime as ImageBlockParam['source']['media_type'])
    : null;
}

export interface AnthropicVisionClientOpts {
  /**
   * Override the SDK client (test hook). When unset, a default
   * `new Anthropic()` is constructed which reads `ANTHROPIC_API_KEY` and
   * optionally `ANTHROPIC_BASE_URL` from the environment.
   */
  client?: Anthropic;
  /**
   * Default `max_tokens` when caller does not supply
   * `VisionAnalyzeInput.maxOutputTokens`. The SDK requires the field to be
   * present. Matches VisionStepHandler's `DEFAULT_MAX_OUTPUT_TOKENS`.
   */
  defaultMaxOutputTokens?: number;
}

const DEFAULT_MAX_OUTPUT_TOKENS = 2048;

/**
 * SDK-based vision client. Use via `visionServices.ts` — do not
 * instantiate per-request.
 */
export class AnthropicVisionClient implements VisionClientLike {
  private readonly client: Anthropic;
  private readonly defaultMaxOutputTokens: number;

  constructor(opts: AnthropicVisionClientOpts = {}) {
    this.client = opts.client ?? new Anthropic();
    this.defaultMaxOutputTokens =
      opts.defaultMaxOutputTokens ?? DEFAULT_MAX_OUTPUT_TOKENS;
  }

  async analyze(
    input: VisionAnalyzeInput,
    onToken?: VisionTokenCallback,
  ): Promise<VisionClientCallResult> {
    const t0 = Date.now();
    const first = await this.attempt(input, false, onToken);
    if (first.output) return first;
    if (first.failureReason === 'malformed') {
      const retry = await this.attempt(input, true, onToken);
      retry.usage = mergeUsage(first.usage, retry.usage);
      retry.cacheHit = (retry.usage.cache_read_input_tokens ?? 0) > 0;
      retry.latencyMs = Date.now() - t0;
      return retry;
    }
    return first;
  }

  private async attempt(
    input: VisionAnalyzeInput,
    corrective: boolean,
    onToken?: VisionTokenCallback,
  ): Promise<VisionClientCallResult> {
    const t0 = Date.now();

    // ── Coerce media type ──────────────────────────────────────────────
    const mediaType = coerceMediaType(input.imageMimeType);
    if (!mediaType) {
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: 'unknown',
      };
    }

    // ── Build the message payload ──────────────────────────────────────
    const systemPrompt = buildVisionSystemPrompt(
      input.systemPrompt,
      input.outputSchema,
    );
    let userPromptText = input.userPrompt;
    if (corrective) {
      userPromptText = [
        'Your previous response was malformed. Respond with ONLY a JSON',
        'object matching the schema in the system prompt. No prose, no',
        'markdown fences.',
        '',
        userPromptText,
      ].join('\n');
    }

    const imageBlock: ImageBlockParam = {
      type: 'image',
      source: {
        type: 'base64',
        media_type: mediaType,
        data: input.imageBytes.toString('base64'),
      },
    };
    const textBlock: TextBlockParam = { type: 'text', text: userPromptText };
    const messages: MessageParam[] = [
      { role: 'user', content: [imageBlock, textBlock] },
    ];

    const params: MessageStreamParams = {
      model: input.model || 'claude-sonnet-4-6',
      max_tokens: input.maxOutputTokens ?? this.defaultMaxOutputTokens,
      messages,
      ...(systemPrompt ? { system: systemPrompt } : {}),
    };

    let fullResponse = '';
    let tokensIn = 0;
    let tokensOut = 0;
    let cacheReadIn = 0;
    let cacheCreationIn = 0;
    let stopReason: string | null = null;
    let aborted = false;

    try {
      const stream = this.client.messages.stream(params, {
        signal: input.abortSignal,
      });

      // Per-delta callback. The SDK's typed `text` event matches what
      // VisionStepHandler.onToken expects, byte-for-byte.
      stream.on('text', (delta) => {
        fullResponse += delta;
        try {
          onToken?.(delta);
        } catch {
          // never let a bad subscriber kill the call
        }
      });

      // Await the full final message — gives us authoritative usage + stop_reason.
      const finalMessage = await stream.finalMessage();

      tokensIn = finalMessage.usage.input_tokens ?? 0;
      tokensOut = finalMessage.usage.output_tokens ?? 0;
      // Cache fields are only typed on the beta prompt-caching variant of
      // the SDK; the non-beta `Usage` type omits them but the wire shape
      // still includes them when ANTHROPIC_BETA_PROMPT_CACHING is enabled
      // upstream. Read defensively so we pick them up either way without
      // forcing every caller onto the beta typings.
      const usageAny = finalMessage.usage as unknown as Record<string, unknown>;
      const cacheRead = usageAny['cache_read_input_tokens'];
      const cacheCreate = usageAny['cache_creation_input_tokens'];
      cacheReadIn = typeof cacheRead === 'number' ? cacheRead : 0;
      cacheCreationIn = typeof cacheCreate === 'number' ? cacheCreate : 0;
      stopReason = finalMessage.stop_reason ?? null;

      // Fallback: if the stream produced no text deltas (rare — image-only
      // responses can do this), pull text blocks from the final message.
      if (!fullResponse) {
        for (const block of finalMessage.content) {
          if (block.type === 'text') fullResponse += block.text;
        }
      }
    } catch (err) {
      const e = err as Error;
      aborted =
        e.name === 'AbortError' ||
        e.message?.toLowerCase().includes('aborted') === true ||
        input.abortSignal?.aborted === true;
      return {
        output: null,
        usage: { input_tokens: tokensIn, output_tokens: tokensOut },
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: aborted ? 'aborted' : 'unknown',
      };
    }

    const usage: AnthropicVisionUsage = {
      input_tokens: tokensIn,
      output_tokens: tokensOut,
      ...(cacheReadIn ? { cache_read_input_tokens: cacheReadIn } : {}),
      ...(cacheCreationIn ? { cache_creation_input_tokens: cacheCreationIn } : {}),
    };

    // Refusal detection — matches CLI client.
    if (stopReason === 'refusal') {
      return {
        output: null,
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: 'refusal',
      };
    }

    // Caption mode — full text IS the caption.
    if (!input.outputSchema) {
      const output: VisionRawOutput = {
        caption: fullResponse.trim(),
        extracted: null,
      };
      return {
        output,
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
      };
    }

    // Extract mode — defensive JSON parse.
    const extracted = extractJsonObject(fullResponse);
    if (extracted === null) {
      return {
        output: null,
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: 'malformed',
      };
    }

    const captionField = (extracted as Record<string, unknown>)['caption'];
    const output: VisionRawOutput = {
      caption: typeof captionField === 'string' ? captionField : '',
      extracted,
    };
    return {
      output,
      usage,
      rawText: fullResponse,
      latencyMs: Date.now() - t0,
      cacheHit: cacheReadIn > 0,
    };
  }
}

/** Sum usage tokens across the original attempt and a corrective retry. */
function mergeUsage(
  first: AnthropicVisionUsage,
  retry: AnthropicVisionUsage,
): AnthropicVisionUsage {
  return {
    input_tokens: (first.input_tokens ?? 0) + (retry.input_tokens ?? 0),
    output_tokens: (first.output_tokens ?? 0) + (retry.output_tokens ?? 0),
    ...(first.image_tokens || retry.image_tokens
      ? { image_tokens: (first.image_tokens ?? 0) + (retry.image_tokens ?? 0) }
      : {}),
    ...(first.cache_read_input_tokens || retry.cache_read_input_tokens
      ? {
          cache_read_input_tokens:
            (first.cache_read_input_tokens ?? 0) +
            (retry.cache_read_input_tokens ?? 0),
        }
      : {}),
    ...(first.cache_creation_input_tokens || retry.cache_creation_input_tokens
      ? {
          cache_creation_input_tokens:
            (first.cache_creation_input_tokens ?? 0) +
            (retry.cache_creation_input_tokens ?? 0),
        }
      : {}),
  };
}
