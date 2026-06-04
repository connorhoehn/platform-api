// platform-api/src/pipeline/image/VisionClient.ts
//
// Anthropic Messages API client for the image:vision-analyze step.
// Uses direct fetch — NO SDK, NO black-box wrapper. We own every byte
// of the request/response so the visibility surface matches what the
// operator can see in network logs.
//
// === Pragmatic deviation from JudgeClient ===
// Judge spawns `claude -p` and inherits the host's OAuth/Keychain auth.
// Vision can't do that: the Claude Code CLI in stream-json input mode
// does NOT forward image content blocks to the model — empirically
// verified by passing a base64 image and getting "I can't view the
// image." back. So vision uses the HTTP API directly with an
// ANTHROPIC_API_KEY env var. Same observable surface (Promise of
// result, soft-fail, propagate usage) — different transport.
//
// Why direct fetch instead of @anthropic-ai/sdk:
//   - One fewer dependency surface to track
//   - Every byte of the request/response is visible in our code
//   - We can swap to Bedrock by changing one URL (no client-class
//     dispatch fork inside an opaque SDK)
//   - Same posture as the rest of platform-api's HTTP clients
//
// Non-throwing per the same audit rule judge follows: any failure
// path (HTTP error, parse fail, abort, refusal) surfaces as
// VisionClientCallResult.output === null with failureReason populated.
// Usage tokens propagate even when parsing fails so cost telemetry
// stays honest.
//
// Cancellation: pass `helpers.abortSignal` from the step handler — we
// forward it directly to fetch().

import type { AnthropicVisionUsage, VisionAnalyzeInput } from './types';

const ANTHROPIC_API_URL = process.env['ANTHROPIC_API_URL'] ?? 'https://api.anthropic.com/v1/messages';
const ANTHROPIC_API_VERSION = '2023-06-01';
const DEFAULT_MODEL = 'claude-sonnet-4-6';

/**
 * Canonical vision system prompt. Inlines the JSON schema spec when
 * the caller supplies one — same prompt-asked-JSON pattern as judge
 * (we use defensive extraction below since the CLI's --json-schema
 * flag isn't available on the HTTP path).
 */
export function buildVisionSystemPrompt(callerSystem: string | undefined, schema: Record<string, unknown> | undefined): string {
  const base = callerSystem?.trim()
    ?? [
      'You analyze images and respond with concise, accurate observations.',
      'Be specific about what you see; do not speculate beyond the image.',
      'Never invent details that are not visible.',
    ].join(' ');

  if (!schema) return base;

  // Structured-output mode: append the schema spec.
  return [
    base,
    '',
    'You MUST respond with ONLY a JSON object matching the schema below.',
    'No prose before or after. No markdown fences.',
    'If a required field is not derivable from the image, set it to null.',
    '',
    'JSON Schema:',
    JSON.stringify(schema, null, 2),
  ].join('\n');
}

export interface VisionClientCallResult {
  /** Null when parsing/extraction failed AND retry was exhausted (non-throwing failure). */
  output: VisionRawOutput | null;
  usage: AnthropicVisionUsage;
  rawText: string;
  latencyMs: number;
  cacheHit: boolean;
  failureReason?: 'malformed' | 'overloaded' | 'refusal' | 'aborted' | 'http-error' | 'unknown';
}

/**
 * The parsed payload the vision call produces.
 *
 * `caption` is always present (the model's freeform analysis).
 * `extracted` is the parsed JSON object when the caller supplied a
 * schema — null in pure-caption mode.
 */
export interface VisionRawOutput {
  caption: string;
  extracted: unknown | null;
}

export interface VisionClientOpts {
  /** Override the API URL (Bedrock endpoint, proxy, etc.). */
  apiUrl?: string;
  /** API key. Defaults to ANTHROPIC_API_KEY env var. */
  apiKey?: string;
  /** Test hook — replaces global fetch for unit tests. */
  fetchImpl?: typeof fetch;
}

/**
 * Direct HTTP client for Anthropic Messages API vision calls. Spawn
 * via VisionStepHandler; do not call from request handlers directly.
 */
export class VisionClient {
  private readonly apiUrl: string;
  private readonly apiKey: string;
  private readonly fetchImpl: typeof fetch;

  constructor(opts: VisionClientOpts = {}) {
    this.apiUrl = opts.apiUrl ?? ANTHROPIC_API_URL;
    this.apiKey = opts.apiKey ?? process.env['ANTHROPIC_API_KEY'] ?? '';
    this.fetchImpl = opts.fetchImpl ?? fetch;
  }

  /**
   * Single vision call. Always returns a `VisionClientCallResult` —
   * never throws. A malformed structured-output triggers ONE corrective
   * retry before surfacing `failureReason: 'malformed'` with usage
   * still populated.
   */
  async analyze(input: VisionAnalyzeInput): Promise<VisionClientCallResult> {
    const t0 = Date.now();
    const first = await this.attempt(input, false);
    if (first.output) return first;
    if (first.failureReason === 'malformed') {
      const retry = await this.attempt(input, true);
      retry.usage = {
        input_tokens: (first.usage.input_tokens ?? 0) + (retry.usage.input_tokens ?? 0),
        output_tokens: (first.usage.output_tokens ?? 0) + (retry.usage.output_tokens ?? 0),
        ...(first.usage.image_tokens || retry.usage.image_tokens
          ? { image_tokens: (first.usage.image_tokens ?? 0) + (retry.usage.image_tokens ?? 0) }
          : {}),
      };
      retry.latencyMs = Date.now() - t0;
      return retry;
    }
    return first;
  }

  private async attempt(
    input: VisionAnalyzeInput,
    corrective: boolean,
  ): Promise<VisionClientCallResult> {
    const t0 = Date.now();

    if (!this.apiKey) {
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: 0,
        cacheHit: false,
        failureReason: 'http-error',
      };
    }

    const systemPrompt = buildVisionSystemPrompt(input.systemPrompt, input.outputSchema);

    const userContent: Array<Record<string, unknown>> = [
      {
        type: 'image',
        source: {
          type: 'base64',
          media_type: input.imageMimeType,
          data: input.imageBytes.toString('base64'),
        },
      },
      {
        type: 'text',
        text: corrective
          ? [
              'Your previous response was malformed. Respond with ONLY a JSON',
              'object matching the schema in the system prompt. No prose, no',
              'markdown fences.',
              '',
              input.userPrompt,
            ].join('\n')
          : input.userPrompt,
      },
    ];

    const body = {
      model: input.model || DEFAULT_MODEL,
      max_tokens: input.maxOutputTokens,
      system: systemPrompt,
      messages: [{ role: 'user', content: userContent }],
    };

    let response: Response;
    try {
      response = await this.fetchImpl(this.apiUrl, {
        method: 'POST',
        headers: {
          'content-type': 'application/json',
          'x-api-key': this.apiKey,
          'anthropic-version': ANTHROPIC_API_VERSION,
        },
        body: JSON.stringify(body),
        signal: input.abortSignal,
      });
    } catch (err) {
      const e = err as Error;
      const isAbort = e.name === 'AbortError' || e.message.includes('aborted');
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: isAbort ? 'aborted' : 'http-error',
      };
    }

    if (!response.ok) {
      const errText = await response.text().catch(() => '');
      const overloaded = response.status === 529;
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: errText,
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: overloaded ? 'overloaded' : 'http-error',
      };
    }

    let payload: Record<string, unknown>;
    try {
      payload = (await response.json()) as Record<string, unknown>;
    } catch {
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: 'http-error',
      };
    }

    // Extract usage from Anthropic's response.
    const usage = (payload['usage'] ?? {}) as Record<string, unknown>;
    const tokensIn = (usage['input_tokens'] as number) ?? 0;
    const tokensOut = (usage['output_tokens'] as number) ?? 0;
    const cacheRead = (usage['cache_read_input_tokens'] as number) ?? 0;
    const cacheCreation = (usage['cache_creation_input_tokens'] as number) ?? 0;

    // Anthropic responses have `content: [{type:'text', text:'...'}]`.
    const content = payload['content'] as Array<Record<string, unknown>> | undefined;
    let fullText = '';
    if (Array.isArray(content)) {
      for (const block of content) {
        if (block['type'] === 'text' && typeof block['text'] === 'string') {
          fullText += block['text'];
        }
      }
    }

    // Refusal detection — stop_reason === 'refusal' indicates safety filter.
    const stopReason = payload['stop_reason'];
    if (stopReason === 'refusal') {
      return {
        output: null,
        usage: {
          input_tokens: tokensIn,
          output_tokens: tokensOut,
          ...(cacheRead ? { cache_read_input_tokens: cacheRead } : {}),
          ...(cacheCreation ? { cache_creation_input_tokens: cacheCreation } : {}),
        },
        rawText: fullText,
        latencyMs: Date.now() - t0,
        cacheHit: cacheRead > 0,
        failureReason: 'refusal',
      };
    }

    // No schema → caption-only mode: the full text IS the caption.
    if (!input.outputSchema) {
      return {
        output: { caption: fullText.trim(), extracted: null },
        usage: {
          input_tokens: tokensIn,
          output_tokens: tokensOut,
          ...(cacheRead ? { cache_read_input_tokens: cacheRead } : {}),
          ...(cacheCreation ? { cache_creation_input_tokens: cacheCreation } : {}),
        },
        rawText: fullText,
        latencyMs: Date.now() - t0,
        cacheHit: cacheRead > 0,
      };
    }

    // Schema mode: defensively extract the JSON object.
    const extracted = extractJsonObject(fullText);
    if (extracted === null) {
      return {
        output: null,
        usage: {
          input_tokens: tokensIn,
          output_tokens: tokensOut,
          ...(cacheRead ? { cache_read_input_tokens: cacheRead } : {}),
          ...(cacheCreation ? { cache_creation_input_tokens: cacheCreation } : {}),
        },
        rawText: fullText,
        latencyMs: Date.now() - t0,
        cacheHit: cacheRead > 0,
        failureReason: 'malformed',
      };
    }

    return {
      output: {
        caption: typeof (extracted as Record<string, unknown>)['caption'] === 'string'
          ? (extracted as Record<string, unknown>)['caption'] as string
          : '',
        extracted,
      },
      usage: {
        input_tokens: tokensIn,
        output_tokens: tokensOut,
        ...(cacheRead ? { cache_read_input_tokens: cacheRead } : {}),
        ...(cacheCreation ? { cache_creation_input_tokens: cacheCreation } : {}),
      },
      rawText: fullText,
      latencyMs: Date.now() - t0,
      cacheHit: cacheRead > 0,
    };
  }
}

// ─── Pure helpers ──────────────────────────────────────────────────────

/**
 * Defensive JSON extraction (aws-agentcore judge.ts pattern, same as
 * JudgeClient.extractJsonObject). Models regularly wrap JSON in
 * markdown fences or precede it with prose like "Here is the
 * analysis:". Slice between the first `{` and the last `}` (greedy)
 * and JSON.parse. Returns null on any failure.
 */
export function extractJsonObject(text: string): unknown {
  if (!text) return null;
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start < 0 || end < 0 || end <= start) return null;
  const candidate = text.slice(start, end + 1);
  try {
    return JSON.parse(candidate);
  } catch {
    return null;
  }
}
