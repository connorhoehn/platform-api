// platform-api/src/pipeline/image/VisionClient.ts
//
// Claude Code CLI subprocess client for the image:vision-analyze step.
// Mirrors JudgeClient — spawns `claude -p` with `--output-format stream-json`,
// streams stdout, parses jsonl events, accumulates text deltas.
//
// === Image input strategy ===
// The CLI's stream-json input mode does NOT forward image content blocks to
// the model (empirically verified — passing a base64 image returns "I can't
// view the image."). The working approach: write the image to a tempfile
// in a directory the CLI's Read tool can access, then mention the file path
// in the prompt. The Read tool surfaces the image to the model as a base64
// content block automatically (Claude Code 2.1.x+).
//
// We:
//   1. Receive `imageBytes` + `imageMimeType` from VisionStepHandler.
//   2. Mint a per-call temp dir under `/tmp/pipeline-vision/<uuid>/`.
//   3. Write `img.<ext>` inside it.
//   4. Spawn `claude -p` with `--allowedTools Read --add-dir <tempdir>
//      --permission-mode acceptEdits` and a prompt that references the
//      file path explicitly.
//   5. Stream stdout, fold text_delta chunks, capture usage from the
//      `result` event.
//   6. Clean up the temp dir on exit.
//
// === Streaming events ===
// Callers supply `onToken(text)` to receive per-chunk token deltas as they
// arrive. The VisionStepHandler emits `pipeline:image:vision:token` events
// off this callback so the frontend can render live updates the same way
// the LLM node does.
//
// === Structured output ===
// When `outputSchema` is supplied, the canonical vision system prompt
// appends a "respond with ONLY JSON matching this schema" stanza. The
// final response is defensively parsed (slice between first `{` and last
// `}`); malformed responses trigger ONE corrective retry.
//
// Non-throwing per audit rule #1 (matches judge). Cancellation: forward
// `helpers.abortSignal` — we register SIGTERM-then-SIGKILL grace.

import { spawn } from 'node:child_process';
import type { ChildProcessByStdio } from 'node:child_process';
import type { Readable } from 'node:stream';
import { withAbort } from 'distributed-core/common';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, writeFileSync, rmSync, mkdirSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import type { AnthropicVisionUsage, VisionAnalyzeInput } from './types';

const CLAUDE_BIN = process.env['CLAUDE_CLI_PATH'] ?? 'claude';
const KILL_GRACE_MS = 500;

// Bedrock model translation — mirrors JudgeClient + ClaudeCodeLLMClient.
const BEDROCK_MODEL_ALIAS: Record<string, string> = {
  opus:   'us.anthropic.claude-opus-4-20250514-v1:0',
  sonnet: 'us.anthropic.claude-sonnet-4-20250514-v1:0',
  haiku:  'us.anthropic.claude-haiku-4-5-20251001-v1:0',
  'claude-opus-4-7':            'us.anthropic.claude-opus-4-20250514-v1:0',
  'claude-sonnet-4-6':          'us.anthropic.claude-sonnet-4-20250514-v1:0',
  'claude-haiku-4-5-20251001':  'us.anthropic.claude-haiku-4-5-20251001-v1:0',
};
function bedrockMode(): boolean {
  const v = process.env['CLAUDE_CODE_USE_BEDROCK'];
  return v === '1' || v?.toLowerCase() === 'true';
}
function resolveModel(model: string): string {
  if (!model?.trim()) return model;
  if (bedrockMode()) return BEDROCK_MODEL_ALIAS[model] ?? model;
  return model;
}

const MIME_TO_EXT: Record<string, string> = {
  'image/jpeg': 'jpg',
  'image/png': 'png',
  'image/webp': 'webp',
};

/**
 * Canonical vision system prompt. Appends schema instructions when the
 * caller supplies a schema (extract mode).
 */
export function buildVisionSystemPrompt(callerSystem: string | undefined, schema: Record<string, unknown> | undefined): string {
  const base = callerSystem?.trim()
    ?? [
      'You analyze images and respond with concise, accurate observations.',
      'Be specific about what you see; do not speculate beyond the image.',
      'Never invent details that are not visible.',
    ].join(' ');

  if (!schema) return base;

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
  /** Override the CLI binary path. */
  claudeBin?: string;
  /** Override the spawn implementation (test hook). */
  spawnImpl?: typeof spawn;
}

/**
 * Callback fired once per token delta as the CLI streams stdout. The
 * VisionStepHandler uses this to emit `pipeline:image:vision:token`
 * events so the frontend can render live updates.
 */
export type VisionTokenCallback = (delta: string, accumulated: string) => void;

/**
 * Spawn-based vision client. Use via the singleton in `visionServices.ts`;
 * do not instantiate per-request.
 */
export class VisionClient {
  private readonly claudeBin: string;
  private readonly spawnImpl: typeof spawn;

  constructor(opts: VisionClientOpts = {}) {
    this.claudeBin = opts.claudeBin ?? CLAUDE_BIN;
    this.spawnImpl = opts.spawnImpl ?? spawn;
  }

  /**
   * Single vision call. Always resolves to a `VisionClientCallResult` —
   * never throws. Malformed structured-output triggers ONE corrective
   * retry; usage tokens accumulate across both attempts so cost telemetry
   * stays honest.
   */
  async analyze(
    input: VisionAnalyzeInput,
    onToken?: VisionTokenCallback,
  ): Promise<VisionClientCallResult> {
    const t0 = Date.now();
    const first = await this.attempt(input, false, onToken);
    if (first.output) return first;
    if (first.failureReason === 'malformed') {
      const retry = await this.attempt(input, true, onToken);
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
    onToken: VisionTokenCallback | undefined,
  ): Promise<VisionClientCallResult> {
    const t0 = Date.now();

    // ── Stage the image in a temp dir the CLI can read ─────────────────
    const baseDir = join(tmpdir(), 'pipeline-vision');
    try { mkdirSync(baseDir, { recursive: true }); } catch { /* ignore */ }
    const callDir = mkdtempSync(join(baseDir, `${randomUUID()}-`));
    const ext = MIME_TO_EXT[input.imageMimeType] ?? 'jpg';
    const imgPath = join(callDir, `img.${ext}`);
    try {
      writeFileSync(imgPath, input.imageBytes);
    } catch (err) {
      this.cleanup(callDir);
      const msg = err instanceof Error ? err.message : String(err);
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: `tempfile write failed: ${msg}`,
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: 'unknown',
      };
    }

    // ── Build prompt: instruct the model to Read the file ──────────────
    const systemPrompt = buildVisionSystemPrompt(input.systemPrompt, input.outputSchema);
    let userPrompt = [
      `Read the image at ${imgPath} and follow the user instructions below.`,
      '',
      input.userPrompt,
    ].join('\n');
    if (corrective) {
      userPrompt = [
        'Your previous response was malformed. Respond with ONLY a JSON',
        'object matching the schema in the system prompt. No prose, no',
        'markdown fences.',
        '',
        userPrompt,
      ].join('\n');
    }

    const args = [
      '-p',
      '--output-format', 'stream-json',
      '--include-partial-messages',
      '--no-session-persistence',
      '--verbose',
      '--allowedTools', 'Read',
      '--add-dir', callDir,
      '--permission-mode', 'acceptEdits',
    ];
    if (systemPrompt?.trim()) {
      args.push('--append-system-prompt', systemPrompt);
    }
    if (input.model?.trim()) {
      args.push('--model', resolveModel(input.model));
    }
    args.push(userPrompt);

    const child: ChildProcessByStdio<null, Readable, Readable> = this.spawnImpl(
      this.claudeBin,
      args,
      { stdio: ['ignore', 'pipe', 'pipe'], cwd: callDir },
    ) as ChildProcessByStdio<null, Readable, Readable>;

    const abortReg = input.abortSignal
      ? withAbort(input.abortSignal, () => {
          try { child.kill('SIGTERM'); } catch { /* already gone */ }
          setTimeout(() => { try { child.kill('SIGKILL'); } catch { /* already gone */ } }, KILL_GRACE_MS);
        })
      : null;

    const stderrChunks: Buffer[] = [];
    child.stderr.on('data', (b: Buffer) => stderrChunks.push(b));

    const exitPromise = new Promise<{ code: number | null; sig: NodeJS.Signals | null }>((resolve) => {
      child.on('exit', (code, sig) => resolve({ code, sig }));
    });

    let fullResponse = '';
    let tokensIn = 0;
    let tokensOut = 0;
    let cacheReadIn = 0;
    let cacheCreationIn = 0;
    let buffer = '';
    let stopReason: string | undefined;

    try {
      for await (const chunk of child.stdout as AsyncIterable<Buffer>) {
        if (input.abortSignal?.aborted) {
          return this.finalize(callDir, {
            output: null,
            usage: { input_tokens: tokensIn, output_tokens: tokensOut },
            rawText: fullResponse,
            latencyMs: Date.now() - t0,
            cacheHit: cacheReadIn > 0,
            failureReason: 'aborted',
          });
        }
        buffer += chunk.toString('utf8');
        let nl = buffer.indexOf('\n');
        while (nl !== -1) {
          const line = buffer.slice(0, nl);
          buffer = buffer.slice(nl + 1);
          nl = buffer.indexOf('\n');
          if (!line.trim()) continue;

          let evt: Record<string, unknown>;
          try {
            evt = JSON.parse(line);
          } catch {
            continue;
          }

          const type = evt['type'] as string | undefined;

          if (type === 'stream_event') {
            const event = evt['event'] as Record<string, unknown> | undefined;
            if (event?.['type'] === 'content_block_delta') {
              const delta = event['delta'] as Record<string, unknown> | undefined;
              if (delta?.['type'] === 'text_delta' && typeof delta['text'] === 'string') {
                const token = delta['text'] as string;
                fullResponse += token;
                try {
                  onToken?.(token, fullResponse);
                } catch {
                  // never let a bad subscriber kill the call
                }
              }
            }
          } else if (type === 'assistant') {
            // Fallback when stream_event didn't fire — pull final text.
            if (fullResponse === '') {
              const message = evt['message'] as Record<string, unknown> | undefined;
              const content = message?.['content'] as Array<Record<string, unknown>> | undefined;
              if (Array.isArray(content)) {
                for (const block of content) {
                  if (block['type'] === 'text' && typeof block['text'] === 'string') {
                    fullResponse += block['text'];
                  }
                }
              }
            }
          } else if (type === 'result') {
            const usage = evt['usage'] as Record<string, unknown> | undefined;
            if (typeof usage?.['input_tokens'] === 'number') tokensIn = usage['input_tokens'] as number;
            if (typeof usage?.['output_tokens'] === 'number') tokensOut = usage['output_tokens'] as number;
            if (typeof usage?.['cache_read_input_tokens'] === 'number') cacheReadIn = usage['cache_read_input_tokens'] as number;
            if (typeof usage?.['cache_creation_input_tokens'] === 'number') cacheCreationIn = usage['cache_creation_input_tokens'] as number;
            stopReason = evt['stop_reason'] as string | undefined;
            const result = evt['result'];
            if (typeof result === 'string' && fullResponse === '') {
              fullResponse = result;
            }
          }
        }
      }

      const { code } = await exitPromise;
      if (code !== 0 && !input.abortSignal?.aborted) {
        const stderr = Buffer.concat(stderrChunks).toString('utf8').trim();
        return this.finalize(callDir, {
          output: null,
          usage: { input_tokens: tokensIn, output_tokens: tokensOut },
          rawText: fullResponse || stderr,
          latencyMs: Date.now() - t0,
          cacheHit: cacheReadIn > 0,
          failureReason: 'unknown',
        });
      }
    } catch (err) {
      const e = err as Error;
      const isAbort = e.name === 'AbortError' || e.message.includes('aborted');
      return this.finalize(callDir, {
        output: null,
        usage: { input_tokens: tokensIn, output_tokens: tokensOut },
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: isAbort ? 'aborted' : 'unknown',
      });
    } finally {
      abortReg?.dispose?.();
      if (child.exitCode === null && child.signalCode === null) {
        try { child.kill('SIGTERM'); } catch { /* already exited */ }
      }
    }

    // Token estimate fallback (CLI omits these for refusal/empty outputs).
    if (tokensIn === 0) tokensIn = Math.max(1, Math.round(userPrompt.length / 4));
    if (tokensOut === 0) tokensOut = Math.max(1, Math.round(fullResponse.length / 4));

    const usage: AnthropicVisionUsage = {
      input_tokens: tokensIn,
      output_tokens: tokensOut,
      ...(cacheReadIn ? { cache_read_input_tokens: cacheReadIn } : {}),
      ...(cacheCreationIn ? { cache_creation_input_tokens: cacheCreationIn } : {}),
    };

    // Refusal detection.
    if (stopReason === 'refusal') {
      return this.finalize(callDir, {
        output: null,
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: 'refusal',
      });
    }

    // Caption mode: full text IS the caption.
    if (!input.outputSchema) {
      return this.finalize(callDir, {
        output: { caption: fullResponse.trim(), extracted: null },
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
      });
    }

    // Extract mode: defensively parse the JSON.
    const extracted = extractJsonObject(fullResponse);
    if (extracted === null) {
      return this.finalize(callDir, {
        output: null,
        usage,
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: cacheReadIn > 0,
        failureReason: 'malformed',
      });
    }

    return this.finalize(callDir, {
      output: {
        caption: typeof (extracted as Record<string, unknown>)['caption'] === 'string'
          ? (extracted as Record<string, unknown>)['caption'] as string
          : '',
        extracted,
      },
      usage,
      rawText: fullResponse,
      latencyMs: Date.now() - t0,
      cacheHit: cacheReadIn > 0,
    });
  }

  /** Tempfile cleanup + result passthrough. */
  private finalize<T>(dir: string, result: T): T {
    this.cleanup(dir);
    return result;
  }

  private cleanup(dir: string): void {
    try {
      rmSync(dir, { recursive: true, force: true });
    } catch {
      // best-effort; tmp will reap eventually
    }
  }
}

// ─── Pure helpers ──────────────────────────────────────────────────────

/**
 * Defensive JSON extraction. Models regularly wrap JSON in markdown fences
 * or prefix it with "Here is the analysis:". Slice between the first `{`
 * and the last `}` (greedy) and JSON.parse. Returns null on any failure.
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
