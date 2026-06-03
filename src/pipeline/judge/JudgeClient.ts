// platform-api/src/pipeline/judge/JudgeClient.ts
//
// Claude Code CLI subprocess provider for LLM-as-judge. Spawns `claude -p`
// with --output-format stream-json (same invocation shape as
// `ClaudeCodeLLMClient.ts`) so an operator running on a machine with a
// Claude Code subscription (no Anthropic API key, no Bedrock) executes
// judge calls through their existing CLI session — OAuth tokens in the
// host's Keychain, mounted at /root/.claude in containerized deploys.
//
// Structured output: NOT via tool-use (CLI's -p flag doesn't expose tools
// in the way the SDK does). Instead we use the prompt-asked-JSON pattern
// pioneered by aws-agentcore (judge.ts:62-72) with defensive extraction:
//   1. Build a system prompt that includes the rubric + JSON schema spec.
//   2. Ask the model to "respond with ONLY a JSON object matching the schema".
//   3. Defensively slice between the first { and the last } before JSON.parse
//      — survives stray prose, markdown fences, "Here is the JSON:" preamble.
//   4. On parse failure: ONE corrective retry with the schema repeated and
//      a "your previous response was malformed" preamble.
//
// Non-throwing per audit rule #1: any failure path (CLI crash, parse fail,
// abort, refusal) surfaces as JudgeClientCallResult.output === null with
// failureReason populated. Usage tokens are reported even when parsing
// fails so cost telemetry stays honest.
//
// Cancellation: pass `helpers.abortSignal` from the step handler — we
// register a SIGTERM-then-SIGKILL grace identical to ClaudeCodeLLMClient.

import { spawn } from 'node:child_process';
import type { ChildProcessByStdio } from 'node:child_process';
import type { Readable } from 'node:stream';
import { withAbort } from 'distributed-core/common';
import type { AnthropicUsage, JudgeInput, RawJudgmentToolOutput } from './types';

const CLAUDE_BIN = process.env['CLAUDE_CLI_PATH'] ?? 'claude';
const KILL_GRACE_MS = 500;

// Bedrock model translation — mirrors ClaudeCodeLLMClient.ts so the same
// CLAUDE_CODE_USE_BEDROCK env flag works for the judge.
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

/**
 * Canonical judge system prompt. Builds on top of whatever the caller
 * supplies (rubric prompt + caller-overrideable system). The JSON schema
 * is described inline so the model knows exactly what to produce — no
 * tool-use API available via the CLI -p path.
 */
export const DEFAULT_JUDGE_SYSTEM_PROMPT = [
  'You are a strict, fair evaluator. Your job is to score candidate outputs ',
  'against the rubric criteria the user provides. You must:',
  '  1. Respond with ONLY a JSON object. No prose before or after.',
  '  2. Score every criterion in the rubric — do not skip any.',
  '  3. Use the exact criterionId values from the rubric (do not invent new ids).',
  '  4. Be concise: one or two sentences per rationale.',
  '  5. Never invent scores for criteria the rubric did not list.',
  '',
  'JSON schema (return EXACTLY this shape):',
  '{',
  '  "criteria": [',
  '    {"criterionId": "<id from rubric>", "score": <number on the rubric scale>, "rationale": "<one or two sentences>"}',
  '  ],',
  '  "confidence": <number 0..1, optional>,',
  '  "overallNotes": "<optional brief overall comment>"',
  '}',
].join('\n');

export interface JudgeClientCallResult {
  /** Null when parsing failed AND retry was exhausted (non-throwing failure). */
  output: RawJudgmentToolOutput | null;
  usage: AnthropicUsage;
  rawText: string;
  latencyMs: number;
  cacheHit: boolean;
  failureReason?: 'malformed' | 'overloaded' | 'refusal' | 'aborted' | 'unknown';
}

export interface JudgeClientOpts {
  /** Path to the claude binary. Defaults to env CLAUDE_CLI_PATH then 'claude'. */
  claudeBin?: string;
  /** Test hook — replaces `spawn` for unit tests. Return `ChildProcessByStdio`-shaped. */
  spawnImpl?: typeof spawn;
}

/**
 * Subprocess provider that shells out to `claude -p` and parses the
 * response into a `RawJudgmentToolOutput`. Auth path: the host's Claude
 * Code OAuth credentials (no ANTHROPIC_API_KEY required).
 */
export class JudgeClient {
  private readonly claudeBin: string;
  private readonly spawnImpl: typeof spawn;

  constructor(opts: JudgeClientOpts = {}) {
    this.claudeBin = opts.claudeBin ?? CLAUDE_BIN;
    this.spawnImpl = opts.spawnImpl ?? spawn;
  }

  /**
   * Single judge call. Always returns a `JudgeClientCallResult` — never
   * throws. A malformed model output triggers ONE corrective retry before
   * surfacing `failureReason: 'malformed'` with usage still populated.
   */
  async judge(input: JudgeInput): Promise<JudgeClientCallResult> {
    const t0 = Date.now();
    const first = await this.attempt(input, false);
    if (first.output) return first;
    if (first.failureReason === 'malformed') {
      const retry = await this.attempt(input, true);
      // Combine usage across both attempts so cost telemetry counts every token.
      retry.usage = {
        input_tokens: (first.usage.input_tokens ?? 0) + (retry.usage.input_tokens ?? 0),
        output_tokens: (first.usage.output_tokens ?? 0) + (retry.usage.output_tokens ?? 0),
      };
      retry.latencyMs = Date.now() - t0;
      return retry;
    }
    return first;
  }

  private async attempt(
    input: JudgeInput,
    corrective: boolean,
  ): Promise<JudgeClientCallResult> {
    const t0 = Date.now();

    // Build prompts. System = caller-supplied JSON-instruction wrapper.
    // User prompt is the rubric + candidate (+ reference for v2 comparison
    // mode + corrective preamble on retry).
    const systemPrompt = input.systemPrompt;
    let userPromptParts: string[] = [];
    userPromptParts.push(input.rubricPrompt);
    userPromptParts.push('');
    userPromptParts.push('Candidate output to judge:');
    userPromptParts.push(input.candidateText);
    if (input.referenceText) {
      userPromptParts.push('');
      userPromptParts.push('Reference output (compare against):');
      userPromptParts.push(input.referenceText);
    }
    if (corrective) {
      userPromptParts = [
        'Your previous response was malformed. Respond with ONLY a JSON object ',
        'matching the schema in the system prompt. No prose, no markdown ',
        'fences. Use the criterionId values exactly as they appear in the rubric.',
        '',
        ...userPromptParts,
      ];
    }
    const userPrompt = userPromptParts.join('\n');

    const args = [
      '-p',
      '--output-format', 'stream-json',
      '--include-partial-messages',
      '--no-session-persistence',
      '--verbose',
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
      { stdio: ['ignore', 'pipe', 'pipe'] },
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
    let buffer = '';

    try {
      for await (const chunk of child.stdout as AsyncIterable<Buffer>) {
        if (input.abortSignal?.aborted) {
          return this.finalize({
            output: null,
            usage: { input_tokens: tokensIn, output_tokens: tokensOut },
            rawText: fullResponse,
            latencyMs: Date.now() - t0,
            cacheHit: false,
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
                fullResponse += delta['text'];
              }
            }
          } else if (type === 'assistant') {
            // Non-streaming fallback — only used when stream_event didn't appear
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
        return this.finalize({
          output: null,
          usage: { input_tokens: tokensIn, output_tokens: tokensOut },
          rawText: fullResponse || stderr,
          latencyMs: Date.now() - t0,
          cacheHit: false,
          failureReason: 'unknown',
        });
      }
    } catch (err) {
      const e = err as Error;
      const isAbort = e.name === 'AbortError' || e.message.includes('aborted');
      return this.finalize({
        output: null,
        usage: { input_tokens: tokensIn, output_tokens: tokensOut },
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: isAbort ? 'aborted' : 'unknown',
      });
    } finally {
      abortReg?.dispose?.();
      if (child.exitCode === null && child.signalCode === null) {
        try { child.kill('SIGTERM'); } catch { /* already exited */ }
      }
    }

    if (tokensIn === 0) tokensIn = Math.max(1, Math.round(userPrompt.length / 4));
    if (tokensOut === 0) tokensOut = Math.max(1, Math.round(fullResponse.length / 4));

    // ── Parse the JSON out of the response ─────────────────────────────
    const parsed = extractJsonObject(fullResponse);
    if (!parsed) {
      return this.finalize({
        output: null,
        usage: { input_tokens: tokensIn, output_tokens: tokensOut },
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: 'malformed',
      });
    }

    // Validate the shape minimally — `criteria` array of `{criterionId, score, rationale}`.
    const valid = validateRawJudgment(parsed);
    if (!valid) {
      return this.finalize({
        output: null,
        usage: { input_tokens: tokensIn, output_tokens: tokensOut },
        rawText: fullResponse,
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: 'malformed',
      });
    }

    return this.finalize({
      output: valid,
      usage: { input_tokens: tokensIn, output_tokens: tokensOut },
      rawText: fullResponse,
      latencyMs: Date.now() - t0,
      cacheHit: false,
    });
  }

  /** Hook for subclassing/testing. Currently a passthrough. */
  private finalize(result: JudgeClientCallResult): JudgeClientCallResult {
    return result;
  }
}

// ─── Pure helpers ──────────────────────────────────────────────────────

/**
 * Defensive JSON extraction (aws-agentcore judge.ts pattern). Models
 * regularly wrap JSON in markdown fences or precede it with prose like
 * "Here is my judgment:". Slice between the first `{` and the last `}`
 * (greedy) and JSON.parse. Returns null on any failure.
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

/**
 * Minimal validator for the raw tool-output shape. Type-narrows on
 * success so the caller doesn't need a second cast.
 */
function validateRawJudgment(value: unknown): RawJudgmentToolOutput | null {
  if (!value || typeof value !== 'object') return null;
  const v = value as Record<string, unknown>;
  if (!Array.isArray(v.criteria)) return null;
  const criteria: RawJudgmentToolOutput['criteria'] = [];
  for (const item of v.criteria) {
    if (!item || typeof item !== 'object') return null;
    const c = item as Record<string, unknown>;
    if (typeof c.criterionId !== 'string') return null;
    if (typeof c.score !== 'number' && typeof c.score !== 'string') return null;
    if (typeof c.rationale !== 'string') return null;
    criteria.push({
      criterionId: c.criterionId,
      score: c.score as number | string,
      rationale: c.rationale,
    });
  }
  const out: RawJudgmentToolOutput = { criteria };
  if (typeof v.confidence === 'number') out.confidence = v.confidence;
  if (typeof v.overallNotes === 'string') out.overallNotes = v.overallNotes;
  return out;
}
