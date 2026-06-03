// platform-api/src/pipeline/judge/JudgeClient.ts
//
// Anthropic SDK call for the llm:judge step. Uses tool-use for constrained
// structured output (`record_judgment` tool) — much more reliable than
// asking for JSON in the prompt. Prompt-caches the system + rubric +
// tool schema via `cache_control: ephemeral` so repeated runs of the same
// rubric pay ~10% of the regular input cost.
//
// Lazy-requires `@anthropic-ai/sdk` so the module is only loaded when
// `judge()` is called — matches the existing AnthropicLLMClient pattern.
//
// Critical contracts (per audit synthesis):
//   1. Judge failures are non-throwing — return null + capture usage.
//      A judge outage MUST NOT cascade into pipeline 5xxs.
//   2. Aggregation is computed downstream (`aggregateScore.ts`), never
//      asked of the model — letting the model do the math collapses
//      auditability.
//   3. `tool_choice` is forced — no free-form output ever.

import type {
  AnthropicUsage,
  JudgeInput,
  RawJudgmentToolOutput,
} from './types';

// Tool spec the model invokes to record its judgment. Mirrors the criterion
// shape but accepts the model's chosen score format (we normalize at the
// handler layer). Per-criterion `rationale` is bounded so a verbose model
// can't blow the output token budget.
export const RECORD_JUDGMENT_TOOL = {
  name: 'record_judgment',
  description:
    'Record the structured judgment of the candidate output against each rubric criterion. ' +
    'Score each criterion on the scale the rubric specifies. Provide a one-sentence rationale per criterion.',
  input_schema: {
    type: 'object',
    required: ['criteria'],
    properties: {
      criteria: {
        type: 'array',
        items: {
          type: 'object',
          required: ['criterionId', 'score', 'rationale'],
          properties: {
            criterionId: { type: 'string' },
            score: {
              // accept either a number (numeric scales) or string (pass-fail / binary)
              type: ['number', 'string'],
            },
            rationale: { type: 'string', maxLength: 1000 },
          },
        },
      },
      overallNotes: { type: 'string', maxLength: 2000 },
      confidence: { type: 'number', minimum: 0, maximum: 1 },
    },
  },
} as const;

/**
 * Canonical judge system prompt. Kept terse — the rubric prompt carries
 * the actual evaluation logic. Marked cacheable along with the rubric so
 * repeated judgments on different inputs benefit from prompt caching.
 */
export const DEFAULT_JUDGE_SYSTEM_PROMPT = [
  'You are a strict, fair evaluator. Your job is to score candidate outputs ',
  'against the rubric criteria the user provides. You must:',
  '  1. Use the `record_judgment` tool. Do not answer in free text.',
  '  2. Score every criterion in the rubric — do not skip any.',
  '  3. Use the exact criterionId values from the rubric (do not invent new ids).',
  '  4. Be concise: one or two sentences per rationale.',
  '  5. Never invent scores for criteria the rubric did not list.',
].join('');

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
  apiKey?: string;
  /** Lazy SDK client override — primarily for tests. */
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  anthropicClient?: any;
}

export class JudgeClient {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private clientRef: any = null;
  private readonly apiKey?: string;

  constructor(opts: JudgeClientOpts = {}) {
    this.apiKey = opts.apiKey ?? process.env.ANTHROPIC_API_KEY;
    if (opts.anthropicClient) this.clientRef = opts.anthropicClient;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  private async getClient(): Promise<any> {
    if (this.clientRef) return this.clientRef;
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const sdk = require('@anthropic-ai/sdk');
    const AnthropicCtor = sdk.default ?? sdk.Anthropic ?? sdk;
    this.clientRef = new AnthropicCtor({ apiKey: this.apiKey });
    return this.clientRef;
  }

  /**
   * Single judge call. Always returns a `JudgeClientCallResult` — never
   * throws. A malformed tool-output triggers ONE corrective retry before
   * surfacing `failureReason: 'malformed'` with usage still populated
   * (we paid for those tokens; observability stays honest).
   *
   * Cancellation: pass the executor's `helpers.abortSignal` so the SDK's
   * underlying fetch is aborted on `executor.cancel()`. Without this, a
   * wedged judge call keeps running for its full timeout.
   */
  async judge(input: JudgeInput): Promise<JudgeClientCallResult> {
    const t0 = Date.now();
    try {
      const result = await this.attempt(input, false);
      if (result.output) return result;
      // Malformed → one corrective retry with the schema appended
      if (result.failureReason === 'malformed') {
        return await this.attempt(input, true);
      }
      return result;
    } catch (err) {
      // Per audit rule #1: judge failures non-throwing. Surface the reason
      // but always return.
      const e = err as Error;
      const isAbort = e.name === 'AbortError' || e.message.includes('aborted');
      return {
        output: null,
        usage: { input_tokens: 0, output_tokens: 0 },
        rawText: '',
        latencyMs: Date.now() - t0,
        cacheHit: false,
        failureReason: isAbort ? 'aborted' : 'unknown',
      };
    }
  }

  private async attempt(
    input: JudgeInput,
    corrective: boolean,
  ): Promise<JudgeClientCallResult> {
    const t0 = Date.now();
    const client = await this.getClient();

    // Build the messages list. The rubric + system + tool spec are all
    // marked cacheable; only the candidate text is per-call uncacheable.
    const systemBlocks = [
      { type: 'text', text: input.systemPrompt, cache_control: { type: 'ephemeral' } },
      { type: 'text', text: input.rubricPrompt, cache_control: { type: 'ephemeral' } },
    ];

    let userText = `Candidate output to judge:\n\n${input.candidateText}`;
    if (input.referenceText) {
      userText += `\n\nReference output (compare against):\n\n${input.referenceText}`;
    }
    if (corrective) {
      userText +=
        '\n\nNote: your previous response was malformed. ' +
        'Call the `record_judgment` tool with one entry per rubric criterion. ' +
        'Use the criterionId values exactly as they appear in the rubric.';
    }

    const response = await client.messages.create({
      model: input.model,
      max_tokens: input.maxOutputTokens,
      system: systemBlocks,
      tools: [
        {
          ...RECORD_JUDGMENT_TOOL,
          cache_control: { type: 'ephemeral' },
        },
      ],
      tool_choice: { type: 'tool', name: 'record_judgment' },
      messages: [{ role: 'user', content: userText }],
      // SDK threads this through to the fetch call. Optional but
      // load-bearing — without it, cancellation doesn't propagate.
      ...(input.abortSignal ? { signal: input.abortSignal } : {}),
    });

    const usage: AnthropicUsage = {
      input_tokens: response.usage?.input_tokens ?? 0,
      output_tokens: response.usage?.output_tokens ?? 0,
      cache_creation_input_tokens: response.usage?.cache_creation_input_tokens,
      cache_read_input_tokens: response.usage?.cache_read_input_tokens,
    };
    const cacheHit = (usage.cache_read_input_tokens ?? 0) > 0;

    // Find the tool_use block. SDK shape: response.content is an array
    // of `{type:'text',...} | {type:'tool_use',...}` blocks.
    interface ContentBlock {
      type: string;
      name?: string;
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      input?: any;
      text?: string;
    }
    const blocks = (response.content as ContentBlock[] | undefined) ?? [];
    const toolBlock = blocks.find((b) => b.type === 'tool_use' && b.name === 'record_judgment');

    const rawText = blocks
      .filter((b) => b.type === 'text' && typeof b.text === 'string')
      .map((b) => b.text)
      .join('\n');

    if (!toolBlock || !toolBlock.input) {
      // Safety refusal usually surfaces as a text block with no tool_use
      const stopReason = response.stop_reason as string | undefined;
      const refusal = stopReason === 'refusal' || stopReason === 'safety';
      return {
        output: null,
        usage,
        rawText,
        latencyMs: Date.now() - t0,
        cacheHit,
        failureReason: refusal ? 'refusal' : 'malformed',
      };
    }

    return {
      output: toolBlock.input as RawJudgmentToolOutput,
      usage,
      rawText,
      latencyMs: Date.now() - t0,
      cacheHit,
    };
  }
}
