export type ErrorCategory =
  | 'NetworkError'
  | 'TimeoutError'
  | 'ValidationError'
  | 'LLMError'
  | 'RegistryConflict'
  | 'Refused'
  | 'UnknownError';

export type Retryability = 'retryable' | 'terminal' | 'unknown';

const CATEGORY_RETRYABILITY: Record<ErrorCategory, Retryability> = {
  NetworkError:     'retryable',
  TimeoutError:     'retryable',
  ValidationError:  'terminal',
  RegistryConflict: 'terminal',
  Refused:          'terminal',
  LLMError:         'retryable',  // refined by classifyRetryability for 4xx auth
  UnknownError:     'unknown',
};

/**
 * The codes a step uses to say it looked at the request and said no — the
 * edit did not do what was asked, the requester may not edit, the base
 * revision moved, the request is malformed. A refusal is a decision about
 * the inputs the run already has; running the same step again over the same
 * recorded inputs decides the same way. So it is terminal: no retry, no
 * resume — the person changes the ask (or the sharing, or the base) and
 * starts a new run.
 */
export const REFUSAL_CODES = ['constraint-not-met', 'forbidden', 'revision-conflict', 'invalid-request'] as const;
export type RefusalCode = (typeof REFUSAL_CODES)[number];

// A refusal is spelled `<step> <code>: <reason>` by the step that refuses
// (`deck-revise constraint-not-met: Title has 2 words; …`), optionally
// behind the `<nodeId>: ` prefix the run row stores. The code word is
// matched only in that position: a model's "403 Forbidden" mid-sentence is
// an LLMError, not a refusal.
const REFUSAL_RE = new RegExp(`(?:^|: )[a-z][\\w-]* (${REFUSAL_CODES.join('|')}): `, 'i');

/** The refusal code a step failed with, or null when the failure is not a refusal. */
export function refusalCodeOf(message: string): RefusalCode | null {
  const m = REFUSAL_RE.exec(message);
  return m ? (m[1].toLowerCase() as RefusalCode) : null;
}

/**
 * Returns whether an error with the given category and message is safe to
 * retry automatically. LLMErrors are retryable by default except for 4xx
 * authentication/permission failures, which require human intervention.
 */
export function classifyRetryability(category: ErrorCategory, message: string): Retryability {
  if (category === 'LLMError') {
    const lower = message.toLowerCase();
    if (
      lower.includes('unauthorized') ||
      lower.includes('invalid api key') ||
      lower.includes('invalid_api_key') ||
      lower.includes('authentication') ||
      lower.includes('permission denied') ||
      lower.includes('403') ||
      lower.includes('401')
    ) {
      return 'terminal';
    }
  }
  return CATEGORY_RETRYABILITY[category];
}

export function classifyPipelineError(message: string, stack?: string): ErrorCategory {
  // Before the keyword scans: `revision-conflict` reads as a conflict,
  // `invalid-request` as a validation error, and the other two as unknown.
  if (refusalCodeOf(message)) return 'Refused';

  const lowerMsg = message.toLowerCase();
  const lowerStack = (stack || '').toLowerCase();
  const combined = lowerMsg + ' ' + lowerStack;

  if (
    combined.includes('econnrefused') ||
    combined.includes('enotfound') ||
    combined.includes('network') ||
    combined.includes('fetch failed') ||
    combined.includes('socket hang up')
  ) {
    return 'NetworkError';
  }

  if (
    combined.includes('conflict') ||
    combined.includes('lock') ||
    combined.includes('lease') ||
    combined.includes('registry') ||
    combined.includes('version mismatch')
  ) {
    return 'RegistryConflict';
  }

  if (
    combined.includes('timeout') ||
    combined.includes('timed out') ||
    combined.includes('etimedout')
  ) {
    return 'TimeoutError';
  }

  if (
    combined.includes('rate limit') ||
    combined.includes('rate_limit') ||
    combined.includes('model_error') ||
    combined.includes('anthropic') ||
    combined.includes('bedrock') ||
    combined.includes('overloaded') ||
    combined.includes('quota exceeded')
  ) {
    return 'LLMError';
  }

  if (
    combined.includes('validation') ||
    combined.includes('required field') ||
    combined.includes('invalid') ||
    combined.includes('schema') ||
    combined.includes('type mismatch') ||
    // Executor grammar rejections — e.g. `Condition expression is not
    // supported: "1 === 1". Supported grammar: context.<path> === <literal>.`
    // (distributed-core PipelineExecutor fails loudly instead of guessing).
    // These are authoring defects, not mystery failures — they must never
    // surface as "Unknown Error", and retrying can never fix them.
    combined.includes('not supported') ||
    combined.includes('unsupported')
  ) {
    return 'ValidationError';
  }

  return 'UnknownError';
}
