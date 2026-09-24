// social-api/src/pipeline/__tests__/error-classification.test.ts

import { describe, it, expect } from '@jest/globals';
import { classifyPipelineError, classifyRetryability, refusalCodeOf } from '../error-classification';

describe('classifyPipelineError()', () => {
  it('returns NetworkError for connection-refused messages', () => {
    expect(classifyPipelineError('connect ECONNREFUSED 127.0.0.1:5432')).toBe('NetworkError');
    expect(classifyPipelineError('fetch failed')).toBe('NetworkError');
    expect(classifyPipelineError('socket hang up')).toBe('NetworkError');
    expect(classifyPipelineError('ENOTFOUND api.example.com')).toBe('NetworkError');
  });

  it('returns TimeoutError for timeout messages', () => {
    expect(classifyPipelineError('request timed out after 30s')).toBe('TimeoutError');
    expect(classifyPipelineError('ETIMEDOUT')).toBe('TimeoutError');
    expect(classifyPipelineError('timeout waiting for response')).toBe('TimeoutError');
  });

  it('returns LLMError for Anthropic / rate-limit messages', () => {
    expect(classifyPipelineError('anthropic API returned 500')).toBe('LLMError');
    expect(classifyPipelineError('rate limit exceeded')).toBe('LLMError');
    expect(classifyPipelineError('rate_limit_error')).toBe('LLMError');
    expect(classifyPipelineError('model_error: overloaded')).toBe('LLMError');
    expect(classifyPipelineError('bedrock request failed')).toBe('LLMError');
    expect(classifyPipelineError('quota exceeded')).toBe('LLMError');
  });

  it('returns ValidationError for schema / invalid-input messages', () => {
    expect(classifyPipelineError('validation failed: required field missing')).toBe('ValidationError');
    expect(classifyPipelineError('schema mismatch')).toBe('ValidationError');
    expect(classifyPipelineError('type mismatch in step output')).toBe('ValidationError');
  });

  // Regression: the executor's deliberate grammar rejection surfaced in the
  // FE error popover as "Unknown Error". A well-formed executor failure that
  // names its own cause is the opposite of unknown — it is an authoring
  // (validation) defect and retrying can never fix it.
  it('classifies executor "not supported" step errors as ValidationError, never Unknown', () => {
    const executorMsg =
      'Condition expression is not supported: "1 === 1". ' +
      'Supported grammar: context.<path> === <string|number|boolean literal>.';
    expect(classifyPipelineError(executorMsg)).not.toBe('UnknownError');
    expect(classifyPipelineError(executorMsg)).toBe('ValidationError');
    expect(classifyRetryability('ValidationError', executorMsg)).toBe('terminal');
    // Generic unsupported-operation step errors get the same treatment.
    expect(classifyPipelineError('unsupported transform type: xslt')).toBe('ValidationError');
  });

  it('returns RegistryConflict for lock/version-conflict messages', () => {
    expect(classifyPipelineError('registry conflict detected')).toBe('RegistryConflict');
    expect(classifyPipelineError('lock acquisition failed')).toBe('RegistryConflict');
    expect(classifyPipelineError('version mismatch in entity store')).toBe('RegistryConflict');
  });

  it('returns UnknownError for unrecognised messages', () => {
    expect(classifyPipelineError('something completely unrecognised happened')).toBe('UnknownError');
    expect(classifyPipelineError('')).toBe('UnknownError');
  });

  it('matches against stack trace text too', () => {
    expect(
      classifyPipelineError('execution failed', 'at fetch (node:internal)\nECONNREFUSED'),
    ).toBe('NetworkError');
  });
});

describe('classifyRetryability()', () => {
  it('NetworkError and TimeoutError are retryable', () => {
    expect(classifyRetryability('NetworkError', 'connection refused')).toBe('retryable');
    expect(classifyRetryability('TimeoutError', 'timed out')).toBe('retryable');
  });

  it('ValidationError and RegistryConflict are terminal', () => {
    expect(classifyRetryability('ValidationError', 'schema invalid')).toBe('terminal');
    expect(classifyRetryability('RegistryConflict', 'lock held')).toBe('terminal');
  });

  it('UnknownError returns unknown', () => {
    expect(classifyRetryability('UnknownError', 'mystery failure')).toBe('unknown');
  });

  it('LLMError is retryable by default', () => {
    expect(classifyRetryability('LLMError', 'server overloaded')).toBe('retryable');
  });

  it('LLMError is terminal for auth failures', () => {
    expect(classifyRetryability('LLMError', 'Unauthorized')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'Invalid API key')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'invalid_api_key')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'authentication failed')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'Permission denied')).toBe('terminal');
    expect(classifyRetryability('LLMError', '403 Forbidden')).toBe('terminal');
    expect(classifyRetryability('LLMError', '401 Unauthorized')).toBe('terminal');
  });
});

describe('refusals (a step said no; the same inputs decide the same way)', () => {
  it('classifies each refusal code as Refused, terminal — before the keyword scans', () => {
    const cases: Array<[string, string]> = [
      ['deck-revise constraint-not-met: The edit did not do what was asked: Title has 2 words; it must have exactly 0. Measured: Title: 2 → 2 words. Nothing was saved.', 'constraint-not-met'],
      ['deck-revise forbidden: Only the owner and editors can edit this deck.', 'forbidden'],
      ['deck-revise revision-conflict: The deck moved to v8 while this edit was based on v7.', 'revision-conflict'],
      ['deck-revise invalid-request: The edit is missing its document, request id or instruction.', 'invalid-request'],
    ];
    for (const [message, code] of cases) {
      expect(refusalCodeOf(message)).toBe(code);
      expect(classifyPipelineError(message)).toBe('Refused');
      expect(classifyRetryability('Refused', message)).toBe('terminal');
      // The run row stores `<nodeId>: <message>`; the classification is the same.
      expect(classifyPipelineError(`check: ${message}`)).toBe('Refused');
    }
  });

  it('is not fooled by the code words mid-sentence', () => {
    expect(refusalCodeOf('anthropic API returned 403 Forbidden')).toBeNull();
    expect(classifyPipelineError('anthropic API returned 403 Forbidden')).toBe('LLMError');
    expect(refusalCodeOf('registry version mismatch: revision-conflict seen')).toBeNull();
    expect(classifyPipelineError('deck-revise cancelled: The edit was stopped, so nothing was changed.')).toBe('UnknownError');
    expect(classifyPipelineError('deck-revise failed: model unavailable (503)')).not.toBe('Refused');
  });
});

describe('ModelAuth (Loop 29 ppt)', () => {
  it('a Claude CLI login failure is ModelAuth and terminal, not a RegistryConflict', () => {
    const msg = 'deck-revise model-failed: The agent could not answer — claude is not authenticated in this environment: Not logged in · Please run /login';
    expect(classifyPipelineError(msg)).toBe('ModelAuth');
    expect(classifyRetryability('ModelAuth', msg)).toBe('terminal');
    expect(classifyPipelineError('deck-revise model-auth: The agent could not sign in to the model')).toBe('ModelAuth');
  });
  it('"please" alone no longer reads as a lease conflict', () => {
    expect(classifyPipelineError('Please try again later')).toBe('UnknownError');
    expect(classifyPipelineError('lease expired for node')).toBe('RegistryConflict');
  });
});
