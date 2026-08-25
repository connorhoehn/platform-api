// social-api/src/pipeline/__tests__/error-classification.test.ts

import { describe, it, expect } from '@jest/globals';
import { classifyPipelineError, classifyRetryability } from '../error-classification';

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
