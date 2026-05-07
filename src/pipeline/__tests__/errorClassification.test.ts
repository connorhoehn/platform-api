import { classifyPipelineError, classifyRetryability } from '../error-classification';

describe('classifyPipelineError', () => {
  it('classifies network errors', () => {
    expect(classifyPipelineError('ECONNREFUSED connecting to upstream')).toBe('NetworkError');
    expect(classifyPipelineError('fetch failed: ENOTFOUND')).toBe('NetworkError');
    expect(classifyPipelineError('socket hang up')).toBe('NetworkError');
  });

  it('classifies timeout errors', () => {
    expect(classifyPipelineError('request timed out after 30s')).toBe('TimeoutError');
    expect(classifyPipelineError('ETIMEDOUT')).toBe('TimeoutError');
  });

  it('classifies LLM errors', () => {
    expect(classifyPipelineError('rate limit exceeded')).toBe('LLMError');
    expect(classifyPipelineError('Anthropic API overloaded')).toBe('LLMError');
    expect(classifyPipelineError('quota exceeded for model')).toBe('LLMError');
  });

  it('classifies validation errors', () => {
    expect(classifyPipelineError('validation failed: required field missing')).toBe('ValidationError');
    expect(classifyPipelineError('invalid schema')).toBe('ValidationError');
  });

  it('classifies registry conflicts', () => {
    expect(classifyPipelineError('lease conflict')).toBe('RegistryConflict');
    expect(classifyPipelineError('version mismatch')).toBe('RegistryConflict');
  });

  it('returns UnknownError for unrecognized messages', () => {
    expect(classifyPipelineError('something went wrong')).toBe('UnknownError');
  });
});

describe('classifyRetryability', () => {
  it('marks NetworkError and TimeoutError as retryable', () => {
    expect(classifyRetryability('NetworkError', 'ECONNREFUSED')).toBe('retryable');
    expect(classifyRetryability('TimeoutError', 'request timed out')).toBe('retryable');
  });

  it('marks ValidationError and RegistryConflict as terminal', () => {
    expect(classifyRetryability('ValidationError', 'invalid schema')).toBe('terminal');
    expect(classifyRetryability('RegistryConflict', 'lock conflict')).toBe('terminal');
  });

  it('marks generic LLMError as retryable (rate limit, overloaded)', () => {
    expect(classifyRetryability('LLMError', 'rate limit exceeded')).toBe('retryable');
    expect(classifyRetryability('LLMError', 'Anthropic overloaded')).toBe('retryable');
    expect(classifyRetryability('LLMError', '500 Internal Server Error')).toBe('retryable');
  });

  it('marks LLMError 4xx auth failures as terminal', () => {
    expect(classifyRetryability('LLMError', '401 unauthorized')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'invalid api key')).toBe('terminal');
    expect(classifyRetryability('LLMError', '403 permission denied')).toBe('terminal');
    expect(classifyRetryability('LLMError', 'authentication failed')).toBe('terminal');
  });

  it('marks UnknownError as unknown', () => {
    expect(classifyRetryability('UnknownError', 'something went wrong')).toBe('unknown');
  });
});
