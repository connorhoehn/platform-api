// test/pipeline-error-classification.test.ts
//
// Tests for classifyPipelineError and classifyRetryability — pure functions
// with no external dependencies.

import {
    classifyPipelineError,
    classifyRetryability,
} from '../src/pipeline/error-classification';

// ─── classifyPipelineError ────────────────────────────────────────────────────

describe('classifyPipelineError — NetworkError', () => {
    test('ECONNREFUSED → NetworkError', () => {
        expect(classifyPipelineError('connect ECONNREFUSED 127.0.0.1:6379')).toBe('NetworkError');
    });

    test('ENOTFOUND → NetworkError', () => {
        expect(classifyPipelineError('getaddrinfo ENOTFOUND api.example.com')).toBe('NetworkError');
    });

    test('fetch failed → NetworkError', () => {
        expect(classifyPipelineError('fetch failed')).toBe('NetworkError');
    });

    test('socket hang up → NetworkError', () => {
        expect(classifyPipelineError('socket hang up')).toBe('NetworkError');
    });

    test('network error in message → NetworkError', () => {
        expect(classifyPipelineError('network error occurred')).toBe('NetworkError');
    });
});

describe('classifyPipelineError — TimeoutError', () => {
    test('timeout keyword → TimeoutError', () => {
        expect(classifyPipelineError('request timeout after 5000ms')).toBe('TimeoutError');
    });

    test('timed out → TimeoutError', () => {
        expect(classifyPipelineError('connection timed out')).toBe('TimeoutError');
    });

    test('ETIMEDOUT → TimeoutError', () => {
        expect(classifyPipelineError('connect ETIMEDOUT')).toBe('TimeoutError');
    });
});

describe('classifyPipelineError — RegistryConflict', () => {
    test('conflict → RegistryConflict', () => {
        expect(classifyPipelineError('version conflict')).toBe('RegistryConflict');
    });

    test('lock → RegistryConflict', () => {
        expect(classifyPipelineError('lock acquisition failed')).toBe('RegistryConflict');
    });

    test('lease → RegistryConflict', () => {
        expect(classifyPipelineError('lease expired')).toBe('RegistryConflict');
    });

    test('registry → RegistryConflict', () => {
        expect(classifyPipelineError('registry write failed')).toBe('RegistryConflict');
    });

    test('version mismatch → RegistryConflict', () => {
        expect(classifyPipelineError('version mismatch detected')).toBe('RegistryConflict');
    });
});

describe('classifyPipelineError — LLMError', () => {
    test('rate limit → LLMError', () => {
        expect(classifyPipelineError('rate limit exceeded')).toBe('LLMError');
    });

    test('rate_limit → LLMError', () => {
        expect(classifyPipelineError('error: rate_limit')).toBe('LLMError');
    });

    test('model_error → LLMError', () => {
        expect(classifyPipelineError('model_error from provider')).toBe('LLMError');
    });

    test('anthropic in message → LLMError', () => {
        expect(classifyPipelineError('anthropic api error')).toBe('LLMError');
    });

    test('bedrock in message → LLMError', () => {
        expect(classifyPipelineError('bedrock request failed')).toBe('LLMError');
    });

    test('overloaded → LLMError', () => {
        expect(classifyPipelineError('service overloaded')).toBe('LLMError');
    });

    test('quota exceeded → LLMError', () => {
        expect(classifyPipelineError('quota exceeded')).toBe('LLMError');
    });
});

describe('classifyPipelineError — ValidationError', () => {
    test('validation → ValidationError', () => {
        expect(classifyPipelineError('validation failed for field')).toBe('ValidationError');
    });

    test('required field → ValidationError', () => {
        expect(classifyPipelineError('required field missing')).toBe('ValidationError');
    });

    test('invalid input → ValidationError', () => {
        expect(classifyPipelineError('invalid input provided')).toBe('ValidationError');
    });

    test('schema → ValidationError', () => {
        expect(classifyPipelineError('schema mismatch')).toBe('ValidationError');
    });

    test('type mismatch → ValidationError', () => {
        expect(classifyPipelineError('type mismatch in payload')).toBe('ValidationError');
    });
});

describe('classifyPipelineError — UnknownError', () => {
    test('unrecognized message → UnknownError', () => {
        expect(classifyPipelineError('something unexpected happened')).toBe('UnknownError');
    });

    test('empty message → UnknownError', () => {
        expect(classifyPipelineError('')).toBe('UnknownError');
    });
});

describe('classifyPipelineError — stack fallback', () => {
    test('uses stack when message does not match', () => {
        expect(classifyPipelineError('unexpected error', 'at fetch -> ECONNREFUSED')).toBe('NetworkError');
    });

    test('message takes precedence over stack for different categories', () => {
        // message has "timeout", stack has "validation" — timeout wins (checked first)
        expect(classifyPipelineError('timeout', 'validation stack')).toBe('TimeoutError');
    });
});

// ─── classifyRetryability ─────────────────────────────────────────────────────

describe('classifyRetryability', () => {
    test('NetworkError is retryable', () => {
        expect(classifyRetryability('NetworkError', 'connection refused')).toBe('retryable');
    });

    test('TimeoutError is retryable', () => {
        expect(classifyRetryability('TimeoutError', 'timed out')).toBe('retryable');
    });

    test('ValidationError is terminal', () => {
        expect(classifyRetryability('ValidationError', 'missing field')).toBe('terminal');
    });

    test('RegistryConflict is terminal', () => {
        expect(classifyRetryability('RegistryConflict', 'version conflict')).toBe('terminal');
    });

    test('UnknownError is unknown', () => {
        expect(classifyRetryability('UnknownError', 'unexpected')).toBe('unknown');
    });

    test('LLMError is retryable for generic rate limit', () => {
        expect(classifyRetryability('LLMError', 'rate limit exceeded')).toBe('retryable');
    });

    test('LLMError with unauthorized → terminal', () => {
        expect(classifyRetryability('LLMError', 'Unauthorized request')).toBe('terminal');
    });

    test('LLMError with invalid api key → terminal', () => {
        expect(classifyRetryability('LLMError', 'invalid api key provided')).toBe('terminal');
    });

    test('LLMError with invalid_api_key → terminal', () => {
        expect(classifyRetryability('LLMError', 'invalid_api_key')).toBe('terminal');
    });

    test('LLMError with authentication → terminal', () => {
        expect(classifyRetryability('LLMError', 'authentication failed')).toBe('terminal');
    });

    test('LLMError with permission denied → terminal', () => {
        expect(classifyRetryability('LLMError', 'permission denied')).toBe('terminal');
    });

    test('LLMError with 403 → terminal', () => {
        expect(classifyRetryability('LLMError', 'HTTP 403 Forbidden')).toBe('terminal');
    });

    test('LLMError with 401 → terminal', () => {
        expect(classifyRetryability('LLMError', 'error 401')).toBe('terminal');
    });
});
