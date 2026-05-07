// test/llm-client-factory.test.ts
//
// Tests for createLLMClient: provider selection via argument, env var, and
// default; all three providers return a valid LLMClient (has stream method).

import { createLLMClient } from '../src/pipeline/createLLMClient';

const ORIG_PROVIDER = process.env['PIPELINE_LLM_PROVIDER'];
afterEach(() => {
    if (ORIG_PROVIDER === undefined) delete process.env['PIPELINE_LLM_PROVIDER'];
    else process.env['PIPELINE_LLM_PROVIDER'] = ORIG_PROVIDER;
});

function isLLMClient(obj: unknown): boolean {
    return typeof obj === 'object' && obj !== null && typeof (obj as Record<string, unknown>)['stream'] === 'function';
}

describe('createLLMClient', () => {
    test('defaults to anthropic when neither arg nor env is set', () => {
        delete process.env['PIPELINE_LLM_PROVIDER'];
        const client = createLLMClient();
        expect(isLLMClient(client)).toBe(true);
        expect(client.constructor.name).toBe('AnthropicLLMClient');
    });

    test('returns BedrockLLMClient when provider="bedrock"', () => {
        const client = createLLMClient('bedrock');
        expect(isLLMClient(client)).toBe(true);
        expect(client.constructor.name).toBe('BedrockLLMClient');
    });

    test('returns ClaudeCodeLLMClient when provider="claude-code"', () => {
        const client = createLLMClient('claude-code');
        expect(isLLMClient(client)).toBe(true);
        expect(client.constructor.name).toBe('ClaudeCodeLLMClient');
    });

    test('returns AnthropicLLMClient when provider="anthropic"', () => {
        const client = createLLMClient('anthropic');
        expect(client.constructor.name).toBe('AnthropicLLMClient');
    });

    test('reads PIPELINE_LLM_PROVIDER env var when no arg', () => {
        process.env['PIPELINE_LLM_PROVIDER'] = 'bedrock';
        const client = createLLMClient();
        expect(client.constructor.name).toBe('BedrockLLMClient');
    });

    test('explicit arg overrides env var', () => {
        process.env['PIPELINE_LLM_PROVIDER'] = 'bedrock';
        const client = createLLMClient('claude-code');
        expect(client.constructor.name).toBe('ClaudeCodeLLMClient');
    });

    test('unknown provider falls back to anthropic', () => {
        const client = createLLMClient('unknown' as never);
        expect(client.constructor.name).toBe('AnthropicLLMClient');
    });
});
