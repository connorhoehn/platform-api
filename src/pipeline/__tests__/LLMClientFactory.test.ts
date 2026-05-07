// social-api/src/pipeline/__tests__/LLMClientFactory.test.ts
//
// Tests for createLLMClient factory: provider selection by argument and env var.

import { createLLMClient } from '../factories/LLMClientFactory';
import { AnthropicLLMClient } from '../AnthropicLLMClient';
import { BedrockLLMClient } from '../BedrockLLMClient';
import { ClaudeCodeLLMClient } from '../ClaudeCodeLLMClient';

const originalEnv = process.env.PIPELINE_LLM_PROVIDER;

afterEach(() => {
    if (originalEnv === undefined) {
        delete process.env.PIPELINE_LLM_PROVIDER;
    } else {
        process.env.PIPELINE_LLM_PROVIDER = originalEnv;
    }
});

describe('createLLMClient — explicit provider argument', () => {
    test('returns AnthropicLLMClient for provider="anthropic"', () => {
        const client = createLLMClient('anthropic');
        expect(client).toBeInstanceOf(AnthropicLLMClient);
    });

    test('returns BedrockLLMClient for provider="bedrock"', () => {
        const client = createLLMClient('bedrock');
        expect(client).toBeInstanceOf(BedrockLLMClient);
    });

    test('returns ClaudeCodeLLMClient for provider="claude-code"', () => {
        const client = createLLMClient('claude-code');
        expect(client).toBeInstanceOf(ClaudeCodeLLMClient);
    });
});

describe('createLLMClient — env var fallback', () => {
    test('uses PIPELINE_LLM_PROVIDER=bedrock when no argument', () => {
        process.env.PIPELINE_LLM_PROVIDER = 'bedrock';
        const client = createLLMClient();
        expect(client).toBeInstanceOf(BedrockLLMClient);
    });

    test('uses PIPELINE_LLM_PROVIDER=claude-code when no argument', () => {
        process.env.PIPELINE_LLM_PROVIDER = 'claude-code';
        const client = createLLMClient();
        expect(client).toBeInstanceOf(ClaudeCodeLLMClient);
    });

    test('defaults to AnthropicLLMClient when env var unset', () => {
        delete process.env.PIPELINE_LLM_PROVIDER;
        const client = createLLMClient();
        expect(client).toBeInstanceOf(AnthropicLLMClient);
    });
});
