// social-api/src/pipeline/factories/__tests__/LLMClientFactory.test.ts
//
// Tests for createLLMClient: provider selection via explicit arg and
// PIPELINE_LLM_PROVIDER env var, with default fallback to anthropic.

const mockAnthropicCtor = jest.fn();
const mockBedrockCtor = jest.fn();
const mockClaudeCodeCtor = jest.fn();

jest.mock('../../AnthropicLLMClient', () => ({
  AnthropicLLMClient: jest.fn().mockImplementation(() => {
    mockAnthropicCtor();
    return { _type: 'anthropic' };
  }),
}));

jest.mock('../../BedrockLLMClient', () => ({
  BedrockLLMClient: jest.fn().mockImplementation(() => {
    mockBedrockCtor();
    return { _type: 'bedrock' };
  }),
}));

jest.mock('../../ClaudeCodeLLMClient', () => ({
  ClaudeCodeLLMClient: jest.fn().mockImplementation(() => {
    mockClaudeCodeCtor();
    return { _type: 'claude-code' };
  }),
}));

import { createLLMClient } from '../LLMClientFactory';

const originalEnv = process.env;

beforeEach(() => {
  process.env = { ...originalEnv };
  delete process.env.PIPELINE_LLM_PROVIDER;
});

afterAll(() => {
  process.env = originalEnv;
});

describe('createLLMClient — explicit provider argument', () => {
  it('returns an AnthropicLLMClient when provider="anthropic"', () => {
    const client = createLLMClient('anthropic');
    expect((client as { _type: string })._type).toBe('anthropic');
  });

  it('returns a BedrockLLMClient when provider="bedrock"', () => {
    const client = createLLMClient('bedrock');
    expect((client as { _type: string })._type).toBe('bedrock');
  });

  it('returns a ClaudeCodeLLMClient when provider="claude-code"', () => {
    const client = createLLMClient('claude-code');
    expect((client as { _type: string })._type).toBe('claude-code');
  });
});

describe('createLLMClient — PIPELINE_LLM_PROVIDER env var', () => {
  it('uses bedrock when env=bedrock and no explicit arg', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'bedrock';
    const client = createLLMClient();
    expect((client as { _type: string })._type).toBe('bedrock');
  });

  it('uses claude-code when env=claude-code and no explicit arg', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'claude-code';
    const client = createLLMClient();
    expect((client as { _type: string })._type).toBe('claude-code');
  });

  it('explicit arg overrides env var', () => {
    process.env.PIPELINE_LLM_PROVIDER = 'bedrock';
    const client = createLLMClient('claude-code');
    expect((client as { _type: string })._type).toBe('claude-code');
  });
});

describe('createLLMClient — default fallback', () => {
  it('defaults to anthropic when no arg and no env var', () => {
    const client = createLLMClient();
    expect((client as { _type: string })._type).toBe('anthropic');
  });
});
