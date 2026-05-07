import type { LLMClient } from '../LLMClient';
import { AnthropicLLMClient } from '../AnthropicLLMClient';
import { BedrockLLMClient } from '../BedrockLLMClient';
import { ClaudeCodeLLMClient } from '../ClaudeCodeLLMClient';

export type LLMProvider = 'anthropic' | 'bedrock' | 'claude-code';

export function createLLMClient(provider?: LLMProvider): LLMClient {
  const resolved: LLMProvider =
    provider ??
    (process.env['PIPELINE_LLM_PROVIDER'] as LLMProvider | undefined) ??
    'anthropic';

  switch (resolved) {
    case 'bedrock':
      return new BedrockLLMClient();
    case 'claude-code':
      return new ClaudeCodeLLMClient();
    case 'anthropic':
    default:
      return new AnthropicLLMClient();
  }
}
