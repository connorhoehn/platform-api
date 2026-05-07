// Re-export the canonical LLMClient contract from distributed-core. Concrete
// implementations (AnthropicLLMClient, BedrockLLMClient) live in this folder
// and `implements LLMClient` against this re-exported type.
export type { LLMClient, LLMChunk, LLMStreamOptions } from 'distributed-core';
