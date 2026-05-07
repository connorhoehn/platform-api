// social-api/src/pipeline/llm-pricing.ts
//
// Hardcoded LLM pricing table for cost estimation. Prices are per million tokens.
// Can be made configurable (env or DDB table) in a later iteration.

export interface ModelPricing {
  /** USD per million input tokens. */
  inputPerMTok: number;
  /** USD per million output tokens. */
  outputPerMTok: number;
}

/**
 * Known model pricing. Keys are matched case-insensitively against the model
 * string from pipeline:llm:prompt events.
 */
const PRICING_TABLE: Record<string, ModelPricing> = {
  'claude-3-haiku': { inputPerMTok: 0.25, outputPerMTok: 1.25 },
  'claude-3-sonnet': { inputPerMTok: 3, outputPerMTok: 15 },
  'claude-sonnet-4-6': { inputPerMTok: 3, outputPerMTok: 15 },
  'claude-3-opus': { inputPerMTok: 15, outputPerMTok: 75 },
};

/** Fallback for unknown models. */
const DEFAULT_PRICING: ModelPricing = { inputPerMTok: 1, outputPerMTok: 5 };

/**
 * Resolve pricing for a model string. Performs case-insensitive prefix matching
 * against known models, falling back to the default pricing for unknowns.
 */
export function getPricing(model: string): ModelPricing {
  const lower = model.toLowerCase();
  // Exact match first
  if (PRICING_TABLE[lower]) return PRICING_TABLE[lower];
  // Prefix match (e.g. "claude-3-haiku-20240307" → "claude-3-haiku")
  for (const [key, pricing] of Object.entries(PRICING_TABLE)) {
    if (lower.startsWith(key)) return pricing;
  }
  return DEFAULT_PRICING;
}

/**
 * Compute estimated cost in USD given token counts and a model identifier.
 */
export function computeCost(model: string, tokensIn: number, tokensOut: number): number {
  const pricing = getPricing(model);
  return (tokensIn / 1_000_000) * pricing.inputPerMTok + (tokensOut / 1_000_000) * pricing.outputPerMTok;
}
