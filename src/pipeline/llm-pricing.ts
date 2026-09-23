// platform-api/src/pipeline/llm-pricing.ts
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
 * Pricing by model FAMILY, for the ids that do not start with a table key.
 *
 * Agent steps report the model as the bare family they were configured with —
 * `haiku`, not `claude-3-haiku-20240307` — so prefix matching missed every one
 * of them and they were priced at the unknown-model default. Since agent steps
 * are what this deployment actually runs, that was every priced step.
 *
 * Values are the existing table's, one tier each: this is a matching fix, not a
 * repricing. The table itself is stale — it knows claude-3-* and
 * claude-sonnet-4-6 and nothing from the Claude 5 family — and wants a pass
 * from someone who can check today's published rates.
 */
const FAMILY_PRICING: ReadonlyArray<readonly [string, ModelPricing]> = [
  ['opus', PRICING_TABLE['claude-3-opus']!],
  ['sonnet', PRICING_TABLE['claude-3-sonnet']!],
  ['haiku', PRICING_TABLE['claude-3-haiku']!],
];

/**
 * Resolve pricing for a model string. Case-insensitive: exact key, then prefix,
 * then family, then the unknown-model default.
 */
export function getPricing(model: string): ModelPricing {
  const lower = model.toLowerCase();
  // Exact match first
  if (PRICING_TABLE[lower]) return PRICING_TABLE[lower];
  // Prefix match (e.g. "claude-3-haiku-20240307" → "claude-3-haiku")
  for (const [key, pricing] of Object.entries(PRICING_TABLE)) {
    if (lower.startsWith(key)) return pricing;
  }
  // Family match, so `haiku`, `us.anthropic.claude-opus-5` and
  // `claude-sonnet-5` all price as the tier they are.
  for (const [family, pricing] of FAMILY_PRICING) {
    if (lower.includes(family)) return pricing;
  }
  return DEFAULT_PRICING;
}

/**
 * Whether `model` is one this table actually prices — an exact key, a key
 * prefix, or a family (`haiku`, `sonnet`, `opus`) — rather than falling to the
 * unknown-model default. This is THE list of models a caller may choose: the
 * estimate endpoint (`GET /api/pipelines/:id/estimate?model=`) and a run's
 * trigger-level model override (runOverrides.ts) both refuse anything else,
 * so a run can never be priced at a made-up rate.
 */
export function isPricedModel(model: string): boolean {
  const lower = model.trim().toLowerCase();
  if (!lower) return false;
  if (PRICING_TABLE[lower]) return true;
  if (Object.keys(PRICING_TABLE).some((key) => lower.startsWith(key))) return true;
  return FAMILY_PRICING.some(([family]) => lower.includes(family));
}

/** The names a person picks from; any priced model id is accepted too (`isPricedModel`). */
export const PRICED_MODEL_FAMILIES: readonly string[] = FAMILY_PRICING.map(([family]) => family);

/**
 * Compute estimated cost in USD given token counts and a model identifier.
 */
export function computeCost(model: string, tokensIn: number, tokensOut: number): number {
  const pricing = getPricing(model);
  return (tokensIn / 1_000_000) * pricing.inputPerMTok + (tokensOut / 1_000_000) * pricing.outputPerMTok;
}
