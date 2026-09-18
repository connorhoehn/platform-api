// platform-api/src/pipeline/image/ScatterStepHandler.ts
//
// Custom step handler (DC-21) that dispatches `node.data.type === 'scatter'`.
// Reads an array from context via a JSONPath-style expression and returns
// it on the `StepOutcome.fanOut` discriminant — the executor walks the
// downstream subgraph once per item with cloned context. See
// distributed-core/src/applications/pipeline/PipelineExecutor.ts for the
// fan-out execution semantics.
//
// === Why a handler, not a built-in NodeType ===
// `scatter` could be a canonical executor concept, but landing it as a
// custom step handler keeps the DC-side change minimal (only the
// `fanOut` discriminant on `StepOutcome` is universal) and lets us
// iterate on the resolution semantics (JSONPath dialect, alias rules,
// concurrency cap defaults) without rebuilding distributed-core.
//
// === Resolution ===
// `itemsPath` is a dotted/bracketed path interpreted against the live
// context (e.g. `$.steps.visionAnalyze.extracted.regions`). When
// the path resolves to:
//   - an Array → fan out to N arms.
//   - a string → JSON-parse it first (some upstream nodes serialize).
//   - null/undefined/missing → fail with a descriptive error.
//   - not an array (after JSON-parse) → fail with shape diagnostics.
//
// The fan-out path itself is type-erased in distributed-core
// (`items: unknown[]`) so the handler doesn't need to inspect element
// shape — downstream handlers (`image:crop` etc) do.
//
// Non-throwing: all error paths return `{status:'failed', error:...}`
// per the audit rule the existing handlers follow.

import type { DescribedStepHandler } from '../customStepHandlers';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';

/**
 * Node-data shape consumed by the scatter handler. Mirrors the wsg-frontend
 * `ScatterNodeData` interface — kept here as a structural type so the
 * handler doesn't import frontend code.
 */
interface ScatterNodeData {
  type: 'scatter';
  /**
   * JSONPath into context, e.g. `$.steps.visionAnalyze.extracted.regions`.
   * Leading `$.` is optional and stripped.
   */
  itemsPath: string;
  /**
   * Key written to each per-arm context (default `'item'`). The downstream
   * handler reads `context[itemAlias]` to get the per-arm payload.
   */
  itemAlias?: string;
  /**
   * Maximum concurrent arms (default 4). The executor's `runWithConcurrency`
   * bounds parallel dispatch — too-high values can saturate the libuv thread
   * pool or hit Anthropic rate limits when arms include LLM calls.
   */
  maxConcurrency?: number;
}

const DEFAULT_MAX_CONCURRENCY = 4;

export function createScatterStepHandler(): DescribedStepHandler {
  return {
    kind: 'scatter',
    description:
      'Fans the rest of the branch out over an array: the executor walks the downstream subgraph once per item, with the item at context.<alias> and also at context.scatter.item alongside its index and total.',
    reads: 'itemsPath, a JSONPath into the context that must resolve to an array. Also itemAlias (default \'item\') and maxConcurrency (default 4).',
    emits:
      'A fan-out rather than an ordinary output — there is one arm per element, and each arm carries its own copy of the context.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      _helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as ScatterNodeData;
      if (!data.itemsPath || typeof data.itemsPath !== 'string') {
        return {
          status: 'failed',
          error: `scatter: 'itemsPath' is required (got: ${JSON.stringify(data.itemsPath)})`,
        };
      }

      let resolved: unknown;
      try {
        resolved = readPath(context, data.itemsPath);
      } catch (err) {
        return {
          status: 'failed',
          error: `scatter: failed to resolve itemsPath '${data.itemsPath}': ${
            err instanceof Error ? err.message : String(err)
          }`,
        };
      }

      // Some upstream nodes serialize arrays as JSON strings (LLM JSON-mode
      // outputs are a common case). Soft-parse before failing.
      if (typeof resolved === 'string') {
        try {
          resolved = JSON.parse(resolved);
        } catch {
          /* fall through to the array check below */
        }
      }

      if (resolved == null) {
        return {
          status: 'failed',
          error: `scatter: itemsPath '${data.itemsPath}' resolved to null/undefined`,
        };
      }
      if (!Array.isArray(resolved)) {
        return {
          status: 'failed',
          error: `scatter: itemsPath '${data.itemsPath}' resolved to ${typeof resolved}, expected array`,
        };
      }

      return {
        status: 'completed',
        output: { items: resolved, count: resolved.length },
        fanOut: {
          items: resolved,
          itemAlias: data.itemAlias ?? 'item',
          maxConcurrency: data.maxConcurrency ?? DEFAULT_MAX_CONCURRENCY,
        },
      };
    },
  };
}

/**
 * Minimal JSONPath-lite resolver. Supports:
 *   - leading `$.` (optional)
 *   - dotted keys: `a.b.c`
 *   - bracketed keys: `a['b'].c` or `a["b"].c`
 *   - numeric indices: `a[0].b`
 *
 * Does NOT support recursive descent, filters, wildcards, or unions. The
 * goal is "feels like a path" — not RFC-compliant JSONPath. Adopt
 * `jsonpath-plus` later if richer expressions are needed.
 */
export function readPath(obj: Record<string, unknown>, path: string): unknown {
  if (!path) return obj;
  let p = path.trim();
  if (p.startsWith('$')) p = p.slice(1);
  if (p.startsWith('.')) p = p.slice(1);
  if (!p) return obj;

  const segments: Array<string | number> = [];
  let i = 0;
  while (i < p.length) {
    const ch = p[i];
    if (ch === '.') {
      i++;
      continue;
    }
    if (ch === '[') {
      const close = p.indexOf(']', i);
      if (close === -1) throw new Error(`unclosed '[' at position ${i}`);
      let inner = p.slice(i + 1, close).trim();
      if (
        (inner.startsWith("'") && inner.endsWith("'")) ||
        (inner.startsWith('"') && inner.endsWith('"'))
      ) {
        inner = inner.slice(1, -1);
        segments.push(inner);
      } else if (/^-?\d+$/.test(inner)) {
        segments.push(parseInt(inner, 10));
      } else {
        segments.push(inner);
      }
      i = close + 1;
      continue;
    }
    // dot-delimited key
    let j = i;
    while (j < p.length && p[j] !== '.' && p[j] !== '[') j++;
    segments.push(p.slice(i, j));
    i = j;
  }

  let cur: unknown = obj;
  for (const seg of segments) {
    if (cur == null) return undefined;
    if (typeof seg === 'number') {
      if (!Array.isArray(cur)) return undefined;
      cur = cur[seg];
    } else {
      if (typeof cur !== 'object') return undefined;
      cur = (cur as Record<string, unknown>)[seg];
    }
  }
  return cur;
}
