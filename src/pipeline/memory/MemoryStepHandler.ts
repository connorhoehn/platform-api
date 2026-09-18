// platform-api/src/pipeline/memory/MemoryStepHandler.ts
//
// Custom step handler for `node.data.type === 'memory'` - the read/write
// memory node the editor configures via MemoryConfig in ui-components.
//
// Two deliberate departures from how other handlers treat templates:
//
//  1. An unresolved token in the KEY EXPRESSION fails the step. The executor's
//     own `interpolate` collapses a missing `{{context.x}}` to empty string,
//     which is the right call for an LLM prompt - a blank in a sentence is
//     better than a dead run. It is the wrong call for a key. A typo'd or
//     not-yet-populated path would silently collapse every run onto the SAME
//     degenerate key, so run A reads back run B's value and one user sees
//     another user's remembered data. Failing loudly is the only safe option.
//
//  2. A `valueField` is read as a context PATH, not interpolated as a string.
//     Interpolating would stringify the value, so persisting an object would
//     store the literal "[object Object]" and the matching read would hand
//     that back downstream. Paths preserve the value's type.
//
// The handler never throws: every failure is `{status:'failed', error}` so the
// executor records a failed step instead of an unhandled rejection.

import type { DescribedStepHandler } from '../customStepHandlers';
import type {
  PipelineNode,
  StepHandler,
  StepHandlerHelpers,
  StepOutcome,
} from 'distributed-core/applications/pipeline';
import type { MemoryStore, MemoryScopeRef } from './MemoryStore';

export type MemoryMode = 'read' | 'write';
export type MemoryScopeName = 'session' | 'long-term';

interface MemoryNodeData {
  type: 'memory';
  mode: MemoryMode;
  scope: MemoryScopeName;
  namespace: string;
  keyExpression: string;
  /** Write only: context path whose value is persisted. */
  valueField?: string;
  /** Optional; applied by the executor, not here. */
  outputKey?: string;
}

export interface MemoryStepHandlerOpts {
  store: MemoryStore;
  /**
   * Resolves a runId to its pipelineId, for 'long-term' scope. Mirrors the
   * lookup the judge handler uses; returns undefined when the module is not
   * ready or the run is unknown.
   */
  pipelineIdLookup: (runId: string) => string | undefined;
}

const TOKEN_RE = /\{\{\s*context\.([\w.]+)\s*\}\}/g;

/** Read a dotted path from the run context root. Mirrors the executor's own
 *  resolution so `{{context.a.b}}` means the same thing in both places. */
export function readContextPath(
  context: Record<string, unknown>,
  path: string,
): unknown {
  // Authors write `context.a.b` in the editor; the leading segment names the
  // context root itself and is not a key inside it.
  const trimmed = path.trim().replace(/^context\./, '');
  if (trimmed === '') return undefined;

  let value: unknown = context;
  for (const seg of trimmed.split('.')) {
    if (value === null || typeof value !== 'object' || !(seg in value)) {
      return undefined;
    }
    value = (value as Record<string, unknown>)[seg];
  }
  return value;
}

export interface KeyResolution {
  key?: string;
  /** Paths that appeared in the template but resolved to nothing. */
  unresolved: string[];
}

/**
 * Resolve `{{context.*}}` tokens in a key expression, reporting any that did
 * not resolve rather than collapsing them to empty string.
 */
export function resolveKeyExpression(
  template: string,
  context: Record<string, unknown>,
): KeyResolution {
  const unresolved: string[] = [];
  const key = template.replace(TOKEN_RE, (_match, path: string) => {
    const value = readContextPath(context, path);
    if (value === undefined || value === null) {
      unresolved.push(`context.${path}`);
      return '';
    }
    return String(value);
  });
  return unresolved.length > 0 ? { unresolved } : { key, unresolved };
}

export function createMemoryStepHandler(
  opts: MemoryStepHandlerOpts,
): DescribedStepHandler {
  return {
    kind: 'memory',
    description:
      'Reads or writes AgentCore Memory under a namespace and a key built from the run context. Refuses a key whose template has unresolved paths rather than substituting an empty string, because that would collapse every run onto one key.',
    reads: 'namespace and keyExpression, both required; valueField as well when the mode is write.',
    emits:
      'On read, what was stored. On write, confirmation of what was put there.',
    async execute(
      node: PipelineNode,
      context: Record<string, unknown>,
      helpers: StepHandlerHelpers,
    ): Promise<StepOutcome> {
      const data = node.data as unknown as MemoryNodeData;

      const namespace = (data.namespace ?? '').trim();
      if (!namespace) {
        return { status: 'failed', error: 'memory: namespace is required' };
      }

      const template = (data.keyExpression ?? '').trim();
      if (!template) {
        return { status: 'failed', error: 'memory: keyExpression is required' };
      }

      const { key, unresolved } = resolveKeyExpression(template, context);
      if (!key) {
        return {
          status: 'failed',
          error:
            `memory: key expression '${template}' has unresolved ` +
            `path(s): ${unresolved.join(', ')}. Resolving them to an empty ` +
            `string would collapse every run onto the same key.`,
        };
      }

      // Build the scope reference. 'long-term' needs the pipelineId, which is
      // only reachable through the module ref - if the module is not ready the
      // step must fail rather than silently fall back to session scope, which
      // would write a value that no later run could ever read.
      let ref: MemoryScopeRef;
      if (data.scope === 'long-term') {
        const pipelineId = opts.pipelineIdLookup(helpers.runId);
        if (!pipelineId) {
          return {
            status: 'failed',
            error:
              `memory: could not resolve pipelineId for run '${helpers.runId}'; ` +
              `'long-term' scope cannot be bound`,
          };
        }
        ref = { scope: 'long-term', pipelineId };
      } else {
        ref = { scope: 'session', runId: helpers.runId };
      }

      try {
        if (data.mode === 'write') {
          const path = (data.valueField ?? '').trim();
          if (!path) {
            return {
              status: 'failed',
              error: "memory: mode 'write' requires a valueField",
            };
          }

          const value = readContextPath(context, path);
          if (value === undefined) {
            return {
              status: 'failed',
              error: `memory: valueField '${path}' did not resolve in the run context`,
            };
          }

          await opts.store.put(ref, namespace, key, value);
          return {
            status: 'completed',
            output: { written: true, namespace, key, scope: data.scope },
          };
        }

        // Read. A miss is a normal outcome, not a failure: a first-run lookup
        // of a long-term key has nothing to find, and callers branch on
        // `found` rather than on the step having failed.
        const value = await opts.store.get(ref, namespace, key);
        return {
          status: 'completed',
          output: {
            found: value !== null && value !== undefined,
            value: value ?? null,
            namespace,
            key,
            scope: data.scope,
          },
        };
      } catch (err) {
        return {
          status: 'failed',
          error: `memory: ${err instanceof Error ? err.message : String(err)}`,
        };
      }
    },
  };
}
