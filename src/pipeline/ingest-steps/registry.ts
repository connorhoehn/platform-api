// platform-api/src/pipeline/ingest-steps/registry.ts
//
// Step-handler registry for ingest-aware pipeline node types. The pipeline
// executor in distributed-core dispatches a fixed set of node types
// (trigger / llm / transform / condition / action / fork / join / approval).
// To extend that surface without forking the executor, the integrator wires
// this registry into the executor's `action` dispatch path: an action node
// with `actionType: 'ingest:fetch'` (etc.) routes here.
//
// The registry is a simple map; handlers are project-local and have no
// dependency on distributed-core's internals — they take the same
// `(config, ctx) => Promise<StepOutcome>` shape used by the existing
// action handlers. The integrator's wrapper is a ~10-line `case 'action'`
// extension that calls `dispatchIngestStep(node.data.actionType, ...)`
// when the prefix matches.

import type { StepHandler, StepOutcome, StepContext } from './types';

export type IngestStepType =
  | 'ingest:fetch'
  | 'ingest:map'
  | 'ingest:materialize'
  | 'ingest:writeback';

export const INGEST_STEP_TYPES: readonly IngestStepType[] = [
  'ingest:fetch',
  'ingest:map',
  'ingest:materialize',
  'ingest:writeback',
];

export class StepHandlerRegistry {
  private readonly handlers = new Map<string, StepHandler<Record<string, unknown>>>();

  register<Cfg extends Record<string, unknown>>(
    stepType: string,
    handler: StepHandler<Cfg>,
  ): void {
    this.handlers.set(stepType, handler as StepHandler<Record<string, unknown>>);
  }

  has(stepType: string): boolean {
    return this.handlers.has(stepType);
  }

  list(): string[] {
    return Array.from(this.handlers.keys());
  }

  /**
   * Invoke the registered handler. Returns a `failed` outcome when the step
   * type is unknown so the caller can route to the error handle without
   * crashing the run. Wrapping handler exceptions into `failed` outcomes is
   * the handler's responsibility; this method propagates throws as-is so
   * the executor's existing retry/backoff path stays in charge.
   */
  async dispatch(
    stepType: string,
    config: Record<string, unknown>,
    ctx: StepContext,
  ): Promise<StepOutcome> {
    const handler = this.handlers.get(stepType);
    if (!handler) {
      return {
        status: 'failed',
        error: `Unknown ingest step type: ${stepType}`,
        sourceHandle: 'error',
      };
    }
    return handler(config, ctx);
  }
}

/**
 * Returns true when the supplied string matches a known ingest step type.
 * Convenience for the integrator's dispatch shim — narrower than calling
 * `registry.has()` (which depends on registration order).
 */
export function isIngestStepType(s: string): s is IngestStepType {
  return (INGEST_STEP_TYPES as readonly string[]).includes(s);
}
