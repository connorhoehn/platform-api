// The LIVE registry of custom step handlers — the one Map the pipeline module runs on.
//
// distributed-core's `PipelineModule` keeps `config.customStepHandlers` by
// reference and hands that same Map to every `PipelineExecutor` it builds at
// `triggerRun`; the executor looks a node's `data.type` up in it at dispatch
// time (`this.customStepHandlers?.get(type)`). Nothing is copied. So a kind
// added to THIS map after the module was constructed is dispatchable by the
// next run — which is what lets a chat-command plugin loaded at boot (or a
// test) bring its own step kinds without the bootstrap knowing its name.
//
// The bootstrap seeds this map with the platform's own handlers (judge,
// vision, memory, agent, the chat commands, recordings) and passes it to the
// module; plugins register into it through `ChatCommandPluginContext.
// registerStepHandlers`. DC validates the map once, at construction, so the
// same two checks are applied here to every late entry: the key must equal
// `handler.kind`, and it may not shadow a canonical NodeType (except `agent`,
// which DC lets a consumer override).
import type { StepHandler } from 'distributed-core/applications/pipeline';

/** DC's canonical NodeType union; a custom kind may not be one of these. */
const CANONICAL_NODE_TYPES: ReadonlySet<string> = new Set(['trigger', 'llm', 'transform', 'condition', 'action', 'fork', 'join', 'approval', 'agent']);
/** The canonical types DC lets a custom handler replace. */
const OVERRIDABLE_NODE_TYPES: ReadonlySet<string> = new Set(['agent']);

const HANDLERS = new Map<string, StepHandler>();

/** Throws the same way DC's `validateCustomStepHandlers` would, for one entry. */
export function assertCustomStepHandler(key: string, handler: StepHandler): void {
  if (!key || typeof key !== 'string') throw new Error('customStepHandlers: a handler needs a non-empty kind');
  if (!handler || typeof handler.execute !== 'function') throw new Error(`customStepHandlers: '${key}' has no execute()`);
  if (CANONICAL_NODE_TYPES.has(key) && !OVERRIDABLE_NODE_TYPES.has(key)) {
    throw new Error(`customStepHandlers: key '${key}' shadows a built-in NodeType — choose a namespaced kind like 'people:search'`);
  }
  if (handler.kind !== key) throw new Error(`customStepHandlers: handler.kind='${handler.kind}' does not match key '${key}'`);
}

/** Register one handler under its kind (or an explicit key). A later registration replaces an earlier one. */
export function registerCustomStepHandler(handler: StepHandler, key: string = handler?.kind): void {
  assertCustomStepHandler(key, handler);
  HANDLERS.set(key, handler);
}

/** Register several — a Map keyed by kind, or an array whose entries name their own kind. */
export function registerCustomStepHandlers(handlers: Map<string, StepHandler> | readonly StepHandler[]): void {
  if (handlers instanceof Map) { for (const [k, h] of handlers) registerCustomStepHandler(h, k); return; }
  for (const h of handlers) registerCustomStepHandler(h);
}

/** THE map — by reference, never a copy. The bootstrap hands this object to the module. */
export function getCustomStepHandlers(): Map<string, StepHandler> {
  return HANDLERS;
}

export function getCustomStepHandler(kind: string): StepHandler | undefined {
  return HANDLERS.get(kind);
}

export function hasCustomStepHandler(kind: string): boolean {
  return HANDLERS.has(kind);
}

/** Test seam. */
export function clearCustomStepHandlers(): void {
  HANDLERS.clear();
}
