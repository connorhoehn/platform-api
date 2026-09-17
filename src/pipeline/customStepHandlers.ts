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

/**
 * A handler that says what it does.
 *
 * DC's `StepHandler` is `kind` plus `execute`, which is all the executor needs
 * and less than a reader needs. The editor could report that
 * `document:review-plan` is registered and executable, and not one word about
 * what it reviews or what it decides — every registered kind rendered the same
 * generic paragraph. For a system whose definitions are increasingly written by
 * agents and read back by people auditing them, "this string is real" is the
 * least interesting true thing to say about a step.
 *
 * The description lives on the handler, next to the code that implements it,
 * so the two cannot drift apart the way a central catalog would.
 */
/**
 * What travels down an edge, coarsely.
 *
 * Deliberately two values. The cost of a false refusal — someone cannot build
 * a pipeline that would have worked — is much higher than the cost of a miss,
 * and every extra class is another chance to refuse something valid.
 */
export type PayloadClass = 'image' | 'data';

export interface DescribedStepHandler extends StepHandler {
  /** One or two sentences: what this step does, in the run's terms. */
  readonly description?: string;
  /** What it expects to find in the run context, when that is not obvious. */
  readonly reads?: string;
  /** What it puts back into the run context. */
  readonly emits?: string;
  /**
   * The outgoing handles this step can route on, when they are not the usual
   * `out` and `error`.
   *
   * Nothing declared handles anywhere before this, so both ends guessed and
   * both guessed wrong in opposite directions. `llm:judge` routes on `fail`
   * when the judgement does not pass — the editor assumes a registered kind
   * has `out` and `error` only, so it REFUSED a perfectly good
   * `llm:judge.fail → …` edge; the server validated no handles at all, so it
   * happily published `condition.out`, a handle a condition does not have.
   *
   * A kind that declares its handles gets checked against them, at both ends.
   * One that declares none is not second-guessed — silence means "unknown",
   * never "only the defaults".
   */
  readonly outputs?: readonly string[];
  /** The incoming handles, when they are not just `in`. */
  readonly inputs?: readonly string[];
  /**
   * The class of payload this step needs on its input, when it needs a
   * particular one.
   *
   * Handles say which wires exist; this says what may travel down them.
   * `image:resize` takes pixels, and `image:vision-tag.out → image:resize.in`
   * connects two real handles while handing a list of tags to a resizer — so
   * it published, and failed at run time holding the wrong thing.
   *
   * Left undeclared means "anything", never "data". An image step that reads a
   * prompt rather than pixels — `image:generate` — declares nothing here, and
   * an LLM wired into it is exactly right.
   */
  readonly consumes?: PayloadClass;
  /** The class of payload this step's success output carries. */
  readonly produces?: PayloadClass;
}

/** One entry in the step-kind catalog. */
export interface StepKindInfo {
  kind: string;
  /**
   * `builtin` — one of DC's canonical NodeTypes, always executable.
   * `registered` — a namespaced kind a module registered a handler for.
   */
  source: 'builtin' | 'registered';
  /** Present only when the handler describes itself. Absent is not an error. */
  description?: string;
  reads?: string;
  emits?: string;
  /** Declared outgoing handles. Absent means the kind did not say. */
  outputs?: readonly string[];
  /** Declared incoming handles. Absent means the kind did not say. */
  inputs?: readonly string[];
  /** Declared input payload class. Absent means it takes anything. */
  consumes?: PayloadClass;
  /** Declared output payload class. Absent means unknown. */
  produces?: PayloadClass;
}

/**
 * Every step kind this server can actually execute.
 *
 * Exists because nothing could answer the question before. Step kinds are
 * plain strings on a node (`transcription:transcribe`), registered ad-hoc by
 * whichever module owns them, and an author — increasingly an agent posting a
 * definition at the API — had no way to discover which strings are real. A
 * typo produced a pipeline that saved, published, and then failed at run time.
 *
 * Sorted, so the response is stable and diffable.
 */
export function listStepKinds(): StepKindInfo[] {
  const out: StepKindInfo[] = [...CANONICAL_NODE_TYPES].map((kind) => ({
    kind,
    source: 'builtin' as const,
  }));
  for (const kind of HANDLERS.keys()) {
    // An overridable canonical type (`agent`) can also carry a handler; it is
    // already listed as builtin and stays that way.
    if (CANONICAL_NODE_TYPES.has(kind)) continue;
    // Only carry the fields the handler actually set — an absent description is
    // absent from the response, not an empty string the UI has to special-case.
    const h = HANDLERS.get(kind) as DescribedStepHandler | undefined;
    const entry: StepKindInfo = { kind, source: 'registered' };
    if (h?.description) entry.description = h.description;
    if (h?.reads) entry.reads = h.reads;
    if (h?.emits) entry.emits = h.emits;
    if (h?.outputs?.length) entry.outputs = [...h.outputs];
    if (h?.inputs?.length) entry.inputs = [...h.inputs];
    if (h?.consumes) entry.consumes = h.consumes;
    if (h?.produces) entry.produces = h.produces;
    out.push(entry);
  }
  out.sort((a, b) => (a.kind < b.kind ? -1 : a.kind > b.kind ? 1 : 0));
  return out;
}

/** Test seam. */
export function clearCustomStepHandlers(): void {
  HANDLERS.clear();
}
