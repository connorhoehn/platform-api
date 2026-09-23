// Retries are a platform property, not a per-handler favour. The executor
// (distributed-core ≥ 0.45.8) retries EVERY node kind from `data.retryPolicy`,
// so the only way a system pipeline ends up with a step that gives up on the
// first blip is a definition that forgot to say otherwise. This lint makes
// that forgetting fail boot loudly instead of surfacing as a flaky /doc edit
// weeks later.
//
// What is checked, for every node of a system pipeline definition:
//   - `agent` / `llm` nodes and every custom kind whose type contains ':'
//     (document:read, recording:persist, transcription:transcribe — the steps
//     that talk to a model, a network, or a document) must carry a
//     `retryPolicy` with `maxAttempts >= 2`.
//   - `approval` nodes must name at least one approver, or the run parks
//     forever with nobody able to release it.

//   - the definition itself must say what it is for the pipelines page:
//     `workType` (a WorkType), `kind` (a PipelineKind) and `route` (a
//     non-empty list of short step labels). The page infers these for a
//     person's legacy rows; a seed we ship has no excuse for a guess.

import { isPipelineKind, isRoute, isWorkType } from './definition-metadata';

export type LintRule = 'retry-policy' | 'approvers' | 'work-type' | 'kind' | 'route';

export interface LintViolation {
  pipelineId: string;
  nodeId: string;
  rule: LintRule;
  message: string;
}

interface NodeLike { id?: unknown; type?: unknown; data?: unknown }

/** Kinds the executor may have to call more than once: models, the network, a document. */
export function needsRetryPolicy(type: string): boolean {
  return type === 'agent' || type === 'llm' || type.includes(':');
}

export interface LintOptions {
  /**
   * Also require `workType`, `kind` and `route`. On for the registry the code
   * ships (boot fails naming the builder); off for pipelines a chat command
   * or plugin registers at runtime, whose fields the page infers on read.
   */
  metadata?: boolean;
}

export function lintSystemPipeline(def: Record<string, unknown>, options: LintOptions = {}): LintViolation[] {
  const pipelineId = typeof def.id === 'string' ? def.id : '(unnamed)';
  const nodes = Array.isArray(def.nodes) ? (def.nodes as NodeLike[]) : [];
  const out: LintViolation[] = [];
  for (const node of nodes) {
    const nodeId = typeof node.id === 'string' ? node.id : '(no id)';
    const type = typeof node.type === 'string' ? node.type : '';
    const data = (node.data && typeof node.data === 'object' ? node.data : {}) as Record<string, unknown>;
    if (needsRetryPolicy(type)) {
      const rp = (data.retryPolicy ?? null) as { maxAttempts?: unknown } | null;
      const maxAttempts = rp && typeof rp === 'object' ? rp.maxAttempts : undefined;
      if (typeof maxAttempts !== 'number' || maxAttempts < 2) {
        out.push({ pipelineId, nodeId, rule: 'retry-policy', message: `node "${nodeId}" (${type}) has no retryPolicy with maxAttempts >= 2` });
      }
    }
    if (type === 'approval') {
      const approvers = data.approvers;
      if (!Array.isArray(approvers) || approvers.length === 0) {
        out.push({ pipelineId, nodeId, rule: 'approvers', message: `approval node "${nodeId}" names no approvers` });
      }
    }
  }
  if (!options.metadata) return out;
  if (!isWorkType(def.workType)) {
    out.push({ pipelineId, nodeId: '(definition)', rule: 'work-type', message: 'definition has no workType (documents | agents | conversations | other)' });
  }
  if (!isPipelineKind(def.kind)) {
    out.push({ pipelineId, nodeId: '(definition)', rule: 'kind', message: 'definition has no kind (document | presentation | diagram | agent | experiment | recording | call | conversation | media | workflow)' });
  }
  if (!isRoute(def.route)) {
    out.push({ pipelineId, nodeId: '(definition)', rule: 'route', message: 'definition has no route (a non-empty list of short step labels)' });
  }
  return out;
}

/** Throws when any registered definition violates the rules; the message names every offender. */
export function assertSystemPipelinesLint(defs: Array<Record<string, unknown>>, options: LintOptions = {}): void {
  const violations = defs.flatMap((d) => lintSystemPipeline(d, options));
  if (violations.length === 0) return;
  const lines = violations.map((v) => `  - ${v.pipelineId}: ${v.message}`);
  throw new Error(`system pipeline lint failed (${violations.length} violation${violations.length === 1 ? '' : 's'}):\n${lines.join('\n')}`);
}
