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

export type LintRule = 'retry-policy' | 'approvers';

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

export function lintSystemPipeline(def: Record<string, unknown>): LintViolation[] {
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
  return out;
}

/** Throws when any registered definition violates the rules; the message names every offender. */
export function assertSystemPipelinesLint(defs: Array<Record<string, unknown>>): void {
  const violations = defs.flatMap((d) => lintSystemPipeline(d));
  if (violations.length === 0) return;
  const lines = violations.map((v) => `  - ${v.pipelineId}: ${v.message}`);
  throw new Error(`system pipeline lint failed (${violations.length} violation${violations.length === 1 ? '' : 's'}):\n${lines.join('\n')}`);
}
