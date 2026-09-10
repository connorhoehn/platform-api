// Retries and approvers are platform properties: every system pipeline must
// declare them, and a definition that forgets fails boot. The first test is
// the one that matters — it lints what the registry actually ships.
import { lintSystemPipeline, assertSystemPipelinesLint, needsRetryPolicy } from '../systemPipelineLint';
import { listSystemPipelines } from '../systemPipelines';

const node = (id: string, type: string, data: Record<string, unknown> = {}) => ({ id, type, position: { x: 0, y: 0 }, data: { type, ...data } });

describe('every registered system pipeline', () => {
  it('passes the lint (module load already asserted it; this names the offender if it regresses)', () => {
    const defs = listSystemPipelines();
    expect(defs.length).toBeGreaterThanOrEqual(4);
    for (const def of defs) expect({ id: def.id, violations: lintSystemPipeline(def) }).toEqual({ id: def.id, violations: [] });
  });
});

describe('lintSystemPipeline', () => {
  it('flags agent, llm and colon-typed custom steps without a retryPolicy of at least two attempts', () => {
    const def = { id: 'p', nodes: [
      node('trigger', 'trigger', { triggerType: 'manual' }),
      node('plan', 'agent', { prompt: 'x' }),
      node('ask', 'llm', { retryPolicy: { maxAttempts: 1, backoffMs: 1, backoffMultiplier: 1 } }),
      node('read', 'document:read'),
      node('ok', 'document:apply', { retryPolicy: { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2 } }),
      node('branch', 'condition', { expression: 'context.x === 1' }),
    ] };
    expect(lintSystemPipeline(def).map((v) => `${v.nodeId}:${v.rule}`)).toEqual(['plan:retry-policy', 'ask:retry-policy', 'read:retry-policy']);
  });

  it('flags an approval node with nobody to approve it', () => {
    const def = { id: 'p', nodes: [
      node('approve', 'approval', { approvers: [], requiredCount: 1 }),
      node('approve-2', 'approval', { approvers: [{ type: 'user', value: 'u1' }], requiredCount: 1 }),
    ] };
    expect(lintSystemPipeline(def)).toEqual([expect.objectContaining({ pipelineId: 'p', nodeId: 'approve', rule: 'approvers' })]);
  });

  it('tolerates a definition with no nodes and names an unnamed one', () => {
    expect(lintSystemPipeline({})).toEqual([]);
    expect(lintSystemPipeline({ nodes: [node('a', 'agent')] })[0]).toMatchObject({ pipelineId: '(unnamed)' });
  });

  it('knows which kinds need a policy', () => {
    expect(['agent', 'llm', 'document:read', 'recording:persist'].map(needsRetryPolicy)).toEqual([true, true, true, true]);
    expect(['trigger', 'condition', 'approval', 'transform', 'memory'].map(needsRetryPolicy)).toEqual([false, false, false, false, false]);
  });

  it('assertSystemPipelinesLint throws once, naming every offender', () => {
    expect(() => assertSystemPipelinesLint([{ id: 'a', nodes: [node('plan', 'agent')] }, { id: 'b', nodes: [node('approve', 'approval')] }]))
      .toThrow(/2 violations[\s\S]*a: node "plan"[\s\S]*b: approval node "approve"/);
    expect(() => assertSystemPipelinesLint([{ id: 'fine', nodes: [node('t', 'trigger')] }])).not.toThrow();
  });
});
