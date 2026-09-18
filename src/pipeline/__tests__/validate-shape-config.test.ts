// Configuration a step cannot run without.
//
// The editor has always refused to publish an `llm` with no prompt or a
// `condition` with no expression. This side validated none of it, so an agent
// posting the same definition got a 200 and a live pipeline that failed at its
// first step. "Per-node-type config stays in the editor" was a reasonable line
// while people were the only authors.

import { validateShape, isPublishable } from '../validate-shape';

const trigger = { id: 't', type: 'trigger', data: { type: 'trigger', triggerType: 'manual' } };

function shape(nodes: unknown[], edges: unknown[] = []) {
  return validateShape({ nodes, edges });
}

const fullLlm = {
  id: 'l', type: 'llm',
  data: { type: 'llm', provider: 'anthropic', model: 'claude-opus-5', systemPrompt: 'be brief', userPromptTemplate: 'summarise {{context.doc}}' },
};

describe('validateShape — required step configuration', () => {
  it('refuses an llm with no prompt', () => {
    const r = shape([trigger, { id: 'l', type: 'llm', data: { type: 'llm' } }]);
    const issues = r.errors.filter((e) => e.code === 'NODE_CONFIG_MISSING');
    expect(issues.map((i) => i.message).join(' ')).toContain('userPromptTemplate');
    expect(isPublishable(r)).toBe(false);
  });

  it('names the step and the field, so an agent can fix it without guessing', () => {
    const r = shape([trigger, { id: 'decide', type: 'condition', data: { type: 'condition' } }]);
    const issue = r.errors.find((e) => e.code === 'NODE_CONFIG_MISSING');
    expect(issue?.message).toBe('Step "decide" is a condition and has no expression.');
    // "an llm", not "a llm" — agents and people read the same sentence.
    const llm = validateShape({ nodes: [trigger, { id: 'l', type: 'llm', data: { type: 'llm' } }], edges: [] })
      .errors.find((e) => e.code === 'NODE_CONFIG_MISSING');
    expect(llm?.message).toContain('is an llm');
    expect(issue?.nodeId).toBe('decide');
  });

  it('accepts a fully configured step', () => {
    const r = shape([trigger, fullLlm]);
    expect(r.errors.filter((e) => e.code === 'NODE_CONFIG_MISSING')).toHaveLength(0);
  });

  it('treats a blank string as missing, not supplied', () => {
    const r = shape([trigger, { ...fullLlm, data: { ...fullLlm.data, userPromptTemplate: '   ' } }]);
    expect(r.errors.some((e) => e.code === 'NODE_CONFIG_MISSING')).toBe(true);
  });

  it('reports every missing field, not just the first', () => {
    const r = shape([trigger, { id: 'l', type: 'llm', data: { type: 'llm' } }]);
    expect(r.errors.filter((e) => e.code === 'NODE_CONFIG_MISSING')).toHaveLength(4);
  });

  // The same rule the handle and payload checks follow: a registered kind's
  // configuration is the executor's business, and guessing at it would refuse
  // definitions that are perfectly good.
  it('does not guess at a registered kind’s configuration', () => {
    const r = shape([trigger, { id: 'x', type: 'catalog:upsert', data: { type: 'catalog:upsert' } }]);
    expect(r.errors.filter((e) => e.code === 'NODE_CONFIG_MISSING')).toHaveLength(0);
  });

  it('survives a node with no data at all', () => {
    expect(() => shape([trigger, { id: 'l', type: 'llm' }])).not.toThrow();
    const r = shape([trigger, { id: 'l', type: 'llm' }]);
    expect(r.errors.some((e) => e.code === 'NODE_CONFIG_MISSING')).toBe(true);
  });
});
