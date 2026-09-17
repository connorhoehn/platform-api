// Handle checks: an edge must leave and enter by a handle that exists.
//
// The gap these close: the editor has always refused `condition.out` (a
// condition routes on `true`/`false`), but an agent POSTing the same definition
// straight at the API got a 200 and a published pipeline that routed nowhere.

import { validateShape, isPublishable } from '../validate-shape';

const trigger = { id: 't', type: 'trigger' };
const action = { id: 'a', type: 'action' };
const condition = { id: 'c', type: 'condition' };

function shape(nodes: unknown[], edges: unknown[]) {
  return validateShape({ nodes, edges }, { knownKinds: ['trigger', 'action', 'condition', 'fork', 'join', 'llm:judge'] });
}

describe('validateShape — edge handles', () => {
  it('rejects a handle the node type does not have', () => {
    const r = shape([trigger, condition, action], [
      { id: 'e1', source: 't', sourceHandle: 'out', target: 'c', targetHandle: 'in' },
      { id: 'e2', source: 'c', sourceHandle: 'out', target: 'a', targetHandle: 'in' },
    ]);
    const issue = r.errors.find((e) => e.code === 'EDGE_HANDLE_UNKNOWN');
    expect(issue).toBeDefined();
    expect(issue!.message).toContain('"out"');
    expect(isPublishable(r)).toBe(false);
  });

  it('accepts the handles a condition really has', () => {
    const r = shape([trigger, condition, action], [
      { id: 'e1', source: 't', sourceHandle: 'out', target: 'c', targetHandle: 'in' },
      { id: 'e2', source: 'c', sourceHandle: 'true', target: 'a', targetHandle: 'in' },
    ]);
    expect(r.errors.filter((e) => e.code === 'EDGE_HANDLE_UNKNOWN')).toHaveLength(0);
  });

  it('refuses an edge pointing into a trigger', () => {
    const r = shape([trigger, action], [
      { id: 'e1', source: 'a', sourceHandle: 'out', target: 't', targetHandle: 'in' },
    ]);
    const issue = r.errors.find((e) => e.code === 'EDGE_HANDLE_UNKNOWN');
    expect(issue?.message).toContain('a trigger is where a pipeline begins');
  });

  it('matches dynamic fork and join handles by shape', () => {
    const r = shape(
      [trigger, { id: 'f', type: 'fork' }, { id: 'j', type: 'join' }],
      [
        { id: 'e1', source: 't', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
        { id: 'e2', source: 'f', sourceHandle: 'branch-2', target: 'j', targetHandle: 'in-1' },
      ],
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_HANDLE_UNKNOWN')).toHaveLength(0);
  });

  it('rejects a fork branch that is not a branch', () => {
    const r = shape([trigger, { id: 'f', type: 'fork' }, action], [
      { id: 'e1', source: 't', sourceHandle: 'out', target: 'f', targetHandle: 'in' },
      { id: 'e2', source: 'f', sourceHandle: 'out', target: 'a', targetHandle: 'in' },
    ]);
    expect(r.errors.some((e) => e.code === 'EDGE_HANDLE_UNKNOWN')).toBe(true);
  });

  // The other half of the gap: guessing produced FALSE refusals. `llm:judge`
  // routes on `fail`, which is not `out` and is not `error`.
  it('honours a handle a registered kind declares', () => {
    const r = validateShape(
      {
        nodes: [trigger, { id: 'j', type: 'llm:judge' }, action],
        edges: [
          { id: 'e1', source: 't', sourceHandle: 'out', target: 'j', targetHandle: 'in' },
          { id: 'e2', source: 'j', sourceHandle: 'fail', target: 'a', targetHandle: 'in' },
        ],
      },
      {
        knownKinds: ['trigger', 'action', 'llm:judge'],
        handlesFor: (k) => (k === 'llm:judge' ? { outputs: ['out', 'fail'], inputs: ['in'] } : undefined),
      },
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_HANDLE_UNKNOWN')).toHaveLength(0);
  });

  it('leaves an undeclared registered kind alone rather than guessing', () => {
    const r = validateShape(
      {
        nodes: [trigger, { id: 'x', type: 'catalog:upsert' }, action],
        edges: [
          { id: 'e1', source: 't', sourceHandle: 'out', target: 'x', targetHandle: 'in' },
          { id: 'e2', source: 'x', sourceHandle: 'whatever', target: 'a', targetHandle: 'in' },
        ],
      },
      { knownKinds: ['trigger', 'action', 'catalog:upsert'] },
    );
    expect(r.errors.filter((e) => e.code === 'EDGE_HANDLE_UNKNOWN')).toHaveLength(0);
  });
});
