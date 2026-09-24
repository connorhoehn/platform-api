import { describe, it, expect } from '@jest/globals';
import { narrowToTarget } from '../orchestrator';
import type { WorkProjectionState } from '../../repositories/WorkGraphRepository';

const node = (id: string, kind: string, source: string, resourceId: string) => ({
  id, kind, organizationId: 'o', actorId: 'p', sourceRef: { source, resourceId }, policyRef: {},
});
const edge = (id: string, fromId: string, toId: string, relation: string) => ({ id, fromId, toId, relation, organizationId: 'o', actorId: 'p' });

const projection = {
  schemaVersion: 1, organizationId: 'o', actorId: 'p', revision: 1, sessions: {}, sourceCheckpoints: {}, appliedEventIds: [],
  nodes: Object.fromEntries([
    node('doc', 'document', 'document', 'd1'),
    node('stepA', 'run', 'pipeline', 'run1::a'),
    node('stepB', 'run', 'pipeline', 'run1::b'),
    node('other', 'run', 'pipeline', 'run2::a'),
    node('chat', 'conversation', 'conversation', 'c1'),
  ].map((n) => [n.id, n])),
  edges: Object.fromEntries([
    edge('e1', 'stepB', 'doc', 'produced'),
    edge('e2', 'chat', 'stepA', 'triggered'),
    edge('e3', 'other', 'chat', 'mentioned'),
  ].map((e) => [e.id, e])),
} as unknown as WorkProjectionState;

describe('narrowToTarget (NFR #215)', () => {
  it('keeps a run step with its run and the document the run produced (what a grant derives it from)', () => {
    const narrowed = narrowToTarget(projection, { kind: 'node', id: 'stepA' });
    expect(Object.keys(narrowed.nodes).sort()).toEqual(['doc', 'stepA', 'stepB']);
    expect(Object.keys(narrowed.edges)).toEqual(['e1']);
  });
  it('keeps a plain node alone, an edge with its ends, and nothing for an unknown id', () => {
    expect(Object.keys(narrowToTarget(projection, { kind: 'node', id: 'chat' }).nodes)).toEqual(['chat']);
    expect(Object.keys(narrowToTarget(projection, { kind: 'edge', id: 'e3' }).nodes).sort()).toEqual(['chat', 'other']);
    expect(Object.keys(narrowToTarget(projection, { kind: 'node', id: 'nope' }).nodes)).toEqual([]);
  });
});
