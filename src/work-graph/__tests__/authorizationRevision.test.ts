import { workGraphAuthorizationRevision } from '../authorizationRevision';
import type { AuthorizedWorkGraph, ViewerWorkNode } from '../policy';

const node: ViewerWorkNode = {
  id: 'document-1', kind: 'document', title: 'Plan', status: 'running',
  updatedAt: '2026-09-20T12:00:00.000Z', disclosure: 'details',
  capabilities: ['view-document', 'edit-document'],
};
const graph: Pick<AuthorizedWorkGraph, 'nodes' | 'edges' | 'sources'> = {
  nodes: [node], edges: [],
  sources: [{ source: 'document', health: 'available', checkedAt: node.updatedAt }],
};
const revision = (value = graph) => workGraphAuthorizationRevision('grants-and-membership', value);

describe('effective work graph authorization revision', () => {
  it('ignores content, progress, heartbeat time and capability ordering', () => {
    expect(revision({
      ...graph,
      nodes: [{ ...node, title: 'New plan', status: 'completed', updatedAt: '2026-09-20T13:00:00.000Z', capabilities: ['edit-document', 'view-document', 'view-document'] }],
      sources: [{ ...graph.sources[0], checkedAt: '2026-09-20T13:00:00.000Z' }],
    })).toBe(revision());
  });

  it.each([
    { ...graph, nodes: [] },
    { ...graph, nodes: [{ ...node, disclosure: 'summary' as const }] },
    { ...graph, nodes: [{ ...node, capabilities: ['view-document' as const] }] },
    { ...graph, nodes: [], sources: [{ ...graph.sources[0], health: 'authorization-unavailable' as const }] },
  ])('changes when the previously disclosed permissions change', (next) => {
    expect(revision(next)).not.toBe(revision());
  });

  it('still includes grant and membership revisions', () => {
    expect(workGraphAuthorizationRevision('revoked-membership', graph)).not.toBe(revision());
  });
});
