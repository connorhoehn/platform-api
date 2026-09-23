import {
  authorizationEntityDigest,
  revisionExplainedByAdditions,
  workGraphAuthorizationRevision,
} from '../authorizationRevision';
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

  it('is a gateway- and cursor-safe id (letters, digits, dots; under 128 chars)', () => {
    expect(revision()).toMatch(/^wa2\.[0-9a-f]{32}\.[0-9a-f]{64}$/);
    expect(revision().length).toBeLessThanOrEqual(128);
  });
});

describe('advancing a stream past created entities (NFR #22)', () => {
  const added: ViewerWorkNode = { ...node, id: 'change-2', kind: 'change' as ViewerWorkNode['kind'], capabilities: [] };
  const withAdded = { ...graph, nodes: [node, added] };

  it('a difference made only of created entities is explained by their digests', () => {
    expect(revisionExplainedByAdditions(revision(), revision(withAdded), [authorizationEntityDigest('node', added)])).toBe(true);
  });

  it('an unchanged revision needs no explanation', () => {
    expect(revisionExplainedByAdditions(revision(), revision(), [])).toBe(true);
  });

  it.each([
    ['nothing was created', revision(withAdded), []],
    ['an existing node lost disclosure too', revision({ ...graph, nodes: [{ ...node, disclosure: 'summary' as const }, added] }), 'added'],
    ['an existing node vanished too', revision({ ...graph, nodes: [added] }), 'added'],
    ['grants changed', workGraphAuthorizationRevision('revoked-membership', withAdded), 'added'],
    ['a source became unavailable', revision({ ...withAdded, sources: [{ ...graph.sources[0], health: 'authorization-unavailable' as const }] }), 'added'],
    ['the created node is claimed with other permissions', revision(withAdded), 'other'],
    ['an old-format revision', revision(withAdded), 'added'],
  ] as const)('fails closed when %s', (label, next, which) => {
    const digests = which === 'added'
      ? [authorizationEntityDigest('node', added)]
      : which === 'other'
        ? [authorizationEntityDigest('node', { ...added, disclosure: 'summary' })]
        : [];
    const from = label === 'an old-format revision' ? 'legacy-base64url-revision' : revision();
    expect(revisionExplainedByAdditions(from, next, digests)).toBe(false);
  });
});

