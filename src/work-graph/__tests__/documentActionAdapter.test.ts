/**
 * `DynamoSourceActionAdapter.exchangeAction`'s `document` branch turns a
 * node's `sourceRef.resourceId` into `/documents/<id>` — bare documentId for
 * a `document`-kind node, `documentId:revisionId` for a `change` (revision)
 * node (see projectEvent.js's `kind: 'change'` construction). It recovers
 * the documentId by splitting on the LAST colon, which is correct for both
 * shapes EXCEPT when the resourceId's only colon sits at position 0 — no
 * real documentId before it. That shape should never pass validated
 * ingestion (`isId()` requires an alphanumeric first character), but
 * `activity.ts`'s own revision-history builder already treats it as
 * untrustworthy rather than as ordinary data (`separator <= 0`, in its
 * `revisionsByDocument` loop) — so the exchange should refuse it too,
 * instead of returning `/documents/` (a syntactically "safe" path that
 * names no document and silently lands the host on the bare explorer).
 */
import { DynamoSourceActionAdapter } from '../runtime';
import type { WorkGraphPolicyScope, WorkAclTarget } from '../policy';
import type { StoredWorkNode } from '../../repositories/WorkGraphRepository';

const scope: WorkGraphPolicyScope = {
  organizationId: 'org-1',
  viewerId: 'viewer-1',
  personId: 'owner-1',
  day: '2026-09-22',
  timezone: 'America/New_York',
  policyRevision: 'policy-1',
};

function documentTarget(resourceId: string, kind: StoredWorkNode['kind'] = 'change'): WorkAclTarget {
  const node: StoredWorkNode = {
    id: 'node-1',
    organizationId: 'org-1',
    actorId: 'owner-1',
    kind,
    sourceRef: { source: 'document', resourceId },
    policyRef: `document:${resourceId}`,
    title: 'Sprint review',
    status: 'completed',
    updatedAt: '2026-09-22T13:31:00.000Z',
    sourceEventId: 'event-1',
    revision: 1,
  };
  return { kind: 'node', id: 'node-1', policyRef: node.policyRef, sourceRef: node.sourceRef, node };
}

// The `client` param is unused by the `document` branch, so a dummy stands
// in for it — this adapter never touches DynamoDB to answer this capability.
const adapter = new DynamoSourceActionAdapter('document', {} as never);

describe('DynamoSourceActionAdapter — view-document exchange', () => {
  it('resolves a document-kind node (bare documentId resourceId)', async () => {
    const result = await adapter.exchangeAction(scope, documentTarget('active-now-2026-09-22-sprint-review', 'document'), 'view-document');
    expect(result).toEqual({
      decision: 'allow',
      descriptor: { kind: 'route', capability: 'view-document', path: '/documents/active-now-2026-09-22-sprint-review' },
    });
  });

  it('resolves a change (revision) node — documentId:revisionId', async () => {
    const result = await adapter.exchangeAction(scope, documentTarget('active-now-2026-09-22-sprint-review:v2'), 'view-document');
    expect(result).toEqual({
      decision: 'allow',
      descriptor: { kind: 'route', capability: 'view-document', path: '/documents/active-now-2026-09-22-sprint-review' },
    });
  });

  it('refuses a degenerate resourceId whose colon leaves no documentId prefix, rather than answering /documents/', async () => {
    const result = await adapter.exchangeAction(scope, documentTarget(':v2'), 'view-document');
    expect(result).toEqual({ decision: 'deny' });
  });

  it('still splits on the LAST colon when the real documentId itself contains one', async () => {
    const result = await adapter.exchangeAction(scope, documentTarget('folder:doc-1:v3'), 'view-document');
    expect(result).toEqual({
      decision: 'allow',
      descriptor: { kind: 'route', capability: 'view-document', path: '/documents/folder%3Adoc-1' },
    });
  });
});
