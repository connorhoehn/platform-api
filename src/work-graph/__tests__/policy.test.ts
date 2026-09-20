import {
  authorizeWorkGraph,
  grantAppliesToScope,
  type WorkGraphPolicyScope,
  type WorkSourceAclAdapter,
} from '../policy';
import type { WorkProjectionState } from '../../repositories/WorkGraphRepository';
import type { StoredWorkGrant, WorkHistoryGrant } from '../../repositories/WorkSharingRepository';

const now = '2026-09-19T12:00:00.000Z';
const scope: WorkGraphPolicyScope = {
  organizationId: 'org-1',
  viewerId: 'viewer-1',
  personId: 'owner-1',
  day: '2026-09-19',
  timezone: 'America/New_York',
  policyRevision: 'policy-1',
};

const projection: WorkProjectionState = {
  schemaVersion: 1,
  organizationId: 'org-1',
  actorId: 'owner-1',
  revision: 1,
  sessions: {},
  nodes: {
    'node-1': {
      id: 'node-1', organizationId: 'org-1', actorId: 'owner-1', kind: 'document',
      sourceRef: { source: 'document', resourceId: 'doc-private' }, policyRef: 'document:doc-private',
      title: 'Private document', status: 'running', updatedAt: now, sourceEventId: 'event-1', revision: 1,
    },
  },
  edges: {},
  sourceCheckpoints: {},
  appliedEventIds: [],
};

function grant(overrides: Partial<StoredWorkGrant> = {}): StoredWorkGrant {
  return {
    schemaVersion: 1,
    id: 'grant-1', organizationId: 'org-1', ownerId: 'owner-1',
    audience: { kind: 'people', personIds: ['viewer-1'] },
    selection: { nodeIds: ['node-1'], edgeIds: [], disclosure: 'details' },
    state: 'active', createdAt: '2026-09-19T11:00:00.000Z', updatedAt: '2026-09-19T11:00:00.000Z',
    expiresAt: '2026-09-19T13:00:00.000Z', revision: 1,
    ...overrides,
  } as StoredWorkGrant;
}

const adapter: WorkSourceAclAdapter = {
  source: 'document',
  async authorizeDisclosure(currentScope, targets) {
    return new Map(targets.map((target) => [target.id, {
      targetId: target.id,
      decision: 'allow' as const,
      maximumDisclosure: 'details' as const,
      capabilities: ['view-document' as const],
      policyRevision: currentScope.policyRevision,
    }]));
  },
  async authorizeCapability() { return 'allow' as const; },
};

describe('work graph policy', () => {
  it('never applies a live-only grant to an arbitrary historical local day', () => {
    expect(grantAppliesToScope(grant(), { ...scope, day: '2026-09-18' }, now)).toBe(false);
    expect(grantAppliesToScope(grant(), scope, now)).toBe(true);
  });

  it('allows explicitly bounded history and keeps browser filtering in the shared library', async () => {
    const historical = {
      ...grant(),
      id: 'history-1',
      dayFrom: '2026-09-18',
      dayThrough: '2026-09-18',
    } as WorkHistoryGrant;
    const result = await authorizeWorkGraph(
      projection,
      { ...scope, day: '2026-09-18' },
      [historical],
      new Map([['document', adapter]]),
      now,
    );
    expect(result.nodes).toEqual([expect.objectContaining({ id: 'node-1', title: 'Private document' })]);
  });
});
