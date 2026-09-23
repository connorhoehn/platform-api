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

  describe('work on a shared document after the grant named it (NFR #125)', () => {
    const t = (id: string, kind: 'document' | 'change' | 'run' | 'conversation', source: 'document' | 'pipeline' | 'conversation', resourceId: string) => ({
      id, organizationId: 'org-1', actorId: 'owner-1', kind, sourceRef: { source, resourceId },
      policyRef: `${source}:${resourceId}`, title: id, status: 'running' as const, updatedAt: now, sourceEventId: `event-${id}`, revision: 1,
    });
    const e = (id: string, fromId: string, toId: string, relation: 'edited' | 'produced' | 'derived-from' | 'runs-in') => ({
      id, organizationId: 'org-1', actorId: 'owner-1', fromId, toId, relation, policyRef: `edge:${id}`,
      status: 'running' as const, updatedAt: now, sourceEventId: `event-${id}`, provenance: 'source-event' as const, revision: 1,
    });
    // The deck the grant named, then a run that edited it this morning: the
    // run, its two steps, the revision the write step saved, and the chat the
    // run was asked in — none of which the grant could have listed.
    const later: WorkProjectionState = {
      ...projection,
      nodes: {
        deck: t('deck', 'document', 'document', 'deck-1'),
        rev: t('rev', 'change', 'document', 'deck-1:v2'),
        run: t('run', 'run', 'pipeline', 'run-9'),
        load: t('load', 'run', 'pipeline', 'run-9::load'),
        write: t('write', 'run', 'pipeline', 'run-9::write'),
        chat: t('chat', 'conversation', 'conversation', 'dm-1'),
        other: t('other', 'document', 'document', 'deck-2'),
        otherRun: t('otherRun', 'run', 'pipeline', 'run-8'),
      },
      edges: {
        edited: e('edited', 'rev', 'deck', 'edited'),
        produced: e('produced', 'write', 'deck', 'produced'),
        runProduced: e('runProduced', 'run', 'deck', 'produced'),
        loadOf: e('loadOf', 'load', 'run', 'derived-from'),
        writeOf: e('writeOf', 'write', 'run', 'derived-from'),
        writeAfterLoad: e('writeAfterLoad', 'write', 'load', 'derived-from'),
        askedIn: e('askedIn', 'run', 'chat', 'derived-from'),
        otherProduced: e('otherProduced', 'otherRun', 'other', 'produced'),
      },
    };
    const allow = (source: 'document' | 'pipeline' | 'conversation'): WorkSourceAclAdapter => ({ ...adapter, source });
    const adapters = new Map<'document' | 'pipeline' | 'conversation', WorkSourceAclAdapter>([
      ['document', allow('document')], ['pipeline', allow('pipeline')], ['conversation', allow('conversation')],
    ]);
    const ids = (items: Array<{ id: string }>) => items.map((item) => item.id).sort();

    it('discloses the run, its steps and the revision to the grant\'s audience, at the grant\'s disclosure', async () => {
      const result = await authorizeWorkGraph(
        later, scope, [grant({ selection: { nodeIds: ['deck'], edgeIds: [], disclosure: 'summary' } })], adapters, now,
      );
      expect(ids(result.nodes)).toEqual(['deck', 'load', 'rev', 'run', 'write']);
      expect(ids(result.edges)).toEqual(['edited', 'loadOf', 'produced', 'runProduced', 'writeAfterLoad', 'writeOf']);
      expect(new Set(result.nodes.map((node) => node.disclosure))).toEqual(new Set(['summary']));
    });

    it('widens to nothing the grant did not touch: not the chat the run read, not another deck', async () => {
      const result = await authorizeWorkGraph(
        later, scope, [grant({ selection: { nodeIds: ['deck'], edgeIds: [], disclosure: 'details' } })], adapters, now,
      );
      expect(ids(result.nodes)).not.toEqual(expect.arrayContaining(['chat', 'other', 'otherRun']));
      expect(ids(result.edges)).not.toContain('askedIn');
      const named = await authorizeWorkGraph(
        later, scope, [grant({ selection: { nodeIds: ['chat', 'run'], edgeIds: [], disclosure: 'details' } })], adapters, now,
      );
      // A grant on a run or a conversation derives nothing: only a document
      // carries the work done to it.
      expect(ids(named.nodes)).toEqual(['chat', 'run']);
    });

    it('still answers a source that denies the derived work, and a viewer with no grant sees nothing', async () => {
      const denyPipeline: WorkSourceAclAdapter = {
        ...adapter,
        source: 'pipeline',
        async authorizeDisclosure(currentScope, targets) {
          return new Map(targets.map((target) => [target.id, {
            targetId: target.id, decision: 'deny' as const, policyRevision: currentScope.policyRevision,
          }]));
        },
      };
      const denied = await authorizeWorkGraph(
        later, scope, [grant({ selection: { nodeIds: ['deck'], edgeIds: [], disclosure: 'details' } })],
        new Map([['document', allow('document')], ['pipeline', denyPipeline]]), now,
      );
      expect(ids(denied.nodes)).toEqual(['deck', 'rev']);
      const stranger = await authorizeWorkGraph(later, scope, [], adapters, now);
      expect(stranger.nodes).toEqual([]);
      expect(stranger.edges).toEqual([]);
    });
  });
});
