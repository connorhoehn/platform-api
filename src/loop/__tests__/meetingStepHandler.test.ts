// "Create a meeting with the platform team" has to leave a meeting behind.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createMeetingStepHandler, meetingSlug, proposedStart, resolveOrganization, teamNameFrom } from '../meetingStepHandler';

const helpers = { runId: 'run-9', stepId: 'meeting', emit: async () => undefined } as unknown as StepHandlerHelpers;
const node = (data: Record<string, unknown> = {}): PipelineNode => ({
  id: 'meeting', type: 'meeting:create', position: { x: 0, y: 0 },
  data: { type: 'meeting:create', ...data },
} as unknown as PipelineNode);

function deps(over: Record<string, unknown> = {}) {
  const created: Array<Record<string, unknown>> = [];
  const added: Array<{ roomId: string; userId: string; role: string }> = [];
  return {
    created,
    added,
    opts: {
      rooms: {
        createRoom: async (input: Record<string, unknown>) => {
          created.push(input);
          return { ...input, roomId: 'room-1', state: 'active', createdAt: '2026-09-22T09:00:00.000Z', lastActiveAt: '2026-09-22T09:00:00.000Z' };
        },
      },
      members: {
        addMember: async (roomId: string, userId: string, role: string) => {
          added.push({ roomId, userId, role });
          return { roomId, userId, role, addedBy: 'x', addedAt: '' };
        },
      },
      teams: {
        getTeamBySlug: async (_org: string, slug: string) => (slug === 'platform' ? { teamId: 'team-platform', name: 'Platform' } : null),
        listTeamsForUser: async () => [],
        listMembers: async () => [
          { teamId: 'team-platform', userId: 'dev-connor', role: 'admin' },
          { teamId: 'team-platform', userId: 'dev-frank', role: 'member' },
        ],
      },
      profiles: { searchProfiles: async () => [] },
      now: () => new Date('2026-09-22T09:12:00.000Z'),
      ...over,
    } as never,
  };
}

const context = {
  organizationId: 'dev-org',
  requestedBy: 'dev-connor',
  instruction: 'create a meeting with the platform team and create a ppt',
};

describe('the meeting step', () => {
  it('creates a real room with the named team’s members on it', async () => {
    const { opts, created, added } = deps();
    const outcome = await createMeetingStepHandler(opts).execute(node({ title: 'Platform sync' }), context, helpers);

    expect(outcome.status).toBe('completed');
    const output = outcome.output as Record<string, unknown>;
    expect(output).toMatchObject({
      meetingId: 'room-1',
      title: 'Platform sync',
      attendees: ['dev-frank'],
      calendar: 'none',
      createdByRunId: 'run-9',
    });
    expect(output.team).toEqual({ teamId: 'team-platform', name: 'Platform' });
    expect(created[0]).toMatchObject({ orgId: 'dev-org', name: 'Platform sync', createdBy: 'dev-connor' });
    // The requester owns the room; the rest are members, and nobody is added twice.
    expect(added).toEqual([
      { roomId: 'room-1', userId: 'dev-frank', role: 'member' },
      { roomId: 'room-1', userId: 'dev-connor', role: 'owner' },
    ]);
  });

  it('proposes a time and says no calendar was booked', async () => {
    const { opts } = deps();
    const outcome = await createMeetingStepHandler(opts).execute(node(), context, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.startsAt).toBe('2026-09-23T09:00:00.000Z');
    expect(output.calendar).toBe('none');
  });

  it('declares the room as the run’s artifact', async () => {
    const { opts } = deps();
    const outcome = await createMeetingStepHandler(opts).execute(node({ title: 'Platform sync' }), context, helpers);
    expect((outcome.output as Record<string, unknown>).artifacts).toEqual([
      { artifactId: 'room-1', kind: 'meeting', title: 'Platform sync', startsAt: '2026-09-23T09:00:00.000Z' },
    ]);
  });

  it('leaves the attendees empty, with a notice, when the directory has no such team or person', async () => {
    const { opts, added } = deps({
      teams: { getTeamBySlug: async () => null, listTeamsForUser: async () => [], listMembers: async () => [] },
      profiles: { searchProfiles: async () => [] },
    });
    const outcome = await createMeetingStepHandler(opts).execute(node(), { ...context, instruction: 'a meeting with the marketing team' }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.attendees).toEqual([]);
    expect((output.notices as string[])[0]).toContain('No team or person named "marketing"');
    expect(added.map((entry) => entry.userId)).toEqual(['dev-connor']);
  });

  it('says so when the step named no team at all', async () => {
    const { opts } = deps();
    const outcome = await createMeetingStepHandler(opts).execute(node({ title: 'Sync' }), { ...context, instruction: 'set something up' }, helpers);
    expect(((outcome.output as Record<string, unknown>).notices as string[])[0]).toContain('named no team');
  });

  it('refuses to make a room when nothing names an organization', async () => {
    const { opts } = deps();
    const outcome = await createMeetingStepHandler(opts).execute(node(), { requestedBy: 'dev-connor' }, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('no organization');
  });

  it('uses the deployment’s configured organization when the run named none, and says so', async () => {
    const original = process.env.WORK_GRAPH_ORG;
    process.env.WORK_GRAPH_ORG = 'dev-org';
    try {
      const { opts, created } = deps();
      const outcome = await createMeetingStepHandler(opts).execute(node({ title: 'Platform sync' }), { requestedBy: 'dev-connor', instruction: 'with the platform team' }, helpers);
      expect(outcome.status).toBe('completed');
      expect(created[0]).toMatchObject({ orgId: 'dev-org' });
      expect((outcome.output as Record<string, unknown>).notices).toContain('The run named no organization, so the room was made in dev-org.');
    } finally {
      if (original === undefined) delete process.env.WORK_GRAPH_ORG; else process.env.WORK_GRAPH_ORG = original;
    }
  });

  it('adds the person of that name when the directory has no such team', async () => {
    const { opts, added } = deps({
      teams: { getTeamBySlug: async () => null, listTeamsForUser: async () => [], listMembers: async () => [] },
      profiles: { searchProfiles: async () => [{ userId: 'dev-frank', displayName: 'Frank' }] },
    });
    const outcome = await createMeetingStepHandler(opts).execute(node(), { ...context, instruction: 'a meeting with the Frank team' }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.attendees).toEqual(['dev-frank']);
    expect((output.notices as string[])[0]).toContain('No team named "Frank"');
    expect(added.map((entry) => entry.userId)).toEqual(['dev-frank', 'dev-connor']);
  });

  it('finds the team by name when the slug misses', async () => {
    const { opts } = deps({
      teams: {
        getTeamBySlug: async () => null,
        listTeamsForUser: async () => [{ team: { teamId: 'team-p', name: 'Platform', slug: 'the-platform' }, role: 'member', favorite: false }],
        listMembers: async () => [{ teamId: 'team-p', userId: 'dev-frank', role: 'member' }],
      },
    });
    const outcome = await createMeetingStepHandler(opts).execute(node(), context, helpers);
    expect((outcome.output as Record<string, unknown>).attendees).toEqual(['dev-frank']);
  });
});

describe('reading the instruction', () => {
  it('takes the team a sentence names, and nothing it does not', () => {
    expect(teamNameFrom('create a meeting with the platform team and create a ppt')).toBe('platform');
    expect(teamNameFrom('meet the Data Platform team on Tuesday')).toBe('Data Platform');
    expect(teamNameFrom('set up a call')).toBeUndefined();
  });

  it('makes a slug a URL can carry', () => {
    expect(meetingSlug('Platform sync — Q4!', '123456')).toBe('platform-sync-q4-123456');
    expect(meetingSlug('!!!', '1')).toBe('meeting-1');
  });

  it('proposes a whole hour', () => {
    expect(proposedStart(new Date('2026-09-22T09:12:00.000Z'), 60)).toBe('2026-09-22T10:00:00.000Z');
  });
});


describe('resolveOrganization', () => {
  const original = { org: process.env.WORK_GRAPH_ORG, skip: process.env.SKIP_AUTH };
  afterEach(() => {
    if (original.org === undefined) delete process.env.WORK_GRAPH_ORG; else process.env.WORK_GRAPH_ORG = original.org;
    if (original.skip === undefined) delete process.env.SKIP_AUTH; else process.env.SKIP_AUTH = original.skip;
  });

  it('takes what the run declared, without a notice', () => {
    expect(resolveOrganization({ organizationId: 'org-1' })).toEqual({ organizationId: 'org-1' });
  });

  it('falls back to the deployment’s org, and to local dev, saying which', () => {
    delete process.env.WORK_GRAPH_ORG;
    process.env.SKIP_AUTH = 'true';
    expect(resolveOrganization({})).toEqual({
      organizationId: 'default-org',
      notice: 'The run named no organization, so the room was made in default-org.',
    });
  });

  it('answers with nothing when neither the run nor the deployment says', () => {
    delete process.env.WORK_GRAPH_ORG;
    delete process.env.SKIP_AUTH;
    expect(resolveOrganization({})).toEqual({});
  });
});
