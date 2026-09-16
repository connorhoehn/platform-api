// Integration tests for /api/teams — teams and the channels inside them.
//
// The structural claims worth pinning:
//   * creating a team creates its General channel (a team with no channel
//     is not a team)
//   * a standard channel starts with the whole team in it; a private one
//     starts with only its creator
//   * a private team does not admit it exists to a non-member
//   * leaving a team takes you out of every one of its channels
//   * slug collisions suffix rather than 409 on the first clash
//
// Repos and the gateway mirror are mocked at module level so we exercise
// route wiring without DDB or a socket. Harness mirrors hangoutRooms.test.ts.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateTeam = jest.fn();
const mockGetTeam = jest.fn();
const mockGetTeamBySlug = jest.fn();
const mockListTeamsForUser = jest.fn();
const mockAddTeamMember = jest.fn();
const mockRemoveTeamMember = jest.fn();
const mockListTeamMembers = jest.fn();
const mockGetMembership = jest.fn();

const mockCreateRoom = jest.fn();
const mockListRoomsByTeam = jest.fn();
const mockAddRoomMember = jest.fn();
const mockRemoveRoomMember = jest.fn();
const mockIsRoomMember = jest.fn();
const mockGetProfile = jest.fn();

const mockMirrorRoomMembers = jest.fn();
const mockMirrorRoomMemberRemoval = jest.fn();
jest.mock('../../gateway/chatMembership', () => ({
  mirrorRoomMembers: (...a: unknown[]) => mockMirrorRoomMembers(...a),
  mirrorRoomMemberRemoval: (...a: unknown[]) => mockMirrorRoomMemberRemoval(...a),
  syncRoomOnce: jest.fn(),
}));

jest.mock('../../repositories', () => {
  const actual = jest.requireActual('../../repositories');
  return {
    ...actual,
    teamRepo: {
      createTeam: mockCreateTeam,
      getTeam: mockGetTeam,
      getTeamBySlug: mockGetTeamBySlug,
      listTeamsForUser: mockListTeamsForUser,
      addMember: mockAddTeamMember,
      removeMember: mockRemoveTeamMember,
      listMembers: mockListTeamMembers,
      getMembership: mockGetMembership,
    },
    hangoutRoomRepo: {
      createRoom: mockCreateRoom,
      listRoomsByTeam: mockListRoomsByTeam,
      getRoomBySlug: jest.fn().mockResolvedValue(null),
      touchLastActive: jest.fn(),
    },
    hangoutRoomMemberRepo: {
      addMember: mockAddRoomMember,
      removeMember: mockRemoveRoomMember,
      isMember: mockIsRoomMember,
      listMembers: jest.fn().mockResolvedValue([]),
    },
    profileRepo: { getProfile: mockGetProfile },
  };
});

import { teamsRouter, channelSlug } from '../teams';
import { errorHandler } from '../../middleware/error-handler';
import { RoomSlugConflictError, type HangoutRoom, type TeamItem } from '../../repositories';

// ---------------------------------------------------------------------------
// Harness
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  setHeader(_n: string, _v: unknown): MockRes;
  getHeader(_n: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolve!: () => void;
  const finished = new Promise<void>((r) => { resolve = r; });
  const r: MockRes = {
    statusCode: 200,
    body: undefined,
    ended: false,
    finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolve(); return this; },
    send(body) { this.body = body; this.ended = true; resolve(); return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end(..._args) { this.ended = true; resolve(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    for (const [k, v] of params.entries()) query[k] = v;
  }
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query,
    body: opts.body,
    user: { sub: opts.userId ?? 'dev-eve' },
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolveOuter, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) res.status(404).json({ error: 'route not found' });
      resolveOuter();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolveOuter()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/teams', teamsRouter);
  app.use(errorHandler);
  return app;
}

function makeTeam(over: Partial<TeamItem> = {}): TeamItem {
  return {
    orgId: 'default-org',
    teamId: '01TEAM',
    slug: 'design',
    name: 'Design',
    visibility: 'org',
    createdBy: 'dev-eve',
    createdAt: '2026-09-16T00:00:00.000Z',
    updatedAt: '2026-09-16T00:00:00.000Z',
    state: 'active',
    ...over,
  };
}

function makeChannel(over: Partial<HangoutRoom> = {}): HangoutRoom {
  return {
    orgId: 'default-org',
    roomId: '01ROOM',
    slug: 'design-general',
    name: 'General',
    createdBy: 'dev-eve',
    createdAt: '2026-09-16T00:00:00.000Z',
    visibility: 'org',
    settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
    state: 'active',
    lastActiveAt: '2026-09-16T00:00:00.000Z',
    teamId: '01TEAM',
    channelPrivacy: 'standard',
    ...over,
  };
}

const originalSkipAuth = process.env.SKIP_AUTH;

beforeEach(() => {
  process.env.SKIP_AUTH = 'true';
  mockCreateTeam.mockReset().mockImplementation(async (input) => makeTeam({
    orgId: input.orgId,
    slug: input.slug,
    name: input.name,
    visibility: input.visibility,
    createdBy: input.createdBy,
  }));
  mockGetTeam.mockReset().mockResolvedValue(makeTeam());
  mockGetTeamBySlug.mockReset().mockResolvedValue(null);
  mockListTeamsForUser.mockReset().mockResolvedValue([]);
  mockAddTeamMember.mockReset().mockImplementation(async (teamId, userId, role, addedBy) => ({
    teamId, userId, role, addedBy, joinedAt: '2026-09-16T00:00:00.000Z',
  }));
  mockRemoveTeamMember.mockReset().mockResolvedValue(undefined);
  mockListTeamMembers.mockReset().mockResolvedValue([
    { teamId: '01TEAM', userId: 'dev-eve', role: 'owner', addedBy: 'dev-eve', joinedAt: '' },
  ]);
  mockGetMembership.mockReset().mockResolvedValue({
    teamId: '01TEAM', userId: 'dev-eve', role: 'owner', addedBy: 'dev-eve', joinedAt: '',
  });

  mockCreateRoom.mockReset().mockImplementation(async (input) => makeChannel({
    slug: input.slug,
    name: input.name,
    visibility: input.visibility,
    createdBy: input.createdBy,
    teamId: input.teamId,
    channelPrivacy: input.channelPrivacy,
  }));
  mockListRoomsByTeam.mockReset().mockResolvedValue([]);
  mockAddRoomMember.mockReset().mockResolvedValue(undefined);
  mockRemoveRoomMember.mockReset().mockResolvedValue(undefined);
  mockIsRoomMember.mockReset().mockResolvedValue(false);
  mockGetProfile.mockReset().mockResolvedValue(null);
  mockMirrorRoomMembers.mockReset();
  mockMirrorRoomMemberRemoval.mockReset();
});

afterAll(() => {
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
  else process.env.SKIP_AUTH = originalSkipAuth;
});

// ---------------------------------------------------------------------------

describe('POST /api/teams', () => {
  it('creates the team, its owner, and a General channel', async () => {
    const res = await run(buildApp(), 'POST', '/api/teams', {
      body: { name: 'Design' },
      userId: 'dev-eve',
    });

    expect(res.statusCode).toBe(201);
    const body = res.body as { team: TeamItem; channels: HangoutRoom[] };
    expect(body.team.slug).toBe('design');
    expect(mockAddTeamMember).toHaveBeenCalledWith('01TEAM', 'dev-eve', 'owner', 'dev-eve');

    expect(body.channels).toHaveLength(1);
    expect(body.channels[0]?.name).toBe('General');
    expect(mockCreateRoom).toHaveBeenCalledWith(
      expect.objectContaining({ slug: 'design-general', teamId: '01TEAM', channelPrivacy: 'standard' }),
    );
  });

  it('suffixes the slug when the name is taken', async () => {
    mockCreateTeam
      .mockRejectedValueOnce(Object.assign(new Error('taken'), { statusCode: 409 }))
      .mockImplementationOnce(async (input) => makeTeam({ slug: input.slug }));

    const res = await run(buildApp(), 'POST', '/api/teams', { body: { name: 'Design' } });

    expect(res.statusCode).toBe(201);
    expect(mockCreateTeam).toHaveBeenNthCalledWith(1, expect.objectContaining({ slug: 'design' }));
    expect(mockCreateTeam).toHaveBeenNthCalledWith(2, expect.objectContaining({ slug: 'design-2' }));
  });

  it('rejects a missing name', async () => {
    const res = await run(buildApp(), 'POST', '/api/teams', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it("puts a private team's General in a private room", async () => {
    await run(buildApp(), 'POST', '/api/teams', {
      body: { name: 'Design', visibility: 'private' },
    });
    expect(mockCreateRoom).toHaveBeenCalledWith(
      expect.objectContaining({ visibility: 'private', channelPrivacy: 'standard' }),
    );
  });
});

describe('POST /api/teams/:teamId/channels', () => {
  it('starts a standard channel with the whole team in it', async () => {
    mockListTeamMembers.mockResolvedValue([
      { teamId: '01TEAM', userId: 'dev-eve', role: 'owner', addedBy: 'dev-eve', joinedAt: '' },
      { teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '' },
    ]);

    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/channels', {
      body: { name: 'Standups' },
      userId: 'dev-eve',
    });

    expect(res.statusCode).toBe(201);
    expect(mockCreateRoom).toHaveBeenCalledWith(
      expect.objectContaining({ slug: 'design-standups', channelPrivacy: 'standard', visibility: 'org' }),
    );
    const added = mockAddRoomMember.mock.calls.map((c) => c[1]);
    expect(added.sort()).toEqual(['dev-carol', 'dev-eve']);
    expect(mockMirrorRoomMembers).toHaveBeenCalledWith(
      expect.objectContaining({
        slug: 'design-standups',
        ownerId: 'dev-eve',
        members: [expect.objectContaining({ userId: 'dev-carol' })],
      }),
    );
  });

  it('starts a private channel with only its creator', async () => {
    mockListTeamMembers.mockResolvedValue([
      { teamId: '01TEAM', userId: 'dev-eve', role: 'owner', addedBy: 'dev-eve', joinedAt: '' },
      { teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '' },
    ]);

    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/channels', {
      body: { name: 'Leads only', privacy: 'private' },
      userId: 'dev-eve',
    });

    expect(res.statusCode).toBe(201);
    expect(mockCreateRoom).toHaveBeenCalledWith(
      expect.objectContaining({ channelPrivacy: 'private', visibility: 'private' }),
    );
    expect(mockAddRoomMember.mock.calls.map((c) => c[1])).toEqual(['dev-eve']);
  });

  it('lets a plain member create one, as Teams does', async () => {
    mockGetMembership.mockResolvedValue({
      teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '',
    });
    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/channels', {
      body: { name: 'Ideas' },
      userId: 'dev-carol',
    });
    expect(res.statusCode).toBe(201);
  });

  it('suffixes a channel slug that is taken', async () => {
    mockCreateRoom
      .mockRejectedValueOnce(new RoomSlugConflictError('design-standups'))
      .mockImplementationOnce(async (input) => makeChannel({ slug: input.slug }));

    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/channels', {
      body: { name: 'Standups' },
    });

    expect(res.statusCode).toBe(201);
    expect(mockCreateRoom).toHaveBeenNthCalledWith(2, expect.objectContaining({ slug: 'design-standups-2' }));
  });
});

describe('private teams', () => {
  it('do not admit they exist to a non-member', async () => {
    mockGetTeam.mockResolvedValue(makeTeam({ visibility: 'private' }));
    mockGetMembership.mockResolvedValue(null);

    const res = await run(buildApp(), 'GET', '/api/teams/01TEAM', { userId: 'dev-stranger' });
    expect(res.statusCode).toBe(404);
  });

  it('are readable by a member', async () => {
    mockGetTeam.mockResolvedValue(makeTeam({ visibility: 'private' }));
    mockListRoomsByTeam.mockResolvedValue([makeChannel()]);

    const res = await run(buildApp(), 'GET', '/api/teams/01TEAM', { userId: 'dev-eve' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { role: string; channels: HangoutRoom[] };
    expect(body.role).toBe('owner');
    expect(body.channels).toHaveLength(1);
  });

  it('keep a private channel out of a non-member’s view', async () => {
    mockListRoomsByTeam.mockResolvedValue([
      makeChannel(),
      makeChannel({ roomId: '02ROOM', slug: 'design-leads', channelPrivacy: 'private', createdBy: 'dev-eve' }),
    ]);
    mockGetMembership.mockResolvedValue({
      teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '',
    });
    mockIsRoomMember.mockResolvedValue(false);

    const res = await run(buildApp(), 'GET', '/api/teams/01TEAM', { userId: 'dev-carol' });
    const body = res.body as { channels: HangoutRoom[] };
    expect(body.channels.map((c) => c.slug)).toEqual(['design-general']);
  });
});

describe('team membership', () => {
  it('adds a member to every standard channel, not the private ones', async () => {
    mockListRoomsByTeam.mockResolvedValue([
      makeChannel(),
      makeChannel({ roomId: '02ROOM', slug: 'design-leads', channelPrivacy: 'private' }),
    ]);

    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/members', {
      body: { userId: 'dev-carol', displayName: 'Carol Johnson', byName: 'Eve Thompson' },
      userId: 'dev-eve',
    });

    expect(res.statusCode).toBe(201);
    expect(mockAddTeamMember).toHaveBeenCalledWith('01TEAM', 'dev-carol', 'member', 'dev-eve');
    expect(mockAddRoomMember.mock.calls.map((c) => c[0])).toEqual(['01ROOM']);
    expect(mockMirrorRoomMembers).toHaveBeenCalledWith(
      expect.objectContaining({
        slug: 'design-general',
        members: [{ userId: 'dev-carol', displayName: 'Carol Johnson' }],
      }),
    );
  });

  it('refuses a non-owner adding people', async () => {
    mockGetMembership.mockResolvedValue({
      teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '',
    });
    const res = await run(buildApp(), 'POST', '/api/teams/01TEAM/members', {
      body: { userId: 'dev-dave' },
      userId: 'dev-carol',
    });
    expect(res.statusCode).toBe(403);
  });

  it('leaving takes you out of every channel, private ones included', async () => {
    mockListRoomsByTeam.mockResolvedValue([
      makeChannel(),
      makeChannel({ roomId: '02ROOM', slug: 'design-leads', channelPrivacy: 'private' }),
    ]);
    mockGetMembership.mockResolvedValue({
      teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '',
    });

    const res = await run(buildApp(), 'DELETE', '/api/teams/01TEAM/members/dev-carol', {
      userId: 'dev-carol',
    });

    expect(res.statusCode).toBe(204);
    expect(mockRemoveTeamMember).toHaveBeenCalledWith('01TEAM', 'dev-carol');
    expect(mockRemoveRoomMember.mock.calls.map((c) => c[0])).toEqual(['01ROOM', '02ROOM']);
    expect(mockMirrorRoomMemberRemoval).toHaveBeenCalledTimes(2);
  });

  it('refuses removing someone else when you are not the owner', async () => {
    mockGetMembership.mockResolvedValue({
      teamId: '01TEAM', userId: 'dev-carol', role: 'member', addedBy: 'dev-eve', joinedAt: '',
    });
    const res = await run(buildApp(), 'DELETE', '/api/teams/01TEAM/members/dev-dave', {
      userId: 'dev-carol',
    });
    expect(res.statusCode).toBe(403);
  });
});

describe('GET /api/teams', () => {
  it('returns each team with its channels, for the rail', async () => {
    mockListTeamsForUser.mockResolvedValue([{ team: makeTeam(), role: 'owner' }]);
    mockListRoomsByTeam.mockResolvedValue([makeChannel()]);

    const res = await run(buildApp(), 'GET', '/api/teams', { userId: 'dev-eve' });

    expect(res.statusCode).toBe(200);
    const body = res.body as { teams: Array<TeamItem & { role: string; channels: HangoutRoom[] }> };
    expect(body.teams).toHaveLength(1);
    expect(body.teams[0]?.role).toBe('owner');
    expect(body.teams[0]?.channels.map((c) => c.name)).toEqual(['General']);
  });
});

describe('channelSlug', () => {
  it('carries the team so two teams can each have a General', () => {
    expect(channelSlug('design', 'General')).toBe('design-general');
    expect(channelSlug('sales', 'General')).toBe('sales-general');
    expect(channelSlug('design', 'Weekly Standup!')).toBe('design-weekly-standup');
  });
});
