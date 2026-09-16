// Integration tests for the W5 hardening surface on /api/rooms:
//   * canCreateRoom hook gates POST (deny → 403 ROOM_CREATE_DENIED)
//   * Sliding-window rate limit (6th create / 1h → 429)
//   * Archive requires owner OR ROOM_ADMIN_USER_IDS member
//
// Repo + member-repo are mocked at module level so we exercise the route
// wiring without touching DDB. The route module reads ROOM_ADMIN_USER_IDS
// at call time (no cache), so we mutate process.env inline per test.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockCreateRoom = jest.fn();
const mockGetRoomBySlug = jest.fn();
const mockArchiveRoom = jest.fn();
const mockTouchLastActive = jest.fn();
const mockListRoomsByOrg = jest.fn();
const mockSearchRooms = jest.fn();
const mockIsMember = jest.fn();
const mockAddMember = jest.fn();
const mockRemoveMember = jest.fn(async () => undefined);
const mockListMembers = jest.fn(async () => [] as unknown[]);

const mockMirrorRoomMembers = jest.fn();
const mockMirrorRoomMemberRemoval = jest.fn();
const mockSyncRoomOnce = jest.fn();
jest.mock('../../gateway/chatMembership', () => ({
  mirrorRoomMembers: (...a: unknown[]) => mockMirrorRoomMembers(...a),
  mirrorRoomMemberRemoval: (...a: unknown[]) => mockMirrorRoomMemberRemoval(...a),
  syncRoomOnce: (...a: unknown[]) => mockSyncRoomOnce(...a),
}));

jest.mock('../../repositories', () => {
  const actual = jest.requireActual('../../repositories');
  return {
    ...actual,
    hangoutRoomRepo: {
      createRoom: mockCreateRoom,
      getRoomBySlug: mockGetRoomBySlug,
      archiveRoom: mockArchiveRoom,
      touchLastActive: mockTouchLastActive,
      listRoomsByOrg: mockListRoomsByOrg,
      searchRooms: mockSearchRooms,
    },
    hangoutRoomMemberRepo: {
      isMember: mockIsMember,
      addMember: mockAddMember,
      removeMember: mockRemoveMember,
      listMembers: mockListMembers,
    },
  };
});

import {
  hangoutRoomsRouter,
  _resetCreateRateLimit,
} from '../hangoutRooms';
import { errorHandler } from '../../middleware/error-handler';
import {
  setCanCreateRoomHook,
  resetCanCreateRoomHook,
} from '../../middleware/canCreateRoom';
import type { HangoutRoom } from '../../repositories';

// ---------------------------------------------------------------------------
// Test harness — minimal Express runner that doesn't require supertest.
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
    user: { sub: opts.userId ?? 'tester' },
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
  app.use('/api/rooms', hangoutRoomsRouter);
  app.use(errorHandler);
  return app;
}

function makeRoom(over: Partial<HangoutRoom> = {}): HangoutRoom {
  return {
    orgId: 'default-org',
    roomId: '01HX',
    slug: 'team-standup',
    name: 'Team Standup',
    createdBy: 'tester',
    createdAt: '2026-05-23T00:00:00.000Z',
    visibility: 'org',
    settings: { audioOnly: false, recordingEnabled: false, softCap: 25 },
    state: 'active',
    lastActiveAt: '2026-05-23T00:00:00.000Z',
    ...over,
  };
}

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

const originalSkipAuth = process.env.SKIP_AUTH;
const originalAdmins = process.env.ROOM_ADMIN_USER_IDS;

beforeEach(() => {
  process.env.SKIP_AUTH = 'true';
  delete process.env.ROOM_ADMIN_USER_IDS;
  mockCreateRoom.mockReset().mockImplementation(async (input) => makeRoom({
    orgId: input.orgId,
    slug: input.slug,
    name: input.name,
    createdBy: input.createdBy,
    visibility: input.visibility,
    settings: input.settings,
  }));
  mockGetRoomBySlug.mockReset().mockResolvedValue(null);
  mockArchiveRoom.mockReset().mockResolvedValue(undefined);
  mockTouchLastActive.mockReset().mockResolvedValue(undefined);
  mockListRoomsByOrg.mockReset().mockResolvedValue({ items: [] });
  mockSearchRooms.mockReset().mockResolvedValue([]);
  mockIsMember.mockReset().mockResolvedValue(false);
  mockAddMember.mockReset().mockResolvedValue(undefined);
  resetCanCreateRoomHook();
  _resetCreateRateLimit();
});

afterAll(() => {
  if (originalSkipAuth === undefined) delete process.env.SKIP_AUTH;
  else process.env.SKIP_AUTH = originalSkipAuth;
  if (originalAdmins === undefined) delete process.env.ROOM_ADMIN_USER_IDS;
  else process.env.ROOM_ADMIN_USER_IDS = originalAdmins;
});

// ---------------------------------------------------------------------------
// canCreateRoom hook
// ---------------------------------------------------------------------------

describe('POST /api/rooms — canCreateRoom hook', () => {
  it('default-open hook allows creation', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'Allowed Room' },
    });
    expect(res.statusCode).toBe(201);
    expect(mockCreateRoom).toHaveBeenCalledTimes(1);
  });

  it('hook returns false → 403 ROOM_CREATE_DENIED', async () => {
    setCanCreateRoomHook(() => false);
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'Denied Room' },
    });
    expect(res.statusCode).toBe(403);
    expect(res.body).toEqual({ error: { code: 'ROOM_CREATE_DENIED' } });
    expect(mockCreateRoom).not.toHaveBeenCalled();
  });

  it('hook that throws → fails closed (403)', async () => {
    setCanCreateRoomHook(() => { throw new Error('downstream blew up'); });
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'Blown Up' },
    });
    expect(res.statusCode).toBe(403);
    expect(mockCreateRoom).not.toHaveBeenCalled();
  });

  it('async hook is awaited', async () => {
    setCanCreateRoomHook(async () => false);
    const res = await run(buildApp(), 'POST', '/api/rooms', {
      body: { name: 'Async Denied' },
    });
    expect(res.statusCode).toBe(403);
  });
});

// ---------------------------------------------------------------------------
// Per-user rate limit (5 / rolling 1h)
// ---------------------------------------------------------------------------

describe('POST /api/rooms — create rate limit', () => {
  it('allows 5 creates then 429s on the 6th within 1h', async () => {
    const app = buildApp();
    // 5 OK
    for (let i = 0; i < 5; i++) {
      const res = await run(app, 'POST', '/api/rooms', {
        body: { name: `Room ${i}` },
      });
      expect(res.statusCode).toBe(201);
    }
    // 6th denied
    const blocked = await run(app, 'POST', '/api/rooms', {
      body: { name: 'Six' },
    });
    expect(blocked.statusCode).toBe(429);
    const body = blocked.body as { error: { code: string } };
    expect(body.error.code).toBe('ROOM_CREATE_RATE_LIMITED');
    // Only the first 5 should have hit the repo.
    expect(mockCreateRoom).toHaveBeenCalledTimes(5);
  });

  it('rate limit is per-user — userA exhausting does not affect userB', async () => {
    const app = buildApp();
    for (let i = 0; i < 5; i++) {
      const res = await run(app, 'POST', '/api/rooms', {
        body: { name: `A-${i}` }, userId: 'user-a',
      });
      expect(res.statusCode).toBe(201);
    }
    const blockedA = await run(app, 'POST', '/api/rooms', {
      body: { name: 'A-extra' }, userId: 'user-a',
    });
    expect(blockedA.statusCode).toBe(429);

    const okB = await run(app, 'POST', '/api/rooms', {
      body: { name: 'B-1' }, userId: 'user-b',
    });
    expect(okB.statusCode).toBe(201);
  });
});

// ---------------------------------------------------------------------------
// Archive: owner OR admin
// ---------------------------------------------------------------------------

describe('POST /api/rooms/:slug/archive — owner or admin gate', () => {
  it('owner can archive', async () => {
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'tester' }));
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/archive', {
      userId: 'tester',
    });
    expect(res.statusCode).toBe(200);
    expect(mockArchiveRoom).toHaveBeenCalledTimes(1);
  });

  it('non-owner non-admin → 403', async () => {
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'owner-1' }));
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/archive', {
      userId: 'stranger',
    });
    expect(res.statusCode).toBe(403);
    expect(mockArchiveRoom).not.toHaveBeenCalled();
  });

  it('ROOM_ADMIN_USER_IDS member can archive even if not owner', async () => {
    process.env.ROOM_ADMIN_USER_IDS = 'admin-1, admin-2';
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'owner-1' }));
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/archive', {
      userId: 'admin-2',
    });
    expect(res.statusCode).toBe(200);
    expect(mockArchiveRoom).toHaveBeenCalledTimes(1);
  });

  it('empty ROOM_ADMIN_USER_IDS means only owner', async () => {
    process.env.ROOM_ADMIN_USER_IDS = '';
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'owner-1' }));
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/archive', {
      userId: 'someone-else',
    });
    expect(res.statusCode).toBe(403);
  });
});

// ---------------------------------------------------------------------------
// GET /api/rooms?q= search filter
// ---------------------------------------------------------------------------

describe('GET /api/rooms?q= — search route', () => {
  it('delegates to repo.searchRooms when q is non-empty', async () => {
    const lounge = makeRoom({ roomId: 'r-a', slug: 'engineering-lounge', name: 'Engineering Lounge' });
    mockSearchRooms.mockResolvedValue([lounge]);

    const res = await run(buildApp(), 'GET', '/api/rooms?q=lounge');
    expect(res.statusCode).toBe(200);
    const body = res.body as { rooms: HangoutRoom[]; cursor?: string };
    expect(body.rooms).toHaveLength(1);
    expect(body.rooms[0]?.slug).toBe('engineering-lounge');
    expect(body.cursor).toBeUndefined();

    expect(mockSearchRooms).toHaveBeenCalledTimes(1);
    expect(mockSearchRooms.mock.calls[0][1]).toMatchObject({ q: 'lounge', limit: 100 });
    expect(mockListRoomsByOrg).not.toHaveBeenCalled();
  });

  it('treats empty/whitespace q as no filter (uses list path)', async () => {
    mockListRoomsByOrg.mockResolvedValue({ items: [makeRoom()] });

    const res = await run(buildApp(), 'GET', '/api/rooms?q=%20');
    expect(res.statusCode).toBe(200);
    expect(mockSearchRooms).not.toHaveBeenCalled();
    expect(mockListRoomsByOrg).toHaveBeenCalledTimes(1);
  });

  it('forwards state=archived to repo.searchRooms', async () => {
    mockSearchRooms.mockResolvedValue([]);

    const res = await run(buildApp(), 'GET', '/api/rooms?q=lounge&state=archived');
    expect(res.statusCode).toBe(200);
    expect(mockSearchRooms.mock.calls[0][1]).toMatchObject({
      q: 'lounge',
      state: 'archived',
    });
  });
});

// ---------------------------------------------------------------------------
// GET /api/rooms?state=archived — archived view passes through fields
// ---------------------------------------------------------------------------

describe('GET /api/rooms?state=archived', () => {
  it('returns rooms with archivedAt + ttl populated', async () => {
    const archived = makeRoom({
      state: 'archived',
      archivedAt: '2026-05-22T00:00:00.000Z',
      ttl: 1234567890,
    });
    mockListRoomsByOrg.mockResolvedValue({ items: [archived] });

    const res = await run(buildApp(), 'GET', '/api/rooms?state=archived');
    expect(res.statusCode).toBe(200);
    const body = res.body as { rooms: HangoutRoom[] };
    expect(body.rooms).toHaveLength(1);
    expect(body.rooms[0]?.archivedAt).toBe('2026-05-22T00:00:00.000Z');
    expect(body.rooms[0]?.ttl).toBe(1234567890);
    expect(mockListRoomsByOrg).toHaveBeenCalledWith(
      'default-org',
      expect.objectContaining({ state: 'archived' }),
    );
  });
});

// ---------------------------------------------------------------------------
// POST /api/rooms/:slug/join — the room bridge, and the double-join it causes
// ---------------------------------------------------------------------------
//
// Entering a room joins the video session TWICE: once here, and again when the
// call widget calls useVideoCall.startCall against the same lobby (the client
// deliberately drops the token this route returns — see
// HangoutDomainProvider.joinRoomBySlug). Leaving hangs up ONCE. That net +1
// participant per call is why room sessions never reach a zero ref-count.
//
// videoSessions' /join now collapses repeat joins from one client. When the
// client identifies its tab, that `clientId` has to reach BOTH joins or they
// look like two different tabs and the leak comes straight back.

describe('POST /api/rooms/:slug/join — clientId reaches the video join', () => {
  const realFetch = globalThis.fetch;
  let calls: Array<{ url: string; body: Record<string, unknown> }>;

  beforeEach(() => {
    calls = [];
    (globalThis as unknown as { fetch: unknown }).fetch = jest.fn(
      async (url: string, init: RequestInit) => {
        const body = init?.body ? JSON.parse(init.body as string) : {};
        calls.push({ url: String(url), body });
        const payload = String(url).includes('/join')
          ? { token: 'jwt', participantId: 'alice-bridge' }
          : { sessionId: 'vs-1' };
        return {
          status: 200,
          text: async () => JSON.stringify(payload),
        } as unknown as globalThis.Response;
      },
    );
    mockGetRoomBySlug.mockResolvedValue(makeRoom());
  });

  afterEach(() => {
    (globalThis as unknown as { fetch: unknown }).fetch = realFetch;
  });

  it('forwards the caller clientId to /api/video/sessions/:id/join', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/join', {
      body: { clientId: 'tab-7' },
    });

    expect(res.statusCode).toBe(200);
    const videoJoin = calls.find((c) => c.url.includes('/api/video/sessions/vs-1/join'));
    expect(videoJoin).toBeDefined();
    expect(videoJoin!.body).toMatchObject({ lobbyName: 'room:team-standup', clientId: 'tab-7' });
  });

  it('omits clientId entirely when the caller does not send one', async () => {
    // Older clients: /join then collapses on userId instead, which still
    // catches the bridge + widget pair.
    await run(buildApp(), 'POST', '/api/rooms/team-standup/join', { body: {} });

    const videoJoin = calls.find((c) => c.url.includes('/api/video/sessions/vs-1/join'));
    expect(videoJoin!.body).not.toHaveProperty('clientId');
  });
});

describe('POST /api/rooms/:slug/leave — clientId reaches the video end', () => {
  const realFetch = globalThis.fetch;
  let calls: Array<{ url: string; body: Record<string, unknown> }>;

  beforeEach(() => {
    calls = [];
    (globalThis as unknown as { fetch: unknown }).fetch = jest.fn(
      async (url: string, init: RequestInit) => {
        calls.push({ url: String(url), body: init?.body ? JSON.parse(init.body as string) : {} });
        return { status: 200, text: async () => JSON.stringify({ ended: true }) } as unknown as globalThis.Response;
      },
    );
    mockGetRoomBySlug.mockResolvedValue(makeRoom());
  });

  afterEach(() => {
    (globalThis as unknown as { fetch: unknown }).fetch = realFetch;
  });

  it('forwards participantId and clientId to the video /end', async () => {
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/leave', {
      body: { sessionId: 'vs-1', participantId: 'alice-widget', clientId: 'tab-7' },
    });

    expect(res.statusCode).toBe(200);
    expect(calls[0].url).toContain('/api/video/sessions/vs-1/end');
    expect(calls[0].body).toMatchObject({
      lobbyName: 'room:team-standup',
      participantId: 'alice-widget',
      clientId: 'tab-7',
    });
  });
});

// ---------------------------------------------------------------------------
// The room's conversation follows the room's membership (gateway mirror)
// ---------------------------------------------------------------------------

describe('room membership → chat membership mirror', () => {
  beforeEach(() => { mockMirrorRoomMembers.mockClear(); mockMirrorRoomMemberRemoval.mockClear(); mockSyncRoomOnce.mockClear(); });

  it('adding a member mirrors them into the conversation as the owner', async () => {
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'tester', visibility: 'private' }));
    mockAddMember.mockResolvedValue({ roomId: 'r1', userId: 'carol', role: 'member', addedBy: 'tester', addedAt: 'now' });
    const res = await run(buildApp(), 'POST', '/api/rooms/team-standup/members', { userId: 'tester', body: { userId: 'carol' } });
    expect(res.statusCode).toBe(201);
    expect(mockMirrorRoomMembers).toHaveBeenCalledWith({ slug: 'team-standup', ownerId: 'tester', members: [{ userId: 'carol' }] });
  });

  it('removing a member mirrors the removal', async () => {
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'tester', visibility: 'private' }));
    const res = await run(buildApp(), 'DELETE', '/api/rooms/team-standup/members/carol', { userId: 'tester' });
    expect(res.statusCode).toBe(204);
    expect(mockMirrorRoomMemberRemoval).toHaveBeenCalledWith({ slug: 'team-standup', userId: 'carol', byOwnerId: 'tester' });
  });

  it('loading a private room syncs its roster once; an org room does not', async () => {
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'tester', visibility: 'private' }));
    mockIsMember.mockResolvedValue(true);
    let res = await run(buildApp(), 'GET', '/api/rooms/team-standup', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    expect(mockSyncRoomOnce).toHaveBeenCalledWith('team-standup', expect.any(Function));
    mockSyncRoomOnce.mockClear();
    mockGetRoomBySlug.mockResolvedValue(makeRoom({ createdBy: 'tester', visibility: 'org' }));
    res = await run(buildApp(), 'GET', '/api/rooms/team-standup', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    expect(mockSyncRoomOnce).not.toHaveBeenCalled();
  });
});
