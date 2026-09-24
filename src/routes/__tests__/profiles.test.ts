// Integration tests for /api/profiles routes.
//
// Mocks profileRepo, cache, and docClient at module level. Covers create,
// read, update, search with visibility gating, and validation.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockGetProfile = jest.fn();
const mockCreateProfile = jest.fn();
const mockUpdateProfile = jest.fn();
const mockSearchProfiles = jest.fn();

jest.mock('../../repositories', () => ({
  profileRepo: {
    getProfile: mockGetProfile,
    createProfile: mockCreateProfile,
    updateProfile: mockUpdateProfile,
    searchProfiles: mockSearchProfiles,
  },
}));

jest.mock('../../lib/cache', () => ({
  getCachedProfile: jest.fn().mockResolvedValue(null),
  setCachedProfile: jest.fn().mockResolvedValue(undefined),
  invalidateProfileCache: jest.fn().mockResolvedValue(undefined),
}));

jest.mock('../../lib/aws-clients', () => ({
  docClient: {
    send: jest.fn().mockResolvedValue({ Items: [] }),
  },
}));

import { profilesRouter } from '../profiles';
import { errorHandler } from '../../middleware/error-handler';

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
    statusCode: 200, body: undefined, ended: false, finished,
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
  app.use('/api/profiles', profilesRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockGetProfile.mockReset();
  mockCreateProfile.mockReset().mockResolvedValue(undefined);
  mockUpdateProfile.mockReset();
  mockSearchProfiles.mockReset();
});

describe('POST /api/profiles', () => {
  it('returns 201 with the created profile', async () => {
    mockGetProfile.mockResolvedValue(null);
    const res = await run(buildApp(), 'POST', '/api/profiles', {
      body: { displayName: 'Alice', bio: 'hello' },
    });
    expect(res.statusCode).toBe(201);
    const body = res.body as { userId: string; displayName: string; bio: string };
    expect(body.displayName).toBe('Alice');
    expect(body.bio).toBe('hello');
    expect(mockCreateProfile).toHaveBeenCalledTimes(1);
  });

  it('rejects missing displayName with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/profiles', { body: {} });
    expect(res.statusCode).toBe(400);
  });

  it('rejects displayName over 50 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/profiles', {
      body: { displayName: 'x'.repeat(51) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects bio over 160 chars with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/profiles', {
      body: { displayName: 'Alice', bio: 'x'.repeat(161) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('rejects invalid visibility with 400', async () => {
    const res = await run(buildApp(), 'POST', '/api/profiles', {
      body: { displayName: 'Alice', visibility: 'hidden' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 409 when profile already exists', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Existing' });
    const res = await run(buildApp(), 'POST', '/api/profiles', {
      body: { displayName: 'Alice' },
    });
    expect(res.statusCode).toBe(409);
  });
});

describe('GET /api/profiles/:userId', () => {
  it('returns 200 with the profile', async () => {
    mockGetProfile.mockResolvedValue({
      userId: 'user-1', displayName: 'Bob', visibility: 'public',
    });
    const res = await run(buildApp(), 'GET', '/api/profiles/user-1');
    expect(res.statusCode).toBe(200);
    expect((res.body as { displayName: string }).displayName).toBe('Bob');
  });

  it('returns 404 when profile does not exist', async () => {
    mockGetProfile.mockResolvedValue(null);
    const res = await run(buildApp(), 'GET', '/api/profiles/nobody');
    expect(res.statusCode).toBe(404);
  });

  it('returns 403 for private profile viewed by non-owner', async () => {
    mockGetProfile.mockResolvedValue({
      userId: 'other', displayName: 'Private Person', visibility: 'private',
    });
    const res = await run(buildApp(), 'GET', '/api/profiles/other', { userId: 'tester' });
    expect(res.statusCode).toBe(403);
  });

  it('returns 200 for private profile viewed by owner', async () => {
    mockGetProfile.mockResolvedValue({
      userId: 'tester', displayName: 'Me', visibility: 'private',
    });
    const res = await run(buildApp(), 'GET', '/api/profiles/tester');
    expect(res.statusCode).toBe(200);
  });
});

describe('GET /api/profiles/batch — NFR #175', () => {
  it('a private profile of someone else shrinks to forwarding status; public and own rows are whole', async () => {
    const rows: Record<string, unknown> = {
      pub: { userId: 'pub', displayName: 'Pub', bio: 'hi', visibility: 'public' },
      priv: { userId: 'priv', displayName: 'Secret', bio: 'hidden', visibility: 'private', callForwarding: { mode: 'voicemail' } },
      tester: { userId: 'tester', displayName: 'Me', bio: 'mine', visibility: 'private' },
    };
    mockGetProfile.mockImplementation(async (id: string) => rows[id] ?? null);
    const res = await run(buildApp(), 'GET', '/api/profiles/batch?ids=pub,priv,tester', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    const profiles = (res.body as { profiles: Array<Record<string, unknown>> }).profiles;
    expect(profiles[0]).toMatchObject({ displayName: 'Pub', bio: 'hi' });
    expect(profiles[1]).toEqual({ userId: 'priv', visibility: 'private', callForwarding: { mode: 'voicemail' } });
    expect(profiles[2]).toMatchObject({ displayName: 'Me', bio: 'mine' });
  });
});

describe('PUT /api/profiles', () => {
  it('returns 200 with updated profile', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Old' });
    mockUpdateProfile.mockResolvedValue({ userId: 'tester', displayName: 'New' });

    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { displayName: 'New' },
    });
    expect(res.statusCode).toBe(200);
    expect((res.body as { displayName: string }).displayName).toBe('New');
  });

  it('returns 404 when profile does not exist', async () => {
    mockGetProfile.mockResolvedValue(null);
    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { displayName: 'New' },
    });
    expect(res.statusCode).toBe(404);
  });

  it('rejects invalid visibility with 400', async () => {
    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { visibility: 'secret' },
    });
    expect(res.statusCode).toBe(400);
  });
});

describe('GET /api/profiles?q=<search>', () => {
  it('returns 200 with matching public profiles', async () => {
    mockSearchProfiles.mockResolvedValue([
      { userId: 'u1', displayName: 'Alice Smith', visibility: 'public' },
    ]);
    const res = await run(buildApp(), 'GET', '/api/profiles?q=alice');
    expect(res.statusCode).toBe(200);
    expect((res.body as { profiles: unknown[] }).profiles).toHaveLength(1);
  });

  it('returns 400 when q is missing', async () => {
    const res = await run(buildApp(), 'GET', '/api/profiles');
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 when q exceeds 100 chars', async () => {
    const res = await run(buildApp(), 'GET', `/api/profiles?q=${'x'.repeat(101)}`);
    expect(res.statusCode).toBe(400);
  });

  it('excludes private profiles of non-mutual users from results', async () => {
    mockSearchProfiles.mockResolvedValue([
      { userId: 'alice', displayName: 'Alice', visibility: 'public' },
      { userId: 'bob', displayName: 'Bob', visibility: 'private' }, // not a mutual
    ]);

    // docClient.send for following query returns empty (tester follows nobody)
    const { docClient } = await import('../../lib/aws-clients.js');
    (docClient.send as jest.Mock).mockResolvedValue({ Items: [] });

    const res = await run(buildApp(), 'GET', '/api/profiles?q=bob', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { profiles: Array<{ userId: string }> };
    expect(body.profiles.every((p) => p.userId !== 'bob')).toBe(true);
    expect(body.profiles.some((p) => p.userId === 'alice')).toBe(true);
  });

  it('includes private profile when requester is the owner', async () => {
    mockSearchProfiles.mockResolvedValue([
      { userId: 'tester', displayName: 'Me', visibility: 'private' },
    ]);

    const { docClient } = await import('../../lib/aws-clients.js');
    (docClient.send as jest.Mock).mockResolvedValue({ Items: [] });

    const res = await run(buildApp(), 'GET', '/api/profiles?q=me', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { profiles: Array<{ userId: string }> };
    expect(body.profiles.some((p) => p.userId === 'tester')).toBe(true);
  });

  it('includes private profile when requester and owner mutually follow each other', async () => {
    mockSearchProfiles.mockResolvedValue([
      { userId: 'charlie', displayName: 'Charlie', visibility: 'private' },
    ]);

    const { docClient } = await import('../../lib/aws-clients.js');
    // First call: query for who tester follows → charlie is in the list
    // Second call: get-command to verify charlie follows tester back → Item present
    (docClient.send as jest.Mock)
      .mockResolvedValueOnce({ Items: [{ followeeId: 'charlie' }] }) // tester follows charlie
      .mockResolvedValueOnce({ Item: { followerId: 'charlie', followeeId: 'tester' } }); // charlie follows tester

    const res = await run(buildApp(), 'GET', '/api/profiles?q=charlie', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { profiles: Array<{ userId: string }> };
    expect(body.profiles.some((p) => p.userId === 'charlie')).toBe(true);
  });

  it('excludes private profile when requester follows them but they do not follow back', async () => {
    mockSearchProfiles.mockResolvedValue([
      { userId: 'dave', displayName: 'Dave', visibility: 'private' },
    ]);

    const { docClient } = await import('../../lib/aws-clients.js');
    // tester follows dave, but dave does not follow back
    (docClient.send as jest.Mock)
      .mockResolvedValueOnce({ Items: [{ followeeId: 'dave' }] }) // tester follows dave
      .mockResolvedValueOnce({ Item: undefined }); // dave does NOT follow tester

    const res = await run(buildApp(), 'GET', '/api/profiles?q=dave', { userId: 'tester' });
    expect(res.statusCode).toBe(200);
    const body = res.body as { profiles: Array<{ userId: string }> };
    expect(body.profiles.every((p) => p.userId !== 'dave')).toBe(true);
  });
});

describe('PUT /api/profiles — displayName / bio validation', () => {
  it('returns 400 when displayName is empty string', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Old' });
    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { displayName: '' },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 when displayName exceeds 50 chars', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Old' });
    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { displayName: 'a'.repeat(51) },
    });
    expect(res.statusCode).toBe(400);
  });

  it('returns 400 when bio exceeds 160 chars', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Old' });
    const res = await run(buildApp(), 'PUT', '/api/profiles', {
      body: { bio: 'x'.repeat(161) },
    });
    expect(res.statusCode).toBe(400);
  });
});

// Consent to record DM calls lives on the profile; both parties must have it.
describe('PUT /api/profiles — recordCalls', () => {
  it('stores the flag and returns it', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Grace' });
    mockUpdateProfile.mockResolvedValue({ userId: 'tester', displayName: 'Grace', recordCalls: true });
    const res = await run(buildApp(), 'PUT', '/api/profiles', { body: { recordCalls: true } });
    expect(res.statusCode).toBe(200);
    expect(mockUpdateProfile).toHaveBeenCalledWith('tester', { recordCalls: true });
    expect((res.body as { recordCalls: boolean }).recordCalls).toBe(true);
  });

  it('lets the flag be turned off again', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Grace', recordCalls: true });
    mockUpdateProfile.mockResolvedValue({ userId: 'tester', displayName: 'Grace', recordCalls: false });
    const res = await run(buildApp(), 'PUT', '/api/profiles', { body: { recordCalls: false } });
    expect(res.statusCode).toBe(200);
    expect(mockUpdateProfile).toHaveBeenCalledWith('tester', { recordCalls: false });
  });

  it('rejects anything that is not a boolean', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Grace' });
    const res = await run(buildApp(), 'PUT', '/api/profiles', { body: { recordCalls: 'yes' } });
    expect(res.statusCode).toBe(400);
    expect(mockUpdateProfile).not.toHaveBeenCalled();
  });

  it('leaves the flag alone when the body does not mention it', async () => {
    mockGetProfile.mockResolvedValue({ userId: 'tester', displayName: 'Old' });
    mockUpdateProfile.mockResolvedValue({ userId: 'tester', displayName: 'New' });
    await run(buildApp(), 'PUT', '/api/profiles', { body: { displayName: 'New' } });
    expect(mockUpdateProfile).toHaveBeenCalledWith('tester', { displayName: 'New' });
  });
});
