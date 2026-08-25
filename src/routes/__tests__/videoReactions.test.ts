// Integration tests for GET /api/video/reactions — in-call emoji
// reaction playback endpoint. Mirrors the videoSessions.test.ts harness
// (module-level mocks of auth + repositories, hand-rolled req/res).

import express, { type NextFunction, type Request, type Response } from 'express';

const mockListByLobby = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
  optionalAuth: (_req: Request, _res: Response, next: NextFunction) => next(),
}));

jest.mock('../../lib/video-auth', () => ({
  VIDEO_API_URL: 'https://video.test',
}));

jest.mock('../../repositories', () => ({
  videoSessionRepo: {},
  callReactionRepo: {
    listByLobby: (...args: unknown[]) => mockListByLobby(...args),
  },
}));

import { videoSessionsRouter } from '../videoSessions';
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

async function run(app: express.Express, url: string): Promise<MockRes> {
  const res = mockRes();
  const qIdx = url.indexOf('?');
  const query: Record<string, string> = {};
  if (qIdx >= 0) {
    const params = new URLSearchParams(url.slice(qIdx + 1));
    for (const [k, v] of params.entries()) query[k] = v;
  }
  const req = {
    method: 'GET',
    url,
    originalUrl: url,
    path: qIdx >= 0 ? url.slice(0, qIdx) : url,
    headers: { 'content-type': 'application/json' },
    query,
    user: { sub: 'tester' },
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
  app.use('/api/video', videoSessionsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  mockListByLobby.mockReset();
});

describe('GET /api/video/reactions', () => {
  it('returns the lobby reactions ascending, passing the from/to window through', async () => {
    const rows = [
      { reactionId: 'rx-1', lobbyName: 'room:lounge', emoji: '👍', at: '2026-08-25T10:00:05.000Z' },
      { reactionId: 'rx-2', lobbyName: 'room:lounge', emoji: '🎉', at: '2026-08-25T10:01:00.000Z' },
    ];
    mockListByLobby.mockResolvedValue(rows);

    const res = await run(
      buildApp(),
      '/api/video/reactions?lobbyName=room%3Alounge&from=2026-08-25T10%3A00%3A00.000Z&to=2026-08-25T10%3A30%3A00.000Z',
    );

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ reactions: rows });
    expect(mockListByLobby).toHaveBeenCalledWith(
      'room:lounge',
      '2026-08-25T10:00:00.000Z',
      '2026-08-25T10:30:00.000Z',
    );
  });

  it('omits from/to when not supplied', async () => {
    mockListByLobby.mockResolvedValue([]);

    const res = await run(buildApp(), '/api/video/reactions?lobbyName=room%3Alounge');

    expect(res.statusCode).toBe(200);
    expect(res.body).toEqual({ reactions: [] });
    expect(mockListByLobby).toHaveBeenCalledWith('room:lounge', undefined, undefined);
  });

  it('400s when lobbyName is missing', async () => {
    const res = await run(buildApp(), '/api/video/reactions');
    expect(res.statusCode).toBe(400);
    expect(mockListByLobby).not.toHaveBeenCalled();
  });
});
