// platform-api/src/routes/__tests__/callLog.test.ts
//
// Route-level coverage for the two call-log gaps the Calls page exposed:
//
//   * a call you JOINED (a room, the shared hangout) had no way into the log
//     at all, because a place broadcasts no ring for the clients to report.
//     `joined` is that way in — and because it is a claim about yourself, the
//     route takes the caller id from the token, not the body.
//   * an outgoing ring nobody answered had no terminal event, so it sat at
//     `ringing` indefinitely. The caller's tab now reports `missed`, which
//     this route has to accept from the caller as readily as from the callee.
//
// The repository is mocked: what is under test here is the event vocabulary
// the route admits and the identity it stamps on a join. The reducer's own
// behaviour lives in repositories/__tests__/callLogReducer.test.ts.

import express, { type NextFunction, type Request, type Response } from 'express';

const mockApplyEvent = jest.fn();
const mockListForUser = jest.fn();

jest.mock('../../middleware/auth', () => ({
  requireAuth: (req: Request, _res: Response, next: NextFunction) => {
    (req as unknown as { user: { sub: string } }).user = { sub: 'eve' };
    next();
  },
}));

jest.mock('../../repositories', () => ({
  callLogRepo: {
    applyEvent: (...a: unknown[]) => mockApplyEvent(...a),
    listForUser: (...a: unknown[]) => mockListForUser(...a),
    unseenMissed: jest.fn(),
    markSeen: jest.fn(),
  },
  voicemailRepo: { save: jest.fn(), listForUser: jest.fn(), get: jest.fn(), markListened: jest.fn() },
}));

import { callLogRouter } from '../callLog';
import { errorHandler } from '../../middleware/error-handler';
import type { CallEvent } from '../../repositories/CallLogRepository';

// --- harness (no supertest in this repo) ------------------------------------

interface MockRes {
  statusCode: number;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  send(body?: unknown): MockRes;
  set(_h: unknown): MockRes;
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
    set() { return this; },
    setHeader() { return this; },
    getHeader() { return undefined; },
    end() { this.ended = true; resolve(); return this; },
  };
  return r;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/calls', callLogRouter);
  app.use(errorHandler);
  return app;
}

async function post(app: express.Express, url: string, body: unknown): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'POST',
    url,
    originalUrl: url,
    path: url,
    headers: { 'content-type': 'application/json' },
    query: {},
    body,
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

const lastEvent = (): CallEvent => mockApplyEvent.mock.calls.at(-1)![0] as CallEvent;

beforeEach(() => {
  jest.clearAllMocks();
  mockApplyEvent.mockResolvedValue([]);
});

describe('POST /api/calls/log/events', () => {
  it('accepts a join, so a call you walked into can reach the log', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-1', action: 'joined', callerId: 'eve',
      targetUserIds: [], lobbyName: 'room:design-standup',
    });
    expect(res.statusCode).toBe(200);
    expect(lastEvent()).toMatchObject({ action: 'joined', lobbyName: 'room:design-standup', targetUserIds: [] });
  });

  it('stamps a join with the token holder, whatever the body claims', async () => {
    // Attendance is a claim about yourself: nobody gets to write a room join
    // into somebody else's history.
    await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-2', action: 'joined', callerId: 'mallory',
      targetUserIds: ['ana'], lobbyName: 'global-hangout',
    });
    expect(lastEvent().callerId).toBe('eve');
  });

  it('takes the caller reporting their own unanswered ring as missed', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-3', action: 'missed', callerId: 'eve',
      targetUserIds: ['ana'], lobbyName: 'dm:ana:eve',
    });
    expect(res.statusCode).toBe(200);
    expect(lastEvent()).toMatchObject({ action: 'missed', callerId: 'eve', targetUserIds: ['ana'] });
  });

  it('NFR #175: a reporter who is neither the caller nor a target is 403, and nothing is written', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-5', action: 'missed', callerId: 'ana', targetUserIds: ['bob'], lobbyName: 'global-hangout',
    });
    expect(res.statusCode).toBe(403);
    expect(mockApplyEvent).not.toHaveBeenCalled();
  });

  it('NFR #175: a DM lobby the reporter is not named in is 403 even when the body lists them', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-6', action: 'missed', callerId: 'ana', targetUserIds: ['eve'], lobbyName: 'dm:ana:bob',
    });
    expect(res.statusCode).toBe(403);
    expect(mockApplyEvent).not.toHaveBeenCalled();
  });

  it('NFR #175: a target reporting the ring is accepted', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-7', action: 'missed', callerId: 'ana', targetUserIds: ['eve'], lobbyName: 'dm:ana:eve',
    });
    expect(res.statusCode).toBe(200);
  });

  it('still refuses an action that is not part of the vocabulary', async () => {
    const res = await post(buildApp(), '/api/calls/log/events', {
      callId: 'call-4', action: 'wandered-in', callerId: 'eve',
      targetUserIds: [], lobbyName: 'global-hangout',
    });
    expect(res.statusCode).toBe(400);
    expect(mockApplyEvent).not.toHaveBeenCalled();
  });
});
