/**
 * The gateway's internal v2 contract. The additive `schemaVersion`/`view`
 * pair must be accepted, echoed, and carried onto every replay batch, while a
 * body without them stays byte-for-byte the v1 request and response.
 */
const authorize = jest.fn();
const replay = jest.fn();

jest.mock('../../work-graph/runtime', () => ({
  authorizeGatewayWorkGraph: (...args: unknown[]) => authorize(...args),
  replayGatewayWorkGraph: (...args: unknown[]) => replay(...args),
  workGraphRepository: {},
}));
jest.mock('../../repositories', () => ({ workSourceAccountRepo: {} }));
jest.mock('../../services/work-graph-consumer', () => ({
  ingestAuthenticatedWorkEvent: jest.fn(),
  WorkGraphIngestionDeniedError: class extends Error {},
}));

import express, { type NextFunction, type Request, type Response } from 'express';
import { SERVICE_AUTH_HEADER, signEnvelope } from '@connorhoehn/service-runtime';
import { internalWorkGraphRouter } from '../internal-work-graph';

const SECRET = 'internal-test-secret-that-is-long-enough';
const query = { personId: 'connor', day: '2026-09-21', timezone: 'America/New_York' };
const view = { windowStart: '2026-09-21T13:30:00.000Z', windowEnd: '2026-09-21T13:45:00.000Z', mode: 'live' as const };
const activityView = {
  query: { schemaVersion: 2, ...query, ...view },
  temporal: { mode: 'live', observedAt: view.windowEnd, coverage: { from: view.windowStart, through: view.windowEnd, complete: false } },
  efforts: [], details: [], operations: [], eventBuckets: [],
};

function app(): express.Express {
  const instance = express();
  instance.use(express.json());
  instance.use('/internal/work-graph', internalWorkGraphRouter);
  instance.use((error: unknown, _req: Request, res: Response, _next: NextFunction) => {
    res.status(500).json({ error: (error as Error).message });
  });
  return instance;
}

async function post(
  path: string,
  body: Record<string, unknown>,
  signingSecret = SECRET,
): Promise<{ statusCode: number; body: unknown }> {
  const captured = { statusCode: 200, body: undefined as unknown };
  let settle!: () => void;
  const finished = new Promise<void>((resolve) => { settle = resolve; });
  const res = {
    status(code: number) { captured.statusCode = code; return this; },
    json(value: unknown) { captured.body = value; settle(); return this; },
    setHeader() { return this; },
    end() { settle(); return this; },
  };
  const headers: Record<string, string> = {
    'content-type': 'application/json',
    [SERVICE_AUTH_HEADER.toLowerCase()]: signEnvelope(body, {
      secret: signingSecret,
      serviceId: 'realtime-examples',
    }),
  };
  const req = {
    method: 'POST',
    url: path,
    originalUrl: path,
    headers,
    header: (name: string) => headers[name.toLowerCase()],
    body,
    params: {},
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    (app() as unknown as (request: Request, response: Response, next: NextFunction) => void)(
      req, res as unknown as Response, (error?: unknown) => (error ? reject(error) : resolve()),
    );
    finished.then(resolve, reject);
  });
  return captured;
}

const replayBody = {
  organizationId: 'org-1', viewerId: 'frank', query,
  afterWatermark: 0, throughWatermark: 3, since: '2026-09-21T13:15:00.000Z', limit: 100,
};

describe('internal work graph routes, schemaVersion 2', () => {
  beforeEach(() => {
    process.env.SERVICE_AUTH_SECRET = SECRET;
    authorize.mockReset();
    replay.mockReset();
    authorize.mockResolvedValue({ decision: 'allow', policyRevision: 'policy-7', grantState: 'active', watermark: 3 });
    replay.mockResolvedValue({ batches: [], complete: true });
  });

  it('forwards the requested window and leaves a v1 body untouched', async () => {
    await post('/internal/work-graph/authorize', { organizationId: 'org-1', viewerId: 'frank', query });
    expect(authorize).toHaveBeenLastCalledWith('org-1', 'frank', query, undefined);

    await post('/internal/work-graph/authorize', { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2, view });
    expect(authorize).toHaveBeenLastCalledWith('org-1', 'frank', query, view);

    await post('/internal/work-graph/replay', replayBody);
    expect(replay.mock.calls[0][4]).toBeUndefined();
    await post('/internal/work-graph/replay', { ...replayBody, schemaVersion: 2, view });
    expect(replay.mock.calls[1][4]).toEqual(view);
  });

  it('returns the echoed version and the view the platform derived', async () => {
    authorize.mockResolvedValue({
      decision: 'allow', policyRevision: 'policy-7', grantState: 'active', watermark: 3,
      schemaVersion: 2, view: activityView,
    });
    replay.mockResolvedValue({
      batches: [{ previousWatermark: 0, watermark: 3, policyRevision: 'policy-7', operations: [], committedAt: view.windowEnd, view: activityView }],
      complete: true, schemaVersion: 2, view: activityView,
    });
    const allowed = await post('/internal/work-graph/authorize', { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2, view });
    expect((allowed.body as Record<string, unknown>).schemaVersion).toBe(2);
    const batches = await post('/internal/work-graph/replay', { ...replayBody, schemaVersion: 2, view });
    expect((batches.body as { batches: Array<{ view?: unknown }> }).batches[0].view).toEqual(activityView);
  });

  it('rejects a half-formed or malformed v2 request instead of answering it', async () => {
    for (const body of [
      { organizationId: 'org-1', viewerId: 'frank', query, view },
      { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2 },
      { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 3, view },
      { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2, view: { ...view, windowStart: 'not-a-time' } },
      { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2, view: { ...view, mode: 'yesterday' } },
    ]) {
      expect((await post('/internal/work-graph/authorize', body)).statusCode).toBe(400);
    }
    expect((await post('/internal/work-graph/replay', { ...replayBody, view })).statusCode).toBe(400);
    expect(authorize).not.toHaveBeenCalled();
    expect(replay).not.toHaveBeenCalled();
  });

  it('passes the live-feed flag and base view id through, and rejects a base without the feed', async () => {
    await post('/internal/work-graph/replay', { ...replayBody, schemaVersion: 2, view, feed: 1, baseViewId: 'view-1' });
    expect(replay.mock.calls[0][3]).toEqual(expect.objectContaining({ feed: true, baseViewId: 'view-1' }));
    await post('/internal/work-graph/replay', { ...replayBody, schemaVersion: 2, view, feed: 1 });
    expect(replay.mock.calls[1][3]).toEqual(expect.objectContaining({ feed: true }));
    expect(replay.mock.calls[1][3].baseViewId).toBeUndefined();
    for (const body of [
      { ...replayBody, baseViewId: 'view-1' },
      { ...replayBody, feed: true },
      { ...replayBody, feed: 1, baseViewId: 'not a valid id!' },
    ]) {
      expect((await post('/internal/work-graph/replay', body)).statusCode).toBe(400);
    }
    expect(replay).toHaveBeenCalledTimes(2);
  });

  it('still requires a verified service credential for a v2 request', async () => {
    const body = { organizationId: 'org-1', viewerId: 'frank', query, schemaVersion: 2, view };
    // Signed with one secret, verified against another: the v2 fields do not
    // give the request any way around the service credential check.
    const response = await post('/internal/work-graph/authorize', body, 'a-completely-different-service-secret');
    expect(response.statusCode).toBe(401);
    expect(authorize).not.toHaveBeenCalled();
  });
});
