// Live captions as a run: the tap functions against a fake LVS, the stream
// step against a fake caption bus and a fake clock, the definition against
// the system-pipeline lint, and the toggle lifecycle against a recorded
// startRun. Nothing here reaches Redis, LVS, DDB or the gateway.
const getSessionMock = jest.fn();
const findByChannelArnMock = jest.fn().mockResolvedValue([]);
jest.mock('../../repositories', () => ({
  videoSessionRepo: {
    getSession: (...a: unknown[]) => getSessionMock(...a),
    findByChannelArn: (...a: unknown[]) => findByChannelArnMock(...a),
  },
  callRecordingRepo: {},
  callReactionRepo: {},
}));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../lib/video-auth', () => ({ VIDEO_API_URL: 'https://video.test' }));
const postChatMessageMock = jest.fn().mockResolvedValue(undefined);
jest.mock('../../gateway/GatewayServiceClient', () => ({
  postChatMessage: (...a: unknown[]) => postChatMessageMock(...a),
  PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' },
}));
const getRunMock = jest.fn();
jest.mock('../../pipeline/bridge-types', () => ({ getPipelineBridge: () => ({ getRun: (...a: unknown[]) => getRunMock(...a) }) }));

import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import {
  armCaptionTap, disarmCaptionTaps, listCaptionTaps, buildLiveCaptionsDefinition, buildLiveCaptionsHandlers,
  CAPTION_KINDS, LIVE_CAPTIONS_PIPELINE_ID, startLiveCaptions, stopLiveCaptions, isStopRequested,
  getLiveCaptionsProgress, resolveCallForArn, __resetLiveCaptionsForTests, type CaptionSubscriber, type CallRef,
} from '../liveCaptions';
import { lintSystemPipeline } from '../../pipeline/systemPipelineLint';

const SINK = 'http://sidecar.test/pcm';
const ARN = 'arn:lvs:channel/abc';
const node = { id: 'x', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;

// The module's `fetch` dep is `typeof fetch`, so the mock takes what fetch takes.
type FetchInput = string | URL | Request;
type FetchMock = jest.Mock<Promise<Response>, [FetchInput, RequestInit?]>;
const jsonRes = (status: number, body: unknown): Response => ({ ok: status < 300, status, json: async () => body, text: async () => JSON.stringify(body) } as unknown as Response);

/** A fake LVS taps API: list / create / delete, remembering what exists. */
function fakeLvs(initial: Array<{ tapId: string; url: string }> = []) {
  const taps = [...initial];
  let n = 0;
  const calls: Array<{ method: string; url: string }> = [];
  const fetchMock: FetchMock = jest.fn(async (input: FetchInput, init?: RequestInit) => {
    const url = String(input);
    const method = (init?.method ?? 'GET').toUpperCase();
    calls.push({ method, url });
    if (method === 'GET') return jsonRes(200, { taps: taps.map((t) => ({ tapId: t.tapId, destination: { url: t.url } })) });
    if (method === 'POST') { const tapId = `tap-${++n}`; taps.push({ tapId, url: (JSON.parse(String(init!.body)) as { destination: { url: string } }).destination.url }); return jsonRes(201, { tapId }); }
    if (method === 'DELETE') { const id = decodeURIComponent(url.split('/').pop()!); const i = taps.findIndex((t) => t.tapId === id); if (i < 0) return jsonRes(404, {}); taps.splice(i, 1); return jsonRes(204, {}); }
    return jsonRes(405, {});
  });
  return { fetch: fetchMock, taps, calls };
}

beforeEach(() => { __resetLiveCaptionsForTests(); getSessionMock.mockReset(); findByChannelArnMock.mockReset().mockResolvedValue([]); postChatMessageMock.mockClear(); getRunMock.mockReset(); });

describe('the definition', () => {
  it('passes the system-pipeline lint and runs trigger → arm → stream → disarm → publish', () => {
    const def = buildLiveCaptionsDefinition();
    expect(lintSystemPipeline(def)).toEqual([]);
    expect(def.id).toBe(LIVE_CAPTIONS_PIPELINE_ID);
    expect((def.nodes as Array<{ id: string }>).map((n) => n.id)).toEqual(['trigger', 'arm', 'stream', 'disarm', 'publish']);
    expect((def.edges as Array<{ id: string }>).map((e) => e.id)).toEqual(['trigger->arm', 'arm->stream', 'stream->disarm', 'disarm->publish']);
  });
  it('registers a handler for every step kind it names', () => {
    const handlers = buildLiveCaptionsHandlers();
    for (const kind of Object.values(CAPTION_KINDS)) expect(handlers.get(kind)?.kind).toBe(kind);
  });
});

describe('armCaptionTap / disarmCaptionTaps', () => {
  it('creates a per-participant webhook-pcm tap at our sink and returns its id', async () => {
    const lvs = fakeLvs();
    const out = await armCaptionTap(ARN, { fetch: lvs.fetch, sinkUrl: SINK });
    expect(out).toEqual({ tapIds: ['tap-1'], created: true });
    const create = lvs.fetch.mock.calls.find(([, init]) => init?.method === 'POST')!;
    expect(create[0]).toBe(`https://video.test/api/channels/${encodeURIComponent(ARN)}/taps`);
    expect(JSON.parse(String(create[1]!.body))).toEqual({ destination: { type: 'webhook-pcm', url: SINK, perParticipant: true, chunkMs: 200 } });
    expect((create[1]!.headers as Record<string, string>).Authorization).toMatch(/^Bearer /);
  });
  it('reuses a tap already pointing at our sink instead of stacking a second one', async () => {
    const lvs = fakeLvs([{ tapId: 'old', url: SINK }, { tapId: 'other', url: 'http://elsewhere/pcm' }]);
    const out = await armCaptionTap(ARN, { fetch: lvs.fetch, sinkUrl: SINK });
    expect(out).toEqual({ tapIds: ['old'], created: false });
    expect(lvs.calls.filter((c) => c.method === 'POST')).toHaveLength(0);
  });
  it('a 409 "no audio producer" is not a failure but a tap to arm later', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'POST' ? jsonRes(409, { error: 'no audio producer in channel' }) : jsonRes(200, { taps: [] })));
    expect(await armCaptionTap(ARN, { fetch: fetchMock, sinkUrl: SINK })).toEqual({ tapIds: [], created: false, waitingFor: 'audio' });
  });
  it('throws when LVS refuses the create', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'POST' ? jsonRes(503, { error: 'no' }) : jsonRes(200, { taps: [] })));
    await expect(armCaptionTap(ARN, { fetch: fetchMock, sinkUrl: SINK })).rejects.toThrow(/LVS tap create failed \(503\)/);
  });
  it('removes the ids it is given AND any other tap at our sink, leaving other sinks alone', async () => {
    const lvs = fakeLvs([{ tapId: 'mine', url: SINK }, { tapId: 'orphan', url: SINK }, { tapId: 'other', url: 'http://elsewhere/pcm' }]);
    const out = await disarmCaptionTaps(ARN, ['mine', 'gone-already'], { fetch: lvs.fetch, sinkUrl: SINK });
    expect(out.removed.sort()).toEqual(['gone-already', 'mine', 'orphan']);
    expect(out).toMatchObject({ orphaned: [], failed: [] });
    expect(lvs.taps.map((t) => t.tapId)).toEqual(['other']);
  });
  it('after a hang-up LVS has forgotten the channel and 403s the delete: the tap is reported orphaned, not removed', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'DELETE' ? jsonRes(403, { error: 'invalid credentials' }) : jsonRes(200, { taps: [{ tapId: 'left-behind', destination: { url: SINK } }] })));
    expect(await disarmCaptionTaps(ARN, ['left-behind'], { fetch: fetchMock, sinkUrl: SINK })).toEqual({ removed: [], orphaned: ['left-behind'], failed: [] });
  });
  it('any other refusal is reported as failed so the step can retry', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'DELETE' ? jsonRes(503, {}) : jsonRes(200, { taps: [] })));
    expect(await disarmCaptionTaps(ARN, ['t1'], { fetch: fetchMock, sinkUrl: SINK })).toEqual({ removed: [], orphaned: [], failed: [{ tapId: 't1', status: 503 }] });
  });
  it('lists nothing when LVS cannot answer', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, _init?: RequestInit): Promise<Response> => { throw new Error('ECONNREFUSED'); });
    expect(await listCaptionTaps(ARN, { fetch: fetchMock, sinkUrl: SINK })).toEqual([]);
  });
});

// --- the stream step ---------------------------------------------------------

/** A fake caption bus and a fake clock; `helpers.sleep` advances the clock. */
function harness(opts: { maxMs?: number; pollMs?: number } = {}) {
  let clock = 1_000_000;
  const listeners = new Map<string, (raw: string) => void>();
  const unsubscribe = jest.fn(async () => { listeners.clear(); });
  const subscribe: CaptionSubscriber = jest.fn(async (channel, onMessage) => { listeners.set(channel, onMessage); return unsubscribe; });
  const ended = { value: false }; const stop = { value: false }; const cancelled = { value: false };
  const isSessionEnded = jest.fn(async (_c: CallRef) => ended.value);
  const isStopRequested = jest.fn(async () => stop.value);
  const ensureArmed = jest.fn(async (_arn: string, ids: string[]) => (ids.length ? ids : ['re-armed']));
  const disarm = jest.fn(async () => undefined);
  const pollMs = opts.pollMs ?? 5_000;
  const helpers = {
    runId: 'run-1', stepId: 'stream', ownerNodeId: 'n1', emit: jest.fn(),
    abortSignal: new AbortController().signal,
    sleep: jest.fn(async (ms: number) => { clock += ms; }),
    isCancelled: () => cancelled.value,
  } as unknown as StepHandlerHelpers;
  const handlers = buildLiveCaptionsHandlers({ stream: { subscribe, isSessionEnded, isStopRequested, ensureArmed, disarm, now: () => clock, pollMs, verifyEveryMs: 60_000, maxMs: () => opts.maxMs ?? 4 * 3600_000 } });
  const speak = (text: string, participantId = 'p1', interim = false) => listeners.get(`captions:${ARN}`)?.(JSON.stringify({ channelArn: ARN, participantId, id: `${participantId}-1`, seq: 1, text, interim, at: new Date(clock).toISOString() }));
  const ctx = { channelArn: ARN, sessionId: 's1', lobbyName: 'room:pipeline-live', tapIds: ['tap-1'] };
  const run = () => handlers.get(CAPTION_KINDS.stream)!.execute(node, ctx, helpers);
  return { run, speak, ended, stop, cancelled, subscribe, unsubscribe, isSessionEnded, ensureArmed, disarm, helpers, clock: () => clock, ctx };
}

describe('captions:stream', () => {
  it('counts final lines and words while the call runs and resolves when the session row says ended', async () => {
    const h = harness();
    // Three polls later the call ends.
    let polls = 0;
    h.isSessionEnded.mockImplementation(async () => ++polls >= 3);
    const pending = h.run();
    await Promise.resolve();
    // The subscription is live before the first poll; lines spoken now count.
    h.speak('hello there everyone'); h.speak('partial', 'p2', true); h.speak('and again', 'p2');
    expect(getLiveCaptionsProgress('run-1')).toMatchObject({ stepId: 'stream', lines: 2, words: 5, speakers: 2 });
    const out = await pending;
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ lines: 2, words: 5, speakers: 2, endedBecause: 'call-ended', tapIds: ['tap-1'], durationMs: 2 * 5_000 });
    expect(h.subscribe).toHaveBeenCalledWith(`captions:${ARN}`, expect.any(Function));
    expect(h.unsubscribe).toHaveBeenCalledTimes(1);
    expect(h.disarm).not.toHaveBeenCalled(); // the disarm STEP does that on a normal end
    expect(getLiveCaptionsProgress('run-1')).toBeNull();
  });

  it('resolves normally when the toggle asks it to stop, so disarm and publish still run', async () => {
    const h = harness();
    let polls = 0;
    (h.helpers.sleep as jest.Mock).mockImplementation(async (ms: number) => { polls += 1; if (polls === 2) h.stop.value = true; (h as unknown as { _c: number })._c = ms; });
    const out = await h.run();
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ endedBecause: 'stopped' });
    expect(h.disarm).not.toHaveBeenCalled();
  });

  it('gives up at the maximum duration', async () => {
    const h = harness({ maxMs: 12_000, pollMs: 5_000 });
    const out = await h.run();
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ endedBecause: 'max-duration' });
    expect(h.helpers.sleep).toHaveBeenCalledTimes(3); // 0 → 5s → 10s → 15s ≥ 12s
  });

  it('on executor cancellation it removes the tap itself and reports cancelled', async () => {
    const h = harness();
    (h.helpers.sleep as jest.Mock).mockImplementation(async () => { h.cancelled.value = true; });
    const out = await h.run();
    expect(out.status).toBe('cancelled');
    expect(out.output).toMatchObject({ endedBecause: 'cancelled' });
    expect(h.disarm).toHaveBeenCalledWith(ARN, ['tap-1']);
    expect(h.unsubscribe).toHaveBeenCalledTimes(1);
  });

  it('verifies the tap every minute and adopts the re-armed ids (a resumed run whose tap vanished)', async () => {
    const h = harness({ maxMs: 130_000, pollMs: 5_000 });
    h.ensureArmed.mockResolvedValue(['tap-2']);
    const out = await h.run();
    expect(h.ensureArmed).toHaveBeenCalledTimes(2); // at 60s and 120s
    expect(out.output).toMatchObject({ tapIds: ['tap-2'], endedBecause: 'max-duration', armed: true });
  });

  it('unarmed (captions on before anyone published audio) it retries the tap every poll until it takes, then once a minute', async () => {
    const h = harness({ maxMs: 100_000, pollMs: 5_000 });
    h.ctx.tapIds = [];
    let asks = 0;
    h.ensureArmed.mockImplementation(async () => (++asks >= 3 ? ['tap-late'] : []));
    const out = await h.run();
    // polls at 5s,10s,15s arm it (3 asks); armed, the next verify is 60s later (75s) — 4 asks in 100s.
    expect(h.ensureArmed).toHaveBeenCalledTimes(4);
    expect(out.output).toMatchObject({ tapIds: ['tap-late'], armed: true });
  });

  it('fails fast without a channelArn', async () => {
    const h = harness();
    const out = await buildLiveCaptionsHandlers({ stream: { subscribe: h.subscribe } }).get(CAPTION_KINDS.stream)!.execute(node, {}, h.helpers);
    expect(out).toEqual({ status: 'failed', error: 'channelArn is required' });
    expect(h.subscribe).not.toHaveBeenCalled();
  });

  it('by default reads the session row: ended, or gone, means the call is over', async () => {
    const h = harness();
    const handlers = buildLiveCaptionsHandlers({ stream: { subscribe: h.subscribe, isStopRequested: async () => false, now: h.clock, pollMs: 5_000 } });
    getSessionMock.mockResolvedValueOnce({ status: 'active' }).mockResolvedValueOnce({ status: 'ended' });
    const out = await handlers.get(CAPTION_KINDS.stream)!.execute(node, h.ctx, h.helpers);
    expect(out.output).toMatchObject({ endedBecause: 'call-ended' });
    expect(getSessionMock).toHaveBeenCalledWith('room:pipeline-live', 's1');
  });
});

describe('captions:arm / captions:disarm / captions:publish steps', () => {
  it('arm outputs the tap ids the later steps use; disarm removes them', async () => {
    const lvs = fakeLvs();
    const handlers = buildLiveCaptionsHandlers({ tap: { fetch: lvs.fetch, sinkUrl: SINK } });
    const helpers = { runId: 'run-9', stepId: 'arm' } as unknown as StepHandlerHelpers;
    const armed = await handlers.get(CAPTION_KINDS.arm)!.execute(node, { channelArn: ARN }, helpers);
    expect(armed.status).toBe('completed');
    expect(armed.output).toMatchObject({ tapIds: ['tap-1'], created: true, armed: true, captionChannel: `captions:${ARN}` });
    const disarmed = await handlers.get(CAPTION_KINDS.disarm)!.execute(node, { channelArn: ARN, ...(armed.output as object) }, helpers);
    expect(disarmed.output).toEqual({ removed: ['tap-1'], orphaned: [] });
    expect(lvs.taps).toEqual([]);
  });
  it('disarm fails (for the retry) when LVS refuses with something other than "channel gone"', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'DELETE' ? jsonRes(503, {}) : jsonRes(200, { taps: [] })));
    const handlers = buildLiveCaptionsHandlers({ tap: { fetch: fetchMock, sinkUrl: SINK } });
    const out = await handlers.get(CAPTION_KINDS.disarm)!.execute(node, { channelArn: ARN, tapIds: ['t1'] }, { runId: 'r', stepId: 'disarm' } as unknown as StepHandlerHelpers);
    expect(out).toEqual({ status: 'failed', error: 'LVS refused to delete t1 (503)' });
  });
  it('arm completes as waiting when nobody publishes audio yet', async () => {
    const fetchMock: FetchMock = jest.fn(async (_u: FetchInput, init?: RequestInit) => (init?.method === 'POST' ? jsonRes(409, { error: 'no audio producer in channel' }) : jsonRes(200, { taps: [] })));
    const handlers = buildLiveCaptionsHandlers({ tap: { fetch: fetchMock, sinkUrl: SINK } });
    const out = await handlers.get(CAPTION_KINDS.arm)!.execute(node, { channelArn: ARN }, { runId: 'r', stepId: 'arm' } as unknown as StepHandlerHelpers);
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ tapIds: [], armed: false, waitingFor: 'audio' });
  });
  it('publish posts a `captions` agent-run card into the lobby\'s conversation', async () => {
    const handlers = buildLiveCaptionsHandlers();
    const helpers = { runId: 'run-9', stepId: 'publish' } as unknown as StepHandlerHelpers;
    const out = await handlers.get(CAPTION_KINDS.publish)!.execute(node, { channelArn: ARN, sessionId: 's1', lobbyName: 'room:pipeline-live', lines: 7, words: 41, durationMs: 90_000, endedBecause: 'call-ended' }, helpers);
    expect(out.output).toMatchObject({ posted: true, channel: 'room:pipeline-live', instruction: '7 lines captioned' });
    expect(postChatMessageMock).toHaveBeenCalledWith(expect.objectContaining({
      channel: 'room:pipeline-live', message: 'captions 7 lines captioned',
      metadata: expect.objectContaining({ kind: 'agent-run', runId: 'run-9', pipelineId: 'live-captions', command: 'captions', sessionId: 's1', words: 41 }),
    }));
  });
  it('publish respects clientPostsCard and a lobby with no conversation', async () => {
    const handlers = buildLiveCaptionsHandlers();
    const helpers = { runId: 'run-9', stepId: 'publish' } as unknown as StepHandlerHelpers;
    const a = await handlers.get(CAPTION_KINDS.publish)!.execute(node, { lobbyName: 'room:x', lines: 1, clientPostsCard: true }, helpers);
    expect(a.output).toMatchObject({ posted: false, instruction: '1 line captioned' });
    const b = await handlers.get(CAPTION_KINDS.publish)!.execute(node, { lobbyName: 'adhoc-lobby', lines: 0 }, helpers);
    expect(b.output).toMatchObject({ posted: false, channel: null });
    expect(postChatMessageMock).not.toHaveBeenCalled();
  });
});

describe('resolveCallForArn', () => {
  it('prefers the newest ACTIVE session for the channel over a stale index hint', async () => {
    // No Redis in this suite, so the hint is absent; the table decides.
    findByChannelArnMock.mockResolvedValue([
      { sessionId: 'old', documentId: 'room:x', status: 'ended', startedAt: '2026-09-10T01:00:00Z' },
      { sessionId: 'earlier-live', documentId: 'room:x', status: 'active', startedAt: '2026-09-10T02:00:00Z' },
      { sessionId: 'newest-live', documentId: 'room:x', status: 'active', startedAt: '2026-09-10T03:00:00Z' },
    ]);
    expect(await resolveCallForArn(ARN)).toEqual({ sessionId: 'newest-live', lobbyName: 'room:x' });
  });
  it('falls back to an ended row when nothing is live, and to null when the channel is unknown', async () => {
    findByChannelArnMock.mockResolvedValue([{ sessionId: 'old', documentId: 'room:x', status: 'ended' }]);
    expect(await resolveCallForArn(ARN)).toEqual({ sessionId: 'old', lobbyName: 'room:x' });
    findByChannelArnMock.mockResolvedValue([]);
    expect(await resolveCallForArn(ARN)).toBeNull();
  });
});

describe('startLiveCaptions / stopLiveCaptions', () => {
  it('starts a live-captions run with the call the channel belongs to, and returns the same run on a second enable', async () => {
    const startRun = jest.fn().mockResolvedValue('run-42');
    findByChannelArnMock.mockResolvedValue([{ sessionId: 's1', documentId: 'room:pipeline-live', status: 'active' }]);
    const a = await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun });
    expect(a).toEqual({ runId: 'run-42', pipelineId: 'live-captions', reused: false });
    expect(startRun).toHaveBeenCalledWith('live-captions', expect.objectContaining({ id: 'live-captions' }), { sessionId: 's1', channelArn: ARN, lobbyName: 'room:pipeline-live', startedBy: 'grace' }, 'grace');
    getRunMock.mockResolvedValue({ status: 'running' });
    const b = await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun });
    expect(b).toEqual({ runId: 'run-42', pipelineId: 'live-captions', reused: true });
    expect(startRun).toHaveBeenCalledTimes(1);
  });
  it('starts a fresh run when the remembered one is no longer running', async () => {
    const startRun = jest.fn().mockResolvedValueOnce('run-1').mockResolvedValueOnce('run-2');
    await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun });
    getRunMock.mockResolvedValue({ status: 'completed' });
    const b = await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun });
    expect(b.runId).toBe('run-2');
  });
  it('stop asks the running step to stop; the step observes it', async () => {
    const startRun = jest.fn().mockResolvedValue('run-42');
    await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun });
    const out = await stopLiveCaptions({ channelArn: ARN });
    expect(out).toEqual({ runId: 'run-42', stopped: true, removed: [] });
    expect(await isStopRequested('run-42')).toBe(true);
  });
  it('stop with no run known removes any tap at our sink directly', async () => {
    const lvs = fakeLvs([{ tapId: 'legacy', url: SINK }]);
    const out = await stopLiveCaptions({ channelArn: ARN }, { tap: { fetch: lvs.fetch, sinkUrl: SINK } });
    expect(out).toEqual({ runId: null, stopped: false, removed: ['legacy'] });
  });
  it('without a pipeline engine it arms the tap directly so captions still work', async () => {
    const lvs = fakeLvs();
    const startRun = jest.fn().mockRejectedValue(new Error('pipeline engine is not available'));
    const out = await startLiveCaptions({ channelArn: ARN, userId: 'grace' }, { startRun, tap: { fetch: lvs.fetch, sinkUrl: SINK } });
    expect(out).toEqual({ runId: null, pipelineId: 'live-captions', reused: false, tapIds: ['tap-1'] });
  });
});
