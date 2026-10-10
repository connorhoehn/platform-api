// NFR #159 (realtime-examples Loop 37): the transcript-ready step tells the
// Activity graph, whichever real flow started the run. Neither the finalize
// pipeline's child run nor POST /recordings/:id/transcribe put the org and
// owner in the run context, so the step reads them from the recording row.
const updateTranscriptStatusMock = jest.fn().mockResolvedValue(undefined);
const getRecordingMock = jest.fn();
const meetingMock = jest.fn().mockResolvedValue({ published: true });
const reactionsMock = jest.fn().mockResolvedValue([]);
jest.mock('../../repositories', () => ({
  callRecordingRepo: { updateTranscriptStatus: updateTranscriptStatusMock, getRecording: getRecordingMock },
  videoSessionRepo: {},
  callReactionRepo: { listByLobby: reactionsMock },
}));
jest.mock('../../work-graph/sources/platformPublisher', () => ({ platformWorkGraphSourcePublisher: { meeting: meetingMock } }));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));

import { buildRecordingPipelineHandlers, KINDS, parseSpeechResponse } from '../recordingPipelines';
import type { PipelineNode } from 'distributed-core/applications/pipeline';

const node = { id: 'x', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1' } as never;
const segments = [{ startMs: 0, endMs: 4000, speakerId: 'a', speakerLabel: 'Connor', text: 'Ship it.' }];

beforeEach(() => { jest.clearAllMocks(); getRecordingMock.mockReset(); meetingMock.mockResolvedValue({ published: true }); reactionsMock.mockResolvedValue([]); });

describe('transcript:store → Activity graph', () => {
  it.each([
    ['segment', { segments: [{ startMs: 1250, endMs: 2340, text: 'Native first.' }, { startMs: 7125, endMs: 8875, text: 'Native second.' }] }],
    ['text', { text: 'Legacy first.\nLegacy second.' }],
  ] as const)('keeps %s timing provenance and speech offsets through reaction merge and inline storage', async (granularity, response) => {
    const speech = parseSpeechResponse(JSON.stringify(response), 10_000);
    getRecordingMock.mockResolvedValue({ callId: 'test-call', recordingId: 'test-recording', orgId: 'org-1', lobbyName: 'orgiq:initiative:test-only',
      startedAt: '2026-01-01T00:00:00.000Z', endedAt: '2026-01-01T00:00:10.000Z' });
    reactionsMock.mockResolvedValue([{ tenant: 'org-1', lobbyName: 'orgiq:initiative:test-only', reactionId: 'r1',
      userId: 'u-reader', displayName: 'Reader', emoji: '👍', at: '2026-01-01T00:00:04.000Z' }]);
    const handlers = buildRecordingPipelineHandlers();
    const merged = await handlers.get(KINDS.mergeReactions)!.execute(node, {
      callId: 'test-call', recordingId: 'test-recording', segments: speech,
      organizationId: 'untrusted-org', lobbyName: 'untrusted-lobby', startedAt: '2026-12-01T00:00:00.000Z',
    }, helpers);
    expect(merged.status).toBe('completed');
    expect(getRecordingMock).toHaveBeenCalledWith('test-call', 'test-recording');
    expect(reactionsMock).toHaveBeenCalledWith('org-1', 'orgiq:initiative:test-only', '2026-01-01T00:00:00.000Z', '2026-01-01T00:00:10.000Z', 500);
    const transcript = (merged as { output: { transcript: { timestampGranularity: string; segments: Array<{ kind?: string }> } } }).output.transcript;
    expect(transcript.timestampGranularity).toBe(granularity);
    expect(transcript.segments.filter(segment => segment.kind !== 'reaction')).toEqual(speech);
    expect(transcript.segments.find(segment => segment.kind === 'reaction')).toMatchObject({ startMs: 4000, endMs: 5000 });
    await handlers.get(KINDS.store)!.execute(node, { callId: 'test-call', recordingId: 'test-recording', transcript }, helpers);
    const stored = updateTranscriptStatusMock.mock.calls[0][2];
    expect(stored.status).toBe('ready');
    expect(JSON.parse(stored.transcriptJson)).toEqual(transcript);
  });

  it('publishes transcript ready from the row when the run context has no org or owner', async () => {
    getRecordingMock.mockResolvedValue({ orgId: 'org-1', startedBy: 'dev-connor' });
    const handlers = buildRecordingPipelineHandlers();
    await handlers.get(KINDS.store)!.execute(node, { callId: 'c1', recordingId: 'r1', transcript: { segments } }, helpers);
    expect(getRecordingMock).toHaveBeenCalledWith('c1', 'r1');
    expect(meetingMock).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'transcript', recordingId: 'r1', status: 'ready', organizationId: 'org-1', recordingOwnerId: 'dev-connor' }),
      { organizationId: 'org-1', sourceSubject: 'dev-connor' },
    );
  });

  it('uses the context when it carries both, and stays silent for an org-less row', async () => {
    const handlers = buildRecordingPipelineHandlers();
    await handlers.get(KINDS.store)!.execute(node, { callId: 'c1', recordingId: 'r1', organizationId: 'o', startedBy: 'u', transcript: { segments } }, helpers);
    expect(getRecordingMock).not.toHaveBeenCalled();
    expect(meetingMock).toHaveBeenCalledTimes(1);
    getRecordingMock.mockResolvedValue({ startedBy: 'u' });
    await handlers.get(KINDS.store)!.execute(node, { callId: 'c1', recordingId: 'r2', transcript: { segments } }, helpers);
    expect(meetingMock).toHaveBeenCalledTimes(1);
  });

  it('the finalize pipeline hands org and owner to the transcription run', async () => {
    const startRun = jest.fn().mockResolvedValue('child-1');
    const handlers = buildRecordingPipelineHandlers({ startRun } as never);
    const prev = process.env.TRANSCRIBE_EVERY_RECORDING; process.env.TRANSCRIBE_EVERY_RECORDING = 'true';
    try {
      await handlers.get(KINDS.startTranscription)!.execute(node, { callId: 'c1', recordingId: 'r1', composeStatus: 'ok', organizationId: 'org-1', startedBy: 'dev-connor' }, helpers);
    } finally { process.env.TRANSCRIBE_EVERY_RECORDING = prev; }
    expect(startRun.mock.calls[0][2]).toMatchObject({ organizationId: 'org-1', startedBy: 'dev-connor' });
  });
});

describe('pipeline reaction scope', () => {
  const row = { callId: 'test-call', recordingId: 'test-recording', orgId: 'org-1', lobbyName: 'orgiq:initiative:test-only',
    startedAt: '2026-01-01T00:00:00.000Z', endedAt: '2026-01-01T00:00:10.000Z' };
  const speech = parseSpeechResponse('{"segments":[{"startMs":1250,"endMs":2340,"text":"Native words."}]}', 10_000);
  const context = { callId: row.callId, recordingId: row.recordingId, segments: speech,
    lobbyName: 'wrong-lobby', organizationId: 'wrong-tenant', startedAt: 'bad-date', endedAt: 'bad-date' };
  const reaction = (reactionId: string, at = '2026-01-01T00:00:04.000Z') => ({
    reactionId, at, tenant: row.orgId, lobbyName: row.lobbyName, userId: 'u-reader', displayName: 'Reader', emoji: '👍',
  });
  const merge = () => buildRecordingPipelineHandlers().get(KINDS.mergeReactions)!.execute(node, context, helpers);

  it('queries exact native tenant/lobby/window and bounds/deduplicates enrichment on every retry', async () => {
    getRecordingMock.mockResolvedValue(row);
    reactionsMock.mockResolvedValue([
      reaction('r1'), reaction('r1'), reaction('distinct-same-emoji'),
      reaction('at-start', row.startedAt), reaction('at-end', row.endedAt),
      reaction('before', '2025-12-31T23:59:59.999Z'), reaction('after', '2026-01-01T00:00:10.001Z'),
      { ...reaction('other-lobby'), lobbyName: 'orgiq:initiative:other' },
      { ...reaction('other-tenant'), tenant: 'org-2' }, reaction('invalid-at', 'invalid'),
    ]);
    const first = await merge(), second = await merge();
    expect(first).toEqual(second);
    expect(reactionsMock.mock.calls).toEqual([
      ['org-1', row.lobbyName, row.startedAt, row.endedAt, 500],
      ['org-1', row.lobbyName, row.startedAt, row.endedAt, 500],
    ]);
    const output = (first as { output: { reactions: number; reactionStatus: string; transcript: { segments: Array<{ kind?: string; startMs: number }> } } }).output;
    expect(output.reactions).toBe(4);
    expect(output.reactionStatus).toBe('ready');
    expect(output.transcript.segments.filter(segment => segment.kind === 'reaction').map(segment => segment.startMs)).toEqual([0, 4000, 4000, 10_000]);
    expect(output.transcript.segments.filter(segment => segment.kind !== 'reaction')).toEqual(speech);
    expect(context.segments).toEqual(speech);
  });

  it('separates another recording in the same reused lobby by its persisted window', async () => {
    getRecordingMock.mockResolvedValue({ ...row, startedAt: '2026-01-01T00:00:20.000Z', endedAt: '2026-01-01T00:00:30.000Z' });
    reactionsMock.mockResolvedValue([reaction('older-call'), reaction('this-call', '2026-01-01T00:00:24.000Z')]);
    const outcome = await merge();
    expect(reactionsMock).toHaveBeenCalledWith('org-1', row.lobbyName, '2026-01-01T00:00:20.000Z', '2026-01-01T00:00:30.000Z', 500);
    const output = (outcome as { output: { reactions: number; transcript: { segments: Array<{ kind?: string; startMs: number }> } } }).output;
    expect(output.reactions).toBe(1);
    expect(output.transcript.segments.find(segment => segment.kind === 'reaction')).toMatchObject({ startMs: 4000 });
  });

  it.each([
    null, { ...row, orgId: undefined }, { ...row, orgId: 'invalid|tenant' }, { ...row, lobbyName: '' },
    { ...row, startedAt: undefined }, { ...row, endedAt: undefined }, { ...row, startedAt: 'invalid' },
    { ...row, endedAt: '2025-01-01T00:00:00.000Z' }, { ...row, callId: 'other-call' }, { ...row, recordingId: 'other-recording' },
  ])('does not query with missing/mismatched trusted recording scope: %j', async recording => {
    getRecordingMock.mockResolvedValue(recording);
    const outcome = await merge();
    expect(reactionsMock).not.toHaveBeenCalled();
    expect(outcome).toMatchObject({ output: { reactions: 0, reactionStatus: 'unavailable', transcript: { segments: speech } } });
  });

  it('keeps speech when the recording lookup or reaction store is unavailable', async () => {
    getRecordingMock.mockRejectedValueOnce(new Error('lookup unavailable'));
    expect(await merge()).toMatchObject({ output: { reactionStatus: 'unavailable', transcript: { segments: speech } } });
    expect(reactionsMock).not.toHaveBeenCalled();
    getRecordingMock.mockResolvedValue(row);
    reactionsMock.mockRejectedValueOnce(new Error('reaction store unavailable'));
    expect(await merge()).toMatchObject({ output: { reactionStatus: 'unavailable', transcript: { segments: speech } } });
  });
});
