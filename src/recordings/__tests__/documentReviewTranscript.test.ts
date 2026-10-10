// A call never edits the document it reviews. Before this gate the
// transcription pipeline treated any UUID lobby as a transcript target and
// wrote "Call transcript" + the segments into the body of the page under
// review (docs/design/document-calls/SPEC.md shows nothing of the sort).
// These tests run the real steps — resolve, start-transcription, store,
// store-summary and the shared document:apply — against mocked stores, and
// check that a document-review call keeps its transcript on the recording
// row and never opens the document; and that the explicit opt-in
// (`writeTranscriptToDocument: true`, which no starter sets today) still writes.
const updateTranscriptStatusMock = jest.fn().mockResolvedValue(undefined);
const findByChannelArnMock = jest.fn();
jest.mock('../../repositories', () => ({
  callRecordingRepo: { updateTranscriptStatus: updateTranscriptStatusMock, getRecording: jest.fn().mockResolvedValue(null) },
  videoSessionRepo: { findByChannelArn: findByChannelArnMock },
  callReactionRepo: {},
}));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn().mockResolvedValue({ Items: [{ callId: { S: 'call-1' } }] }) }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' }, describeError: (e: unknown) => String(e) }));
// Opening the document is the one thing a review call must never do.
const collaboratorCtor = jest.fn();
jest.mock('../../documents/AgentCollaborator', () => ({
  HAIKU_AGENT: { userId: 'agent:haiku', displayName: 'Haiku', color: '#000' },
  AgentCollaborator: class { constructor(opts: unknown) { collaboratorCtor(opts); throw new Error('the document was opened'); } },
}));

import { buildRecordingPipelineHandlers, buildCallTranscriptionDefinition, KINDS, documentWriteSkipReason, transcriptOps } from '../recordingPipelines';
import { createDocumentApplyHandler, DOCUMENT_APPLY_KIND } from '../../documents/documentAgentPipeline';
import type { PipelineNode } from 'distributed-core/applications/pipeline';

const DOC = '3a6e5bd5-feff-46ad-9ced-41c7315b1974';
const ARN = 'arn:lvs:channel/review';
const node = { id: 'x', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1' } as never;
const startRun = jest.fn().mockResolvedValue('run-child');
const handlers = buildRecordingPipelineHandlers({ startRun });
const run = (kind: string, ctx: Record<string, unknown>) => handlers.get(kind)!.execute(node, ctx, helpers);
const applyNode = (buildCallTranscriptionDefinition() as { nodes: Array<{ type: string }> }).nodes.find((n) => n.type === DOCUMENT_APPLY_KIND) as unknown as PipelineNode;
const apply = createDocumentApplyHandler();

const spoken = [{ startMs: 0, endMs: 4000, speakerId: 'a', speakerLabel: 'Alice', text: 'sounds of the sound' }];
const reviewSession = { sessionId: 's1', channelArn: ARN, documentId: DOC, kind: 'document-review', documentIds: [DOC], startedBy: 'alice', orgId: 'org', participants: [{ userId: 'alice', displayName: 'Alice' }] };

beforeEach(() => { updateTranscriptStatusMock.mockClear(); collaboratorCtor.mockClear(); startRun.mockClear(); process.env.TRANSCRIBE_EVERY_RECORDING = 'true'; });
afterAll(() => { delete process.env.TRANSCRIBE_EVERY_RECORDING; });

describe('documentWriteSkipReason', () => {
  it('never lets a review call write, whatever the flag says', () => {
    expect(documentWriteSkipReason({ callKind: 'document-review' })).toMatch(/review call never edits/);
    expect(documentWriteSkipReason({ callKind: 'document-review', writeTranscriptToDocument: true })).toMatch(/review call never edits/);
  });
  it('lets only an explicitly opted-in run write', () => {
    expect(documentWriteSkipReason({})).toMatch(/not started to produce a transcript document/);
    expect(documentWriteSkipReason({ callKind: null, documentId: DOC })).toMatch(/not started/);
    expect(documentWriteSkipReason({ writeTranscriptToDocument: true })).toBeNull();
  });
});

describe('a document-review call', () => {
  it('resolve carries the call kind alongside the document the call is about', async () => {
    findByChannelArnMock.mockResolvedValueOnce([reviewSession]);
    const out = await run(KINDS.resolve, { arn: ARN, recordingId: 'rec-1', detail: { startedAt: '2026-10-09T19:41:00Z', endedAt: '2026-10-09T19:42:00Z' } });
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ lobbyName: DOC, callId: 'call-1', callKind: 'document-review', documentId: DOC });
  });

  it('start-transcription passes the kind on to the transcription run', async () => {
    const out = await run(KINDS.startTranscription, { arn: ARN, recordingId: 'rec-1', callId: 'call-1', lobbyName: DOC, composeStatus: 'ok', composedKey: 'k', documentId: DOC, callKind: 'document-review', detail: {} });
    expect(out.status).toBe('completed');
    expect(startRun).toHaveBeenCalledTimes(1);
    expect(startRun.mock.calls[0][2]).toMatchObject({ documentId: DOC, callKind: 'document-review' });
    expect(startRun.mock.calls[0][2]).not.toHaveProperty('writeTranscriptToDocument');
  });

  it('keeps the transcript and the summary on the recording row, and the apply step leaves the document alone', async () => {
    const ctx: Record<string, unknown> = { callId: 'call-1', recordingId: 'rec-1', lobbyName: DOC, documentId: DOC, callKind: 'document-review', transcript: { segments: spoken } };
    const stored = await run(KINDS.store, ctx);
    expect(stored.status).toBe('completed');
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('call-1', 'rec-1', expect.objectContaining({ status: 'ready', transcriptKey: 'inline', transcriptJson: JSON.stringify({ segments: spoken }) }));
    expect(stored.output).toMatchObject({ hasSpeech: true, documentWrite: { skipped: expect.stringMatching(/review call/) } });
    Object.assign(ctx, stored.output, { hasSpeech: true, steps: { summarize: { text: '## Summary\nA short call.' } } });

    const summarised = await run(KINDS.storeSummary, ctx);
    expect(summarised.output).toMatchObject({ summaryStatus: 'ready' });
    expect(updateTranscriptStatusMock).toHaveBeenLastCalledWith('call-1', 'rec-1', expect.objectContaining({ summaryStatus: 'ready', summaryMarkdown: '## Summary\nA short call.' }));
    Object.assign(ctx, summarised.output);
    // Neither the summary nor the transcript became a document operation.
    expect(ctx.transcriptOps).toEqual([{ op: 'skip', reason: expect.stringMatching(/review call never edits/) }]);

    const applied = await apply.execute(applyNode, ctx, helpers);
    expect(applied.status).toBe('completed');
    expect(applied.output).toMatchObject({ applied: 0, reason: expect.stringMatching(/review call never edits/) });
    expect(collaboratorCtor).not.toHaveBeenCalled();
  });
});

describe('a call started to produce a transcript document (writeTranscriptToDocument: true)', () => {
  it('still hands the summary and transcript to the apply step as before', async () => {
    const ctx: Record<string, unknown> = { callId: 'call-2', recordingId: 'rec-2', lobbyName: 'room:notes', documentId: DOC, writeTranscriptToDocument: true, transcript: { segments: spoken } };
    const stored = await run(KINDS.store, ctx);
    expect(stored.output).toMatchObject({ documentWrite: { allowed: true }, transcriptOps: transcriptOps(spoken) });
    Object.assign(ctx, stored.output, { hasSpeech: true, steps: { summarize: { text: '## Summary\nNotes.' } } });
    const summarised = await run(KINDS.storeSummary, ctx);
    expect((summarised.output as { transcriptOps: Array<{ text?: string }> }).transcriptOps.map((o) => o.text)).toEqual(['Call summary', '## Summary', 'Notes.', 'Call transcript', 'sounds of the sound']);
    Object.assign(ctx, summarised.output);
    // The apply step now opens the document — the mock throws, which is the proof it tried.
    await expect(apply.execute(applyNode, ctx, helpers)).rejects.toThrow('the document was opened');
    expect(collaboratorCtor).toHaveBeenCalledWith(expect.objectContaining({ documentId: DOC }));
  });
});

describe('a room call with no document', () => {
  it('skips the apply step for want of a document, as before', async () => {
    const stored = await run(KINDS.store, { callId: 'call-3', recordingId: 'rec-3', lobbyName: 'room:x', transcript: { segments: spoken } });
    const applied = await apply.execute(applyNode, { ...(stored.output as Record<string, unknown>), documentId: null }, helpers);
    expect(applied.output).toMatchObject({ applied: 0, skipped: 'no document on this run' });
    expect(collaboratorCtor).not.toHaveBeenCalled();
  });
});
