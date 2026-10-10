// The meeting-notes summary is a step of the call-transcription run, not a
// second writer racing it. These tests run the store and store-summary steps
// against a mocked row store, exactly as the executor would, and check what
// lands on the row for a spoken call and for silence.
const updateTranscriptStatusMock = jest.fn().mockResolvedValue(undefined);
jest.mock('../../repositories', () => ({
  callRecordingRepo: { updateTranscriptStatus: updateTranscriptStatusMock },
  videoSessionRepo: {},
  callReactionRepo: {},
}));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));

import { buildRecordingPipelineHandlers, KINDS, transcriptText, stripFences, summaryOps } from '../recordingPipelines';
import type { PipelineNode } from 'distributed-core/applications/pipeline';

const node = { id: 'x', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1' } as never;
const handlers = buildRecordingPipelineHandlers();
const run = (kind: string, ctx: Record<string, unknown>) => handlers.get(kind)!.execute(node, ctx, helpers);

const spoken = [
  { startMs: 0, endMs: 4000, speakerId: 'a', speakerLabel: 'Connor', text: 'Let us ship the summary step.' },
  { startMs: 65_000, endMs: 70_000, speakerId: 'b', speakerLabel: 'Sam', text: ' Agreed. ' },
];

beforeEach(() => updateTranscriptStatusMock.mockClear());

describe('transcriptText', () => {
  it('is one timestamped, speaker-tagged line per spoken segment', () => {
    expect(transcriptText(spoken)).toBe('[00:00] Connor: Let us ship the summary step.\n[01:05] Sam: Agreed.');
  });
  it('is empty for silence', () => {
    expect(transcriptText([{ text: '' }, { text: '   ' }])).toBe('');
  });
});

describe('stripFences + summaryOps', () => {
  it('lifts the answer out of a code fence', () => {
    expect(stripFences('```markdown\n## Summary\nHi\n```')).toBe('## Summary\nHi');
    expect(stripFences('  ## Summary  ')).toBe('## Summary');
  });
  it('is a heading and one paragraph per non-empty line', () => {
    expect(summaryOps('## Summary\n\nWe agreed.\n')).toEqual([
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call summary' },
      { op: 'appendBlock', kind: 'paragraph', text: '## Summary' },
      { op: 'appendBlock', kind: 'paragraph', text: 'We agreed.' },
    ]);
  });
});

describe('transcript:store', () => {
  it('emits the model-facing text, hasSpeech and the ops, and says the summary is pending', async () => {
    const out = await run(KINDS.store, { callId: 'c1', recordingId: 'r1', transcript: { segments: spoken } });
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ hasSpeech: true, transcriptText: transcriptText(spoken), segments: 2 });
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('c1', 'r1', expect.objectContaining({ status: 'ready', transcriptKey: 'inline', summaryStatus: 'pending' }));
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('c1', 'r1', expect.objectContaining({ summaryRunId: 'run-1', summaryError: null }));
    expect(out.output).toMatchObject({ callId: 'c1', recordingId: 'r1' });
  });
  it('reports no speech for a silent recording', async () => {
    const out = await run(KINDS.store, { callId: 'c1', recordingId: 'r1', writeTranscriptToDocument: true, transcript: { segments: [] } });
    expect(out.output).toMatchObject({ hasSpeech: false, transcriptText: '', transcriptOps: [] });
  });
  it('hands the apply step a skip, not the transcript, when the run was not started to produce a document', async () => {
    const out = await run(KINDS.store, { callId: 'c1', recordingId: 'r1', transcript: { segments: spoken } });
    expect(out.output).toMatchObject({ hasSpeech: true, documentWrite: { skipped: expect.stringContaining('not started to produce') }, transcriptOps: [{ op: 'skip', reason: expect.any(String) }] });
    // The transcript itself is still on the row.
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('c1', 'r1', expect.objectContaining({ status: 'ready', transcriptJson: JSON.stringify({ segments: spoken }) }));
  });
});

describe('transcript:store-summary', () => {
  it('writes ready + the markdown (fences removed) and puts the summary ahead of the transcript ops', async () => {
    const transcriptOps = [{ op: 'appendBlock', kind: 'heading', level: 2, text: 'Call transcript' }];
    const out = await run(KINDS.storeSummary, {
      callId: 'c1', recordingId: 'r1', hasSpeech: true, transcriptOps, writeTranscriptToDocument: true,
      steps: { summarize: { text: '```\n## Summary\nWe shipped it.\n```' } },
    });
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ summaryStatus: 'ready', summaryChars: '## Summary\nWe shipped it.'.length });
    expect((out.output as { transcriptOps: unknown[] }).transcriptOps).toEqual([
      { op: 'appendBlock', kind: 'heading', level: 2, text: 'Call summary' },
      { op: 'appendBlock', kind: 'paragraph', text: '## Summary' },
      { op: 'appendBlock', kind: 'paragraph', text: 'We shipped it.' },
      ...transcriptOps,
    ]);
    expect(updateTranscriptStatusMock).toHaveBeenCalledTimes(1);
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('c1', 'r1', expect.objectContaining({
      status: 'ready', summaryStatus: 'ready', summaryMarkdown: '## Summary\nWe shipped it.', summaryError: null, transcriptGeneratedAt: expect.any(String),
    }));
  });

  it('writes skipped for silence without reading the model, and leaves the ops alone', async () => {
    const out = await run(KINDS.storeSummary, {
      callId: 'c1', recordingId: 'r1', hasSpeech: false, transcriptOps: [],
      steps: { summarize: { text: 'A summary the model made up about nothing.' } },
    });
    expect(out.status).toBe('completed');
    expect(out.output).toEqual({ summaryStatus: 'skipped', summaryChars: 0 });
    expect(updateTranscriptStatusMock).toHaveBeenCalledTimes(1);
    const patch = updateTranscriptStatusMock.mock.calls[0][2];
    expect(patch).toMatchObject({ status: 'ready', summaryStatus: 'skipped' });
    expect(patch).not.toHaveProperty('summaryMarkdown');
  });

  it('marks only the summary failed when the model answered nothing; the transcript stays ready', async () => {
    const out = await run(KINDS.storeSummary, { callId: 'c1', recordingId: 'r1', hasSpeech: true, steps: { summarize: { text: '' } } });
    expect(out.status).toBe('completed');
    expect(out.output).toMatchObject({ summaryStatus: 'failed', summaryChars: 0 });
    expect(updateTranscriptStatusMock).toHaveBeenCalledWith('c1', 'r1', expect.objectContaining({ status: 'ready', summaryStatus: 'failed', summaryError: expect.any(String) }));
  });
});
