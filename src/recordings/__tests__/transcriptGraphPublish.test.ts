// NFR #159 (realtime-examples Loop 37): the transcript-ready step tells the
// Activity graph, whichever real flow started the run. Neither the finalize
// pipeline's child run nor POST /recordings/:id/transcribe put the org and
// owner in the run context, so the step reads them from the recording row.
const updateTranscriptStatusMock = jest.fn().mockResolvedValue(undefined);
const getRecordingMock = jest.fn();
const meetingMock = jest.fn().mockResolvedValue({ published: true });
jest.mock('../../repositories', () => ({
  callRecordingRepo: { updateTranscriptStatus: updateTranscriptStatusMock, getRecording: getRecordingMock },
  videoSessionRepo: {},
  callReactionRepo: {},
}));
jest.mock('../../work-graph/sources/platformPublisher', () => ({ platformWorkGraphSourcePublisher: { meeting: meetingMock } }));
jest.mock('../../lib/aws-clients', () => ({ ddbClient: { send: jest.fn() }, docClient: { send: jest.fn() } }));
jest.mock('../../lib/redis-client', () => ({ getRedisClient: jest.fn().mockResolvedValue(null) }));
jest.mock('../../gateway/GatewayServiceClient', () => ({ postChatMessage: jest.fn(), PIPELINE_IDENTITY: { userId: 'pipeline', displayName: 'Pipeline' } }));

import { buildRecordingPipelineHandlers, KINDS } from '../recordingPipelines';
import type { PipelineNode } from 'distributed-core/applications/pipeline';

const node = { id: 'x', type: 'x', position: { x: 0, y: 0 }, data: { type: 'x' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1' } as never;
const segments = [{ startMs: 0, endMs: 4000, speakerId: 'a', speakerLabel: 'Connor', text: 'Ship it.' }];

beforeEach(() => { jest.clearAllMocks(); meetingMock.mockResolvedValue({ published: true }); });

describe('transcript:store → Activity graph', () => {
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
