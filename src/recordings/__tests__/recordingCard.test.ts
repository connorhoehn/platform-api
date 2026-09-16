
import { recordingCardMetadata } from '../recordingPipelines';

describe('recordingCardMetadata', () => {
  const base = {
    recordingId: 'rec-1', callId: 'call-1', lobbyName: 'room:general', startedAt: '2026-09-15T20:00:00.000Z',
    mediaDurationMs: 32_000, participantCount: 2, startedBy: 'dev-mia',
    participants: [{ userId: 'dev-mia', displayName: 'Mia Zhao' }, { userId: 'dev-eve' }],
    detail: { thumbnailUrl: '/api/channels/arn/recordings/rec-1/thumbnail.jpg' },
    composeStatus: 'ok',
  };

  it('posts the recording itself: kind, length, who by, thumbnail', () => {
    const { message, metadata } = recordingCardMetadata(base, 'run-1');
    expect(message).toBe('Recording — 32s');
    expect(metadata).toMatchObject({ kind: 'recording', recordingId: 'rec-1', callId: 'call-1', durationMs: 32_000, participantCount: 2, status: 'ready', recordedBy: 'Mia Zhao', runId: 'run-1' });
    expect(metadata.participants).toEqual([{ userId: 'dev-mia', displayName: 'Mia Zhao' }, { userId: 'dev-eve', displayName: undefined }]);
    expect(String(metadata.thumbnailUrl)).toMatch(/\/api\/channels\/arn\/recordings\/rec-1\/thumbnail\.jpg$/);
  });

  it('still posts when compose failed, and says so', () => {
    const { message, metadata } = recordingCardMetadata({ ...base, composeStatus: 'failed' }, 'run-2');
    expect(metadata.status).toBe('failed');
    expect(message).toMatch(/could not be processed/);
  });

  it('is "saving" when posted before compose has run', () => {
    const { composeStatus: _omit, ...early } = base;
    const { message, metadata } = recordingCardMetadata(early, 'run-0');
    expect(metadata).toMatchObject({ status: 'processing', stage: 'saving' });
    expect(message).toBe('Recording — 32s');
  });

  it('falls back to the user id when nobody knows the name', () => {
    const { metadata } = recordingCardMetadata({ ...base, participants: [] }, 'run-3');
    expect(metadata.recordedBy).toBe('dev-mia');
  });
});

describe('recordingStage', () => {
  // eslint-disable-next-line @typescript-eslint/no-require-imports
  const { recordingStage } = require('../../routes/recordings');
  it('walks saving → transcribing → summarizing → ready', () => {
    expect(recordingStage({ composeStatus: 'running' })).toBe('saving');
    expect(recordingStage({})).toBe('saving');
    expect(recordingStage({ composeStatus: 'ok', transcriptStatus: 'processing' })).toBe('transcribing');
    expect(recordingStage({ composeStatus: 'ok', transcriptStatus: 'ready', summaryStatus: 'pending' })).toBe('summarizing');
    expect(recordingStage({ composeStatus: 'ok', transcriptStatus: 'ready', summaryStatus: 'ready' })).toBe('ready');
    expect(recordingStage({ composeStatus: 'skipped' })).toBe('ready');
    expect(recordingStage({ composeStatus: 'failed' })).toBe('failed');
  });
});
