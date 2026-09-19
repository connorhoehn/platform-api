import {
  mapMeeting,
  type AuthenticatedMeetingContext,
  type MeetingSourceRecord,
} from '../mapMeeting';

const context: AuthenticatedMeetingContext = {
  actor: {
    actorId: 'person-42',
    organizationId: 'org-1',
    sourceSubject: 'cognito-subject-42',
  },
  producer: {
    serviceId: 'platform-api',
    organizationId: 'org-1',
    credentialId: 'service-key-7',
  },
  receivedAt: '2026-09-19T12:00:05.000Z',
};

const base = {
  meetingId: 'call-7',
  organizationId: 'org-1',
  participantSubject: 'cognito-subject-42',
  occurredAt: '2026-09-19T12:00:01.000Z',
} as const;

function eventOf(result: ReturnType<typeof mapMeeting>) {
  expect(result.supported).toBe(true);
  if (!result.supported) throw new Error(result.reason);
  return result.event;
}

describe('mapMeeting', () => {
  it('maps attendance start and end without fabricating a recording', () => {
    const joined = eventOf(mapMeeting({ ...base, kind: 'attendance', action: 'joined' }, context));
    const ended = eventOf(mapMeeting({
      ...base,
      kind: 'attendance',
      action: 'ended',
      occurredAt: '2026-09-19T12:30:00.000Z',
    }, context));

    expect(joined.payload).toEqual({
      kind: 'meeting', lifecycle: 'attendance-started', meetingId: 'call-7',
    });
    expect(ended.payload.lifecycle).toBe('attendance-ended');
    expect(joined.payload).not.toHaveProperty('recordingId');
    expect(joined.payload).not.toHaveProperty('recordingOwnerId');
  });

  it.each([
    ['sfu.recording.started', 'recording-started'],
    ['sfu.recording.completed', 'recording-ready'],
    ['sfu.recording.failed', 'recording-failed'],
    ['sfu.recording.deleted', 'recording-deleted'],
  ] as const)('maps LVS %s to %s', (eventType, lifecycle) => {
    const event = eventOf(mapMeeting({
      ...base,
      kind: 'recording',
      eventType,
      recordingId: 'recording-9',
      recordingOwnerId: 'person-owner',
    }, context));

    expect(event.payload).toMatchObject({
      lifecycle,
      meetingId: 'call-7',
      recordingId: 'recording-9',
      recordingOwnerId: 'person-owner',
    });
  });

  it('keeps attendance attribution separate from explicit recording ownership', () => {
    const attendance = eventOf(mapMeeting({ ...base, kind: 'attendance', action: 'joined' }, context));
    const recording = eventOf(mapMeeting({
      ...base,
      kind: 'recording',
      eventType: 'sfu.recording.started',
      recordingId: 'recording-9',
      recordingOwnerId: 'somebody-else',
    }, context));

    expect(attendance.actor.actorId).toBe('person-42');
    expect(attendance.payload.recordingOwnerId).toBeUndefined();
    expect(recording.actor.actorId).toBe('person-42');
    expect(recording.payload.recordingOwnerId).toBe('somebody-else');
  });

  it('returns typed partial results while a transcript is pending or processing', () => {
    const pending: MeetingSourceRecord = {
      ...base, kind: 'transcript', recordingId: 'recording-9', status: 'pending',
    };
    const processing: MeetingSourceRecord = {
      ...base, kind: 'transcript', recordingId: 'recording-9', status: 'processing',
    };

    expect(mapMeeting(pending, context)).toEqual({
      supported: false, unsupported: true, partial: true, reason: 'transcript-pending',
    });
    expect(mapMeeting(processing, context)).toEqual({
      supported: false, unsupported: true, partial: true, reason: 'transcript-processing',
    });
  });

  it.each([
    ['ready', 'transcript-ready'],
    ['failed', 'transcript-failed'],
    ['deleted', 'transcript-deleted'],
  ] as const)('maps transcript %s to %s', (status, lifecycle) => {
    const event = eventOf(mapMeeting({
      ...base,
      kind: 'transcript',
      recordingId: 'recording-9',
      transcriptId: 'transcript-4',
      status,
    }, context));

    expect(event.payload).toMatchObject({
      lifecycle,
      meetingId: 'call-7',
      recordingId: 'recording-9',
      transcriptId: 'transcript-4',
    });
  });

  it('accepts late readiness after failure as a newer independent event', () => {
    const failed = eventOf(mapMeeting({
      ...base,
      kind: 'transcript',
      recordingId: 'recording-9',
      status: 'failed',
      occurredAt: '2026-09-19T12:05:00.000Z',
      sourceSequence: '41',
    }, context));
    const ready = eventOf(mapMeeting({
      ...base,
      kind: 'transcript',
      recordingId: 'recording-9',
      status: 'ready',
      occurredAt: '2026-09-19T12:10:00.000Z',
      sourceSequence: '42',
    }, context));

    expect(failed.payload.lifecycle).toBe('transcript-failed');
    expect(ready.payload.lifecycle).toBe('transcript-ready');
    expect(ready.sourceSequence).toBe('42');
    expect(ready.eventId).not.toBe(failed.eventId);
    expect(ready.payload.transcriptId).toBe('recording:recording-9');
  });

  it('never copies transcript content, join secrets, media locations, or tokens', () => {
    const attendance = eventOf(mapMeeting({
      ...base,
      kind: 'attendance',
      action: 'joined',
      joinSecret: 'meeting-join-secret',
    }, context));
    const recording = eventOf(mapMeeting({
      ...base,
      kind: 'recording',
      eventType: 'sfu.recording.completed',
      recordingId: 'recording-9',
      recordingOwnerId: 'person-owner',
      playbackToken: 'play-secret',
      shareToken: 'share-secret',
      destination: 's3://private/recording',
    }, context));
    const transcript = eventOf(mapMeeting({
      ...base,
      kind: 'transcript',
      recordingId: 'recording-9',
      status: 'ready',
      transcriptText: 'PRIVATE transcript words',
      transcriptJson: '{"text":"PRIVATE"}',
      transcriptKey: 's3://private/transcript.json',
      playbackToken: 'play-secret',
      shareToken: 'share-secret',
    }, context));

    const serialized = JSON.stringify([attendance, recording, transcript]);
    expect(serialized).not.toContain('PRIVATE');
    expect(serialized).not.toContain('s3://');
    expect(serialized).not.toContain('meeting-join-secret');
    expect(serialized).not.toContain('play-secret');
    expect(serialized).not.toContain('share-secret');
  });

  it('requires explicit recording ownership instead of inferring it from attendance', () => {
    expect(mapMeeting({
      ...base,
      kind: 'recording',
      eventType: 'sfu.recording.completed',
      recordingId: 'recording-9',
      recordingOwnerId: '',
    }, context)).toMatchObject({
      supported: false, partial: true, reason: 'missing-recording-owner',
    });
  });

  it('rejects cross-organization and mismatched participant attribution', () => {
    expect(mapMeeting({
      ...base, kind: 'attendance', action: 'joined', organizationId: 'other-org',
    }, context)).toMatchObject({ supported: false, reason: 'organization-mismatch' });

    expect(mapMeeting({
      ...base, kind: 'attendance', action: 'joined', participantSubject: 'somebody-else',
    }, context)).toMatchObject({ supported: false, reason: 'actor-does-not-match-participant' });

    expect(mapMeeting({ ...base, kind: 'attendance', action: 'joined' }, {
      ...context,
      producer: { ...context.producer, organizationId: 'other-org' },
    })).toMatchObject({ supported: false, reason: 'missing-identity-context' });
  });

  it('is deterministic for source redelivery', () => {
    const source: MeetingSourceRecord = {
      ...base,
      kind: 'recording',
      eventType: 'sfu.recording.completed',
      recordingId: 'recording-9',
      recordingOwnerId: 'person-owner',
      sourceSequence: 'lvs-17',
    };

    expect(mapMeeting(source, context)).toEqual(mapMeeting(source, context));
    expect(eventOf(mapMeeting(source, context))).toMatchObject({
      sourceSequence: 'lvs-17',
      resource: { resourceVersion: 'lvs-17' },
    });
  });
});
