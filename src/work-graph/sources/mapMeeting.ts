/**
 * Pure adapter for meeting attendance, LVS recording lifecycle, and the
 * platform-owned transcript lifecycle. The output is private projection input,
 * never a browser DTO.
 *
 * The three inputs stay separate on purpose. Attendance is an authenticated
 * participant fact; it does not establish who started a recording. Likewise,
 * a recording or transcript row does not prove that an arbitrary participant
 * attended unless the caller has already joined it to that participant's
 * authoritative attendance record.
 */
import type { CallEventAction } from '../../repositories/CallLogRepository';
import type { CallRecording } from '../../repositories/CallRecordingRepository';

export interface AuthenticatedMeetingContext {
  /** Identity resolved by platform-api from an authenticated source subject. */
  actor: {
    actorId: string;
    organizationId: string;
    sourceSubject: string;
  };
  producer: {
    serviceId: string;
    organizationId: string;
    credentialId: string;
  };
  receivedAt: string;
}

interface MeetingRecordBase {
  meetingId: string;
  organizationId: string;
  /** Subject explicitly attributed by the authoritative source record. */
  participantSubject: string;
  occurredAt: string;
  sourceSequence?: string;
  /** A server-approved label. Source titles are not read implicitly. */
  safeLabel?: string;
}

/**
 * A room that exists and has not started — one an agent loop created, with the
 * people it was made for and the time it was proposed for. Everything a card
 * shows about it is read from the room's own record by the activity reader;
 * this says only that it exists and what it is called.
 */
export interface MeetingScheduledSourceRecord extends MeetingRecordBase {
  kind: 'scheduled';
}

export interface MeetingAttendanceSourceRecord extends MeetingRecordBase {
  kind: 'attendance';
  action: Extract<CallEventAction, 'joined' | 'ended'>;
  /** Source-only fields are accepted structurally but are never copied. */
  joinSecret?: string;
}

export type LvsRecordingEventType =
  | 'sfu.recording.started'
  | 'sfu.recording.completed'
  | 'sfu.recording.failed'
  | 'sfu.recording.deleted';

export interface MeetingRecordingSourceRecord extends MeetingRecordBase {
  kind: 'recording';
  eventType: LvsRecordingEventType;
  recordingId: string;
  /** Platform actor id read from explicit recording ownership metadata. */
  recordingOwnerId: string;
  /** Source-only credentials and media locations are never copied. */
  playbackToken?: string;
  shareToken?: string;
  destination?: string;
}

type PlatformTranscriptStatus = NonNullable<CallRecording['transcriptStatus']>;

export interface MeetingTranscriptSourceRecord extends MeetingRecordBase {
  kind: 'transcript';
  recordingId: string;
  status: PlatformTranscriptStatus | 'deleted';
  /**
   * Optional authoritative id. Older rows have no separate transcript id; in
   * that case the recording id is namespaced into a deterministic internal id.
   */
  transcriptId?: string;
  /** Included only when the source row explicitly carries recording ownership. */
  recordingOwnerId?: string;
  /** Source-only content and blob references are deliberately discarded. */
  transcriptText?: string;
  transcriptJson?: string;
  transcriptKey?: string;
  playbackToken?: string;
  shareToken?: string;
}

export type MeetingSourceRecord =
  | MeetingScheduledSourceRecord
  | MeetingAttendanceSourceRecord
  | MeetingRecordingSourceRecord
  | MeetingTranscriptSourceRecord;

export type MeetingLifecycle =
  | 'meeting-scheduled'
  | 'attendance-started'
  | 'attendance-ended'
  | 'recording-started'
  | 'recording-ready'
  | 'recording-failed'
  | 'recording-deleted'
  | 'transcript-ready'
  | 'transcript-failed'
  | 'transcript-deleted';

/** Structural equivalent of the frozen C0 meeting AuthenticatedWorkEvent. */
export interface MeetingWorkEvent {
  schemaVersion: 1;
  eventId: string;
  idempotencyKey: string;
  source: 'meeting';
  sourceSequence: string;
  occurredAt: string;
  receivedAt: string;
  actor: AuthenticatedMeetingContext['actor'];
  producer: AuthenticatedMeetingContext['producer'];
  resource: {
    source: 'meeting';
    resourceId: string;
    resourceVersion: string;
  };
  payload: {
    kind: 'meeting';
    lifecycle: MeetingLifecycle;
    meetingId: string;
    recordingId?: string;
    transcriptId?: string;
    recordingOwnerId?: string;
    safeLabel?: string;
  };
}

export type MeetingMapUnsupportedReason =
  | 'missing-identity-context'
  | 'organization-mismatch'
  | 'actor-does-not-match-participant'
  | 'missing-meeting-id'
  | 'missing-recording-id'
  | 'missing-recording-owner'
  | 'invalid-timestamp'
  | 'transcript-pending'
  | 'transcript-processing';

export type MeetingMapResult =
  | { supported: true; event: MeetingWorkEvent }
  | {
      supported: false;
      unsupported: true;
      partial: true;
      reason: MeetingMapUnsupportedReason;
    };

function unsupported(reason: MeetingMapUnsupportedReason): MeetingMapResult {
  return { supported: false, unsupported: true, partial: true, reason };
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function isIsoTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

function hasIdentityContext(context: AuthenticatedMeetingContext): boolean {
  const { actor, producer } = context;
  return [
    actor.actorId,
    actor.organizationId,
    actor.sourceSubject,
    producer.serviceId,
    producer.organizationId,
    producer.credentialId,
  ].every(isNonEmptyString)
    && actor.organizationId === producer.organizationId
    && isIsoTimestamp(context.receivedAt);
}

function lifecycleFor(record: MeetingSourceRecord): MeetingLifecycle | undefined {
  switch (record.kind) {
    case 'scheduled':
      return 'meeting-scheduled';
    case 'attendance':
      return record.action === 'joined' ? 'attendance-started' : 'attendance-ended';
    case 'recording':
      return ({
        'sfu.recording.started': 'recording-started',
        'sfu.recording.completed': 'recording-ready',
        'sfu.recording.failed': 'recording-failed',
        'sfu.recording.deleted': 'recording-deleted',
      } as const)[record.eventType];
    case 'transcript':
      return ({
        ready: 'transcript-ready',
        failed: 'transcript-failed',
        deleted: 'transcript-deleted',
      } as const)[record.status as 'ready' | 'failed' | 'deleted'];
  }
}

function resourceIdFor(record: MeetingSourceRecord): string {
  // A scheduled room and its later attendance are the SAME meeting, so they
  // are addressed identically. A second address would be a second meeting.
  if (record.kind === 'scheduled' || record.kind === 'attendance') return `meeting:${record.meetingId}`;
  if (record.kind === 'recording') return `recording:${record.recordingId}`;
  return `transcript:${record.transcriptId ?? record.recordingId}`;
}

/**
 * Maps one authoritative observation. A source consumer must call this once
 * for each explicitly joined participant; this function never fans a roster
 * out into attributed human activity.
 */
export function mapMeeting(
  record: MeetingSourceRecord,
  context: AuthenticatedMeetingContext,
): MeetingMapResult {
  if (!hasIdentityContext(context)) return unsupported('missing-identity-context');
  if (record.organizationId !== context.actor.organizationId) {
    return unsupported('organization-mismatch');
  }
  if (record.participantSubject !== context.actor.sourceSubject) {
    return unsupported('actor-does-not-match-participant');
  }
  if (!isNonEmptyString(record.meetingId)) return unsupported('missing-meeting-id');
  if (!isIsoTimestamp(record.occurredAt)) return unsupported('invalid-timestamp');

  const carriesRecording = record.kind === 'recording' || record.kind === 'transcript';
  if (carriesRecording && !isNonEmptyString(record.recordingId)) {
    return unsupported('missing-recording-id');
  }
  if (record.kind === 'recording' && !isNonEmptyString(record.recordingOwnerId)) {
    return unsupported('missing-recording-owner');
  }
  if (record.kind === 'transcript' && record.status === 'pending') {
    return unsupported('transcript-pending');
  }
  if (record.kind === 'transcript' && record.status === 'processing') {
    return unsupported('transcript-processing');
  }

  const lifecycle = lifecycleFor(record);
  // All remaining values are closed unions, so this is defensive against
  // unvalidated JavaScript callers rather than a reachable TypeScript state.
  if (!lifecycle) return unsupported('transcript-processing');

  const transitionKey = `${lifecycle}:${record.occurredAt}`;
  const sourceSequence = isNonEmptyString(record.sourceSequence)
    ? record.sourceSequence
    : transitionKey;
  const resourceId = resourceIdFor(record);
  const eventId = `meeting:${resourceId}:${transitionKey}`;

  return {
    supported: true,
    event: {
      schemaVersion: 1,
      eventId,
      idempotencyKey: eventId,
      source: 'meeting',
      sourceSequence,
      occurredAt: record.occurredAt,
      receivedAt: context.receivedAt,
      actor: context.actor,
      producer: context.producer,
      resource: {
        source: 'meeting',
        resourceId,
        resourceVersion: sourceSequence,
      },
      payload: {
        kind: 'meeting',
        lifecycle,
        meetingId: record.meetingId,
        ...(carriesRecording ? { recordingId: record.recordingId } : {}),
        ...(record.kind === 'transcript'
          ? { transcriptId: record.transcriptId ?? `recording:${record.recordingId}` }
          : {}),
        ...(carriesRecording && isNonEmptyString(record.recordingOwnerId)
          ? { recordingOwnerId: record.recordingOwnerId }
          : {}),
        ...(isNonEmptyString(record.safeLabel) ? { safeLabel: record.safeLabel } : {}),
      },
    },
  };
}
