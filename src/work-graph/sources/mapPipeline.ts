/**
 * Pure adapter from the canonical pipeline-runtime run surface to the private
 * Active now source event. The result is projection input, never a browser DTO.
 *
 * Pipeline definitions and browser activity relays are deliberately outside
 * this adapter. A caller must join a terminal bus event that lacks pipelineId
 * to an authoritative PipelineRunSnapshot before mapping it.
 */
import {
  normalizeRunStatus,
  type PipelineRun,
  type PipelineRunSnapshot,
} from '@connorhoehn/pipeline-runtime';

export interface AuthenticatedPipelineContext {
  /** Identity established by platform-api authentication, not run metadata. */
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

/** An explicit artifact association, for example from run image lineage. */
export interface PipelineArtifactReference {
  artifactId: string;
  pipelineId: string;
  runId: string;
  /** Source-only fields are accepted structurally but never copied. */
  url?: string;
  payload?: unknown;
}

export interface PipelineRunSourceRecord {
  kind: 'pipeline-run';
  run: PipelineRun | PipelineRunSnapshot;
  /**
   * Authoritative trigger subject joined from PipelineRunTrigger/PipelineRunItem.
   * Synthetic dispatch identities must not be converted into human activity.
   */
  attributedSubject: string;
  /** Time of this authoritative transition/snapshot observation. */
  occurredAt: string;
  /** Optional monotonic position supplied by the source event/WAL. */
  sourceSequence?: string;
  /** Explicit source-approved label; pipeline definition content is not read. */
  safeLabel?: string;
  artifactRefs?: readonly PipelineArtifactReference[];
}

/** Included so definition records fail closed instead of masquerading as runs. */
export interface PipelineDefinitionSourceRecord {
  kind: 'pipeline-definition';
  definitionId: string;
  status?: string;
}

export type PipelineSourceRecord = PipelineRunSourceRecord | PipelineDefinitionSourceRecord;
export type PipelineLifecycle = 'started' | 'waiting' | 'completed' | 'failed' | 'cancelled';

/** Structural equivalent of C0 AuthenticatedWorkEvent for the pipeline source. */
export interface PipelineWorkEvent {
  schemaVersion: 1;
  eventId: string;
  idempotencyKey: string;
  source: 'pipeline';
  sourceSequence: string;
  occurredAt: string;
  receivedAt: string;
  actor: AuthenticatedPipelineContext['actor'];
  producer: AuthenticatedPipelineContext['producer'];
  resource: {
    source: 'pipeline';
    resourceId: string;
    resourceVersion: string;
  };
  payload: {
    kind: 'pipeline';
    lifecycle: PipelineLifecycle;
    pipelineId: string;
    runId: string;
    attempt: number;
    artifactIds?: string[];
    safeLabel?: string;
  };
}

export type PipelineMapUnsupportedReason =
  | 'definition-is-not-run'
  | 'missing-identity-context'
  | 'missing-run-id'
  | 'missing-pipeline-id'
  | 'missing-run-status'
  | 'missing-run-attribution'
  | 'actor-does-not-match-run-trigger'
  | 'synthetic-trigger-is-not-human'
  | 'unsupported-run-status'
  | 'invalid-timestamp'
  | 'invalid-replay-attempts'
  | 'invalid-artifact-reference';

export type PipelineMapResult =
  | { supported: true; event: PipelineWorkEvent }
  | {
      supported: false;
      unsupported: true;
      partial: true;
      reason: PipelineMapUnsupportedReason;
    };

function unsupported(reason: PipelineMapUnsupportedReason): PipelineMapResult {
  return { supported: false, unsupported: true, partial: true, reason };
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function isIsoTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

function hasIdentityContext(context: AuthenticatedPipelineContext): boolean {
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

function lifecycleForStatus(raw: string): PipelineLifecycle | undefined {
  switch (normalizeRunStatus(raw)) {
    case 'pending':
    case 'running':
      return 'started';
    case 'awaiting_approval':
    case 'paused_at_breakpoint':
    case 'stuck':
      return 'waiting';
    case 'completed':
      return 'completed';
    case 'rejected':
    case 'failed':
      return 'failed';
    case 'cancelled':
      return 'cancelled';
    default:
      return undefined;
  }
}

function isSyntheticTrigger(subject: string): boolean {
  return subject === 'system'
    || subject.startsWith('event:')
    || subject.startsWith('schedule:')
    || subject.startsWith('webhook:');
}

function replayAttemptsFrom(run: PipelineRun | PipelineRunSnapshot): number | undefined {
  const value = (run as PipelineRunSnapshot)['replayAttempts'];
  if (value === undefined) return 0;
  return typeof value === 'number' && Number.isInteger(value) && value >= 0
    ? value
    : undefined;
}

function artifactIdsFrom(
  refs: readonly PipelineArtifactReference[] | undefined,
  pipelineId: string,
  runId: string,
): string[] | undefined {
  if (!refs || refs.length === 0) return [];
  const ids = new Set<string>();
  for (const ref of refs) {
    if (
      !isNonEmptyString(ref.artifactId)
      || ref.pipelineId !== pipelineId
      || ref.runId !== runId
    ) {
      return undefined;
    }
    ids.add(ref.artifactId);
  }
  return [...ids].sort();
}

/**
 * Maps one authoritative run observation. Redelivery is deterministic: the
 * same run, replay attempt, lifecycle, timestamp, and sequence yield the same
 * event/idempotency key.
 */
export function mapPipeline(
  record: PipelineSourceRecord,
  context: AuthenticatedPipelineContext,
): PipelineMapResult {
  if (record.kind === 'pipeline-definition') return unsupported('definition-is-not-run');
  if (!hasIdentityContext(context)) return unsupported('missing-identity-context');

  const { run } = record;
  if (!isNonEmptyString(run.runId)) return unsupported('missing-run-id');
  if (!isNonEmptyString(run.pipelineId)) return unsupported('missing-pipeline-id');
  if (!isNonEmptyString(run.status)) return unsupported('missing-run-status');
  if (!isNonEmptyString(record.attributedSubject)) return unsupported('missing-run-attribution');
  if (isSyntheticTrigger(record.attributedSubject)) return unsupported('synthetic-trigger-is-not-human');
  if (record.attributedSubject !== context.actor.sourceSubject) {
    return unsupported('actor-does-not-match-run-trigger');
  }
  if (!isIsoTimestamp(record.occurredAt)) return unsupported('invalid-timestamp');

  const lifecycle = lifecycleForStatus(run.status);
  if (!lifecycle) return unsupported('unsupported-run-status');

  // replayAttempts is a monotonic zero-based count in the runtime snapshot.
  // The work-graph contract exposes the human-readable one-based attempt.
  const replayAttempts = replayAttemptsFrom(run);
  if (replayAttempts === undefined) return unsupported('invalid-replay-attempts');
  const attempt = replayAttempts + 1;

  const artifactIds = artifactIdsFrom(record.artifactRefs, run.pipelineId, run.runId);
  if (artifactIds === undefined) return unsupported('invalid-artifact-reference');

  const transitionKey = `${attempt}:${lifecycle}:${record.occurredAt}`;
  const sourceSequence = isNonEmptyString(record.sourceSequence)
    ? record.sourceSequence
    : transitionKey;
  const eventId = `pipeline:run:${run.runId}:${transitionKey}`;

  return {
    supported: true,
    event: {
      schemaVersion: 1,
      eventId,
      idempotencyKey: eventId,
      source: 'pipeline',
      sourceSequence,
      occurredAt: record.occurredAt,
      receivedAt: context.receivedAt,
      actor: context.actor,
      producer: context.producer,
      resource: {
        source: 'pipeline',
        resourceId: `run:${run.runId}`,
        resourceVersion: sourceSequence,
      },
      payload: {
        kind: 'pipeline',
        lifecycle,
        pipelineId: run.pipelineId,
        runId: run.runId,
        attempt,
        ...(artifactIds.length > 0 ? { artifactIds } : {}),
        ...(isNonEmptyString(record.safeLabel) ? { safeLabel: record.safeLabel } : {}),
      },
    },
  };
}
