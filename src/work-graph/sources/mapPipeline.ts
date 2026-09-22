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

/**
 * A resource the source itself says this run or step consumed. It becomes a
 * `derived-from` relationship to an ALREADY projected node — the reducer never
 * conjures the counterpart — which is how a run started from a chat command
 * reaches the conversation it came from, and how a step reaches the step it
 * depends on. Nothing here is inferred from actor, day, or adjacency.
 */
export interface PipelineInputReference {
  source: 'pipeline' | 'conversation' | 'document' | 'meeting' | 'cloud-compute' | 'local-compute';
  resourceId: string;
}

const INPUT_SOURCES = new Set<PipelineInputReference['source']>([
  'pipeline', 'conversation', 'document', 'meeting', 'cloud-compute', 'local-compute',
]);
const MAX_INPUT_REFERENCES = 16;

/**
 * Separates a run from one of its steps inside a single pipeline resource id.
 * `#` is not legal in a run id, so `runId#stepId` round-trips and an ACL can
 * recover the run whose row actually decides the step's visibility.
 */
export const PIPELINE_STEP_SEPARATOR = '#';

export function pipelineStepResourceId(runId: string, stepId: string): string {
  return `${runId}${PIPELINE_STEP_SEPARATOR}${stepId}`;
}

/** `runId#stepId` → both halves; a plain run id → `{ runId }` with no step. */
export function parsePipelineStepResourceId(resourceId: string): { runId: string; stepId?: string } {
  const raw = resourceId.replace(/^run:/, '');
  const separator = raw.indexOf(PIPELINE_STEP_SEPARATOR);
  if (separator <= 0 || separator === raw.length - 1) return { runId: raw };
  return { runId: raw.slice(0, separator), stepId: raw.slice(separator + 1) };
}

interface PipelineObservationFields {
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
  /** Source-declared evidence this run/step consumed. Never inferred. */
  inputRefs?: readonly PipelineInputReference[];
  /**
   * What this run/step is writing, named while it is still writing it. The
   * document's own save record writes the same relationship once a revision
   * exists; this is the run saying so first, so a step that is generating
   * something reads as connected to it rather than as an isolated node.
   */
  producesRefs?: readonly PipelineInputReference[];
  /**
   * The process this run/step is executing in — the devbox session, as the
   * runtime that opened it names it. It is what turns a running step into a
   * leased operation: a status word cannot expire, and a lease must.
   */
  workspace?: { resourceId: string; safeLabel?: string };
}

export interface PipelineRunSourceRecord extends PipelineObservationFields {
  kind: 'pipeline-run';
  run: PipelineRun | PipelineRunSnapshot;
}

/**
 * One executed node of a run, projected as its own node so a generated
 * pipeline reads as the steps a person was shown rather than a single opaque
 * run. It shares the run's identity and attribution — a step is never a
 * separate run — and is addressed as `runId#stepId`.
 */
export interface PipelineStepSourceRecord extends PipelineObservationFields {
  kind: 'pipeline-step';
  /** The parent run. Its id, pipeline id and attribution govern the step. */
  run: PipelineRun | PipelineRunSnapshot;
  step: {
    /** The definition node id the executor reports, for example `agent:s1`. */
    stepId: string;
    /** The step's own status, not the run's. */
    status: string;
  };
}

/** Included so definition records fail closed instead of masquerading as runs. */
export interface PipelineDefinitionSourceRecord {
  kind: 'pipeline-definition';
  definitionId: string;
  status?: string;
}

export type PipelineSourceRecord =
  | PipelineRunSourceRecord
  | PipelineStepSourceRecord
  | PipelineDefinitionSourceRecord;
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
    inputs?: PipelineInputReference[];
    produces?: PipelineInputReference[];
    workspace?: { resourceId: string; safeLabel?: string };
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
  | 'invalid-artifact-reference'
  | 'missing-step-id'
  | 'missing-step-status'
  | 'unsupported-step-status'
  | 'invalid-input-reference'
  | 'invalid-workspace';

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

/**
 * A step's own vocabulary, which is not the run's. A step the executor has not
 * reached yet is `waiting` — the one honest reading of "planned, not started".
 * It is deliberately never `started`, because a plan is not a running process.
 */
function lifecycleForStepStatus(raw: string): PipelineLifecycle | undefined {
  switch (raw.trim().toLowerCase()) {
    case 'pending':
    case 'queued':
    case 'waiting':
    case 'paused':
      return 'waiting';
    case 'running':
    case 'started':
    case 'resuming':
      return 'started';
    case 'completed':
    case 'succeeded':
      return 'completed';
    case 'failed':
    case 'error':
      return 'failed';
    case 'skipped':
    case 'cancelled':
    case 'canceled':
      return 'cancelled';
    default:
      return undefined;
  }
}

function inputsFrom(
  refs: readonly PipelineInputReference[] | undefined,
): PipelineInputReference[] | undefined {
  if (!refs || refs.length === 0) return [];
  if (refs.length > MAX_INPUT_REFERENCES) return undefined;
  const seen = new Map<string, PipelineInputReference>();
  for (const ref of refs) {
    if (!INPUT_SOURCES.has(ref?.source) || !isNonEmptyString(ref?.resourceId)) return undefined;
    seen.set(`${ref.source}:${ref.resourceId}`, { source: ref.source, resourceId: ref.resourceId });
  }
  return [...seen.values()];
}

/**
 * A workspace the caller declared but cannot name is refused rather than
 * dropped: silently projecting a run with no workspace is how a step ends up
 * looking idle while it is in fact working.
 */
function workspaceFrom(
  value: { resourceId: string; safeLabel?: string } | undefined,
): { ok: false } | { ok: true; workspace?: { resourceId: string; safeLabel?: string } } {
  if (value === undefined) return { ok: true };
  if (!isNonEmptyString(value.resourceId)) return { ok: false };
  const label = isNonEmptyString(value.safeLabel) ? value.safeLabel.trim() : undefined;
  return { ok: true, workspace: { resourceId: value.resourceId, ...(label ? { safeLabel: label } : {}) } };
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
  if (record.kind === 'pipeline-step') {
    if (!isNonEmptyString(record.step?.stepId)) return unsupported('missing-step-id');
    if (record.step.stepId.includes(PIPELINE_STEP_SEPARATOR)) return unsupported('missing-step-id');
    if (!isNonEmptyString(record.step?.status)) return unsupported('missing-step-status');
  }
  if (!isNonEmptyString(record.attributedSubject)) return unsupported('missing-run-attribution');
  if (isSyntheticTrigger(record.attributedSubject)) return unsupported('synthetic-trigger-is-not-human');
  if (record.attributedSubject !== context.actor.sourceSubject) {
    return unsupported('actor-does-not-match-run-trigger');
  }
  if (!isIsoTimestamp(record.occurredAt)) return unsupported('invalid-timestamp');

  const lifecycle = record.kind === 'pipeline-step'
    ? lifecycleForStepStatus(record.step.status)
    : lifecycleForStatus(run.status);
  if (!lifecycle) return unsupported(record.kind === 'pipeline-step' ? 'unsupported-step-status' : 'unsupported-run-status');

  // replayAttempts is a monotonic zero-based count in the runtime snapshot.
  // The work-graph contract exposes the human-readable one-based attempt.
  const replayAttempts = replayAttemptsFrom(run);
  if (replayAttempts === undefined) return unsupported('invalid-replay-attempts');
  const attempt = replayAttempts + 1;

  const artifactIds = artifactIdsFrom(record.artifactRefs, run.pipelineId, run.runId);
  if (artifactIds === undefined) return unsupported('invalid-artifact-reference');

  const inputs = inputsFrom(record.inputRefs);
  if (inputs === undefined) return unsupported('invalid-input-reference');
  const produces = inputsFrom(record.producesRefs);
  if (produces === undefined) return unsupported('invalid-input-reference');
  const workspace = workspaceFrom(record.workspace);
  if (!workspace.ok) return unsupported('invalid-workspace');

  // A step is addressed within its run, so the run's own node and each of its
  // steps get distinct, stable identities without a second source.
  const resourceId = record.kind === 'pipeline-step'
    ? pipelineStepResourceId(run.runId, record.step.stepId)
    : run.runId;

  const transitionKey = `${attempt}:${lifecycle}:${record.occurredAt}`;
  const sourceSequence = isNonEmptyString(record.sourceSequence)
    ? record.sourceSequence
    : transitionKey;
  const eventId = `pipeline:run:${resourceId}:${transitionKey}`;

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
        resourceId: `run:${resourceId}`,
        resourceVersion: sourceSequence,
      },
      payload: {
        kind: 'pipeline',
        lifecycle,
        pipelineId: run.pipelineId,
        runId: resourceId,
        attempt,
        ...(artifactIds.length > 0 ? { artifactIds } : {}),
        ...(isNonEmptyString(record.safeLabel) ? { safeLabel: record.safeLabel } : {}),
        ...(inputs.length > 0 ? { inputs } : {}),
        ...(produces.length > 0 ? { produces } : {}),
        ...(workspace.workspace ? { workspace: workspace.workspace } : {}),
      },
    },
  };
}
