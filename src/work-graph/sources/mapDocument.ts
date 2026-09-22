/**
 * Pure adapter from a presentation the platform's own deck store wrote to the
 * private Active now source event.
 *
 * The platform is a legitimate writer of the document source — a generated
 * deck is a document whose row it writes, and the gateway never sees one — so
 * this publishes the SAME declared event the gateway publishes for a CRDT
 * page. Node identity is the source and the document id, never who observed
 * it, which is what keeps one deck one node however it came to exist.
 *
 * Like the pipeline adapter, it reads nothing from request bodies: the caller
 * must have the document's own stored record in hand.
 */
import type { AuthenticatedPipelineContext } from './mapPipeline';

export interface DocumentRevisionSourceRecord {
  kind: 'document-revision';
  documentId: string;
  revisionId: string;
  /** Time the revision was written, from the store's own record. */
  occurredAt: string;
  /** The document's own title. Never a prompt, a path, or file content. */
  safeLabel?: string;
  /**
   * The run that produced this revision, when the revision IS a run's output.
   * It is the run's own declaration, so provenance is stated rather than
   * guessed from who happened to be running at the time.
   */
  producedByRunId?: string;
  sourceSequence?: string;
}

export type DocumentLifecycle = 'revision-saved' | 'deleted';

export interface DocumentWorkEvent {
  schemaVersion: 1;
  eventId: string;
  idempotencyKey: string;
  source: 'document';
  sourceSequence: string;
  occurredAt: string;
  receivedAt: string;
  actor: AuthenticatedPipelineContext['actor'];
  producer: AuthenticatedPipelineContext['producer'];
  resource: { source: 'document'; resourceId: string; resourceVersion: string };
  payload: {
    kind: 'document';
    lifecycle: DocumentLifecycle;
    documentId: string;
    revisionId: string;
    safeLabel?: string;
    producedByRunId?: string;
  };
}

export type DocumentMapUnsupportedReason =
  | 'missing-identity-context'
  | 'missing-document-id'
  | 'missing-revision-id'
  | 'invalid-timestamp'
  | 'unaddressable-identifier';

export type DocumentMapResult =
  | { supported: true; event: DocumentWorkEvent }
  | { supported: false; unsupported: true; partial: true; reason: DocumentMapUnsupportedReason };

/** The work-graph contract's own id grammar. Outside it, nothing is addressable. */
const ADDRESSABLE = /^[A-Za-z0-9][A-Za-z0-9._:-]*$/;

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.trim().length > 0;
}

function isIsoTimestamp(value: unknown): value is string {
  return isNonEmptyString(value) && Number.isFinite(Date.parse(value));
}

function unsupported(reason: DocumentMapUnsupportedReason): DocumentMapResult {
  return { supported: false, unsupported: true, partial: true, reason };
}

export function mapDocumentRevision(
  record: DocumentRevisionSourceRecord,
  context: AuthenticatedPipelineContext,
): DocumentMapResult {
  const { actor, producer } = context;
  const identified = [
    actor.actorId, actor.organizationId, actor.sourceSubject,
    producer.serviceId, producer.organizationId, producer.credentialId,
  ].every(isNonEmptyString)
    && actor.organizationId === producer.organizationId
    && isIsoTimestamp(context.receivedAt);
  if (!identified) return unsupported('missing-identity-context');

  if (!isNonEmptyString(record.documentId)) return unsupported('missing-document-id');
  if (!isNonEmptyString(record.revisionId)) return unsupported('missing-revision-id');
  if (!isIsoTimestamp(record.occurredAt)) return unsupported('invalid-timestamp');
  // An id the contract cannot address publishes, drains, and is then refused
  // at the consumer with nothing to say which event it was.
  if (!ADDRESSABLE.test(record.documentId) || !ADDRESSABLE.test(record.revisionId)) {
    return unsupported('unaddressable-identifier');
  }
  if (record.producedByRunId !== undefined
    && (!isNonEmptyString(record.producedByRunId) || !ADDRESSABLE.test(record.producedByRunId))) {
    return unsupported('unaddressable-identifier');
  }

  const eventId = `document:revision:${record.documentId}:${record.revisionId}`;
  const sourceSequence = isNonEmptyString(record.sourceSequence)
    ? record.sourceSequence
    : record.occurredAt;
  const label = isNonEmptyString(record.safeLabel) ? record.safeLabel.trim() : undefined;

  return {
    supported: true,
    event: {
      schemaVersion: 1,
      eventId,
      idempotencyKey: eventId,
      source: 'document',
      sourceSequence,
      occurredAt: record.occurredAt,
      receivedAt: context.receivedAt,
      actor,
      producer,
      resource: { source: 'document', resourceId: record.documentId, resourceVersion: record.revisionId },
      payload: {
        kind: 'document',
        lifecycle: 'revision-saved',
        documentId: record.documentId,
        revisionId: record.revisionId,
        ...(label ? { safeLabel: label } : {}),
        ...(record.producedByRunId ? { producedByRunId: record.producedByRunId } : {}),
      },
    },
  };
}
