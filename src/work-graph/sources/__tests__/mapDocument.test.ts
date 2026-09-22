import { mapDocumentRevision, type DocumentRevisionSourceRecord } from '../mapDocument';
import type { AuthenticatedPipelineContext } from '../mapPipeline';

const context: AuthenticatedPipelineContext = {
  actor: { actorId: 'dev-frank', organizationId: 'dev-org', sourceSubject: 'dev-frank' },
  producer: { serviceId: 'platform-api', organizationId: 'dev-org', credentialId: 'platform-api-runtime' },
  receivedAt: '2026-09-22T04:00:05.000Z',
};

const record = (over: Partial<DocumentRevisionSourceRecord> = {}): DocumentRevisionSourceRecord => ({
  kind: 'document-revision',
  documentId: 'deck-1',
  revisionId: 'v1',
  occurredAt: '2026-09-22T04:00:00.000Z',
  safeLabel: 'Sprint review',
  producedByRunId: 'run-1',
  ...over,
});

function eventOf(result: ReturnType<typeof mapDocumentRevision>) {
  expect(result.supported).toBe(true);
  if (!result.supported) throw new Error(result.reason);
  return result.event;
}

describe('mapDocumentRevision', () => {
  it('addresses the document by its own id, which is what makes it one node', () => {
    const event = eventOf(mapDocumentRevision(record(), context));
    expect(event.source).toBe('document');
    expect(event.resource).toEqual({ source: 'document', resourceId: 'deck-1', resourceVersion: 'v1' });
    expect(event.payload).toMatchObject({
      kind: 'document', lifecycle: 'revision-saved', documentId: 'deck-1', revisionId: 'v1',
      safeLabel: 'Sprint review', producedByRunId: 'run-1',
    });
  });

  it('says the platform produced it, not the gateway', () => {
    expect(eventOf(mapDocumentRevision(record(), context)).producer.serviceId).toBe('platform-api');
  });

  it('is deterministic per revision, so a redelivery is the same event', () => {
    expect(eventOf(mapDocumentRevision(record(), context)).eventId)
      .toBe(eventOf(mapDocumentRevision(record(), context)).eventId);
    expect(eventOf(mapDocumentRevision(record({ revisionId: 'v2' }), context)).eventId)
      .not.toBe(eventOf(mapDocumentRevision(record(), context)).eventId);
  });

  it('omits provenance a caller did not declare', () => {
    const event = eventOf(mapDocumentRevision(record({ producedByRunId: undefined }), context));
    expect(event.payload.producedByRunId).toBeUndefined();
  });

  it.each([
    ['missing-document-id', { documentId: '' }],
    ['missing-revision-id', { revisionId: '  ' }],
    ['invalid-timestamp', { occurredAt: 'whenever' }],
  ] as const)('refuses a record that is %s', (reason, over) => {
    expect(mapDocumentRevision(record(over), context)).toMatchObject({ supported: false, reason });
  });

  it.each(['deck 1', 'deck#1', 'deck/1'])('refuses an id the contract cannot address: %s', (documentId) => {
    // Such an id publishes, drains, and is then refused at the consumer with
    // nothing to say which event it was.
    expect(mapDocumentRevision(record({ documentId }), context))
      .toMatchObject({ reason: 'unaddressable-identifier' });
  });

  it('refuses an identity context that does not hang together', () => {
    expect(mapDocumentRevision(record(), {
      ...context, producer: { ...context.producer, organizationId: 'another-org' },
    })).toMatchObject({ reason: 'missing-identity-context' });
  });
});
