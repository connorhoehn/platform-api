import type { EventEnvelope } from '@connorhoehn/event-catalog';
import type { AuthenticatedWorkEvent, WorkProjectionState } from '@connorhoehn/realtime-modules/work-graph';
import {
  ingestAuthenticatedWorkEvent,
  startWorkGraphConsumer,
  WorkGraphIngestionDeniedError,
  WORK_GRAPH_PROJECTION_QUEUE,
  workGraphAccountProducer,
} from '../work-graph-consumer';

jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

function event(overrides: Partial<AuthenticatedWorkEvent> = {}): AuthenticatedWorkEvent {
  return {
    schemaVersion: 1,
    eventId: 'event-1',
    idempotencyKey: 'event-1',
    source: 'cloud-compute',
    occurredAt: '2026-09-19T20:00:00.000Z',
    receivedAt: '2026-09-19T20:00:01.000Z',
    actor: { actorId: 'actor-1', organizationId: 'org-1', sourceSubject: 'subject-1' },
    producer: { serviceId: 'aws-agentcore', organizationId: 'org-1', credentialId: 'credential-1' },
    resource: { source: 'cloud-compute', resourceId: 'box-1' },
    payload: { kind: 'cloud-compute', lifecycle: 'started', boxId: 'box-1' },
    ...overrides,
  };
}

function envelope(payload = event(), producer = 'aws-agentcore'): EventEnvelope<unknown> {
  return {
    eventName: `work-graph.${payload.source}.lifecycle.v1`,
    payload,
    metadata: { id: 'envelope-1', producer, timestamp: Date.now() },
  };
}

function harness() {
  let handler!: (value: EventEnvelope<unknown>) => Promise<void>;
  const cancel = jest.fn();
  const transport = {
    subscribe: jest.fn((_queue, next) => {
      handler = next;
      return cancel;
    }),
  };
  const repository = {
    loadProjection: jest.fn<Promise<WorkProjectionState | null>, [string, string, string]>()
      .mockResolvedValue(null),
    commitEvent: jest.fn().mockResolvedValue({}),
  };
  const accountResolver = {
    resolve: jest.fn().mockResolvedValue({ organizationId: 'org-1', actorId: 'actor-1' }),
  };
  const consumer = startWorkGraphConsumer({ transport, repository, accountResolver });
  return { transport, repository, accountResolver, consumer, cancel, run: (value: EventEnvelope<unknown>) => handler(value) };
}

describe('work-graph durable consumer', () => {
  it('drains the declared shared queue and commits only after the server-owned account join', async () => {
    const h = harness();
    expect(h.transport.subscribe).toHaveBeenCalledWith(
      WORK_GRAPH_PROJECTION_QUEUE,
      expect.any(Function),
      { pollingInterval: 1_000, batchSize: 10, visibilityTimeout: 30 },
    );
    await h.run(envelope());
    expect(h.accountResolver.resolve).toHaveBeenCalledWith(
      'cloud-compute', 'aws-agentcore', 'org-1', 'subject-1',
    );
    expect(h.repository.commitEvent).toHaveBeenCalledWith(expect.objectContaining({ eventId: 'event-1' }), 0, null);
    await h.consumer.stop();
    expect(h.cancel).toHaveBeenCalledTimes(1);
  });

  it('rejects an authenticated producer attempting to claim another actor', async () => {
    const h = harness();
    h.accountResolver.resolve.mockResolvedValue({ organizationId: 'org-1', actorId: 'actor-2' });
    await expect(h.run(envelope())).rejects.toEqual(
      expect.objectContaining<Partial<WorkGraphIngestionDeniedError>>({ reason: 'source-account-claim-mismatch' }),
    );
    expect(h.repository.commitEvent).not.toHaveBeenCalled();
  });

  it('rejects event-name, source, and envelope-producer mismatches before lookup', async () => {
    const h = harness();
    await expect(h.run(envelope(event(), 'websocket-gateway'))).rejects.toBeInstanceOf(WorkGraphIngestionDeniedError);
    expect(h.accountResolver.resolve).not.toHaveBeenCalled();
  });

  it('commits one event at a time per partition from this process (NFR #202)', async () => {
    const h = harness();
    let inFlight = 0; let peak = 0;
    h.repository.commitEvent.mockImplementation(async () => {
      inFlight += 1; peak = Math.max(peak, inFlight);
      await new Promise((r) => setTimeout(r, 5));
      inFlight -= 1;
      return {};
    });
    await Promise.all([1, 2, 3, 4].map((n) => h.run(envelope(event({ eventId: `event-${n}`, idempotencyKey: `event-${n}` })))));
    expect(h.repository.commitEvent).toHaveBeenCalledTimes(4);
    expect(peak).toBe(1);
  });

  it('reloads the projection and retries conditional races without weakening identity checks', async () => {
    const h = harness();
    h.repository.loadProjection
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ revision: 1 } as WorkProjectionState);
    h.repository.commitEvent
      .mockRejectedValueOnce(Object.assign(new Error('race'), { name: 'WorkGraphConcurrencyError' }))
      .mockResolvedValueOnce({});
    await h.run(envelope());
    expect(h.repository.commitEvent).toHaveBeenNthCalledWith(1, expect.anything(), 0, null);
    expect(h.repository.commitEvent).toHaveBeenNthCalledWith(2, expect.anything(), 1, { revision: 1 });
  });
});

describe('work-graph authenticated HTTP ingestion', () => {
  const directHarness = () => ({
    repository: {
      loadProjection: jest.fn().mockResolvedValue(null),
      commitEvent: jest.fn().mockResolvedValue({}),
    },
    accountResolver: {
      resolve: jest.fn().mockResolvedValue({ organizationId: 'org-1', actorId: 'actor-1' }),
    },
  });

  it('commits only after the authenticated service and durable account mapping both match', async () => {
    const h = directHarness();
    await expect(ingestAuthenticatedWorkEvent({
      payload: event(),
      authenticatedServiceId: 'aws-agentcore',
      ...h,
    })).resolves.toEqual(expect.objectContaining({ eventId: 'event-1' }));
    expect(h.accountResolver.resolve).toHaveBeenCalledWith(
      'cloud-compute', 'aws-agentcore', 'org-1', 'subject-1',
    );
    expect(h.repository.commitEvent).toHaveBeenCalledWith(expect.anything(), 0, null);
  });

  it('rejects a correctly signed service presenting a source owned by another producer', async () => {
    const h = directHarness();
    await expect(ingestAuthenticatedWorkEvent({
      payload: event(),
      authenticatedServiceId: 'websocket-gateway',
      ...h,
    })).rejects.toEqual(expect.objectContaining({ reason: 'producer-or-source-mismatch' }));
    expect(h.accountResolver.resolve).not.toHaveBeenCalled();
  });

  it('rejects arbitrary claimed actor and organization values despite service authentication', async () => {
    const h = directHarness();
    h.accountResolver.resolve.mockResolvedValue({ organizationId: 'org-1', actorId: 'actor-2' });
    await expect(ingestAuthenticatedWorkEvent({
      payload: event(),
      authenticatedServiceId: 'aws-agentcore',
      ...h,
    })).rejects.toEqual(expect.objectContaining({ reason: 'source-account-claim-mismatch' }));
    expect(h.repository.commitEvent).not.toHaveBeenCalled();
  });
});

describe('a denied event says which one it was', () => {
  const envelope = (payload: unknown) => ({
    eventName: 'work-graph.pipeline.lifecycle.v1',
    metadata: { producer: 'platform-api' },
    payload,
  });

  it('names the source, the shape and the resource, and never the content', async () => {
    const repository = { loadProjection: jest.fn(), commitEvent: jest.fn() };
    const accountResolver = { resolve: jest.fn() };
    let consume!: (input: unknown) => Promise<void>;
    startWorkGraphConsumer({
      transport: { subscribe: (_queue: string, handler: (input: unknown) => Promise<void>) => { consume = handler; return () => undefined; } } as never,
      repository: repository as never,
      accountResolver: accountResolver as never,
    });
    const denied = await consume(envelope({
      schemaVersion: 1, eventId: 'e1', idempotencyKey: 'e1', source: 'pipeline', sourceSequence: '1',
      occurredAt: '2026-09-21T18:00:00.000Z', receivedAt: '2026-09-21T18:00:00.000Z',
      actor: { actorId: 'a', organizationId: 'o', sourceSubject: 's' },
      producer: { serviceId: 'platform-api', organizationId: 'o', credentialId: 'c' },
      resource: { source: 'pipeline', resourceId: 'run:run-1#agent:s1', resourceVersion: '1' },
      // `#` is outside the contract's id grammar: this is exactly the event
      // that published, drained and was then refused with no way to find it.
      payload: { kind: 'pipeline', lifecycle: 'started', pipelineId: 'p', runId: 'run-1#agent:s1', attempt: 1, safeLabel: 'Draft' },
    })).catch((error: Error) => error);
    expect((denied as Error).message).toContain('invalid-payload');
    expect((denied as Error).message).toContain('source=pipeline');
    expect((denied as Error).message).toContain('kind=pipeline');
    expect((denied as Error).message).toContain('run:run-1#agent:s1');
    expect((denied as Error).message).toContain('fields=attempt|kind|lifecycle|pipelineId|runId|safeLabel');
    expect((denied as Error).message).not.toContain('Draft');
  });
});

describe('a source with two declared writers', () => {
  // Each service stamps its OWN name on the envelope it publishes, so the
  // envelope and the payload always name the same one.
  const documentEvent = (serviceId: string, documentId = 'deck-1') => ({
    eventName: 'work-graph.document.lifecycle.v1',
    metadata: { producer: serviceId },
    payload: {
      schemaVersion: 1, eventId: `e-${documentId}`, idempotencyKey: `e-${documentId}`,
      source: 'document', sourceSequence: '1',
      occurredAt: '2026-09-22T04:00:00.000Z', receivedAt: '2026-09-22T04:00:01.000Z',
      actor: { actorId: 'dev-frank', organizationId: 'dev-org', sourceSubject: 'dev-frank' },
      producer: { serviceId, organizationId: 'dev-org', credentialId: 'c' },
      resource: { source: 'document', resourceId: documentId, resourceVersion: 'v1' },
      payload: { kind: 'document', lifecycle: 'revision-saved', documentId, revisionId: 'v1' },
    },
  });

  function consumer(ownership?: { ownsDocument: (s: string, d: string) => Promise<boolean> }) {
    const repository = {
      loadProjection: jest.fn(async () => ({ revision: 0 })),
      commitEvent: jest.fn(async () => ({ revision: 1 })),
    };
    const accountResolver = {
      resolve: jest.fn(async () => ({ organizationId: 'dev-org', actorId: 'dev-frank' })),
    };

    let consume!: (input: unknown) => Promise<void>;
    startWorkGraphConsumer({
      transport: { subscribe: (_q: string, handler: (input: unknown) => Promise<void>) => { consume = handler; return () => undefined; } } as never,
      repository: repository as never,
      accountResolver: accountResolver as never,
      ...(ownership ? { ownership } : {}),
    });
    return { consume: (e: unknown) => consume(e), repository };
  }

  it('accepts the gateway, as it always did', async () => {
    const { consume, repository } = consumer({ ownsDocument: async () => false });
    await consume(documentEvent('websocket-gateway'));
    expect(repository.commitEvent).toHaveBeenCalled();
  });

  it('accepts the platform for a presentation its own store wrote', async () => {
    const owned = jest.fn(async () => true);
    const { consume, repository } = consumer({ ownsDocument: owned });
    await consume(documentEvent('platform-api'));
    expect(owned).toHaveBeenCalledWith('platform-api', 'deck-1');
    expect(repository.commitEvent).toHaveBeenCalled();
  });

  it('refuses the platform for a document it does not own', async () => {
    // Admitting a second service says who may SPEAK, not what they may say:
    // a CRDT page stays the gateway's to describe.
    const { consume, repository } = consumer({ ownsDocument: async () => false });
    const denied = await consume(documentEvent('platform-api', 'page-9')).catch((e: Error) => e);
    expect((denied as Error).message).toContain('producer-does-not-own-document');
    expect(repository.commitEvent).not.toHaveBeenCalled();
  });

  it('refuses a service nobody declared', async () => {
    const { consume } = consumer({ ownsDocument: async () => true });
    const denied = await consume(documentEvent('some-other-service')).catch((e: Error) => e);
    expect((denied as Error).message).toContain('producer-or-source-mismatch');
  });

  it('keeps every other source to its single writer', async () => {
    const { consume } = consumer({ ownsDocument: async () => true });
    const denied = await consume({
      ...documentEvent('platform-api'),
      eventName: 'work-graph.conversation.lifecycle.v1',
      payload: {
        ...documentEvent('platform-api').payload,
        source: 'conversation',
        resource: { source: 'conversation', resourceId: 'chat:c:general', resourceVersion: '1' },
        payload: { kind: 'conversation', lifecycle: 'contributed', conversationId: 'chat:c:general', conversationKind: 'channel' },
      },
    }).catch((e: Error) => e);
    expect((denied as Error).message).toContain('producer-or-source-mismatch');
  });
});

describe('the account join a second writer looks up', () => {
  it('is the source\'s own, not the publishing service\'s', () => {
    // The join binds a person to a source; its producer records who
    // established it and is not part of the row's key. Both admitted writers
    // must therefore look up the same row.
    expect(workGraphAccountProducer('document')).toBe('websocket-gateway');
    expect(workGraphAccountProducer('pipeline')).toBe('platform-api');
    expect(workGraphAccountProducer('cloud-compute')).toBe('aws-agentcore');
  });

  it('is what the consumer resolves a platform-published deck against', async () => {
    const repository = {
      loadProjection: jest.fn(async () => ({ revision: 0 })),
      commitEvent: jest.fn(async () => ({ revision: 1 })),
    };
    const resolve = jest.fn(async () => ({ organizationId: 'dev-org', actorId: 'dev-frank' }));
    let consume!: (input: unknown) => Promise<void>;
    startWorkGraphConsumer({
      transport: { subscribe: (_q: string, h: (input: unknown) => Promise<void>) => { consume = h; return () => undefined; } } as never,
      repository: repository as never,
      accountResolver: { resolve } as never,
      ownership: { ownsDocument: async () => true },
    });
    await consume({
      eventName: 'work-graph.document.lifecycle.v1',
      metadata: { producer: 'platform-api' },
      payload: {
        schemaVersion: 1, eventId: 'e1', idempotencyKey: 'e1', source: 'document', sourceSequence: '1',
        occurredAt: '2026-09-22T04:36:48.000Z', receivedAt: '2026-09-22T04:36:49.000Z',
        actor: { actorId: 'dev-frank', organizationId: 'dev-org', sourceSubject: 'dev-frank' },
        producer: { serviceId: 'platform-api', organizationId: 'dev-org', credentialId: 'c' },
        resource: { source: 'document', resourceId: 'deck-1', resourceVersion: 'v1' },
        payload: { kind: 'document', lifecycle: 'revision-saved', documentId: 'deck-1', revisionId: 'v1' },
      },
    });
    expect(resolve).toHaveBeenCalledWith('document', 'websocket-gateway', 'dev-org', 'dev-frank');
    expect(repository.commitEvent).toHaveBeenCalled();
  });
});

describe('the envelope and the payload must name the same publisher', () => {
  const envelope = (metadataProducer: string, payloadProducer: string) => ({
    eventName: 'work-graph.document.lifecycle.v1',
    metadata: { producer: metadataProducer },
    payload: {
      schemaVersion: 1, eventId: 'e1', idempotencyKey: 'e1', source: 'document', sourceSequence: '1',
      occurredAt: '2026-09-22T04:46:36.000Z', receivedAt: '2026-09-22T04:46:37.000Z',
      actor: { actorId: 'dev-frank', organizationId: 'dev-org', sourceSubject: 'dev-frank' },
      producer: { serviceId: payloadProducer, organizationId: 'dev-org', credentialId: 'c' },
      resource: { source: 'document', resourceId: 'deck-1', resourceVersion: 'v1' },
      payload: {
        kind: 'document', lifecycle: 'revision-saved', documentId: 'deck-1',
        revisionId: 'v1', producedByRunId: 'run-1', safeLabel: 'Sprint review',
      },
    },
  });

  function consumer() {
    const repository = {
      loadProjection: jest.fn(async () => ({ revision: 0 })),
      commitEvent: jest.fn(async () => ({ revision: 1 })),
    };
    let consume!: (input: unknown) => Promise<void>;
    startWorkGraphConsumer({
      transport: { subscribe: (_q: string, h: (input: unknown) => Promise<void>) => { consume = h; return () => undefined; } } as never,
      repository: repository as never,
      accountResolver: { resolve: jest.fn(async () => ({ organizationId: 'dev-org', actorId: 'dev-frank' })) } as never,
      ownership: { ownsDocument: async () => true },
    });
    return { consume, repository };
  }

  it('accepts a deck the platform published, stamped platform-api on both', async () => {
    // Every service stamps its OWN name on the envelope. Comparing that
    // against the source's PRIMARY writer refused every platform-published
    // deck for telling the truth.
    const { consume, repository } = consumer();
    await consume(envelope('platform-api', 'platform-api'));
    expect(repository.commitEvent).toHaveBeenCalled();
  });

  it('accepts a page the gateway published, stamped websocket-gateway on both', async () => {
    const { consume, repository } = consumer();
    await consume(envelope('websocket-gateway', 'websocket-gateway'));
    expect(repository.commitEvent).toHaveBeenCalled();
  });

  it('refuses an envelope and a payload that disagree about who published', async () => {
    const { consume, repository } = consumer();
    const denied = await consume(envelope('websocket-gateway', 'platform-api')).catch((e: Error) => e);
    expect((denied as Error).message).toContain('producer-or-source-mismatch');
    expect(repository.commitEvent).not.toHaveBeenCalled();
  });

  it('refuses a service the source does not admit, however consistently stamped', async () => {
    const { consume } = consumer();
    const denied = await consume(envelope('aws-agentcore', 'aws-agentcore')).catch((e: Error) => e);
    expect((denied as Error).message).toContain('producer-or-source-mismatch');
  });
});
