import type { EventEnvelope } from '@connorhoehn/event-catalog';
import type { AuthenticatedWorkEvent, WorkProjectionState } from '@connorhoehn/realtime-modules/work-graph';
import {
  startWorkGraphConsumer,
  WorkGraphIngestionDeniedError,
  WORK_GRAPH_PROJECTION_QUEUE,
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
    expect(h.repository.commitEvent).toHaveBeenCalledWith(expect.objectContaining({ eventId: 'event-1' }), 0);
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

  it('reloads the projection and retries conditional races without weakening identity checks', async () => {
    const h = harness();
    h.repository.loadProjection
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce({ revision: 1 } as WorkProjectionState);
    h.repository.commitEvent
      .mockRejectedValueOnce(Object.assign(new Error('race'), { name: 'WorkGraphConcurrencyError' }))
      .mockResolvedValueOnce({});
    await h.run(envelope());
    expect(h.repository.commitEvent).toHaveBeenNthCalledWith(1, expect.anything(), 0);
    expect(h.repository.commitEvent).toHaveBeenNthCalledWith(2, expect.anything(), 1);
  });
});
