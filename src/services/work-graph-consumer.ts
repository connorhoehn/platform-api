import type { EventEnvelope } from '@connorhoehn/event-catalog';
import {
  isWithinWorkGraphEventLimit,
  validateAuthenticatedWorkEvent,
  type AuthenticatedWorkEvent,
  type WorkSourceKind,
} from '@connorhoehn/realtime-modules/work-graph';
import type { WorkGraphRepository, WorkProjectionState } from '../repositories/WorkGraphRepository';
import type { WorkSourceAccountRepository } from '../repositories/WorkSourceAccountRepository';
import log from '../lib/logger';

export const WORK_GRAPH_PROJECTION_QUEUE = 'work-graph-projection-v1';

const PRODUCER_BY_SOURCE: Record<WorkSourceKind, string> = {
  'cloud-compute': 'aws-agentcore',
  'local-compute': 'aws-agentcore',
  document: 'websocket-gateway',
  pipeline: 'platform-api',
  conversation: 'websocket-gateway',
  meeting: 'platform-api',
};

interface DurableTransport {
  subscribe(
    queueName: string,
    handler: (envelope: EventEnvelope<unknown>) => Promise<void>,
    options?: { pollingInterval?: number; batchSize?: number; visibilityTimeout?: number },
  ): () => void;
}

interface ProjectionRepository {
  loadProjection(organizationId: string, actorId: string, day: string): Promise<WorkProjectionState | null>;
  commitEvent(event: AuthenticatedWorkEvent, expectedRevision: number): Promise<WorkProjectionState>;
}

interface SourceAccountResolver {
  resolve(
    source: WorkSourceKind,
    producerServiceId: string,
    producerOrganizationId: string,
    sourceSubject: string,
  ): Promise<{ organizationId: string; actorId: string } | null>;
}

export class WorkGraphIngestionDeniedError extends Error {
  constructor(readonly reason: string) {
    super(`Work graph event denied: ${reason}`);
    this.name = 'WorkGraphIngestionDeniedError';
  }
}

function eventDay(timestamp: string): string {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function verifyEnvelope(envelope: EventEnvelope<unknown>): AuthenticatedWorkEvent {
  if (!isWithinWorkGraphEventLimit(envelope.payload)) {
    throw new WorkGraphIngestionDeniedError('payload-too-large');
  }
  const parsed = validateAuthenticatedWorkEvent(envelope.payload);
  if (!parsed.ok) throw new WorkGraphIngestionDeniedError('invalid-payload');
  const event = parsed.value;
  const expectedProducer = PRODUCER_BY_SOURCE[event.source];
  if (envelope.eventName !== `work-graph.${event.source}.lifecycle.v1`
    || envelope.metadata.producer !== expectedProducer
    || event.producer.serviceId !== expectedProducer
    || event.source !== event.payload.kind
    || event.source !== event.resource.source) {
    throw new WorkGraphIngestionDeniedError('producer-or-source-mismatch');
  }
  return event;
}

function verifyDirectEvent(payload: unknown, authenticatedServiceId: string): AuthenticatedWorkEvent {
  if (!isWithinWorkGraphEventLimit(payload)) {
    throw new WorkGraphIngestionDeniedError('payload-too-large');
  }
  const parsed = validateAuthenticatedWorkEvent(payload);
  if (!parsed.ok) throw new WorkGraphIngestionDeniedError('invalid-payload');
  const event = parsed.value;
  const expectedProducer = PRODUCER_BY_SOURCE[event.source];
  if (authenticatedServiceId !== expectedProducer
    || event.producer.serviceId !== authenticatedServiceId
    || event.source !== event.payload.kind
    || event.source !== event.resource.source) {
    throw new WorkGraphIngestionDeniedError('producer-or-source-mismatch');
  }
  return event;
}

async function commitWithRetry(
  repository: ProjectionRepository,
  event: AuthenticatedWorkEvent,
  maximumAttempts = 4,
): Promise<void> {
  const day = eventDay(event.occurredAt);
  for (let attempt = 1; attempt <= maximumAttempts; attempt += 1) {
    const current = await repository.loadProjection(event.actor.organizationId, event.actor.actorId, day);
    try {
      await repository.commitEvent(event, current?.revision ?? 0);
      return;
    } catch (error) {
      if ((error as { name?: string })?.name !== 'WorkGraphConcurrencyError' || attempt === maximumAttempts) throw error;
    }
  }
}

async function authorizeAndCommit(
  repository: ProjectionRepository,
  accountResolver: SourceAccountResolver,
  event: AuthenticatedWorkEvent,
): Promise<void> {
  const mapped = await accountResolver.resolve(
    event.source,
    event.producer.serviceId,
    event.producer.organizationId,
    event.actor.sourceSubject,
  );
  if (!mapped) throw new WorkGraphIngestionDeniedError('source-account-unmapped');
  if (mapped.organizationId !== event.actor.organizationId
    || mapped.organizationId !== event.producer.organizationId
    || mapped.actorId !== event.actor.actorId) {
    throw new WorkGraphIngestionDeniedError('source-account-claim-mismatch');
  }
  await commitWithRetry(repository, event);
}

/**
 * Synchronous authenticated HTTP ingestion used by non-platform producers.
 * A 2xx is returned only after the server-owned account join and atomic
 * projection/outbox commit have completed.
 */
export async function ingestAuthenticatedWorkEvent(options: {
  payload: unknown;
  authenticatedServiceId: string;
  repository: Pick<WorkGraphRepository, 'loadProjection' | 'commitEvent'>;
  accountResolver: Pick<WorkSourceAccountRepository, 'resolve'>;
}): Promise<AuthenticatedWorkEvent> {
  const event = verifyDirectEvent(options.payload, options.authenticatedServiceId);
  await authorizeAndCommit(
    options.repository as ProjectionRepository,
    options.accountResolver as SourceAccountResolver,
    event,
  );
  return event;
}

/**
 * Drains the queue declared by the work-graph contracts. It intentionally
 * uses EventCatalog's public durable transport: the installed high-level
 * subscribe() derives a queue from the event name and ignores a declaration's
 * shared queue override, which would leave this declared queue undrained.
 */
export function startWorkGraphConsumer(options: {
  transport: DurableTransport;
  repository: Pick<WorkGraphRepository, 'loadProjection' | 'commitEvent'>;
  accountResolver: Pick<WorkSourceAccountRepository, 'resolve'>;
  pollingInterval?: number;
}): { stop(): Promise<void> } {
  const cancel = options.transport.subscribe(
    WORK_GRAPH_PROJECTION_QUEUE,
    async (envelope) => {
      const event = verifyEnvelope(envelope);
      await authorizeAndCommit(
        options.repository as ProjectionRepository,
        options.accountResolver as SourceAccountResolver,
        event,
      );
    },
    { pollingInterval: options.pollingInterval ?? 1_000, batchSize: 10, visibilityTimeout: 30 },
  );

  log.info({ queue: WORK_GRAPH_PROJECTION_QUEUE }, 'work-graph durable consumer started');
  return {
    async stop() {
      cancel();
    },
  };
}
