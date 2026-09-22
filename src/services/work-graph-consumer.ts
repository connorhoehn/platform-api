import type { EventEnvelope } from '@connorhoehn/event-catalog';
import {
  isWithinWorkGraphEventLimit,
  validateAuthenticatedWorkEvent,
  workGraphProducersFor,
  type AuthenticatedWorkEvent,
  type WorkSourceKind,
} from '@connorhoehn/realtime-modules/work-graph';
import type { WorkGraphRepository, WorkProjectionState } from '../repositories/WorkGraphRepository';
import type { WorkSourceAccountRepository } from '../repositories/WorkSourceAccountRepository';
import log from '../lib/logger';

export const WORK_GRAPH_PROJECTION_QUEUE = 'work-graph-projection-v1';

/**
 * The service a source's events are DECLARED to come from, for the envelope's
 * own producer metadata. `workGraphProducersFor` is the authority on who may
 * publish: a document is written either by the gateway (a CRDT page) or by the
 * platform (a generated presentation, whose row the platform's deck store
 * writes), and both must reach the same node.
 */
const PRODUCER_BY_SOURCE: Record<WorkSourceKind, string> = {
  'cloud-compute': 'aws-agentcore',
  'local-compute': 'aws-agentcore',
  document: 'websocket-gateway',
  pipeline: 'platform-api',
  conversation: 'websocket-gateway',
  meeting: 'platform-api',
};

/**
 * The service a source's ACCOUNT JOIN is recorded under.
 *
 * The join binds a person to a source; the producer on it records which
 * service established the binding, and it is not part of the row's key. So a
 * source with two admitted writers has ONE join per person — whoever wrote it
 * first — and both writers must look it up the same way. Resolving by the
 * publishing service instead meant a deck was refused as
 * `source-account-unmapped` for every person whose document join the gateway
 * had already written, which is all of them.
 */
export function workGraphAccountProducer(source: WorkSourceKind): string {
  return PRODUCER_BY_SOURCE[source];
}

/**
 * Admitting a second service says who may SPEAK, never what they may say. A
 * publisher still has to own the thing it describes, checked against that
 * service's own store — platform-api writes presentations, and a CRDT page is
 * the gateway's to describe however the queue is addressed.
 */
export interface WorkGraphOwnershipCheck {
  ownsDocument(serviceId: string, documentId: string): Promise<boolean>;
}

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
  constructor(readonly reason: string, readonly detail?: string) {
    super(`Work graph event denied: ${reason}${detail ? ` (${detail})` : ''}`);
    this.name = 'WorkGraphIngestionDeniedError';
  }
}

/**
 * Says WHICH event was refused, without putting its content in a log.
 *
 * `invalid-payload` on its own is unactionable: every source publishes to the
 * same queue, the transport only reports the reason, and a producer watching
 * its own successful publishes has nothing to go on. Identity and shape are
 * enough to find it — a step whose id carried a character the contract's id
 * grammar does not allow was invisible for an entire evening behind this.
 */
function payloadShape(payload: unknown): string {
  if (!payload || typeof payload !== 'object') return `not-an-object:${typeof payload}`;
  const event = payload as Record<string, unknown>;
  const inner = (event.payload && typeof event.payload === 'object' ? event.payload : {}) as Record<string, unknown>;
  const id = (value: unknown): string => (typeof value === 'string' ? value : `<${typeof value}>`);
  return [
    `source=${id(event.source)}`,
    `kind=${id(inner.kind)}`,
    `lifecycle=${id(inner.lifecycle)}`,
    `resource=${id((event.resource as Record<string, unknown> | undefined)?.resourceId)}`,
    `fields=${Object.keys(inner).sort().join('|')}`,
  ].join(' ');
}

function eventDay(timestamp: string): string {
  return new Date(timestamp).toISOString().slice(0, 10);
}

function verifyEnvelope(envelope: EventEnvelope<unknown>): AuthenticatedWorkEvent {
  if (!isWithinWorkGraphEventLimit(envelope.payload)) {
    throw new WorkGraphIngestionDeniedError('payload-too-large');
  }
  const parsed = validateAuthenticatedWorkEvent(envelope.payload);
  if (!parsed.ok) throw new WorkGraphIngestionDeniedError('invalid-payload', payloadShape(envelope.payload));
  const event = parsed.value;
  const allowed = workGraphProducersFor(event.source);
  if (envelope.eventName !== `work-graph.${event.source}.lifecycle.v1`
    || envelope.metadata.producer !== PRODUCER_BY_SOURCE[event.source]
    || !allowed.includes(event.producer.serviceId)
    || event.source !== event.payload.kind
    || event.source !== event.resource.source) {
    throw new WorkGraphIngestionDeniedError('producer-or-source-mismatch', payloadShape(envelope.payload));
  }
  return event;
}

function verifyDirectEvent(payload: unknown, authenticatedServiceId: string): AuthenticatedWorkEvent {
  if (!isWithinWorkGraphEventLimit(payload)) {
    throw new WorkGraphIngestionDeniedError('payload-too-large');
  }
  const parsed = validateAuthenticatedWorkEvent(payload);
  if (!parsed.ok) throw new WorkGraphIngestionDeniedError('invalid-payload', payloadShape(payload));
  const event = parsed.value;
  if (!workGraphProducersFor(event.source).includes(authenticatedServiceId)
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
  ownership?: WorkGraphOwnershipCheck,
): Promise<void> {
  // A source with more than one writer needs the writer checked against the
  // thing it is describing, not just against the source. The platform may
  // describe a presentation its own deck store wrote; a CRDT page stays the
  // gateway's to describe.
  if (ownership && event.source === 'document'
    && event.producer.serviceId !== PRODUCER_BY_SOURCE.document) {
    const documentId = (event.payload as { documentId?: unknown }).documentId;
    if (typeof documentId !== 'string'
      || !await ownership.ownsDocument(event.producer.serviceId, documentId)) {
      throw new WorkGraphIngestionDeniedError('producer-does-not-own-document');
    }
  }
  const mapped = await accountResolver.resolve(
    event.source,
    workGraphAccountProducer(event.source),
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
  ownership?: WorkGraphOwnershipCheck;
}): Promise<AuthenticatedWorkEvent> {
  const event = verifyDirectEvent(options.payload, options.authenticatedServiceId);
  await authorizeAndCommit(
    options.repository as ProjectionRepository,
    options.accountResolver as SourceAccountResolver,
    event,
    options.ownership,
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
  /** Required for any source with more than one declared writer. */
  ownership?: WorkGraphOwnershipCheck;
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
        options.ownership,
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
