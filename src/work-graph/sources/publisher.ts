/**
 * Authoritative producer boundary for platform-owned Active now sources.
 *
 * Mappers intentionally accept an authenticated context, but callers must
 * not manufacture that context from an HTTP body or a webhook field.  This
 * adapter resolves the source subject through the durable source-account
 * join immediately before publishing.  The event's actor and organization
 * therefore remain a checked copy of server-owned state; the consumer checks
 * the same join again before projection.
 */
import type { AuthenticatedWorkEvent, WorkSourceKind } from '@connorhoehn/realtime-modules/work-graph';
import { mapMeeting, type MeetingSourceRecord } from './mapMeeting';
import { mapPipeline, type PipelineSourceRecord } from './mapPipeline';
import { mapDocumentRevision, type DocumentRevisionSourceRecord } from './mapDocument';
import { workGraphAccountProducer } from '../../services/work-graph-consumer';

export interface WorkGraphSourceAccountResolver {
  resolve(
    source: WorkSourceKind,
    producerServiceId: string,
    producerOrganizationId: string,
    sourceSubject: string,
  ): Promise<{ organizationId: string; actorId: string } | null>;
}

export interface WorkGraphDurablePublisher {
  publish(eventName: string, payload: AuthenticatedWorkEvent): Promise<void>;
}

export type WorkGraphProducerResult =
  | { published: true; event: AuthenticatedWorkEvent }
  | { published: false; reason: 'source-account-unmapped' | 'source-account-mismatch' | 'unsupported-source-record' };

interface PublisherOptions {
  accounts: WorkGraphSourceAccountResolver;
  publisher: WorkGraphDurablePublisher;
  /** Stable source credential identifier, configured by the service. */
  credentialId: string;
  now?: () => Date;
}

function eventName(source: 'pipeline' | 'meeting' | 'document'): string {
  return `work-graph.${source}.lifecycle.v1`;
}

/**
 * Produces platform-api's two lifecycle streams.  `organizationId` and
 * `sourceSubject` must be obtained from authenticated/persisted source state,
 * never accepted from a browser payload.
 */
export class PlatformWorkGraphSourcePublisher {
  private readonly now: () => Date;

  constructor(private readonly options: PublisherOptions) {
    this.now = options.now ?? (() => new Date());
  }

  async pipeline(
    record: PipelineSourceRecord,
    identity: { organizationId: string; sourceSubject: string },
  ): Promise<WorkGraphProducerResult> {
    const mapped = await this.options.accounts.resolve(
      'pipeline', 'platform-api', identity.organizationId, identity.sourceSubject,
    );
    if (!mapped) return { published: false, reason: 'source-account-unmapped' };
    if (mapped.organizationId !== identity.organizationId) {
      return { published: false, reason: 'source-account-mismatch' };
    }
    const result = mapPipeline(record, {
      actor: { actorId: mapped.actorId, organizationId: mapped.organizationId, sourceSubject: identity.sourceSubject },
      producer: { serviceId: 'platform-api', organizationId: mapped.organizationId, credentialId: this.options.credentialId },
      receivedAt: this.now().toISOString(),
    });
    if (!result.supported) return { published: false, reason: 'unsupported-source-record' };
    await this.options.publisher.publish(eventName('pipeline'), result.event as AuthenticatedWorkEvent);
    return { published: true, event: result.event as AuthenticatedWorkEvent };
  }

  /**
   * A presentation the platform's own deck store wrote. It publishes the SAME
   * declared event the gateway publishes for a CRDT page, so one document is
   * one node whichever service observed it — the identity an artifact card and
   * "Open presentation" both address.
   */
  async document(
    record: DocumentRevisionSourceRecord,
    identity: { organizationId: string; sourceSubject: string },
  ): Promise<WorkGraphProducerResult> {
    // The document source's account join is recorded under the gateway, which
    // writes it for every authenticated viewer. It binds the PERSON to the
    // source, not to whichever admitted service is publishing — so a second
    // writer looks up the same one rather than demanding a join of its own.
    const mapped = await this.options.accounts.resolve(
      'document', workGraphAccountProducer('document'), identity.organizationId, identity.sourceSubject,
    );
    if (!mapped) return { published: false, reason: 'source-account-unmapped' };
    if (mapped.organizationId !== identity.organizationId) {
      return { published: false, reason: 'source-account-mismatch' };
    }
    const result = mapDocumentRevision(record, {
      actor: { actorId: mapped.actorId, organizationId: mapped.organizationId, sourceSubject: identity.sourceSubject },
      producer: { serviceId: 'platform-api', organizationId: mapped.organizationId, credentialId: this.options.credentialId },
      receivedAt: this.now().toISOString(),
    });
    if (!result.supported) return { published: false, reason: 'unsupported-source-record' };
    await this.options.publisher.publish(eventName('document'), result.event as unknown as AuthenticatedWorkEvent);
    return { published: true, event: result.event as unknown as AuthenticatedWorkEvent };
  }

  async meeting(
    record: MeetingSourceRecord,
    identity: { organizationId: string; sourceSubject: string },
  ): Promise<WorkGraphProducerResult> {
    const mapped = await this.options.accounts.resolve(
      'meeting', 'platform-api', identity.organizationId, identity.sourceSubject,
    );
    if (!mapped) return { published: false, reason: 'source-account-unmapped' };
    if (mapped.organizationId !== identity.organizationId) {
      return { published: false, reason: 'source-account-mismatch' };
    }
    const result = mapMeeting(record, {
      actor: { actorId: mapped.actorId, organizationId: mapped.organizationId, sourceSubject: identity.sourceSubject },
      producer: { serviceId: 'platform-api', organizationId: mapped.organizationId, credentialId: this.options.credentialId },
      receivedAt: this.now().toISOString(),
    });
    if (!result.supported) return { published: false, reason: 'unsupported-source-record' };
    await this.options.publisher.publish(eventName('meeting'), result.event as AuthenticatedWorkEvent);
    return { published: true, event: result.event as AuthenticatedWorkEvent };
  }
}
