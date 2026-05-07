// social-api/src/pipeline/ingest-steps/event-trigger.ts
//
// `ingest:event` trigger node. Subscribes to the ingest event bus and, for
// every event matching a pipeline's binding criteria, asks the supplied
// `runStarter` to create a pipeline run with the canonical
// `IngestTriggerPayload` shape.
//
// One trigger instance can host many bindings — the operator UI registers
// one binding per pipeline that uses the `ingest:event` trigger node. The
// trigger holds them in a flat list and broadcasts each event to every
// matching binding (multiple pipelines can fire on the same event).

import type {
  IngestEvent,
  IngestSource,
} from '../../ingest/types';
import type { IngestEventBus } from '../../ingest/engine';
import type {
  IngestTriggerBinding,
  IngestTriggerPayload,
} from './types';

/**
 * Internal binding shape — extends the public `IngestTriggerBinding` with
 * the pipeline id to start and an optional matching predicate the trigger
 * uses to decide whether the event maps to a pipeline. The matcher is a
 * narrow function so we don't have to know about the full DocumentType
 * lookup at registration time — Agent E / the integrator passes one in.
 */
export interface RegisteredIngestTrigger {
  pipelineId: string;
  binding: IngestTriggerBinding;
  /**
   * Optional async hook to look up the mapped DocType for an event whose
   * binding filters by `documentTypeId`. Tests pass a stub; production
   * wires the IngestItem repository.
   */
  resolveDocumentTypeId?(event: IngestEvent): Promise<string | null>;
}

/**
 * Surface the trigger uses to start a pipeline run. Mirrors the
 * `PipelineBridge.trigger` signature without depending on it directly so
 * tests don't drag in the bridge.
 */
export interface IRunStarter {
  trigger(args: {
    pipelineId: string;
    triggerPayload: IngestTriggerPayload;
    triggeredBy: { userId: string };
  }): Promise<{ runId: string }>;
}

const DEFAULT_EVENT_TYPES: ReadonlyArray<IngestEvent['type']> = ['item.materialized'];

export class IngestEventTrigger {
  private readonly registrations: RegisteredIngestTrigger[] = [];
  private readonly handler: (event: IngestEvent) => void;
  private subscribed = false;

  constructor(
    private readonly eventBus: IngestEventBus,
    private readonly runStarter: IRunStarter,
    private readonly opts: {
      onError?: (err: unknown, event: IngestEvent, pipelineId: string) => void;
    } = {},
  ) {
    this.handler = (event) => {
      // Fire-and-forget — the engine's eventBus emits synchronously; we don't
      // want to block the publisher on pipeline-run creation. Errors per
      // pipeline are reported via the optional `onError` hook so a single
      // misconfigured binding can't poison sibling pipelines.
      void this.dispatch(event);
    };
  }

  register(reg: RegisteredIngestTrigger): () => void {
    this.registrations.push(reg);
    if (!this.subscribed) {
      this.eventBus.on(this.handler);
      this.subscribed = true;
    }
    return () => {
      const idx = this.registrations.indexOf(reg);
      if (idx >= 0) this.registrations.splice(idx, 1);
      if (this.registrations.length === 0 && this.subscribed) {
        this.eventBus.off(this.handler);
        this.subscribed = false;
      }
    };
  }

  /** Returns the registered count — useful for tests / observability. */
  size(): number {
    return this.registrations.length;
  }

  private async dispatch(event: IngestEvent): Promise<void> {
    for (const reg of this.registrations) {
      let matches = false;
      try {
        matches = await this.matches(event, reg);
      } catch (err) {
        this.opts.onError?.(err, event, reg.pipelineId);
        continue;
      }
      if (!matches) continue;

      const payload = buildTriggerPayload(event);
      try {
        await this.runStarter.trigger({
          pipelineId: reg.pipelineId,
          triggerPayload: payload,
          triggeredBy: { userId: `ingest:${payload.ingestId ?? 'unknown'}` },
        });
      } catch (err) {
        this.opts.onError?.(err, event, reg.pipelineId);
      }
    }
  }

  private async matches(
    event: IngestEvent,
    reg: RegisteredIngestTrigger,
  ): Promise<boolean> {
    const eventTypes = reg.binding.eventTypes ?? DEFAULT_EVENT_TYPES;
    if (!eventTypes.includes(event.type)) return false;

    if (reg.binding.sourceIds?.length) {
      const sid = readSourceId(event);
      if (!sid || !reg.binding.sourceIds.includes(sid)) return false;
    }

    if (reg.binding.documentTypeId) {
      // Two ways to match a documentTypeId: the event itself carries one
      // (`item.mapped` does), or the caller provides a resolver that looks
      // it up by ingestId. If neither matches, the binding doesn't fire.
      const fromEvent = readDocumentTypeId(event);
      if (fromEvent) {
        if (fromEvent !== reg.binding.documentTypeId) return false;
      } else if (reg.resolveDocumentTypeId) {
        const resolved = await reg.resolveDocumentTypeId(event);
        if (resolved !== reg.binding.documentTypeId) return false;
      } else {
        return false;
      }
    }

    return true;
  }
}

/**
 * Build a canonical `IngestTriggerPayload` from an `IngestEvent`. Exposed so
 * tests + adapters can synthesize the same shape without going through the
 * trigger.
 */
export function buildTriggerPayload(event: IngestEvent): IngestTriggerPayload {
  return {
    triggerType: 'ingest:event',
    event,
    ingestId: readIngestId(event),
    documentId: readDocumentId(event),
    sourceId: readSourceId(event),
    documentTypeId: readDocumentTypeId(event),
  };
}

function readIngestId(event: IngestEvent): string | undefined {
  return 'ingestId' in event ? event.ingestId : undefined;
}

function readDocumentId(event: IngestEvent): string | undefined {
  return 'documentId' in event ? (event as { documentId?: string }).documentId : undefined;
}

function readSourceId(event: IngestEvent): string | undefined {
  return 'sourceId' in event ? (event as { sourceId?: string }).sourceId : undefined;
}

function readDocumentTypeId(event: IngestEvent): string | undefined {
  return 'typeId' in event ? (event as { typeId?: string }).typeId : undefined;
}

/**
 * Unused-export hook so TypeScript doesn't complain when only the type side
 * of `IngestSource` is referenced here. (It's load-bearing in the resolver
 * sibling — keeping the type-import path clean.)
 */
export type _IngestSourceRef = IngestSource;
