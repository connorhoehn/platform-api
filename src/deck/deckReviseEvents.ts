// Live progress for `POST /api/deck/revise`, so a second tab, the workbench
// and the filmstrip can show "Generating…" on the slide being edited while the
// request is in flight — and clear it when it ends.
//
// TRANSPORT: the existing pipeline relay, unchanged. The events are published
// straight onto the Redis channel the gateway already pattern-subscribes
// (`dcore:topic:pipeline.events.*`, see `pipeline/event-redis-relay.ts` and
// the gateway's `src/pipeline-relay.ts`), as BusEvent-shaped JSON. The
// gateway's PipelineBridge maps `pipeline:deck:revise:started` to the wire
// event `pipeline.deck.revise.started` and fans it out to
// `pipeline:run:<payload.runId>` and `pipeline:all`. `runId` is therefore the
// DOCUMENT's revise channel key, `deck-revise:<documentId>` — not a pipeline
// run — so a client subscribes once per open document
// (`{ service:'pipeline', action:'subscribe', channel:'pipeline:run:deck-revise:<documentId>' }`)
// and sees every revise on it, whoever started it.
//
// They are NOT put on the platform's own EventBus: its subscribers (run
// persistence, the durable outbox) would take a `pipeline:` event for a run.
// Only the Redis relay path carries them; a deployment with MQ and no Redis
// relay gets no live revise progress (the request/response still works).
//
// CONTENT: ids, the phase and the outcome only — never the instruction, the
// summary or any slide text. The pipeline channels have no per-document
// authorization at the gateway, so nothing here may say what the deck says.
//
// Failure policy, as the relay's: publishing never fails a revise.

import { randomUUID } from 'crypto';
import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';
import { channelForType } from '../pipeline/event-redis-relay';
import type { DeckReviseTarget } from './DeckSpec';
import type { DeckRevisePhase } from './deckRevise';

export type DeckReviseEventKind = 'started' | 'phase' | 'completed' | 'failed';

/** BusEvent `type` per kind; the wire `eventType` is this with ':' → '.'. */
export const DECK_REVISE_EVENT_TYPES: Record<DeckReviseEventKind, string> = {
  started: 'pipeline:deck:revise:started',
  phase: 'pipeline:deck:revise:phase',
  completed: 'pipeline:deck:revise:completed',
  failed: 'pipeline:deck:revise:failed',
};

/** The key a document's revise events ride under (`payload.runId`). */
export function deckReviseChannelKey(documentId: string): string {
  return `deck-revise:${documentId}`;
}

export interface DeckReviseEventPayload {
  /** `deck-revise:<documentId>` — routes the event to the document's channel. */
  runId: string;
  documentId: string;
  requestId: string;
  /** Who asked. */
  userId: string;
  scope: 'slide' | 'deck';
  slideId?: string;
  target?: DeckReviseTarget;
  /** On `phase` (and `started`, as 'started'): where the revise is. */
  phase: 'started' | DeckRevisePhase | 'completed' | 'failed';
  /** On `completed`: the slides the edit changed. */
  changedSlideIds?: string[];
  /** On `failed`: the HTTP status the request answered with (400/502/503/500). */
  status?: number;
  /** On `failed`: the short reason, as the request's `reason` — no deck text. */
  reason?: string;
  occurredAt: string;
}

export interface DeckReviseBusEvent {
  id: string;
  type: string;
  payload: DeckReviseEventPayload;
  timestamp: number;
  version: number;
  sourceNodeId: string;
}

/** Where a revise's events go. Injected in tests; Redis by default. */
export type DeckRevisePublish = (channel: string, message: string) => Promise<void>;

const redisPublish: DeckRevisePublish = async (channel, message) => {
  const client = await getRedisClient();
  if (!client) return;
  await client.publish(channel, message);
};

const NODE_ID = `platform-api:${process.env.HOSTNAME ?? 'local'}`;

export interface DeckReviseEmitter {
  started(): void;
  phase(phase: DeckRevisePhase): void;
  completed(changedSlideIds: string[]): void;
  failed(status: number, reason: string): void;
}

export interface DeckReviseEmitterInit {
  documentId: string;
  requestId: string;
  userId: string;
  slideId?: string;
  target?: DeckReviseTarget;
  publish?: DeckRevisePublish;
  now?: () => number;
}

/**
 * One revise's events. `version` counts up per request so a client can order
 * them; a terminal event is sent at most once and nothing follows it.
 */
export function createDeckReviseEmitter(init: DeckReviseEmitterInit): DeckReviseEmitter {
  const publish = init.publish ?? redisPublish;
  const now = init.now ?? Date.now;
  let version = 0;
  let ended = false;

  const send = (kind: DeckReviseEventKind, extra: Partial<DeckReviseEventPayload> & { phase: DeckReviseEventPayload['phase'] }): void => {
    if (ended) return;
    if (kind === 'completed' || kind === 'failed') ended = true;
    const at = now();
    const type = DECK_REVISE_EVENT_TYPES[kind];
    const event: DeckReviseBusEvent = {
      id: randomUUID(),
      type,
      payload: {
        runId: deckReviseChannelKey(init.documentId),
        documentId: init.documentId,
        requestId: init.requestId,
        userId: init.userId,
        scope: init.slideId ? 'slide' : 'deck',
        ...(init.slideId ? { slideId: init.slideId } : {}),
        ...(init.target ? { target: init.target } : {}),
        ...extra,
        occurredAt: new Date(at).toISOString(),
      },
      timestamp: at,
      version: ++version,
      sourceNodeId: NODE_ID,
    };
    publish(channelForType(type), JSON.stringify(event)).catch((err: unknown) => {
      log.warn({ err: err instanceof Error ? err.message : String(err), type, documentId: init.documentId }, 'deck revise event not published');
    });
  };

  return {
    started: () => send('started', { phase: 'started' }),
    phase: (phase) => send('phase', { phase }),
    completed: (changedSlideIds) => send('completed', { phase: 'completed', changedSlideIds }),
    failed: (status, reason) => send('failed', { phase: 'failed', status, reason: reason.slice(0, 300) }),
  };
}

/** Does nothing: a revise with no documentId has no channel to report on. */
export const NOOP_DECK_REVISE_EMITTER: DeckReviseEmitter = {
  started: () => undefined, phase: () => undefined, completed: () => undefined, failed: () => undefined,
};
