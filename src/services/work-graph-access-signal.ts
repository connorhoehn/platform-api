import { randomUUID } from 'crypto';
import { hostname } from 'os';

import { getRedisClient } from '../lib/redis-client';
import log from '../lib/logger';

/**
 * NFR #21 (realtime-examples): the content-free signal a gateway replica uses
 * to re-check the Active Now streams an access change can touch, instead of
 * asking `/authorize` for every stream every second.
 *
 * Published by whichever platform replica commits a sharing grant (create,
 * edit, pause, resume, stop) — the person whose grants changed — on the same
 * pub/sub channel family as the projection signal. Every gateway replica
 * receives it and acts only on the streams it holds. Lost signals cost
 * latency, never correctness: the gateway keeps a slow safety re-check and
 * schedules the grant-expiry re-check itself from the `expiresAt` that
 * `/authorize` reports, so a failed publish is logged and dropped.
 */
export const ACCESS_CHANGED_SIGNAL_TOPIC = 'work-graph.access.changed';

/** Same prefix as the projection signal: RedisPubSubManager runs with channelPrefix ''. */
const PUBSUB_CHANNEL_PREFIX = '';

export interface WorkGraphAccessChanged {
  organizationId: string;
  /** The person whose sharing grants changed (grant owner). */
  personId?: string;
  /** The viewer whose memberships changed. */
  viewerId?: string;
  reason: 'grant' | 'membership';
  grantId?: string;
  expiresAt?: string;
}

export type AccessChangedPublisher = (change: WorkGraphAccessChanged) => Promise<void>;

export async function publishAccessChangedSignal(change: WorkGraphAccessChanged): Promise<void> {
  const client = await getRedisClient();
  if (!client) return;
  const wire = {
    topic: ACCESS_CHANGED_SIGNAL_TOPIC,
    payload: change,
    metadata: {
      messageId: `pub-platform-api-${Date.now()}-${randomUUID()}`,
      publisherNodeId: `platform-api@${hostname()}`,
      timestamp: Date.now(),
      topic: ACCESS_CHANGED_SIGNAL_TOPIC,
    },
  };
  await client.publish(PUBSUB_CHANNEL_PREFIX + ACCESS_CHANGED_SIGNAL_TOPIC, JSON.stringify(wire));
}

interface GrantLike { id: string; organizationId: string; ownerId: string; expiresAt: string }

/**
 * Wraps a sharing store so every committed grant write publishes the signal
 * after the write returns — the replica that committed is the one that
 * publishes, whichever of them it is. The store's own methods are untouched;
 * a publish failure never fails the write.
 */
export function withAccessChangedSignal<T extends {
  putGrant(grant: any, expectedRevision: number | null): Promise<any>;
  putGrantPair?(current: any, history: any): Promise<any>;
}>(store: T, publish: AccessChangedPublisher = publishAccessChangedSignal): T {
  const signal = (grant: GrantLike) => publish({
    organizationId: grant.organizationId,
    personId: grant.ownerId,
    reason: 'grant',
    grantId: grant.id,
    expiresAt: grant.expiresAt,
  }).catch((error: unknown) => {
    log.warn({ err: error, grantId: grant.id }, 'work-graph access signal not published');
  });
  const putGrant: T['putGrant'] = async (grant, expectedRevision) => {
    const stored = await store.putGrant(grant, expectedRevision);
    void signal(stored);
    return stored;
  };
  const putGrantPair: T['putGrantPair'] = store.putGrantPair
    ? async (current, history) => {
      const stored = await store.putGrantPair!(current, history);
      void signal(stored.current);
      return stored;
    }
    : undefined;
  return new Proxy(store, {
    get(target, property, receiver) {
      if (property === 'putGrant') return putGrant;
      if (property === 'putGrantPair') return putGrantPair;
      const value = Reflect.get(target, property, receiver);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}
