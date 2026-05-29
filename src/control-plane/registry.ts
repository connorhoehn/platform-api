// platform-api/src/control-plane/registry.ts
//
// ControlPlaneSubscriberRegistry owns a single ControlPlaneChannel +
// RedisPubSubManager pair and lets multiple handlers register against
// distinct topic patterns. Without this, every new CRD subscriber
// (AgenticPipeline, FileUploadEndpoint, future kinds) would instantiate
// its own ControlPlaneChannel + open its own Redis connection — burning
// N connections to publish-side state that's already cluster-wide.
//
// Lifecycle:
//   1. const reg = new ControlPlaneSubscriberRegistry({});
//   2. reg.register({ channel, handler, name });   ← repeat per subscriber
//   3. await reg.start();                          ← single Redis bootstrap
//   4. await reg.stop();                           ← single Redis teardown
//
// Non-fatal start(): if Redis is down at boot, the registry logs a warn
// and returns. Handlers can later be re-bootstrapped by calling start()
// again. Mirrors PipelineConfigSubscriber's existing start() semantics
// (the operator can re-push once Redis recovers).
//
// Handler contract:
//   - Each handler is async and is called with a ControlPlanePayload<T>.
//   - Return value is forwarded to ControlPlaneChannel for the ack.
//   - Throw to produce a 'rejected' ack — the channel layer handles
//     wrapping the error in the ReconcileAck envelope.

import { ControlPlaneChannel, RedisPubSubManager } from 'distributed-core';
import type { ControlPlanePayload, ReconcileAck } from 'distributed-core';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'control-plane-registry' });

export type SubscriberHandler<T = unknown> = (
  payload: ControlPlanePayload<T>,
) => Promise<ReconcileAck>;

export interface RegisteredSubscriber<T = unknown> {
  /** Channel pattern this subscriber listens on (e.g. 'feature.file-upload.*'). */
  channel: string;
  /** Display name for log lines. */
  name: string;
  /** Handler invoked per payload. May throw — the channel converts to 'rejected'. */
  handler: SubscriberHandler<T>;
}

export interface ControlPlaneSubscriberRegistryOptions {
  /** Stable node id for ack attribution. Defaults to `${hostname}-${pid}`. */
  nodeId?: string;
  /** Redis URL. Defaults to REDIS_URL env or redis://${REDIS_ENDPOINT}:${REDIS_PORT}. */
  redisUrl?: string;
  /** Pre-constructed ControlPlaneChannel for testing. When set, redisUrl/nodeId are ignored. */
  _controlPlane?: ControlPlaneChannel;
}

export class ControlPlaneSubscriberRegistry {
  private readonly nodeId: string;
  private readonly redisUrl: string;
  private readonly _injectedControlPlane?: ControlPlaneChannel;

  private readonly subscribers: Array<RegisteredSubscriber<unknown>> = [];
  private readonly unsubs: Array<() => Promise<void>> = [];

  private controlPlane: ControlPlaneChannel | null = null;
  private started = false;

  constructor(opts: ControlPlaneSubscriberRegistryOptions = {}) {
    this.nodeId = opts.nodeId
      ?? `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`;
    this.redisUrl = opts.redisUrl
      ?? process.env.REDIS_URL
      ?? `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
    this._injectedControlPlane = opts._controlPlane;
  }

  /**
   * Register a subscriber. Call before start(); registering after start()
   * is a no-op since the channel is already bootstrapped (we don't
   * dynamically add subscriptions to a live channel — the operator pushes
   * are infrequent enough that a restart is fine).
   */
  register<T>(subscriber: RegisteredSubscriber<T>): void {
    if (this.started) {
      log.warn(
        { subscriber: subscriber.name, channel: subscriber.channel },
        'register() called after start() — ignored (restart the registry to add subscriptions)',
      );
      return;
    }
    this.subscribers.push(subscriber as RegisteredSubscriber<unknown>);
  }

  /**
   * Connect to Redis, create ControlPlaneChannel, and subscribe every
   * registered handler. Non-fatal — a Redis hiccup at boot leaves the
   * registry idle until stop()+start() is retried.
   */
  async start(): Promise<void> {
    if (this.started) return;
    if (this.subscribers.length === 0) {
      log.info('no subscribers registered — nothing to start');
      this.started = true;
      return;
    }

    try {
      let cp: ControlPlaneChannel;
      if (this._injectedControlPlane) {
        cp = this._injectedControlPlane;
      } else {
        const pubSub = new RedisPubSubManager({
          localNodeId: this.nodeId,
          url: this.redisUrl,
          channelPrefix: '',
        });
        cp = new ControlPlaneChannel({
          pubSub,
          localNodeId: this.nodeId,
        });
      }

      await cp.start();
      this.controlPlane = cp;

      for (const sub of this.subscribers) {
        const { unsubscribe } = await cp.subscribe<unknown>(
          sub.channel,
          (payload) => sub.handler(payload),
        );
        this.unsubs.push(unsubscribe);
        log.info(
          { subscriber: sub.name, channel: sub.channel, nodeId: this.nodeId },
          'control-plane subscriber registered',
        );
      }

      this.started = true;
    } catch (err: unknown) {
      log.warn(
        {
          err: err instanceof Error ? err.message : String(err),
          subscriberCount: this.subscribers.length,
        },
        'control-plane registry failed to start (non-fatal — handlers idle until next start())',
      );
    }
  }

  /**
   * Unsubscribe every handler and stop the ControlPlaneChannel. Safe to
   * call even if start() failed.
   */
  async stop(): Promise<void> {
    for (const unsub of this.unsubs) {
      try { await unsub(); } catch { /* best-effort */ }
    }
    this.unsubs.length = 0;
    if (this.controlPlane) {
      try { await this.controlPlane.stop(); } catch { /* best-effort */ }
      this.controlPlane = null;
    }
    this.started = false;
    log.info({ subscriberCount: this.subscribers.length }, 'control-plane registry stopped');
  }
}
