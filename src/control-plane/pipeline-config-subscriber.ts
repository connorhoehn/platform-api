// platform-api/src/control-plane/pipeline-config-subscriber.ts
//
// Subscribes to ControlPlaneChannel for AgenticPipeline config pushes from
// the operator. The operator reconciles AgenticPipeline CRDs (Phase 3) and
// pushes pipeline definitions to all platform-api replicas via control plane.
//
// Channel: 'feature.agentic-pipeline.*'
//   - Wildcard-style: one subscription per named pipeline, e.g.
//     'feature.agentic-pipeline.my-pipeline'.
//   - Or the operator can use a single well-known channel for all pipelines:
//     'feature.agentic-pipeline.definitions' (default).
//
// On receipt:
//   1. Writes the definition to DefinitionsRepository (DDB-backed).
//   2. Publishes a cache invalidation notice so sibling replicas refresh.
//   3. Returns ReconcileAck with status='applied' or 'rejected'.
//
// Non-fatal: if Redis is unavailable at start() time, the subscriber logs
// a warning and returns a no-op stop handle. The operator can re-push once
// Redis recovers.

import { ControlPlaneChannel, RedisPubSubManager } from 'distributed-core';
import type { ControlPlanePayload, ReconcileAck } from 'distributed-core';
import type { DefinitionsRepository, PipelineDefinition } from '../pipeline/definitions-repository';
import { publishInvalidate } from '../pipeline/definitions-invalidator';
import { withContext } from '../lib/logger';

const log = withContext({ service: 'pipeline-config-subscriber' });

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/**
 * Shape of the config payload pushed by the operator for an AgenticPipeline.
 * The operator derives this from the CRD spec and serialises it as-is.
 */
export interface AgenticPipelineConfig {
  /** Must match the DefinitionsRepository SK (pipelineId). */
  name: string;
  /** Full pipeline definition — forwarded verbatim to DefinitionsRepository. */
  definition: PipelineDefinition;
  /** Trigger bindings declared in the CRD. */
  triggers: Array<{ type: string; config: object }>;
  /** Optional approval-gate policy from the CRD. */
  approvalPolicy?: object;
  /** Optional audit destination override. */
  auditDestination?: string;
}

/** Operator-side user id that owns CRD-managed pipelines. */
const OPERATOR_USER_ID =
  process.env.CONTROL_PLANE_OPERATOR_USER_ID ?? 'system:operator';

/** Default control-plane channel for all agentic-pipeline pushes. */
const DEFAULT_CHANNEL = 'feature.agentic-pipeline.definitions';

// ---------------------------------------------------------------------------
// PipelineConfigSubscriber
// ---------------------------------------------------------------------------

export interface PipelineConfigSubscriberOptions {
  /**
   * ControlPlaneChannel channel to subscribe to.
   * Default: 'feature.agentic-pipeline.definitions'
   */
  channel?: string;
  /**
   * Stable node id for ack attribution. Defaults to
   * `${hostname}-${pid}`.
   */
  nodeId?: string;
  /** Redis URL — defaults to REDIS_URL env var or redis://redis:6379. */
  redisUrl?: string;
  /**
   * DefinitionsRepository instance.
   * Required: subscriber cannot write definitions without it.
   */
  definitionsRepository: DefinitionsRepository;
  /**
   * Pre-constructed ControlPlaneChannel for testing.
   * When provided, `redisUrl` and `nodeId` are ignored.
   */
  _controlPlane?: ControlPlaneChannel;
}

export class PipelineConfigSubscriber {
  private readonly channel: string;
  private readonly nodeId: string;
  private readonly redisUrl: string;
  private readonly repo: DefinitionsRepository;
  private readonly _controlPlane?: ControlPlaneChannel;

  private controlPlane: ControlPlaneChannel | null = null;
  private unsub: (() => Promise<void>) | null = null;

  constructor(opts: PipelineConfigSubscriberOptions) {
    this.channel = opts.channel ?? DEFAULT_CHANNEL;
    this.nodeId = opts.nodeId
      ?? `${process.env.HOSTNAME ?? 'localhost'}-${process.pid}`;
    this.redisUrl = opts.redisUrl
      ?? process.env.REDIS_URL
      ?? `redis://${process.env.REDIS_ENDPOINT ?? 'redis'}:${process.env.REDIS_PORT ?? '6379'}`;
    this.repo = opts.definitionsRepository;
    this._controlPlane = opts._controlPlane;
  }

  /**
   * Connect to Redis, create ControlPlaneChannel, and subscribe to the
   * agentic-pipeline config channel. Catches up on the latest snapshot
   * (if Redis store is configured on the publisher side).
   *
   * Non-fatal — throws only on programming errors (bad options). Redis
   * connectivity failures produce a warn log and a no-op stop handle.
   */
  async start(): Promise<void> {
    try {
      let cp: ControlPlaneChannel;

      if (this._controlPlane) {
        cp = this._controlPlane;
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

      const { unsubscribe } = await cp.subscribe<AgenticPipelineConfig>(
        this.channel,
        (payload) => this._handleConfig(payload),
      );
      this.unsub = unsubscribe;

      log.info({ channel: this.channel, nodeId: this.nodeId }, 'pipeline-config-subscriber started');
    } catch (err: unknown) {
      log.warn(
        { err: err instanceof Error ? err.message : String(err), channel: this.channel },
        'pipeline-config-subscriber failed to start (non-fatal)',
      );
    }
  }

  /**
   * Unsubscribe and stop the ControlPlaneChannel. Safe to call even if
   * start() failed.
   */
  async stop(): Promise<void> {
    if (this.unsub) {
      try { await this.unsub(); } catch { /* best-effort */ }
      this.unsub = null;
    }
    if (this.controlPlane) {
      try { await this.controlPlane.stop(); } catch { /* best-effort */ }
      this.controlPlane = null;
    }
    log.info({ channel: this.channel }, 'pipeline-config-subscriber stopped');
  }

  // ---------------------------------------------------------------------------
  // Handler
  // ---------------------------------------------------------------------------

  private async _handleConfig(
    payload: ControlPlanePayload<AgenticPipelineConfig>,
  ): Promise<ReconcileAck> {
    const { config, version, channel } = payload;
    const at = new Date().toISOString();

    try {
      // Write the definition to DynamoDB under the operator user id.
      await this.repo.put(OPERATOR_USER_ID, {
        ...config.definition,
        id: config.name,
      });

      // Push invalidation so sibling replicas refresh their definitions cache
      // without waiting for the 60s timer.
      void publishInvalidate(`control-plane:v${version}`).catch(() => {
        // Best-effort — the 60s timer will still surface the change.
      });

      log.info(
        { channel, version, pipelineId: config.name },
        'agentic-pipeline config applied',
      );

      return {
        channel,
        appliedVersion: version,
        appliedAt: at,
        subscriberNodeId: this.nodeId,
        status: 'applied',
      };
    } catch (err: unknown) {
      const error = err instanceof Error ? err.message : String(err);
      log.warn(
        { channel, version, pipelineId: config.name, err: error },
        'agentic-pipeline config write failed',
      );
      // Re-throw so ControlPlaneChannel produces a 'rejected' ack automatically.
      throw err;
    }
  }
}
