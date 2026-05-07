// social-api/src/pipeline/config/hintedHandoffQueue.ts
//
// Wire `HintedHandoffQueue` (distributed-core v0.9.0 / Phase F4) as an OPT-IN
// cross-node delivery primitive. When a peer is in the MembershipTable but
// not ALIVE, a hint can be durably enqueued instead of failing the send;
// once the peer recovers, the queue is drained and the message replayed.
//
// IMPORTANT — INTEGRATION GAP (Stream 10 finding, April 2026):
// -------------------------------------------------------------
// The DC `Cluster` facade (`Cluster.create()`) constructs its own internal
// `PeerMessaging` instance, but does NOT expose a hook to inject a
// `HintedHandoffQueue` into it. `PeerMessagingConfig.hintedHandoff` is a
// constructor-time option that the facade currently ignores.
//
// As a result, the queue created here is a STANDALONE FACILITY: it is
// constructed, started, and shut down cleanly, but it is NOT automatically
// consulted on `cluster.peer.sendToPeer(...)` failures. Without an upstream
// DC change (e.g. a `cluster.attachHintedHandoff(queue)` setter, or a new
// `ClusterConfig.hintedHandoff` field), wiring is provided by this bootstrap
// but the actual interception point is missing.
//
// Until that upstream hook lands, callers that want the cross-node-recovery
// behavior must either:
//   1) reach into the facade's private `_peer` (NOT recommended — coupling
//      to internal field), or
//   2) construct their own `PeerMessaging` on top of `cluster.clusterManager`
//      + `cluster.transport` and pass the queue via its config.
//
// Stream 10 stops at constructing + lifecycle-managing the queue. The
// `node-recovered` drain loop is a no-op without the interception point;
// adding it here would be misleading. TODO upstream: add the wiring hook.
//
// Tunables (env at call time):
//   PIPELINE_HINTED_HANDOFF_ENABLED   'true' | 'false' (default 'false' — opt-in).
//   PIPELINE_HINTED_HANDOFF_PATH      Filesystem dir for durable per-target logs.
//                                     Required-by-default when enabled, falls back
//                                     to '/var/lib/social-api/pipeline-handoff' in
//                                     production / '/tmp/pipeline-handoff' in dev.
//                                     Parent dir writability is verified via
//                                     `verifyWalParentWritable` before construction.
//   PIPELINE_HINTED_HANDOFF_MAX_BYTES Per-target queue cap. Note: distributed-core's
//                                     `HintedHandoffOptions` exposes `maxQueueDepth`
//                                     (count of hints, default 10_000), NOT a byte
//                                     limit. We accept this env var with the
//                                     historical "max-bytes" name and forward the
//                                     value to `maxQueueDepth`. When unset, DC's
//                                     default applies.

import {
  HintedHandoffQueue,
  type HintedHandoffOptions,
} from 'distributed-core';
import type { Cluster } from 'distributed-core';

import { verifyWalParentWritable } from './walPreflight';

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

export interface SetupHintedHandoffOptions {
  /** Override env-derived enabled flag (tests). */
  enabled?: boolean;
  /** Override env-derived data dir (tests). */
  dataDir?: string;
  /** Override env-derived per-target depth cap (tests). */
  maxQueueDepth?: number;
}

export interface HintedHandoffHandle {
  /** The constructed (and started) HintedHandoffQueue instance. */
  queue: HintedHandoffQueue;
  /** Coordinated stop hook — called from bootstrap's shutdown(). */
  stop: () => Promise<void>;
}

/**
 * Construct a HintedHandoffQueue when `PIPELINE_HINTED_HANDOFF_ENABLED=true`,
 * otherwise return `null`. The queue is started before this function resolves.
 *
 * @param _cluster  Reserved for the wiring hook described in the file header.
 *                  Currently unused at runtime — kept in the signature so the
 *                  call-site stays stable when DC adds an attach point.
 */
export async function setupHintedHandoffQueue(
  _cluster: Cluster,
  opts: SetupHintedHandoffOptions = {},
): Promise<HintedHandoffHandle | null> {
  const enabled = resolveEnabled(opts.enabled);
  if (!enabled) return null;

  const dataDir = resolveDataDir(opts.dataDir);
  const maxQueueDepth = resolveMaxQueueDepth(opts.maxQueueDepth);

  // The dataDir is the *parent directory* under which `handoff-{target}.log`
  // files live — distributed-core lazily creates each per-target log as
  // hints are enqueued. We verify the dir's *parent* is writable so the
  // operator gets a clear error if the mount is read-only or missing.
  // Reuse the existing PIPELINE_WAL_PATH-style helper for consistency.
  verifyWalParentWritable(dataDir, 'PIPELINE_HINTED_HANDOFF_PATH', /* disableHint */ false);

  const queueOpts: HintedHandoffOptions = { dataDir };
  if (maxQueueDepth !== undefined) {
    queueOpts.maxQueueDepth = maxQueueDepth;
  }

  const queue = new HintedHandoffQueue(queueOpts);
  await queue.start();

  return {
    queue,
    stop: async () => {
      await queue.stop();
    },
  };
}

// ---------------------------------------------------------------------------
// Env / option resolution
// ---------------------------------------------------------------------------

function resolveEnabled(explicit: boolean | undefined): boolean {
  if (explicit !== undefined) return explicit;
  const raw = process.env.PIPELINE_HINTED_HANDOFF_ENABLED;
  if (raw === undefined || raw === '') return false;
  return raw.trim().toLowerCase() === 'true';
}

function resolveDataDir(explicit: string | undefined): string {
  if (explicit !== undefined && explicit !== '') return explicit;
  const env = process.env.PIPELINE_HINTED_HANDOFF_PATH;
  if (env !== undefined && env !== '') return env;
  return process.env.NODE_ENV === 'production'
    ? '/var/lib/social-api/pipeline-handoff'
    : '/tmp/pipeline-handoff';
}

function resolveMaxQueueDepth(explicit: number | undefined): number | undefined {
  if (explicit !== undefined) return explicit;
  const raw = process.env.PIPELINE_HINTED_HANDOFF_MAX_BYTES;
  if (raw === undefined || raw === '') return undefined;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) {
    throw new Error(
      `[pipeline] PIPELINE_HINTED_HANDOFF_MAX_BYTES must be a positive number, got: ${raw}`,
    );
  }
  return n;
}
