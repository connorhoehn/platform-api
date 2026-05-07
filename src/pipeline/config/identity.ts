// social-api/src/pipeline/config/identity.ts
//
// Resolve the cluster's stable node identity. When PIPELINE_IDENTITY_FILE is
// configured, distributed-core's `loadOrCreateNodeId()` atomically reads-or-
// creates the persisted id; otherwise we mint a fresh ephemeral one with the
// same shape distributed-core uses internally so the visible format stays
// consistent across paths.

import { loadOrCreateNodeId } from 'distributed-core';

export interface ResolveStableNodeIdResult {
  /** Final node id passed into Cluster.create({ nodeId }). */
  nodeId: string;
  /**
   * The id loaded from the identity file when one was configured. `undefined`
   * means we minted an ephemeral id — bootstrap uses this to drive identity-
   * decision logging.
   */
  persistentNodeId: string | undefined;
}

/**
 * Resolve a stable node id. Throws with a clear, env-var-named error when
 * an identity file is configured but unreadable / unwritable.
 *
 * @param identityFilePath  When set, delegate to `loadOrCreateNodeId()`.
 *                          When undefined, mint a fresh ephemeral id.
 */
export async function resolveStableNodeId(
  identityFilePath: string | undefined,
): Promise<ResolveStableNodeIdResult> {
  let persistentNodeId: string | undefined;

  if (identityFilePath) {
    try {
      persistentNodeId = await loadOrCreateNodeId(identityFilePath);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `[pipeline] PIPELINE_IDENTITY_FILE (${identityFilePath}) could not be read or created: ${message}. `
        + `Fix the filesystem permission, set PIPELINE_IDENTITY_FILE to a writable path, `
        + `or set PIPELINE_IDENTITY_FILE=disabled to opt out.`,
      );
    }
  }

  const nodeId = persistentNodeId
    ?? `node-${Math.random().toString(36).slice(2, 10)}-${Date.now()}`;

  return { nodeId, persistentNodeId };
}
