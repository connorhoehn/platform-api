// social-api/src/pipeline/config/signer.ts
//
// Per-node `RaftRpcSigner` factory. Distributed-core's
// `RaftConfig.signer?` slot accepts any value satisfying the duck-typed
// `RaftRpcSigner` interface (signClusterPayload + verifyClusterPayload) —
// `KeyManager` and `RotatingKeyManager` from distributed-core both
// satisfy it, so we just construct a `KeyManager` here and return it.
//
// Provisioning model:
//   - When `keyManagerSecretsDir` (or PIPELINE_RAFT_SIGNER_DIR env var) is
//     set AND points to a readable directory, we attempt to load
//     `<dir>/<nodeId>.private.pem` + `<dir>/<nodeId>.public.pem`. If both
//     files exist, KeyManager loads them; otherwise we synthesize a fresh
//     key pair (KeyManager's default constructor) and write it back to
//     disk for the next boot.
//   - When NO directory is configured, we return `undefined` — RPCs flow
//     unsigned (the back-compat default for distributed-core's
//     `RaftConfig.signer?`).
//
// Why a directory rather than env-var-loaded inline PEM strings?
//   - Operators tend to mount per-node secrets via Kubernetes secret
//     volumes / Vault agent. Both materialise as files. Inline env-var
//     PEMs require shell escaping for newlines, which is error-prone.
//   - The same directory layout works for the gateway's room-ownership
//     signer (which is on a follow-up branch), so the operational
//     surface area stays consistent.
//
// SECURITY NOTE: this signer is deliberately the simplest thing that
// satisfies the duck-typed interface. We do NOT pin peer public keys
// here — verification reuses the receiver's own KeyManager, which means
// a cluster of N nodes effectively shares the public key of whichever
// node receives the RPC. That's fine for development and for clusters
// inside a trusted network; production deployments wanting cross-node
// authenticity should plug in `RotatingKeyManager` (which carries a
// keyset and rotates on a schedule). See
// `node_modules/distributed-core/dist/identity/RotatingKeyManager.d.ts`.

import * as fs from 'fs';
import * as path from 'path';

import { KeyManager } from 'distributed-core';
import type { RaftRpcSigner } from 'distributed-core';

export interface BuildRaftSignerOptions {
  /** This node's stable identity. Used as the on-disk file prefix. */
  nodeId: string;
  /**
   * Optional directory holding `<nodeId>.private.pem` /
   * `<nodeId>.public.pem`. When unset, falls back to
   * `process.env.PIPELINE_RAFT_SIGNER_DIR`. When that's also unset, the
   * factory returns `undefined` (unsigned, back-compat).
   */
  keyManagerSecretsDir?: string;
}

/**
 * Build a `RaftRpcSigner` for the given node, or return `undefined` when
 * no secrets directory is configured.
 *
 * Failure modes:
 *   - Configured directory exists but is unreadable → throws (operators
 *     prefer a fast fail to silently-unsigned RPCs).
 *   - Configured directory exists but the per-node key files are absent
 *     → mints a fresh key pair via `new KeyManager()` and persists it.
 *     A subsequent boot at the same dir reads the persisted PEMs.
 *   - Directory exists, files exist but are malformed → KeyManager
 *     constructor throws; we propagate.
 */
export function buildRaftSigner(opts: BuildRaftSignerOptions): RaftRpcSigner | undefined {
  const dir = opts.keyManagerSecretsDir ?? process.env.PIPELINE_RAFT_SIGNER_DIR;
  if (!dir || dir.trim() === '') {
    // Unsigned — back-compat default. No log here: distributed-core's
    // RaftRpcRouter handles the "no signer" case quietly, and the bootstrap
    // logger emits a single line about whether signing is enabled overall.
    return undefined;
  }

  const resolved = path.resolve(dir);
  // Fail fast if the dir doesn't exist or isn't readable. Operators get
  // an actionable error here rather than a silently-unsigned cluster.
  if (!fs.existsSync(resolved)) {
    // Auto-create the directory tree — typical operator workflow on a
    // fresh machine. We use 0o700 so the keys are owner-only.
    fs.mkdirSync(resolved, { recursive: true, mode: 0o700 });
  } else {
    try {
      fs.accessSync(resolved, fs.constants.R_OK | fs.constants.W_OK);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `[pipeline:signer] PIPELINE_RAFT_SIGNER_DIR (${resolved}) is not readable/writable: ${message}. `
        + `Fix the filesystem permission or unset the variable to run with unsigned RPCs.`,
      );
    }
  }

  const privatePath = path.join(resolved, `${opts.nodeId}.private.pem`);
  const publicPath = path.join(resolved, `${opts.nodeId}.public.pem`);

  let privatePem: string | undefined;
  let publicPem: string | undefined;
  if (fs.existsSync(privatePath) && fs.existsSync(publicPath)) {
    privatePem = fs.readFileSync(privatePath, 'utf8');
    publicPem = fs.readFileSync(publicPath, 'utf8');
  }

  // Construct the manager. When PEMs are present we hand them to
  // KeyManager; otherwise we let it mint a fresh pair (its default
  // behaviour) and persist the result.
  const km = new KeyManager(
    privatePem && publicPem
      ? { privateKeyPem: privatePem, publicKeyPem: publicPem }
      : {},
  );

  if (!privatePem || !publicPem) {
    // Fresh keys — persist for next boot. 0o600 so other users on the
    // host can't read the private key.
    try {
      fs.writeFileSync(privatePath, km.getPrivateKey(), { mode: 0o600 });
      fs.writeFileSync(publicPath, km.getPublicKey(), { mode: 0o644 });
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      throw new Error(
        `[pipeline:signer] Failed to persist freshly-minted Raft signer keys at ${resolved}: ${message}. `
        + `The KeyManager was constructed but could not be saved — running with the in-memory pair would fail on the next boot.`,
      );
    }
  }

  // KeyManager satisfies the duck-typed RaftRpcSigner interface
  // (signClusterPayload<T>(payload) + verifyClusterPayload(payload)).
  // We assign through the interface here so a future signature drift
  // in distributed-core fails at compile time rather than at the first
  // signed RPC.
  const signer: RaftRpcSigner = km;
  return signer;
}
