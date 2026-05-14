// Factory for the `VideoSessionStore` singleton.
//
// Feature flag contract:
//
//   VIDEO_SESSIONS_STORE = "ddb"     (default) — DDB-backed durable store.
//                                     This is the production code path and
//                                     matches the behaviour platform-api has
//                                     shipped since the VideoSessionRepository
//                                     was introduced.
//   VIDEO_SESSIONS_STORE = "memory"  — in-process Map. Lost on restart.
//                                     Reserved for emergency rollback in case
//                                     the DDB code path is the failure mode,
//                                     and for tests that prefer a real store
//                                     over a mock.
//
// The default is `ddb` (NOT `memory`) because video sessions ARE persistent
// today; flipping the default to memory would silently introduce a
// state-loss regression on the next restart. The "safe rollout" framing in
// the wave task brief assumed an in-memory baseline that no longer exists.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { VideoSessionStore } from './VideoSessionStore';
import { VideoSessionRepository } from './VideoSessionRepository';
import { MemoryVideoSessionStore } from './MemoryVideoSessionStore';

export type VideoSessionStoreMode = 'ddb' | 'memory';

/**
 * Read the flag from env. Unknown / unset values fall through to `ddb` so
 * a typo doesn't silently demote a production replica to in-memory mode.
 */
export function resolveVideoSessionStoreMode(
  env: NodeJS.ProcessEnv = process.env,
): VideoSessionStoreMode {
  const raw = (env.VIDEO_SESSIONS_STORE ?? '').trim().toLowerCase();
  if (raw === 'memory') return 'memory';
  return 'ddb';
}

/**
 * Build the singleton store. Caller-injected docClient lets tests pass a
 * stub without import-time side effects.
 */
export function createVideoSessionStore(
  docClient: DynamoDBDocumentClient,
  env: NodeJS.ProcessEnv = process.env,
): VideoSessionStore {
  const mode = resolveVideoSessionStoreMode(env);
  if (mode === 'memory') {
    return new MemoryVideoSessionStore();
  }
  return new VideoSessionRepository(docClient);
}
