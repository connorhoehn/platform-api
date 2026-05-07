// social-api/src/ingest/adapters/writeback-types.ts
//
// Local minimal definition of the WriteBackDriver contract from the
// Agentic ETL vision doc (section D "Bidirectional sync").
//
// Agent A is producing the canonical version of these types in
// `../types.ts`. Until that lands, this file holds a minimal local
// version so Agent B can ship the Google Drive write-back driver
// independently. The integrator can collapse this into the canonical
// types module and re-export from here for backwards compatibility.

import type { SourceConfig } from '../types';

/**
 * A single change pushed back to a source system.
 *
 * The shape is intentionally small and source-agnostic. Per-source
 * drivers translate this into their own update API call.
 *
 * `baseRevision` is the source-side revision the platform last
 * observed; if the remote has moved past it, the driver MUST raise a
 * conflict instead of overwriting.
 */
export interface WriteBackChange {
  /** Canonical document id inside this platform. */
  documentId: string;
  /** Source-system file/page/issue id (e.g. Drive fileId). */
  sourcePath: string;
  /** Full new content the platform wants to write. */
  content: Buffer;
  /** Mime type of `content` — drives any source-side conversion. */
  mimeType: string;
  /** The source revision id this change was authored against. */
  baseRevision: string | null;
  /** Free-form metadata — driver may ignore. */
  metadata?: Record<string, unknown>;
}

/** Successful push outcome. */
export interface WriteBackAck {
  status: 'ok';
  /** New source-side revision id after the write succeeded. */
  newRevision: string;
  /** Echo of the source path for callers who batch pushes. */
  sourcePath: string;
  timestamp: string;
}

/** Conflict outcome — caller must invoke `resolveConflict`. */
export interface WriteBackConflict {
  status: 'conflict';
  sourcePath: string;
  /** Revision the platform thought it was writing against. */
  expectedRevision: string | null;
  /** Revision the source system actually has now. */
  remoteRevision: string;
  /** Snapshot of the remote content at remoteRevision (if fetchable). */
  remoteContent?: Buffer;
}

/** Hard failure (auth, scope, rate limit, network). Non-conflict. */
export interface WriteBackError {
  status: 'error';
  sourcePath: string;
  code:
    | 'missing_scope'
    | 'auth_failed'
    | 'not_found'
    | 'rate_limited'
    | 'unknown';
  message: string;
  retriable: boolean;
}

export type WriteBackResult =
  | WriteBackAck
  | WriteBackConflict
  | WriteBackError;

/**
 * Resolved merge produced by `resolveConflict`. v1 surfaces the
 * conflict to the operator and lets them pick a side; auto-merge is
 * out of scope.
 */
export interface ResolvedChange {
  /** Whichever side won, expressed as a fresh WriteBackChange. */
  resolved: WriteBackChange;
  /** Decision provenance for audit. */
  decision: 'keep-local' | 'keep-remote' | 'manual-merge';
}

/**
 * The peer of `SourceAdapter`. Each connector that supports
 * bidirectional sync implements this. Stays close to the vision-doc
 * shape: `push` is the workhorse; `resolveConflict` is consulted only
 * after `push` returns a conflict result.
 */
export interface WriteBackDriver {
  /**
   * Push a change to the source system. MUST detect version conflicts
   * (using whatever the source's idea of a revision is) and return a
   * conflict result rather than blindly overwriting.
   */
  push(config: SourceConfig, change: WriteBackChange): Promise<WriteBackResult>;

  /**
   * Resolve a previously-detected conflict. v1 implementations may
   * simply return one side — operator UI is the merge surface, not
   * the driver.
   */
  resolveConflict(
    local: WriteBackChange,
    remote: { revision: string; content: Buffer; mimeType: string },
  ): Promise<ResolvedChange>;
}
