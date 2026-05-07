// social-api/src/ingest/adapters/jira.ts
//
// Jira connector — Tier 2 in the agentic-ETL vision (Agent D slice).
//
// Two halves of the connector live in this module:
//
//   1. JiraSourceAdapter — read surface. Pulls issues + comments under a
//      project key. Each issue and each comment becomes its own
//      FileDescriptor (separate IngestItem rows). Cursor checkpointing is
//      JQL-based — `updated >= "<lastSyncAt>"`.
//
//   2. JiraWriteBackDriver — write surface. Two operations, dispatched on
//      the `op` discriminator inside `WriteBackChange.change`:
//
//        op: 'comment.add'         — always safe, no conflict.
//        op: 'issue.fields.update' — rule-based merge for `status`; reject
//                                    other field merges as conflicts.
//
//      Status-field merge rules — codified in mergeStatusUpdate():
//        - Same status: no-op (idempotent).
//        - Remote in a "terminal" state (done/closed/resolved): NEVER auto
//          revert. Local update is dropped, returned as merged=true with
//          remote winning.
//        - Otherwise: apply local change.
//
//      All other field updates with a stale baseVersion are surfaced as
//      conflicts for operator resolution.
//
// Webhook receiver lives in ../routes/ingestJiraWebhook.ts.
//
// The canonical WriteBackDriver interface is being produced by Agent A in
// ../types.ts. Until that lands, we define a minimal local version below
// matching the vision doc § "Bidirectional sync" shape. The integrator
// reconciles this with the canonical interface (and with the parallel
// local version in ./writeback-types.ts that Agent B is using).
//
// Uses plain `fetch` — no new deps. Authenticates via Basic auth with an
// API token (Atlassian's recommended pattern). SourceConfig fields:
//
//   baseUrl       (string, required) — e.g. "https://acme.atlassian.net"
//   email         (string, required) — Atlassian account email
//   apiToken      (string, required) — Atlassian API token
//   projectKey    (string, required) — e.g. "ENG"
//   pageSize      (number, optional) — JQL page size, default 50

import type {
  SourceAdapter,
  SourceConfig,
  SyncCursor,
  FileDescriptor,
  FileContent,
} from '../types';

// ─── Local WriteBackDriver shape (will be reconciled with Agent A) ────────
//
// Minimal version matching the vision doc § "Bidirectional sync". Agent A
// is producing the canonical interface; the integrator reconciles. Kept
// local to this file (rather than re-using the parallel writeback-types.ts
// from Agent B) so each agent's slice stays independently buildable.

/** A single change pushed back to a source system. */
export interface WriteBackChange {
  /** Canonical document id inside the platform. */
  documentId: string;
  /** Source-side identity (e.g. Jira issue key "ENG-123"). */
  sourceRef: string;
  /** Source-side version stamp captured at read time, used for conflict detection. */
  baseVersion?: string;
  /** Driver-specific payload (see JiraWriteBackOp below). */
  change: unknown;
  /** ISO8601 timestamp of when the local edit was finalized. */
  timestamp: string;
}

/** Successful (or auto-merged) writeback outcome. */
export interface WriteBackAck {
  /** New version stamp returned by the source. */
  newVersion?: string;
  /** ISO8601 timestamp acknowledged by the source. */
  acknowledgedAt: string;
  /** True when the driver auto-merged a conflict during push. */
  merged?: boolean;
}

/** Peer of SourceAdapter — replays platform-side edits back to the source. */
export interface WriteBackDriver {
  push(config: SourceConfig, change: WriteBackChange): Promise<WriteBackAck>;
  resolveConflict(
    config: SourceConfig,
    local: WriteBackChange,
    remote: WriteBackChange,
  ): Promise<WriteBackChange>;
}

// ─── Jira REST shapes ─────────────────────────────────────────────────────

interface JiraStatus {
  id?: string;
  name: string;
  statusCategory?: { key?: string; name?: string };
}

interface JiraComment {
  id: string;
  body?: string;
  author?: { displayName?: string; emailAddress?: string };
  created?: string;
  updated?: string;
}

interface JiraIssue {
  id: string;
  key: string;
  self?: string;
  fields: {
    summary?: string;
    description?: string;
    status?: JiraStatus;
    updated?: string;
    created?: string;
    project?: { key?: string };
    comment?: { comments?: JiraComment[]; total?: number };
    [k: string]: unknown;
  };
}

interface JiraSearchResponse {
  issues: JiraIssue[];
  startAt: number;
  maxResults: number;
  total: number;
}

// ─── Jira-specific WriteBack payload ──────────────────────────────────────

/**
 * The polymorphic body that goes inside `WriteBackChange.change` for the
 * Jira driver. The driver dispatches on `op` and ignores other shapes.
 */
export type JiraWriteBackOp =
  | {
      op: 'comment.add';
      /** Issue key (e.g. "ENG-123") — same as WriteBackChange.sourceRef. */
      issueKey: string;
      /** Comment body to add. */
      body: string;
    }
  | {
      op: 'issue.fields.update';
      issueKey: string;
      /** Field name → new value. Only `status` is rule-merge-eligible. */
      fields: Record<string, unknown>;
    };

// ─── Issue ↔ documentId mapping ───────────────────────────────────────────

/**
 * Issue and comment paths inside the ingest system.
 *
 *   issue:   `${projectKey}/${issueKey}`        e.g. "ENG/ENG-123"
 *   comment: `${projectKey}/${issueKey}/comment/${commentId}`
 *
 * The `path` is the canonical identifier on `FileDescriptor` and what the
 * SyncCursor tracks. The materializer turns it into a `documentId`; the
 * write-back driver only cares about the issueKey, which is encoded both
 * in the path and in `WriteBackChange.sourceRef`.
 */
export function issuePath(projectKey: string, issueKey: string): string {
  return `${projectKey}/${issueKey}`;
}

export function commentPath(
  projectKey: string,
  issueKey: string,
  commentId: string,
): string {
  return `${projectKey}/${issueKey}/comment/${commentId}`;
}

/**
 * Reverse: parse an issue or comment path back into its parts. Returns
 * null on a path the Jira connector did not author.
 */
export function parseJiraPath(path: string):
  | { kind: 'issue'; projectKey: string; issueKey: string }
  | { kind: 'comment'; projectKey: string; issueKey: string; commentId: string }
  | null {
  const parts = path.split('/');
  if (parts.length === 2) {
    return { kind: 'issue', projectKey: parts[0]!, issueKey: parts[1]! };
  }
  if (parts.length === 4 && parts[2] === 'comment') {
    return {
      kind: 'comment',
      projectKey: parts[0]!,
      issueKey: parts[1]!,
      commentId: parts[3]!,
    };
  }
  return null;
}

// ─── Helpers ──────────────────────────────────────────────────────────────

interface JiraConfig {
  baseUrl: string;
  email: string;
  apiToken: string;
  projectKey: string;
  pageSize: number;
}

function readConfig(config: SourceConfig): JiraConfig {
  const baseUrl = String(config.baseUrl ?? '').replace(/\/+$/, '');
  const email = String(config.email ?? '');
  const apiToken = String(config.apiToken ?? '');
  const projectKey = String(config.projectKey ?? '');
  const pageSize = Number(config.pageSize ?? 50);
  if (!baseUrl) throw new Error('JiraAdapter: missing baseUrl');
  if (!email) throw new Error('JiraAdapter: missing email');
  if (!apiToken) throw new Error('JiraAdapter: missing apiToken');
  if (!projectKey) throw new Error('JiraAdapter: missing projectKey');
  return { baseUrl, email, apiToken, projectKey, pageSize };
}

function authHeader(cfg: JiraConfig): string {
  const token = Buffer.from(`${cfg.email}:${cfg.apiToken}`).toString('base64');
  return `Basic ${token}`;
}

async function jiraFetch(
  cfg: JiraConfig,
  path: string,
  init: RequestInit = {},
): Promise<Response> {
  const url = `${cfg.baseUrl}${path}`;
  const headers = {
    Authorization: authHeader(cfg),
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...((init.headers as Record<string, string>) ?? {}),
  };
  const res = await fetch(url, { ...init, headers });
  return res;
}

/**
 * Build the JQL `updated` clause from the sync cursor. Jira's JQL accepts
 * `"yyyy-MM-dd HH:mm"` literals; we convert ISO8601 → that format.
 */
function buildSinceClause(cursor: SyncCursor): string | null {
  if (!cursor.lastSyncAt) return null;
  const d = new Date(cursor.lastSyncAt);
  if (isNaN(d.getTime())) return null;
  const pad = (n: number): string => String(n).padStart(2, '0');
  const stamp = `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(
    d.getUTCHours(),
  )}:${pad(d.getUTCMinutes())}`;
  return `updated >= "${stamp}"`;
}

function buildJql(cfg: JiraConfig, cursor: SyncCursor): string {
  const parts = [`project = "${cfg.projectKey}"`];
  const since = buildSinceClause(cursor);
  if (since) parts.push(since);
  parts.push('ORDER BY updated ASC');
  return parts.join(' ');
}

// ─── JiraSourceAdapter ────────────────────────────────────────────────────

export class JiraSourceAdapter implements SourceAdapter {
  static readonly capabilities = {
    readonly: false,
    supportsWriteBack: true,
    requiresOauth: false,
    supportsWebhook: true,
  };

  async *list(config: SourceConfig, cursor: SyncCursor): AsyncGenerator<FileDescriptor> {
    const cfg = readConfig(config);
    const jql = buildJql(cfg, cursor);

    let startAt: number = (cursor.checkpoint && Number.isFinite(Number(cursor.checkpoint)))
      ? Number(cursor.checkpoint)
      : 0;
    let total = Infinity;

    while (startAt < total) {
      const params = new URLSearchParams({
        jql,
        startAt: String(startAt),
        maxResults: String(cfg.pageSize),
        fields: 'summary,description,status,updated,created,project,comment',
      });
      const res = await jiraFetch(cfg, `/rest/api/3/search?${params.toString()}`);
      if (!res.ok) {
        throw new Error(`JiraAdapter.list: ${res.status} ${await res.text()}`);
      }
      const body = (await res.json()) as JiraSearchResponse;
      total = body.total;

      for (const issue of body.issues ?? []) {
        // Issue itself
        yield {
          path: issuePath(cfg.projectKey, issue.key),
          name: issue.key,
          hash: `${issue.key}:${issue.fields.updated ?? ''}`,
          size: issue.fields.description?.length ?? 0,
          mimeType: 'application/vnd.jira.issue+json',
          lastModified: issue.fields.updated ?? new Date().toISOString(),
          metadata: {
            kind: 'issue',
            issueId: issue.id,
            issueKey: issue.key,
            projectKey: cfg.projectKey,
            status: issue.fields.status?.name ?? null,
            statusCategory: issue.fields.status?.statusCategory?.key ?? null,
          },
        };

        // Comments embedded in the search response (Jira returns them
        // by default for each issue). For issues with > N comments, the
        // remainder is paged via /rest/api/3/issue/{key}/comment, which
        // we follow below.
        const embedded = issue.fields.comment?.comments ?? [];
        const totalComments = issue.fields.comment?.total ?? embedded.length;
        for (const c of embedded) {
          yield commentDescriptor(cfg, issue, c);
        }
        if (totalComments > embedded.length) {
          for await (const c of this.listExtraComments(cfg, issue, embedded.length)) {
            yield commentDescriptor(cfg, issue, c);
          }
        }
      }

      startAt += body.issues?.length ?? 0;
      if ((body.issues?.length ?? 0) === 0) break;
    }
  }

  private async *listExtraComments(
    cfg: JiraConfig,
    issue: JiraIssue,
    initialOffset: number,
  ): AsyncGenerator<JiraComment> {
    let startAt = initialOffset;
    while (true) {
      const params = new URLSearchParams({
        startAt: String(startAt),
        maxResults: String(cfg.pageSize),
      });
      const res = await jiraFetch(
        cfg,
        `/rest/api/3/issue/${encodeURIComponent(issue.key)}/comment?${params.toString()}`,
      );
      if (!res.ok) {
        throw new Error(
          `JiraAdapter.listExtraComments(${issue.key}): ${res.status} ${await res.text()}`,
        );
      }
      const body = (await res.json()) as { comments: JiraComment[]; total: number };
      for (const c of body.comments ?? []) yield c;
      startAt += body.comments?.length ?? 0;
      if (!body.comments?.length || startAt >= body.total) break;
    }
  }

  async fetch(config: SourceConfig, file: FileDescriptor): Promise<FileContent> {
    const cfg = readConfig(config);
    const parsed = parseJiraPath(file.path);
    if (!parsed) {
      throw new Error(`JiraAdapter.fetch: not a jira path: ${file.path}`);
    }

    if (parsed.kind === 'issue') {
      const res = await jiraFetch(
        cfg,
        `/rest/api/3/issue/${encodeURIComponent(parsed.issueKey)}`,
      );
      if (!res.ok) {
        throw new Error(`JiraAdapter.fetch issue ${parsed.issueKey}: ${res.status}`);
      }
      const issue = (await res.json()) as JiraIssue;
      const data = Buffer.from(JSON.stringify(issue), 'utf-8');
      return { data, encoding: 'utf-8', parsed: issue };
    }

    // Comment fetch
    const res = await jiraFetch(
      cfg,
      `/rest/api/3/issue/${encodeURIComponent(parsed.issueKey)}/comment/${encodeURIComponent(
        parsed.commentId,
      )}`,
    );
    if (!res.ok) {
      throw new Error(
        `JiraAdapter.fetch comment ${parsed.issueKey}/${parsed.commentId}: ${res.status}`,
      );
    }
    const comment = (await res.json()) as JiraComment;
    const data = Buffer.from(JSON.stringify(comment), 'utf-8');
    return { data, encoding: 'utf-8', parsed: comment };
  }
}

function commentDescriptor(
  cfg: JiraConfig,
  issue: JiraIssue,
  c: JiraComment,
): FileDescriptor {
  return {
    path: commentPath(cfg.projectKey, issue.key, c.id),
    name: `${issue.key} comment ${c.id}`,
    hash: `${c.id}:${c.updated ?? c.created ?? ''}`,
    size: c.body?.length ?? 0,
    mimeType: 'application/vnd.jira.comment+json',
    lastModified: c.updated ?? c.created ?? new Date().toISOString(),
    metadata: {
      kind: 'comment',
      issueId: issue.id,
      issueKey: issue.key,
      commentId: c.id,
      projectKey: cfg.projectKey,
      author: c.author?.displayName ?? null,
    },
  };
}

// ─── JiraWriteBackDriver ──────────────────────────────────────────────────

/**
 * Set of Jira status-category keys treated as "terminal" — once an issue
 * lands here, the platform never reverts it to an earlier state. Matches
 * Jira's standard categories (`done`) plus the conventional name strings
 * (`closed`, `resolved`) that operators set on custom status names.
 *
 * Comparison is case-insensitive and matches either the Jira `name` or
 * the `statusCategory.key`.
 */
const TERMINAL_STATUS_NAMES: ReadonlySet<string> = new Set([
  'done',
  'closed',
  'resolved',
  'completed',
  'cancelled',
  'canceled',
]);

const TERMINAL_STATUS_CATEGORIES: ReadonlySet<string> = new Set(['done']);

export function isTerminalStatus(status: JiraStatus | null | undefined): boolean {
  if (!status) return false;
  if (status.name && TERMINAL_STATUS_NAMES.has(status.name.toLowerCase())) return true;
  const cat = status.statusCategory?.key;
  if (cat && TERMINAL_STATUS_CATEGORIES.has(cat.toLowerCase())) return true;
  return false;
}

/**
 * Outcome of merging a local status update against the remote.
 *
 *   apply  — local wins; driver should issue the transition.
 *   noop   — local and remote agree; driver should do nothing but ack.
 *   reject — remote is terminal; local would revert. Driver acks with
 *            merged=true and the existing remote version. Operator can
 *            override via the conflict queue.
 */
export type StatusMergeDecision =
  | { kind: 'apply'; target: string }
  | { kind: 'noop' }
  | { kind: 'reject'; reason: string };

/**
 * The encoded status-field merge rules (extracted for unit testing):
 *
 *   1. If local target == remote name → noop.
 *   2. If remote is terminal (done/closed/resolved/etc.) → reject. Never
 *      auto-revert.
 *   3. Otherwise → apply local.
 */
export function mergeStatusUpdate(
  localTarget: string,
  remote: JiraStatus | null | undefined,
): StatusMergeDecision {
  if (!remote) return { kind: 'apply', target: localTarget };
  if (remote.name && remote.name.toLowerCase() === localTarget.toLowerCase()) {
    return { kind: 'noop' };
  }
  if (isTerminalStatus(remote)) {
    return {
      kind: 'reject',
      reason: `remote status "${remote.name}" is terminal; refusing to revert to "${localTarget}"`,
    };
  }
  return { kind: 'apply', target: localTarget };
}

export class JiraWriteBackDriver implements WriteBackDriver {
  static readonly capabilities = {
    readonly: false,
    supportsWriteBack: true,
    requiresOauth: false,
    supportsWebhook: true,
  };

  async push(config: SourceConfig, change: WriteBackChange): Promise<WriteBackAck> {
    const cfg = readConfig(config);
    const op = change.change as JiraWriteBackOp;
    if (!op || typeof op !== 'object' || !('op' in op)) {
      throw new Error('JiraWriteBackDriver.push: change.change missing `op` discriminator');
    }

    if (op.op === 'comment.add') {
      return this.pushCommentAdd(cfg, change, op);
    }
    if (op.op === 'issue.fields.update') {
      return this.pushIssueFieldsUpdate(cfg, change, op);
    }
    throw new Error(
      `JiraWriteBackDriver.push: unknown op "${(op as { op: string }).op}"`,
    );
  }

  private async pushCommentAdd(
    cfg: JiraConfig,
    change: WriteBackChange,
    op: Extract<JiraWriteBackOp, { op: 'comment.add' }>,
  ): Promise<WriteBackAck> {
    const issueKey = op.issueKey || change.sourceRef;
    const res = await jiraFetch(cfg, `/rest/api/3/issue/${encodeURIComponent(issueKey)}/comment`, {
      method: 'POST',
      body: JSON.stringify({ body: op.body }),
    });
    if (!res.ok) {
      throw new Error(
        `JiraWriteBackDriver.comment.add(${issueKey}): ${res.status} ${await res.text()}`,
      );
    }
    const created = (await res.json()) as JiraComment;
    return {
      acknowledgedAt: created.created ?? new Date().toISOString(),
      newVersion: created.id,
    };
  }

  private async pushIssueFieldsUpdate(
    cfg: JiraConfig,
    change: WriteBackChange,
    op: Extract<JiraWriteBackOp, { op: 'issue.fields.update' }>,
  ): Promise<WriteBackAck> {
    const issueKey = op.issueKey || change.sourceRef;

    // Fetch current issue to (a) detect remote drift via baseVersion, and
    // (b) feed the status-field merge rule.
    const currentRes = await jiraFetch(cfg, `/rest/api/3/issue/${encodeURIComponent(issueKey)}`);
    if (!currentRes.ok) {
      throw new Error(
        `JiraWriteBackDriver.issue.fields.update read ${issueKey}: ${currentRes.status}`,
      );
    }
    const current = (await currentRes.json()) as JiraIssue;
    const remoteVersion = current.fields.updated ?? '';

    const fieldsToWrite: Record<string, unknown> = { ...op.fields };
    let merged = false;

    if ('status' in fieldsToWrite) {
      const localTarget = String(fieldsToWrite.status);
      const decision = mergeStatusUpdate(localTarget, current.fields.status);
      if (decision.kind === 'reject') {
        // Drop the local status change, keep remote. Other fields (if any)
        // still go through. If status was the only field, this is a
        // no-write ack with merged=true.
        delete fieldsToWrite.status;
        merged = true;
      } else if (decision.kind === 'noop') {
        delete fieldsToWrite.status;
      }
      // 'apply' — fall through; status moved to a transitions[] call below.
    }

    // For non-status fields, if baseVersion is present and stale, surface
    // as a conflict for operator resolution. Status is exempt because the
    // merge rule above already resolved it.
    const nonStatusKeys = Object.keys(fieldsToWrite).filter((k) => k !== 'status');
    if (
      nonStatusKeys.length > 0 &&
      change.baseVersion &&
      remoteVersion &&
      change.baseVersion !== remoteVersion
    ) {
      throw new Error(
        `JiraWriteBackDriver.issue.fields.update(${issueKey}): conflict on ` +
          `non-status fields [${nonStatusKeys.join(', ')}] ` +
          `(baseVersion=${change.baseVersion}, remoteVersion=${remoteVersion})`,
      );
    }

    // Apply status via /transitions (Jira does not let PUT /issue mutate
    // status directly). We look up the transition by target name.
    if (typeof fieldsToWrite.status === 'string') {
      await this.applyStatusTransition(cfg, issueKey, String(fieldsToWrite.status));
      delete fieldsToWrite.status;
    }

    // Apply remaining fields (summary, description, etc.) via PUT.
    if (Object.keys(fieldsToWrite).length > 0) {
      const putRes = await jiraFetch(cfg, `/rest/api/3/issue/${encodeURIComponent(issueKey)}`, {
        method: 'PUT',
        body: JSON.stringify({ fields: fieldsToWrite }),
      });
      if (!putRes.ok) {
        throw new Error(
          `JiraWriteBackDriver.issue.fields.update PUT ${issueKey}: ${putRes.status} ${await putRes.text()}`,
        );
      }
    }

    // Re-read for the new updated stamp.
    const finalRes = await jiraFetch(cfg, `/rest/api/3/issue/${encodeURIComponent(issueKey)}`);
    let newVersion = remoteVersion;
    if (finalRes.ok) {
      const finalIssue = (await finalRes.json()) as JiraIssue;
      newVersion = finalIssue.fields.updated ?? remoteVersion;
    }

    return {
      acknowledgedAt: new Date().toISOString(),
      newVersion,
      merged: merged || undefined,
    };
  }

  private async applyStatusTransition(
    cfg: JiraConfig,
    issueKey: string,
    targetName: string,
  ): Promise<void> {
    const transRes = await jiraFetch(
      cfg,
      `/rest/api/3/issue/${encodeURIComponent(issueKey)}/transitions`,
    );
    if (!transRes.ok) {
      throw new Error(
        `JiraWriteBackDriver.applyStatusTransition list ${issueKey}: ${transRes.status}`,
      );
    }
    const body = (await transRes.json()) as {
      transitions: Array<{ id: string; name: string; to?: { name?: string } }>;
    };
    const match = body.transitions.find(
      (t) =>
        (t.to?.name ?? '').toLowerCase() === targetName.toLowerCase() ||
        t.name.toLowerCase() === targetName.toLowerCase(),
    );
    if (!match) {
      throw new Error(
        `JiraWriteBackDriver.applyStatusTransition(${issueKey}): no transition to "${targetName}"`,
      );
    }
    const postRes = await jiraFetch(
      cfg,
      `/rest/api/3/issue/${encodeURIComponent(issueKey)}/transitions`,
      {
        method: 'POST',
        body: JSON.stringify({ transition: { id: match.id } }),
      },
    );
    if (!postRes.ok) {
      throw new Error(
        `JiraWriteBackDriver.applyStatusTransition(${issueKey}) POST: ${postRes.status} ${await postRes.text()}`,
      );
    }
  }

  /**
   * v1 conflict resolution policy: surface the conflict, keep local. The
   * operator UI is the merge surface; this method is just a deterministic
   * fallback for callers that want a side picked.
   */
  async resolveConflict(
    _config: SourceConfig,
    local: WriteBackChange,
    _remote: WriteBackChange,
  ): Promise<WriteBackChange> {
    return local;
  }
}
