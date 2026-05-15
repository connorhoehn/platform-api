// platform-api/src/pipeline/audit-repository.ts
//
// Append-only audit trail for pipeline operations (trigger, approve, reject,
// cancel, webhook, definition mutations) AND executor lifecycle events
// (step started/completed/failed/breakpoint, run checkpoint/completed/failed).
//
// Wave 2 (hub#2224) wired this in two places:
//   - Route handlers in routes/pipeline*.ts call `auditRepo.record(...)` on
//     mutation success AND error paths.
//   - The `executor-audit-subscriber.ts` subscribes to PipelineModule's
//     EventBus and persists step/run lifecycle rows so operators can answer
//     "what happened to run X 6 hours ago" without trawling Pino logs.
//
// Storage shape (table `pipeline-audit`):
//   PK: auditId (S) — caller-generated ULID, lexicographically time-sortable
//   GSI `actor-time-index`:
//     PK: actorUserId (S)
//     SK: timestamp (S, ISO 8601)
//     Projection: ALL
//   GSI `pipeline-time-index`:
//     PK: pipelineId (S)
//     SK: timestamp (S, ISO 8601)
//     Projection: ALL
//   GSI `runId-time-index` (added Wave 2 — most operator queries are
//     "show me the audit trail for run X"; without a runId GSI we'd be
//     scanning the whole pipeline timeline and filtering client-side):
//     PK: runId (S)
//     SK: timestamp (S, ISO 8601)
//     Projection: ALL
//   TTL attribute: `ttlEpoch` (N, unix-seconds) — DDB drops rows older
//     than the configured retention window (default 30 days). Set at
//     write time so reads never have to filter.
//
// `record` is intentionally fire-and-forget at the persistence layer (no
// transactional guarantees) — pipeline state changes happen in their own
// repositories and we do NOT want a DynamoDB hiccup on the audit table to
// fail the user's request. Callers that need atomic "state + audit" writes
// should switch to `outbox-publisher.ts` style transactions later.

import {
  DynamoDBDocumentClient,
  QueryCommand,
  ScanCommand,
  DeleteCommand,
} from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from '../repositories/BaseRepository';
import { docClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';

const PIPELINE_AUDIT_TABLE = tableName('pipeline-audit');
const ACTOR_TIME_INDEX = 'actor-time-index';
const PIPELINE_TIME_INDEX = 'pipeline-time-index';
const RUN_TIME_INDEX = 'runId-time-index';

/** Default retention window for audit rows. Native DDB TTL drops them. */
const DEFAULT_TTL_DAYS = 30;
const SECONDS_PER_DAY = 24 * 60 * 60;

/** Logical action types recorded in the audit log. */
export type AuditAction =
  // Route-driven mutations.
  | 'pipeline.trigger'
  | 'pipeline.approve'
  | 'pipeline.reject'
  | 'pipeline.cancel'
  | 'pipeline.webhook'
  | 'pipeline.breakpoint.resolve'
  | 'pipeline.step.context.patched'
  | 'pipeline.step.context.patch-reverted'
  | 'pipeline.retry'
  | 'pipeline.replay'
  | 'pipeline.run.resumed-from-step'
  | 'pipeline.force_fail'
  | 'pipeline.definition.put'
  | 'pipeline.definition.delete'
  | 'pipeline.definition.publish'
  // Executor lifecycle (Wave 2 — emitted by executor-audit-subscriber).
  | 'pipeline.step.started'
  | 'pipeline.step.completed'
  | 'pipeline.step.failed'
  | 'pipeline.step.breakpoint.hit'
  | 'pipeline.run.checkpoint'
  | 'pipeline.run.completed'
  | 'pipeline.run.failed'
  // LLM observability — emitted by executor-audit-subscriber. Captures the
  // FULL prompt and FULL response text in `trace` so operators can see what
  // the model was asked and what it actually returned without trawling logs.
  // (We deliberately do NOT subscribe to `pipeline:llm:token` — that fires
  // per-token and would explode the audit row count.)
  | 'pipeline.llm.prompt'
  | 'pipeline.llm.response'
  | 'pipeline.llm.stream.opened'
  // P1 wave19-c additions — run-lifecycle + context-patch unvalidated variant.
  | 'pipeline.run.cancelled'
  | 'pipeline.run.stuck'
  | 'pipeline.step.context.patched-unvalidated'
  // P2 wave21-b additions — orphan/reassign + join lifecycle.
  | 'pipeline.run.orphaned'
  | 'pipeline.run.reassigned'
  | 'pipeline.join.waiting'
  | 'pipeline.join.fired';

/** Optional decision tag attached to the event. */
export type AuditDecision =
  | 'approved'
  | 'rejected'
  | 'cancelled'
  | 'failed'
  | 'completed'
  | 'success'
  | 'error';

/** Caller-supplied input to `record`. The repository fills in id + timestamp. */
export type AuditEvent = {
  action: AuditAction;
  actorUserId: string;
  pipelineId: string;
  runId?: string;
  decision?: AuditDecision;
  details?: Record<string, unknown>;
  /** Snapshot of state BEFORE the operation (route audits). */
  before?: Record<string, unknown>;
  /** Snapshot of state AFTER the operation (route audits). */
  after?: Record<string, unknown>;
  /** Key event payload data (executor lifecycle). */
  trace?: Record<string, unknown>;
  /** Outcome bucket — kept distinct from `decision` so executor events that
   *  don't fit the approve/reject vocabulary still carry success/error. */
  outcome?: 'success' | 'error' | 'rejected';
};

/** Fully-persisted shape (what queries return). */
export interface StoredAuditEvent extends AuditEvent {
  auditId: string;
  timestamp: string;
  /** Unix-seconds. DynamoDB native TTL drops rows older than this. */
  ttlEpoch: number;
}

export interface AuditRepositoryOptions {
  /** Retention window in days. Defaults to 30. */
  ttlDays?: number;
  /** Override the wall clock for tests. Returns ms-since-epoch. */
  now?: () => number;
}

/**
 * Pagination + filter options shared by the per-run / per-actor / per-pipeline
 * list methods. Cursor is the opaque base64-encoded `LastEvaluatedKey` from
 * the previous page; callers should treat it as a black box.
 */
export interface ListAuditOptions {
  /** Page size cap. Defaults to 100; UI route clamps to 1000. */
  limit?: number;
  /** ISO timestamp — only return rows with `timestamp >= since`. */
  since?: string;
  /** ISO timestamp — only return rows with `timestamp <= until`. */
  until?: string;
  /** Filter by exact action types. Multiple → IN-style filter. */
  actions?: AuditAction[];
  /** Opaque pagination token (base64 of DDB's LastEvaluatedKey). */
  cursor?: string;
}

export interface ListAuditResult {
  items: StoredAuditEvent[];
  /** Present iff DDB returned a LastEvaluatedKey. */
  nextCursor?: string;
}

export interface SearchAuditOptions extends ListAuditOptions {
  /** Required when `pipelineId` is absent. */
  actor?: string;
  /** Required when `actor` is absent. */
  pipelineId?: string;
}

/** Encode/decode a DDB LastEvaluatedKey as an opaque base64 cursor. */
function encodeCursor(key: Record<string, unknown> | undefined): string | undefined {
  if (!key) return undefined;
  return Buffer.from(JSON.stringify(key), 'utf8').toString('base64');
}

function decodeCursor(token: string | undefined): Record<string, unknown> | undefined {
  if (!token) return undefined;
  try {
    return JSON.parse(Buffer.from(token, 'base64').toString('utf8')) as Record<string, unknown>;
  } catch {
    return undefined;
  }
}

/** Build a (FilterExpression, ExpressionAttributeNames, ExpressionAttributeValues) triplet for action-list filtering. */
function buildActionFilter(
  actions: AuditAction[] | undefined,
  values: Record<string, unknown>,
): { filter?: string; names?: Record<string, string> } {
  if (!actions || actions.length === 0) return {};
  const placeholders = actions.map((a, i) => {
    const k = `:act${i}`;
    values[k] = a;
    return k;
  });
  return {
    filter: `#action IN (${placeholders.join(', ')})`,
    names: { '#action': 'action' },
  };
}

export class AuditRepository {
  private store: BaseRepository;
  private ttlSeconds: number;
  private now: () => number;

  constructor(
    private docClient: DynamoDBDocumentClient,
    options: AuditRepositoryOptions = {},
  ) {
    this.store = new BaseRepository(PIPELINE_AUDIT_TABLE, docClient);
    this.ttlSeconds = (options.ttlDays ?? DEFAULT_TTL_DAYS) * SECONDS_PER_DAY;
    this.now = options.now ?? (() => Date.now());
  }

  /**
   * Append an event to the audit log. Generates a fresh ULID `auditId`,
   * an ISO 8601 `timestamp`, and a `ttlEpoch` (unix-seconds, now + 30d).
   * Returns void — callers that need the id should generate it themselves
   * and use a different API.
   */
  async record(event: AuditEvent): Promise<void> {
    const auditId = ulid();
    const nowMs = this.now();
    const timestamp = new Date(nowMs).toISOString();
    const ttlEpoch = Math.floor(nowMs / 1000) + this.ttlSeconds;

    const item: StoredAuditEvent = {
      auditId,
      timestamp,
      ttlEpoch,
      action: event.action,
      actorUserId: event.actorUserId,
      pipelineId: event.pipelineId,
      ...(event.runId ? { runId: event.runId } : {}),
      ...(event.decision ? { decision: event.decision } : {}),
      ...(event.outcome ? { outcome: event.outcome } : {}),
      ...(event.details ? { details: event.details } : {}),
      ...(event.before ? { before: event.before } : {}),
      ...(event.after ? { after: event.after } : {}),
      ...(event.trace ? { trace: event.trace } : {}),
    };

    return this.store.putItem(item as unknown as Record<string, unknown>);
  }

  /**
   * List audit events authored by `userId`, newest first. Backwards-compatible
   * with the original `(userId, limit)` signature; pass an options object to
   * use pagination + time/action filters and receive a `ListAuditResult`.
   */
  listByActor(userId: string, limit?: number): Promise<StoredAuditEvent[]>;
  listByActor(userId: string, options: ListAuditOptions): Promise<ListAuditResult>;
  async listByActor(
    userId: string,
    limitOrOptions: number | ListAuditOptions = 100,
  ): Promise<StoredAuditEvent[] | ListAuditResult> {
    if (typeof limitOrOptions === 'number') {
      const items = await this.queryWithFilters(ACTOR_TIME_INDEX, 'actorUserId', userId, {
        limit: limitOrOptions,
      });
      return items.items;
    }
    return this.queryWithFilters(ACTOR_TIME_INDEX, 'actorUserId', userId, limitOrOptions);
  }

  /**
   * List audit events for a given pipeline, newest first. Used to render
   * the per-pipeline activity timeline in the UI.
   */
  listByPipeline(pipelineId: string, limit?: number): Promise<StoredAuditEvent[]>;
  listByPipeline(
    pipelineId: string,
    options: ListAuditOptions,
  ): Promise<ListAuditResult>;
  async listByPipeline(
    pipelineId: string,
    limitOrOptions: number | ListAuditOptions = 100,
  ): Promise<StoredAuditEvent[] | ListAuditResult> {
    if (typeof limitOrOptions === 'number') {
      const items = await this.queryWithFilters(
        PIPELINE_TIME_INDEX,
        'pipelineId',
        pipelineId,
        { limit: limitOrOptions },
      );
      return items.items;
    }
    return this.queryWithFilters(PIPELINE_TIME_INDEX, 'pipelineId', pipelineId, limitOrOptions);
  }

  /**
   * List audit events for a single run, newest first. The most common
   * operator query — "show me the audit trail for run X" — and the primary
   * source for the run-detail Executor Log tab (hub#2225).
   */
  listByRun(runId: string, limit?: number): Promise<StoredAuditEvent[]>;
  listByRun(runId: string, options: ListAuditOptions): Promise<ListAuditResult>;
  async listByRun(
    runId: string,
    limitOrOptions: number | ListAuditOptions = 100,
  ): Promise<StoredAuditEvent[] | ListAuditResult> {
    if (typeof limitOrOptions === 'number') {
      const items = await this.queryWithFilters(RUN_TIME_INDEX, 'runId', runId, {
        limit: limitOrOptions,
      });
      return items.items;
    }
    return this.queryWithFilters(RUN_TIME_INDEX, 'runId', runId, limitOrOptions);
  }

  /**
   * Cross-run search backed by either the actor-time GSI or pipeline-time
   * GSI (whichever filter is supplied). Caller must pass at least one of
   * `actor` / `pipelineId`. Used by the global audit search page.
   */
  async search(options: SearchAuditOptions): Promise<ListAuditResult> {
    if (!options.actor && !options.pipelineId) {
      throw new Error('search requires at least one of actor or pipelineId');
    }
    if (options.pipelineId) {
      return this.queryWithFilters(
        PIPELINE_TIME_INDEX,
        'pipelineId',
        options.pipelineId,
        options,
      );
    }
    return this.queryWithFilters(
      ACTOR_TIME_INDEX,
      'actorUserId',
      options.actor as string,
      options,
    );
  }

  /**
   * Internal — wraps a GSI query with optional time-range KeyConditionExpression
   * + action FilterExpression + cursor pagination. Returns newest-first.
   */
  private async queryWithFilters(
    indexName: string,
    keyAttr: string,
    keyValue: string,
    options: ListAuditOptions,
  ): Promise<ListAuditResult> {
    const limit = Math.max(1, Math.min(options.limit ?? 100, 1000));

    // Pick a per-attribute placeholder so existing call-site assertions
    // (`:rid`, `:pid`, `:uid` from audit-repository.test.ts) keep passing.
    const placeholder =
      keyAttr === 'runId' ? ':rid'
        : keyAttr === 'pipelineId' ? ':pid'
        : keyAttr === 'actorUserId' ? ':uid'
        : ':k';

    const exprValues: Record<string, unknown> = { [placeholder]: keyValue };
    let keyCond = `${keyAttr} = ${placeholder}`;
    if (options.since && options.until) {
      keyCond += ' AND #ts BETWEEN :since AND :until';
      exprValues[':since'] = options.since;
      exprValues[':until'] = options.until;
    } else if (options.since) {
      keyCond += ' AND #ts >= :since';
      exprValues[':since'] = options.since;
    } else if (options.until) {
      keyCond += ' AND #ts <= :until';
      exprValues[':until'] = options.until;
    }
    const exprNames: Record<string, string> = {};
    if (options.since || options.until) exprNames['#ts'] = 'timestamp';

    const actionFilter = buildActionFilter(options.actions, exprValues);
    if (actionFilter.names) Object.assign(exprNames, actionFilter.names);

    const startKey = decodeCursor(options.cursor);

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: PIPELINE_AUDIT_TABLE,
        IndexName: indexName,
        KeyConditionExpression: keyCond,
        ExpressionAttributeValues: exprValues,
        ...(Object.keys(exprNames).length > 0 ? { ExpressionAttributeNames: exprNames } : {}),
        ...(actionFilter.filter ? { FilterExpression: actionFilter.filter } : {}),
        ScanIndexForward: false, // newest first
        Limit: limit,
        ...(startKey ? { ExclusiveStartKey: startKey } : {}),
      }),
    );
    return {
      items: (result.Items ?? []) as StoredAuditEvent[],
      ...(result.LastEvaluatedKey
        ? { nextCursor: encodeCursor(result.LastEvaluatedKey as Record<string, unknown>) }
        : {}),
    };
  }

  /**
   * Belt-and-suspenders sweeper for environments where DDB native TTL is
   * disabled (DDB-local does not honor TTL — only AWS DynamoDB does). Scans
   * the table for rows whose `ttlEpoch` is in the past, deletes them, and
   * returns the count.
   *
   * Bounded by `maxItems` so a runaway sweep can't stall the event loop
   * against a backlog. Operators tune this via env (see bootstrap.ts).
   */
  async sweepExpired(maxItems = 1000): Promise<number> {
    const nowEpoch = Math.floor(this.now() / 1000);
    let deleted = 0;
    let exclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const result = await this.docClient.send(
        new ScanCommand({
          TableName: PIPELINE_AUDIT_TABLE,
          FilterExpression: 'ttlEpoch < :now',
          ExpressionAttributeValues: { ':now': nowEpoch },
          // Only need the PK to issue the delete.
          ProjectionExpression: 'auditId',
          Limit: Math.min(100, maxItems - deleted),
          ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
        }),
      );
      const items = (result.Items ?? []) as Array<{ auditId: string }>;
      for (const it of items) {
        await this.docClient.send(
          new DeleteCommand({
            TableName: PIPELINE_AUDIT_TABLE,
            Key: { auditId: it.auditId },
          }),
        );
        deleted += 1;
        if (deleted >= maxItems) break;
      }
      exclusiveStartKey = result.LastEvaluatedKey as
        | Record<string, unknown>
        | undefined;
    } while (exclusiveStartKey && deleted < maxItems);
    return deleted;
  }
}

/** Singleton instance — shares the same docClient as the other repositories. */
export const auditRepo = new AuditRepository(docClient);
