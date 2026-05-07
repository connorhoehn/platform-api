/**
 * OutboxPublisher — atomically writes a primary domain item and its outbox row
 * in a single DynamoDB transaction. Used by routes that emit social events
 * reliably (posts, reactions, follows, ...).
 *
 * The caller supplies the target Put (including any ConditionExpression) plus
 * the logical event metadata. The helper constructs the outbox row in the exact
 * shape already persisted in `social-outbox` — do NOT change column names here,
 * existing data relies on them.
 *
 * TransactionCanceledException is translated into ConflictError (409) by
 * default, but callers can supply a custom `conflictMessage` to match existing
 * user-facing strings (e.g. 'Already reacted. Delete your existing reaction first.').
 *
 * For cases where the cancellation could be caused by something other than the
 * caller's ConditionExpression, re-throw from the caller after inspecting
 * `err.CancellationReasons`.
 */
import { TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import type { TransactWriteCommandInput } from '@aws-sdk/lib-dynamodb';
import { TransactionCanceledException } from '@aws-sdk/client-dynamodb';
import { ulid } from 'ulid';
import { docClient } from '../lib/aws-clients';
import { ConflictError } from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const OUTBOX_TABLE = tableName('social-outbox');

export interface TargetPut {
  TableName: string;
  Item: Record<string, unknown>;
  ConditionExpression?: string;
  ExpressionAttributeNames?: Record<string, string>;
  ExpressionAttributeValues?: Record<string, unknown>;
}

export interface PublishWithOutboxParams {
  /** The primary Put (e.g. the post, follow, or reaction row). */
  target: TargetPut;
  /** EventBridge-style detail type (e.g. 'social.post.created'). */
  eventType: string;
  /** Downstream queue name (matches column shape already in social-outbox). */
  queueName: string;
  /** Free-form event payload — will be JSON-stringified into the outbox row. */
  eventPayload: Record<string, unknown>;
  /** Optional user-facing message when the target's ConditionExpression fails. */
  conflictMessage?: string;
}

/**
 * Atomically write a target item + outbox row. On ConditionalCheckFailed
 * throws a ConflictError (handled by the central error middleware).
 */
export async function publishWithOutbox(params: PublishWithOutboxParams): Promise<void> {
  const { target, eventType, queueName, eventPayload, conflictMessage } = params;

  const now = new Date().toISOString();
  const outboxId = ulid();

  const outboxItem: Record<string, unknown> = {
    outboxId,
    status: 'UNPROCESSED',
    eventType,
    queueName,
    payload: JSON.stringify({ ...eventPayload, timestamp: now }),
    createdAt: now,
  };

  const input: TransactWriteCommandInput = {
    TransactItems: [
      {
        Put: {
          TableName: target.TableName,
          Item: target.Item,
          ...(target.ConditionExpression ? { ConditionExpression: target.ConditionExpression } : {}),
          ...(target.ExpressionAttributeNames ? { ExpressionAttributeNames: target.ExpressionAttributeNames } : {}),
          ...(target.ExpressionAttributeValues ? { ExpressionAttributeValues: target.ExpressionAttributeValues } : {}),
        },
      },
      {
        Put: {
          TableName: OUTBOX_TABLE,
          Item: outboxItem,
        },
      },
    ],
  };

  try {
    await docClient.send(new TransactWriteCommand(input));
  } catch (err) {
    if (err instanceof TransactionCanceledException) {
      const reasons = err.CancellationReasons ?? [];
      if (reasons[0]?.Code === 'ConditionalCheckFailed') {
        throw new ConflictError(conflictMessage ?? 'Conflict');
      }
    }
    throw err;
  }
}
