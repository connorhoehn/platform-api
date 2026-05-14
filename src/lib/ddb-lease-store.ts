// platform-api/src/lib/ddb-lease-store.ts
//
// DDB adapter for distributed-core's LeaseStore interface (v0.18.2).
// Wraps a single DynamoDB row's conditional-write surface so that
// LeasedKVLock — DC's generic lease primitive — can do its leader-elect
// work without DC importing the AWS SDK.
//
// All four lock sites in this repo (scheduler-leader-lock, outbox-drainer
// lease, ingest engine schedule timer, scheduleEvaluator) consolidate
// onto this one adapter + LeasedKVLock instances pointing at distinct
// `outboxId` row keys in the social-outbox table.
//
// Conditions the store enforces (matches the inline impls these replaced):
//   - tryClaim:      attribute_not_exists(outboxId) OR expiresAt < :now
//   - renewIfOwner:  #owner = :owner   (throws on not-owner / not-found)
//   - releaseIfOwner: #owner = :owner   (silent on not-owner; lease will TTL)

import {
  PutCommand,
  UpdateCommand,
  DeleteCommand,
  type DynamoDBDocumentClient,
} from '@aws-sdk/lib-dynamodb';
import type { LeaseStore } from 'distributed-core';

export interface DDBLeaseStoreOptions {
  docClient: DynamoDBDocumentClient;
  tableName: string;
  /**
   * The DDB primary-key attribute name. Defaults to `outboxId` because the
   * existing platform-api lease rows (scheduler-leader-lock, outbox-drainer,
   * ingest schedule lock) all live in the social-outbox table whose PK is
   * `outboxId`. New tables can pass a different name.
   */
  keyAttribute?: string;
}

export function createDDBLeaseStore(opts: DDBLeaseStoreOptions): LeaseStore {
  const { docClient, tableName } = opts;
  const keyAttr = opts.keyAttribute ?? 'outboxId';

  return {
    async tryClaim(key, value): Promise<boolean> {
      try {
        await docClient.send(
          new PutCommand({
            TableName: tableName,
            Item: {
              [keyAttr]: key,
              owner: value.owner,
              expiresAt: value.expiresAt,
            },
            ConditionExpression: `attribute_not_exists(${keyAttr}) OR expiresAt < :now`,
            ExpressionAttributeValues: { ':now': Date.now() },
          }),
        );
        return true;
      } catch (err: unknown) {
        if ((err as { name?: string }).name === 'ConditionalCheckFailedException') return false;
        throw err;
      }
    },

    async renewIfOwner(key, owner, newExpiresAt): Promise<void> {
      await docClient.send(
        new UpdateCommand({
          TableName: tableName,
          Key: { [keyAttr]: key },
          UpdateExpression: 'SET expiresAt = :expiresAt',
          ConditionExpression: '#owner = :owner',
          ExpressionAttributeNames: { '#owner': 'owner' },
          ExpressionAttributeValues: {
            ':expiresAt': newExpiresAt,
            ':owner': owner,
          },
        }),
      );
    },

    async releaseIfOwner(key, owner): Promise<void> {
      try {
        await docClient.send(
          new DeleteCommand({
            TableName: tableName,
            Key: { [keyAttr]: key },
            ConditionExpression: '#owner = :owner',
            ExpressionAttributeNames: { '#owner': 'owner' },
            ExpressionAttributeValues: { ':owner': owner },
          }),
        );
      } catch {
        // Best-effort release; lease auto-expires via TTL if delete fails.
      }
    },
  };
}
