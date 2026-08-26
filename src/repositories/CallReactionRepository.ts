/**
 * CallReactionRepository — persists in-call emoji reactions drained from
 * the durable `call:reaction.recorded` event (producer: websocket-gateway).
 *
 * Table: `call-reactions`
 *   - PK: `lobbyKey` (S) = `${tenant}|${lobbyName}` — the lobby identifier
 *     the video-session and recording rows carry, TENANT-SCOPED per the
 *     convention in lib/tenant.ts. The tenant is in the partition key, not
 *     an attribute, so a query for one tenant physically cannot return
 *     another's rows — there is no filter to forget.
 *   - SK: `sk` = `${at}#${reactionId}` (S) — ISO timestamp prefix keeps
 *     reactions naturally time-ordered; the reactionId suffix makes
 *     redelivered envelopes overwrite the same row (idempotent PutItem).
 *   - Attributes: reactionId, channel, userId, displayName, emoji,
 *     effect, at, ttl (90-day epoch seconds).
 *
 * Query pattern: listByLobby(lobbyName, fromIso?, toIso?) — BETWEEN on
 * the sort key over a recording's [startedAt, endedAt] window, ascending.
 */

import { DynamoDBDocumentClient, QueryCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import { scopedKey, type TenantId } from '../lib/tenant';

export const CALL_REACTIONS_TABLE = tableName('call-reactions');

const REACTION_RETENTION_DAYS = 90;

/** Payload shape of `call:reaction.recorded` (see src/lib/event-catalog.ts). */
export interface CallReactionInput {
  /** Owning tenant. Required — see lib/tenant.ts. */
  tenant: TenantId;
  reactionId: string;
  channel: string;
  lobbyName: string;
  userId?: string | null;
  displayName?: string;
  emoji: string;
  effect?: string;
  at: string; // ISO
}

export interface CallReaction extends CallReactionInput {
  /** Tenant-scoped partition key: `${tenant}|${lobbyName}`. */
  lobbyKey: string;
  /** Composite sort key: `${at}#${reactionId}`. */
  sk: string;
  ttl: number; // epoch seconds
}

export class CallReactionRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(CALL_REACTIONS_TABLE, docClient);
  }

  /**
   * Persist one reaction. Idempotent — the sort key is derived from
   * (at, reactionId), so a redelivered envelope overwrites the same row.
   */
  async putReaction(input: CallReactionInput): Promise<void> {
    const ttl = Math.floor(Date.now() / 1000) + REACTION_RETENTION_DAYS * 86400;
    const item: CallReaction = {
      lobbyKey: scopedKey(input.tenant, input.lobbyName),
      tenant: input.tenant,
      reactionId: input.reactionId,
      channel: input.channel,
      lobbyName: input.lobbyName,
      userId: input.userId ?? null,
      displayName: input.displayName,
      emoji: input.emoji,
      effect: input.effect,
      at: input.at,
      sk: `${input.at}#${input.reactionId}`,
      ttl,
    };
    await this.putItem(item as unknown as Record<string, unknown>);
  }

  /**
   * Reactions for one tenant's lobby, ascending by (at, reactionId).
   * The tenant is part of the partition key, so cross-tenant reads are
   * impossible by construction rather than by a filter. When from/to
   * are given, BETWEEN on the sort key. `to` is padded with `#￿`
   * so reactions stamped exactly at `toIso` (whose sk carries the
   * `#reactionId` suffix) are included in the window.
   */
  async listByLobby(
    tenant: TenantId,
    lobbyName: string,
    fromIso?: string,
    toIso?: string,
    limit = 500,
  ): Promise<CallReaction[]> {
    let keyCondition = 'lobbyKey = :l';
    const values: Record<string, unknown> = { ':l': scopedKey(tenant, lobbyName) };
    if (fromIso && toIso) {
      keyCondition += ' AND sk BETWEEN :from AND :to';
      values[':from'] = fromIso;
      values[':to'] = `${toIso}#￿`;
    } else if (fromIso) {
      keyCondition += ' AND sk >= :from';
      values[':from'] = fromIso;
    } else if (toIso) {
      keyCondition += ' AND sk <= :to';
      values[':to'] = `${toIso}#￿`;
    }
    const out = await this.docClient.send(
      new QueryCommand({
        TableName: CALL_REACTIONS_TABLE,
        KeyConditionExpression: keyCondition,
        ExpressionAttributeValues: values,
        ScanIndexForward: true, // ascending by at
        Limit: Math.max(1, Math.min(500, limit)),
      }),
    );
    return (out.Items ?? []) as CallReaction[];
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh (realtime-examples repo):
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}call-reactions \
 *     --attribute-definitions \
 *       AttributeName=lobbyKey,AttributeType=S \
 *       AttributeName=sk,AttributeType=S \
 *     --key-schema \
 *       AttributeName=lobbyKey,KeyType=HASH \
 *       AttributeName=sk,KeyType=RANGE \
 *     --billing-mode PAY_PER_REQUEST
 */
