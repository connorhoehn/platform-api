/**
 * HangoutRoomMemberRepository — explicit membership for private hangout rooms.
 *
 * Org-visible rooms don't need a membership row (every user in the org sees
 * them). Private rooms do — their access check is membership-table lookup.
 *
 * Table `hangout-room-members`:
 *   PK: roomId
 *   SK: userId
 *
 *   GSI: userId-roomId-index   (PK userId, SK roomId)
 *     → for "list rooms I'm a member of" on the sidebar.
 */

import {
  DynamoDBDocumentClient,
  QueryCommand,
} from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export const HANGOUT_ROOM_MEMBERS_TABLE = tableName('hangout-room-members');

export type HangoutRoomMemberRole = 'owner' | 'member';

export interface HangoutRoomMember {
  roomId: string;
  userId: string;
  role: HangoutRoomMemberRole;
  addedBy: string;
  addedAt: string;
}

export interface ListRoomsForUserOpts {
  limit?: number;
  cursor?: string;
}

export interface ListRoomsForUserResult {
  items: HangoutRoomMember[];
  cursor?: string;
}

export class HangoutRoomMemberRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(HANGOUT_ROOM_MEMBERS_TABLE, docClient);
  }

  async addMember(
    roomId: string,
    userId: string,
    role: HangoutRoomMemberRole,
    addedBy: string,
  ): Promise<HangoutRoomMember> {
    const member: HangoutRoomMember = {
      roomId,
      userId,
      role,
      addedBy,
      addedAt: new Date().toISOString(),
    };
    await this.putItem(member as unknown as Record<string, unknown>);
    return member;
  }

  async removeMember(roomId: string, userId: string): Promise<void> {
    await this.deleteItem({ roomId, userId });
  }

  async listMembers(roomId: string): Promise<HangoutRoomMember[]> {
    return this.query<HangoutRoomMember>({
      KeyConditionExpression: 'roomId = :rid',
      ExpressionAttributeValues: { ':rid': roomId },
    });
  }

  async isMember(roomId: string, userId: string): Promise<boolean> {
    const item = await this.getItem<HangoutRoomMember>({ roomId, userId });
    return item !== null;
  }

  /**
   * Rooms a given user belongs to, via the reverse GSI. Used to compose
   * the sidebar list with the org-visible rooms.
   */
  async listRoomsForUser(
    userId: string,
    opts: ListRoomsForUserOpts = {},
  ): Promise<ListRoomsForUserResult> {
    const limit = opts.limit ?? 100;

    let exclusiveStartKey: Record<string, unknown> | undefined;
    if (opts.cursor) {
      try {
        exclusiveStartKey = JSON.parse(Buffer.from(opts.cursor, 'base64url').toString('utf8'));
      } catch {
        exclusiveStartKey = undefined;
      }
    }

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: HANGOUT_ROOM_MEMBERS_TABLE,
        IndexName: 'userId-roomId-index',
        KeyConditionExpression: 'userId = :u',
        ExpressionAttributeValues: { ':u': userId },
        Limit: limit,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }),
    );

    const items = (result.Items ?? []) as HangoutRoomMember[];
    const cursor = result.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(result.LastEvaluatedKey), 'utf8').toString('base64url')
      : undefined;

    return cursor ? { items, cursor } : { items };
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh:
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}hangout-room-members \
 *     --attribute-definitions \
 *       AttributeName=roomId,AttributeType=S \
 *       AttributeName=userId,AttributeType=S \
 *     --key-schema \
 *       AttributeName=roomId,KeyType=HASH \
 *       AttributeName=userId,KeyType=RANGE \
 *     --global-secondary-indexes \
 *       'IndexName=userId-roomId-index,KeySchema=[...],Projection=ALL' \
 *     --billing-mode PAY_PER_REQUEST
 */
