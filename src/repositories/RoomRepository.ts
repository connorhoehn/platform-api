import { DynamoDBDocumentClient, BatchGetCommand, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface RoomItem {
  roomId: string;
  channelId: string;
  name: string;
  type: 'standalone' | 'group' | 'dm';
  ownerId: string;
  groupId?: string;
  dmPeerUserId?: string;
  createdAt: string;
  updatedAt: string;
}

export interface RoomMemberItem {
  roomId: string;
  userId: string;
  role: 'owner' | 'member';
  joinedAt: string;
}

const ROOMS_TABLE = tableName('social-rooms');
const ROOM_MEMBERS_TABLE = tableName('social-room-members');
const OUTBOX_TABLE = tableName('social-outbox');

export class RoomRepository {
  private rooms: BaseRepository;
  private members: BaseRepository;

  constructor(private docClient: DynamoDBDocumentClient) {
    this.rooms = new BaseRepository(ROOMS_TABLE, docClient);
    this.members = new BaseRepository(ROOM_MEMBERS_TABLE, docClient);
  }

  // --- Room CRUD ---

  async getRoom(roomId: string): Promise<RoomItem | null> {
    return this.rooms.getItem<RoomItem>({ roomId });
  }

  async createRoom(item: RoomItem): Promise<void> {
    return this.rooms.putItem(item as unknown as Record<string, unknown>);
  }

  async createRoomConditional(item: RoomItem): Promise<void> {
    return this.rooms.putItemConditional(
      item as unknown as Record<string, unknown>,
      'attribute_not_exists(roomId)',
    );
  }

  // --- Membership ---

  async getMembership(roomId: string, userId: string): Promise<RoomMemberItem | null> {
    return this.members.getItem<RoomMemberItem>({ roomId, userId });
  }

  async isMember(roomId: string, userId: string): Promise<boolean> {
    const item = await this.getMembership(roomId, userId);
    return item !== null;
  }

  async addMember(item: RoomMemberItem): Promise<void> {
    return this.members.putItem(item as unknown as Record<string, unknown>);
  }

  async removeMember(roomId: string, userId: string): Promise<void> {
    return this.members.deleteItem({ roomId, userId });
  }

  async getRoomMembers(roomId: string): Promise<RoomMemberItem[]> {
    return this.members.query<RoomMemberItem>({
      KeyConditionExpression: 'roomId = :rid',
      ExpressionAttributeValues: { ':rid': roomId },
    });
  }

  /**
   * List all rooms a user belongs to.
   * Uses GSI userId-roomId-index on social-room-members to avoid full-table Scan.
   */
  async getRoomsByUser(userId: string): Promise<RoomItem[]> {
    const { QueryCommand } = await import('@aws-sdk/lib-dynamodb');
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: ROOM_MEMBERS_TABLE,
        IndexName: 'userId-roomId-index',
        KeyConditionExpression: 'userId = :uid',
        ExpressionAttributeValues: { ':uid': userId },
      }),
    );

    const memberships = result.Items ?? [];
    const roomIds = memberships.map((m) => m['roomId'] as string);

    if (roomIds.length === 0) return [];

    // Batch-get room details
    const batchResult = await this.docClient.send(
      new BatchGetCommand({
        RequestItems: {
          [ROOMS_TABLE]: {
            Keys: roomIds.map((id) => ({ roomId: id })),
          },
        },
      }),
    );

    const rooms = (batchResult.Responses?.[ROOMS_TABLE] ?? []) as RoomItem[];

    // Merge membership role
    const roleMap = new Map(
      memberships.map((m) => [m['roomId'] as string, m['role'] as string]),
    );

    return rooms.map((room) => ({
      ...room,
      role: roleMap.get(room.roomId) ?? 'member',
    })) as RoomItem[];
  }

  /**
   * Atomic member addition with outbox record (transactional write).
   */
  async addMemberWithOutbox(
    memberItem: RoomMemberItem,
    outboxItem: Record<string, unknown>,
  ): Promise<void> {
    await this.docClient.send(
      new TransactWriteCommand({
        TransactItems: [
          { Put: { TableName: ROOM_MEMBERS_TABLE, Item: memberItem as unknown as Record<string, unknown> } },
          { Put: { TableName: OUTBOX_TABLE, Item: outboxItem } },
        ],
      }),
    );
  }
}
