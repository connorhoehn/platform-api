import { DynamoDBDocumentClient, TransactWriteCommand } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface GroupItem {
  groupId: string;
  name: string;
  description: string;
  visibility: 'public' | 'private';
  ownerId: string;
  createdAt: string;
  updatedAt: string;
}

export interface GroupMemberItem {
  groupId: string;
  userId: string;
  role: 'owner' | 'admin' | 'member';
  status?: 'invited' | 'active';
  joinedAt: string;
  invitedAt?: string;
}

const GROUPS_TABLE = tableName('social-groups');
const MEMBERS_TABLE = tableName('social-group-members');

export class GroupRepository {
  private groups: BaseRepository;
  private members: BaseRepository;

  constructor(private docClient: DynamoDBDocumentClient) {
    this.groups = new BaseRepository(GROUPS_TABLE, docClient);
    this.members = new BaseRepository(MEMBERS_TABLE, docClient);
  }

  // --- Group CRUD ---

  async getGroup(groupId: string): Promise<GroupItem | null> {
    return this.groups.getItem<GroupItem>({ groupId });
  }

  async createGroupWithOwner(
    groupItem: GroupItem,
    memberItem: GroupMemberItem,
  ): Promise<void> {
    await this.docClient.send(
      new TransactWriteCommand({
        TransactItems: [
          {
            Put: {
              TableName: GROUPS_TABLE,
              Item: groupItem as unknown as Record<string, unknown>,
              ConditionExpression: 'attribute_not_exists(groupId)',
            },
          },
          {
            Put: {
              TableName: MEMBERS_TABLE,
              Item: memberItem as unknown as Record<string, unknown>,
            },
          },
        ],
      }),
    );
  }

  async deleteGroup(groupId: string): Promise<void> {
    return this.groups.deleteItem({ groupId });
  }

  async updateGroupVisibility(
    groupId: string,
    visibility: 'public' | 'private',
  ): Promise<GroupItem> {
    const result = await this.groups.updateItem({
      Key: { groupId },
      UpdateExpression: 'SET visibility = :v, updatedAt = :u',
      ExpressionAttributeValues: {
        ':v': visibility,
        ':u': new Date().toISOString(),
      },
      ReturnValues: 'ALL_NEW',
    });
    return result as unknown as GroupItem;
  }

  // --- Membership ---

  async getMembership(
    groupId: string,
    userId: string,
  ): Promise<GroupMemberItem | null> {
    return this.members.getItem<GroupMemberItem>({ groupId, userId });
  }

  async addMember(item: GroupMemberItem): Promise<void> {
    return this.members.putItem(item as unknown as Record<string, unknown>);
  }

  async removeMember(groupId: string, userId: string): Promise<void> {
    return this.members.deleteItem({ groupId, userId });
  }

  async updateMemberStatus(
    groupId: string,
    userId: string,
    status: 'active' | 'invited',
    joinedAt?: string,
  ): Promise<void> {
    const exprValues: Record<string, unknown> = { ':s': status };
    let updateExpr = 'SET #s = :s';

    if (joinedAt) {
      updateExpr += ', joinedAt = :j';
      exprValues[':j'] = joinedAt;
    }

    await this.members.updateItem({
      Key: { groupId, userId },
      UpdateExpression: updateExpr,
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: exprValues,
    });
  }

  async getGroupMembers(groupId: string): Promise<GroupMemberItem[]> {
    return this.members.query<GroupMemberItem>({
      KeyConditionExpression: 'groupId = :gid',
      FilterExpression: '#s = :active OR attribute_not_exists(#s)',
      ExpressionAttributeNames: { '#s': 'status' },
      ExpressionAttributeValues: {
        ':gid': groupId,
        ':active': 'active',
      },
    });
  }
}
