import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';

export interface ProfileItem {
  userId: string;
  displayName: string;
  bio: string;
  avatarUrl: string;
  visibility: 'public' | 'private';
  createdAt: string;
  updatedAt: string;
}

export class ProfileRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(tableName('social-profiles'), docClient);
  }

  async getProfile(userId: string): Promise<ProfileItem | null> {
    return this.getItem<ProfileItem>({ userId });
  }

  async createProfile(item: ProfileItem): Promise<void> {
    return this.putItem(item as unknown as Record<string, unknown>);
  }

  async updateProfile(
    userId: string,
    updates: Partial<Pick<ProfileItem, 'displayName' | 'bio' | 'avatarUrl' | 'visibility'>>,
  ): Promise<ProfileItem> {
    const expressions: string[] = [];
    const exprValues: Record<string, unknown> = {};

    if (updates.displayName !== undefined) {
      expressions.push('displayName = :displayName');
      exprValues[':displayName'] = updates.displayName;
    }
    if (updates.bio !== undefined) {
      expressions.push('bio = :bio');
      exprValues[':bio'] = updates.bio;
    }
    if (updates.avatarUrl !== undefined) {
      expressions.push('avatarUrl = :avatarUrl');
      exprValues[':avatarUrl'] = updates.avatarUrl;
    }
    if (updates.visibility !== undefined) {
      expressions.push('visibility = :visibility');
      exprValues[':visibility'] = updates.visibility;
    }

    const now = new Date().toISOString();
    expressions.push('updatedAt = :updatedAt');
    exprValues[':updatedAt'] = now;

    const result = await this.updateItem({
      Key: { userId },
      UpdateExpression: 'SET ' + expressions.join(', '),
      ExpressionAttributeValues: exprValues,
      ReturnValues: 'ALL_NEW',
    });

    return result as unknown as ProfileItem;
  }

  async batchGetProfiles(userIds: string[]): Promise<ProfileItem[]> {
    if (userIds.length === 0) return [];

    // Use BatchGetCommand directly for multi-key lookups
    const { BatchGetCommand } = await import('@aws-sdk/lib-dynamodb');
    const result = await this.docClient.send(
      new BatchGetCommand({
        RequestItems: {
          [this.tableName]: {
            Keys: userIds.map((uid) => ({ userId: uid })),
          },
        },
      }),
    );

    return (result.Responses?.[this.tableName] ?? []) as ProfileItem[];
  }

  /**
   * Case-insensitive substring search across displayName.
   *
   * Phase 1 implementation: full-table Scan with a lowercase-contains filter.
   * This is acceptable for small user bases but MUST be replaced with a GSI
   * (e.g. on a normalized `displayNameLower` attribute) before the profiles
   * table grows — Scan cost scales with total item count, not match count.
   *
   * Note: DynamoDB has no built-in case-insensitive contains; we pull a capped
   * page of items and filter in memory. `limit` bounds the returned matches,
   * not the items scanned.
   */
  async searchProfiles(query: string, limit: number): Promise<ProfileItem[]> {
    const trimmed = query.trim();
    if (!trimmed) return [];

    const { ScanCommand } = await import('@aws-sdk/lib-dynamodb');
    const needle = trimmed.toLowerCase();

    // Cap the Scan page size to keep Phase 1 costs bounded. At scale this
    // should be replaced with a GSI query on a normalized name attribute.
    const SCAN_LIMIT = 500;

    const result = await this.docClient.send(
      new ScanCommand({
        TableName: this.tableName,
        Limit: SCAN_LIMIT,
      }),
    );

    const items = (result.Items ?? []) as ProfileItem[];

    const matches = items.filter((item) => {
      const dn = typeof item.displayName === 'string' ? item.displayName.toLowerCase() : '';
      const uid = typeof item.userId === 'string' ? item.userId.toLowerCase() : '';
      return dn.includes(needle) || uid.includes(needle);
    });

    return matches.slice(0, limit);
  }
}
