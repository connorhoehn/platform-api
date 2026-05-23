/**
 * HangoutRoomRepository — first-class persistent hangout rooms.
 *
 * A "hangout room" is a long-lived, org-scoped video lobby. Unlike the
 * ephemeral VideoSessions (one channel per call), a hangout room is a
 * stable URL (`/rooms/:slug`) that members can drop into and leave at
 * will. The room metadata lives here; the actual live media session is
 * a short-lived VideoSession created on-demand when someone joins.
 *
 * Table `hangout-rooms`:
 *   PK: orgId          — org-scoped multi-tenancy
 *   SK: roomId         — ulid (sortable + unique)
 *
 *   GSI1: orgId-slug-index            (PK orgId, SK slug)
 *     → for "find by slug" + slug-uniqueness enforcement
 *   GSI2: orgId-state-lastActiveAt-index (PK orgId, SK lastActiveAt)
 *     → for sidebar list sorted by recency (ScanIndexForward=false)
 *     NOTE: state is denormalized into the SK key via a filter on the
 *     base attribute; sidebar list calls filter client-side / via
 *     FilterExpression because DDB GSIs only support one SK attribute.
 *
 * On archive: ttl is set (90d) so DDB sweeps the row natively.
 * touchLastActive is fire-and-forget — never throws into the caller.
 */

import {
  DynamoDBDocumentClient,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { ulid } from 'ulid';
import { BaseRepository } from './BaseRepository';
import { tableName } from '../lib/ddb-table-name';
import { AppError } from '../middleware/error-handler';

export const HANGOUT_ROOMS_TABLE = tableName('hangout-rooms');

const ARCHIVE_TTL_SECONDS = 90 * 24 * 60 * 60; // 90 days

export type HangoutRoomVisibility = 'org' | 'private';
export type HangoutRoomState = 'active' | 'archived';

export interface HangoutRoomSettings {
  audioOnly: boolean;
  recordingEnabled: boolean;
  softCap: number;
}

export interface HangoutRoom {
  orgId: string;
  roomId: string;
  slug: string;
  name: string;
  description?: string;
  createdBy: string;
  createdAt: string;
  visibility: HangoutRoomVisibility;
  settings: HangoutRoomSettings;
  state: HangoutRoomState;
  lastActiveAt: string;
  archivedAt?: string;
  ttl?: number;
}

export interface CreateHangoutRoomInput {
  orgId: string;
  slug: string;
  name: string;
  description?: string;
  visibility: HangoutRoomVisibility;
  settings: HangoutRoomSettings;
  createdBy: string;
}

export interface ListRoomsOpts {
  state?: HangoutRoomState;
  limit?: number;
  cursor?: string;
}

export interface ListRoomsResult {
  items: HangoutRoom[];
  cursor?: string;
}

// ---------------------------------------------------------------------------
// Error types — surfaced by the router as their respective HTTP codes via
// the central error-handler middleware.
// ---------------------------------------------------------------------------

export class RoomSlugConflictError extends AppError {
  constructor(slug: string) {
    super(409, `Room slug already exists: ${slug}`);
  }
}

export class RoomNotFoundError extends AppError {
  constructor(slug: string) {
    super(404, `Room not found: ${slug}`);
  }
}

export class RoomAccessDeniedError extends AppError {
  constructor(slug: string) {
    super(403, `Access denied to room: ${slug}`);
  }
}

export class RoomArchivedError extends AppError {
  constructor(slug: string) {
    super(410, `Room is archived: ${slug}`);
  }
}

export class HangoutRoomRepository extends BaseRepository {
  constructor(docClient: DynamoDBDocumentClient) {
    super(HANGOUT_ROOMS_TABLE, docClient);
  }

  /**
   * Create a room. The slug-uniqueness check is performed up-front via the
   * GSI (orgId, slug). There's a theoretical race window between the lookup
   * and the put; we accept it because (a) router-level retry-with-suffix
   * makes practical collisions vanishingly rare, and (b) DDB has no way to
   * enforce a GSI uniqueness constraint atomically without a TransactWrite
   * against a sentinel item — overkill for this surface.
   */
  async createRoom(input: CreateHangoutRoomInput): Promise<HangoutRoom> {
    // Slug-uniqueness check via GSI
    const existing = await this.getRoomBySlug(input.orgId, input.slug);
    if (existing) {
      throw new RoomSlugConflictError(input.slug);
    }

    const now = new Date().toISOString();
    const room: HangoutRoom = {
      orgId: input.orgId,
      roomId: ulid(),
      slug: input.slug,
      name: input.name,
      ...(input.description !== undefined ? { description: input.description } : {}),
      createdBy: input.createdBy,
      createdAt: now,
      visibility: input.visibility,
      settings: input.settings,
      state: 'active',
      lastActiveAt: now,
    };

    await this.putItem(room as unknown as Record<string, unknown>);
    return room;
  }

  async getRoom(orgId: string, roomId: string): Promise<HangoutRoom | null> {
    return this.getItem<HangoutRoom>({ orgId, roomId });
  }

  async getRoomBySlug(orgId: string, slug: string): Promise<HangoutRoom | null> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: HANGOUT_ROOMS_TABLE,
        IndexName: 'orgId-slug-index',
        KeyConditionExpression: 'orgId = :o AND slug = :s',
        ExpressionAttributeValues: { ':o': orgId, ':s': slug },
        Limit: 1,
      }),
    );
    const items = (result.Items ?? []) as HangoutRoom[];
    return items[0] ?? null;
  }

  /**
   * List rooms in an org, newest-active-first. The GSI keys on
   * (orgId, lastActiveAt) so we walk it in reverse for recency ordering;
   * state filtering happens via FilterExpression to avoid a second GSI.
   *
   * Cursor is the base64url-encoded LastEvaluatedKey from a prior page.
   */
  async listRoomsByOrg(orgId: string, opts: ListRoomsOpts = {}): Promise<ListRoomsResult> {
    const limit = opts.limit ?? 50;
    const stateFilter = opts.state ?? 'active';

    let exclusiveStartKey: Record<string, unknown> | undefined;
    if (opts.cursor) {
      try {
        exclusiveStartKey = JSON.parse(Buffer.from(opts.cursor, 'base64url').toString('utf8'));
      } catch {
        // Invalid cursor — start from the beginning rather than error
        exclusiveStartKey = undefined;
      }
    }

    const result = await this.docClient.send(
      new QueryCommand({
        TableName: HANGOUT_ROOMS_TABLE,
        IndexName: 'orgId-state-lastActiveAt-index',
        KeyConditionExpression: 'orgId = :o',
        FilterExpression: '#st = :st',
        ExpressionAttributeNames: { '#st': 'state' },
        ExpressionAttributeValues: { ':o': orgId, ':st': stateFilter },
        ScanIndexForward: false, // newest first
        Limit: limit,
        ...(exclusiveStartKey ? { ExclusiveStartKey: exclusiveStartKey } : {}),
      }),
    );

    const items = (result.Items ?? []) as HangoutRoom[];
    const cursor = result.LastEvaluatedKey
      ? Buffer.from(JSON.stringify(result.LastEvaluatedKey), 'utf8').toString('base64url')
      : undefined;

    return cursor ? { items, cursor } : { items };
  }

  /**
   * Patch a room. Only mutable fields are accepted; identity columns
   * (orgId, roomId, slug, createdBy, createdAt) cannot be changed.
   */
  async updateRoom(
    orgId: string,
    roomId: string,
    partial: {
      name?: string;
      description?: string;
      visibility?: HangoutRoomVisibility;
      settings?: HangoutRoomSettings;
    },
  ): Promise<HangoutRoom> {
    const sets: string[] = [];
    const names: Record<string, string> = {};
    const values: Record<string, unknown> = {};

    if (partial.name !== undefined) {
      sets.push('#n = :n');
      names['#n'] = 'name';
      values[':n'] = partial.name;
    }
    if (partial.description !== undefined) {
      sets.push('description = :d');
      values[':d'] = partial.description;
    }
    if (partial.visibility !== undefined) {
      sets.push('visibility = :v');
      values[':v'] = partial.visibility;
    }
    if (partial.settings !== undefined) {
      sets.push('settings = :s');
      values[':s'] = partial.settings;
    }

    // Always bump lastActiveAt on a write to keep the recency sort honest.
    const now = new Date().toISOString();
    sets.push('lastActiveAt = :la');
    values[':la'] = now;

    const result = await this.docClient.send(
      new UpdateCommand({
        TableName: HANGOUT_ROOMS_TABLE,
        Key: { orgId, roomId },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ...(Object.keys(names).length > 0 ? { ExpressionAttributeNames: names } : {}),
        ExpressionAttributeValues: values,
        ConditionExpression: 'attribute_exists(roomId)',
        ReturnValues: 'ALL_NEW',
      }),
    );

    return result.Attributes as unknown as HangoutRoom;
  }

  /**
   * Soft-archive: flips state, stamps archivedAt, sets ttl so DDB sweeps
   * the row after 90 days. Idempotent — re-archiving updates archivedAt
   * but leaves ttl alone.
   */
  async archiveRoom(orgId: string, roomId: string): Promise<void> {
    const now = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + ARCHIVE_TTL_SECONDS;

    await this.docClient.send(
      new UpdateCommand({
        TableName: HANGOUT_ROOMS_TABLE,
        Key: { orgId, roomId },
        UpdateExpression: 'SET #st = :s, archivedAt = :a, #ttl = :t, lastActiveAt = :a',
        ExpressionAttributeNames: { '#st': 'state', '#ttl': 'ttl' },
        ExpressionAttributeValues: {
          ':s': 'archived',
          ':a': now,
          ':t': ttl,
        },
        ConditionExpression: 'attribute_exists(roomId)',
      }),
    );
  }

  /**
   * Best-effort lastActiveAt bump. Fire-and-forget: routes wrap calls in
   * `void` so a transient DDB hiccup doesn't fail the join. Doesn't update
   * any other attribute to keep the conditional-write footprint minimal.
   */
  async touchLastActive(orgId: string, roomId: string): Promise<void> {
    try {
      await this.docClient.send(
        new UpdateCommand({
          TableName: HANGOUT_ROOMS_TABLE,
          Key: { orgId, roomId },
          UpdateExpression: 'SET lastActiveAt = :la',
          ExpressionAttributeValues: { ':la': new Date().toISOString() },
          ConditionExpression: 'attribute_exists(roomId)',
        }),
      );
    } catch {
      // swallow — best-effort
    }
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh:
 *
 *   aws dynamodb create-table \
 *     --table-name ${DDB_TABLE_PREFIX}hangout-rooms \
 *     --attribute-definitions \
 *       AttributeName=orgId,AttributeType=S \
 *       AttributeName=roomId,AttributeType=S \
 *       AttributeName=slug,AttributeType=S \
 *       AttributeName=lastActiveAt,AttributeType=S \
 *     --key-schema \
 *       AttributeName=orgId,KeyType=HASH \
 *       AttributeName=roomId,KeyType=RANGE \
 *     --global-secondary-indexes \
 *       'IndexName=orgId-slug-index,KeySchema=[...],Projection=ALL' \
 *       'IndexName=orgId-state-lastActiveAt-index,KeySchema=[...],Projection=ALL' \
 *     --billing-mode PAY_PER_REQUEST
 *
 *   # TTL on `ttl` (only populated on archive — drops the row 90d later)
 */
