/**
 * TeamRepository — teams, and who is in them.
 *
 * A **team** is a named group of people. A **channel** is a conversation
 * inside a team, and a channel IS a hangout room (see
 * HangoutRoomRepository) carrying the team's `teamId` — so channels
 * inherit the room's chat (`room:<slug>`), membership mirroring into the
 * gateway, calls and recordings rather than re-implementing any of it.
 * Nothing about channels lives in this file.
 *
 * Table `teams`:
 *   PK: orgId          — org-scoped multi-tenancy, one Query per tenant
 *   SK: teamId         — ulid (sortable + unique)
 *   GSI: orgId-slug-index (PK orgId, SK slug)
 *     → find-by-slug + per-org slug uniqueness
 *
 * Table `team-members`:
 *   PK: teamId         — a team's roster is one Query
 *   SK: userId
 *   GSI: userId-index  (PK userId)
 *     → "my teams" for the rail, one Query instead of an org-wide scan
 *
 * On archive: ttl is set (90d) so DDB sweeps the row natively, matching
 * how hangout rooms are retired.
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

export const TEAMS_TABLE = tableName('teams');
export const TEAM_MEMBERS_TABLE = tableName('team-members');

const ARCHIVE_TTL_SECONDS = 90 * 24 * 60 * 60; // 90 days

export type TeamVisibility = 'org' | 'private';
export type TeamState = 'active' | 'archived';
export type TeamMemberRole = 'owner' | 'member';

export interface TeamItem {
  orgId: string;
  teamId: string;
  slug: string;
  name: string;
  description?: string;
  visibility: TeamVisibility;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  state: TeamState;
  archivedAt?: string;
  ttl?: number;
}

export interface TeamMemberItem {
  teamId: string;
  userId: string;
  role: TeamMemberRole;
  addedBy: string;
  joinedAt: string;
  /** This member's own favourite flag — never set by anyone but themselves. */
  favorite?: boolean;
}

export interface CreateTeamInput {
  orgId: string;
  slug: string;
  name: string;
  description?: string;
  visibility: TeamVisibility;
  createdBy: string;
}

export class TeamSlugConflictError extends AppError {
  constructor(slug: string) {
    super(409, `Team slug already exists: ${slug}`);
  }
}

export class TeamNotFoundError extends AppError {
  constructor(teamId: string) {
    super(404, `Team not found: ${teamId}`);
  }
}

export class TeamAccessDeniedError extends AppError {
  constructor(teamId: string) {
    super(403, `Access denied to team: ${teamId}`);
  }
}

export class TeamRepository extends BaseRepository {
  private members: BaseRepository;

  constructor(docClient: DynamoDBDocumentClient) {
    super(TEAMS_TABLE, docClient);
    this.members = new BaseRepository(TEAM_MEMBERS_TABLE, docClient);
  }

  // --- Teams ---------------------------------------------------------

  /**
   * Create a team. Slug-uniqueness is checked up-front via the GSI; the
   * route retries with a `-2`, `-3` suffix, the same tradeoff the room
   * repository documents (DDB cannot enforce GSI uniqueness atomically
   * without a sentinel TransactWrite, which is overkill here).
   */
  async createTeam(input: CreateTeamInput): Promise<TeamItem> {
    const existing = await this.getTeamBySlug(input.orgId, input.slug);
    if (existing) throw new TeamSlugConflictError(input.slug);

    const now = new Date().toISOString();
    const team: TeamItem = {
      orgId: input.orgId,
      teamId: ulid(),
      slug: input.slug,
      name: input.name,
      ...(input.description !== undefined ? { description: input.description } : {}),
      visibility: input.visibility,
      createdBy: input.createdBy,
      createdAt: now,
      updatedAt: now,
      state: 'active',
    };

    await this.putItem(team as unknown as Record<string, unknown>);
    return team;
  }

  async getTeam(orgId: string, teamId: string): Promise<TeamItem | null> {
    return this.getItem<TeamItem>({ orgId, teamId });
  }

  async getTeamBySlug(orgId: string, slug: string): Promise<TeamItem | null> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: TEAMS_TABLE,
        IndexName: 'orgId-slug-index',
        KeyConditionExpression: 'orgId = :o AND slug = :s',
        ExpressionAttributeValues: { ':o': orgId, ':s': slug },
        Limit: 1,
      }),
    );
    const items = (result.Items ?? []) as TeamItem[];
    return items[0] ?? null;
  }

  /**
   * The teams a person belongs to, with their role in each. One Query on
   * the membership GSI, then a point-get per team — a person is in a
   * handful of teams, so the fan-out is small and needs no BatchGet.
   * Archived teams and rows whose team has been deleted are dropped.
   */
  async listTeamsForUser(
    orgId: string,
    userId: string,
  ): Promise<Array<{ team: TeamItem; role: TeamMemberRole; favorite: boolean }>> {
    const result = await this.docClient.send(
      new QueryCommand({
        TableName: TEAM_MEMBERS_TABLE,
        IndexName: 'userId-index',
        KeyConditionExpression: 'userId = :u',
        ExpressionAttributeValues: { ':u': userId },
      }),
    );
    const memberships = (result.Items ?? []) as TeamMemberItem[];

    const out: Array<{ team: TeamItem; role: TeamMemberRole; favorite: boolean }> = [];
    for (const m of memberships) {
      const team = await this.getTeam(orgId, m.teamId);
      if (!team || team.state === 'archived') continue;
      out.push({ team, role: m.role, favorite: m.favorite === true });
    }
    return out;
  }

  async archiveTeam(orgId: string, teamId: string): Promise<void> {
    const now = new Date().toISOString();
    const ttl = Math.floor(Date.now() / 1000) + ARCHIVE_TTL_SECONDS;
    await this.docClient.send(
      new UpdateCommand({
        TableName: TEAMS_TABLE,
        Key: { orgId, teamId },
        UpdateExpression: 'SET #st = :s, archivedAt = :a, updatedAt = :a, #ttl = :t',
        ExpressionAttributeNames: { '#st': 'state', '#ttl': 'ttl' },
        ExpressionAttributeValues: { ':s': 'archived', ':a': now, ':t': ttl },
        ConditionExpression: 'attribute_exists(teamId)',
      }),
    );
  }

  // --- Membership ----------------------------------------------------

  async addMember(
    teamId: string,
    userId: string,
    role: TeamMemberRole,
    addedBy: string,
  ): Promise<TeamMemberItem> {
    const member: TeamMemberItem = {
      teamId,
      userId,
      role,
      addedBy,
      joinedAt: new Date().toISOString(),
    };
    await this.members.putItem(member as unknown as Record<string, unknown>);
    return member;
  }

  async removeMember(teamId: string, userId: string): Promise<void> {
    await this.members.deleteItem({ teamId, userId });
  }

  async listMembers(teamId: string): Promise<TeamMemberItem[]> {
    return this.members.query<TeamMemberItem>({
      KeyConditionExpression: 'teamId = :t',
      ExpressionAttributeValues: { ':t': teamId },
    });
  }

  async getMembership(teamId: string, userId: string): Promise<TeamMemberItem | null> {
    return this.members.getItem<TeamMemberItem>({ teamId, userId });
  }

  /**
   * Set a member's own favourite flag. Caller-scoped by construction: the
   * route only ever passes the caller's own userId, so this can never
   * touch anyone else's row.
   */
  async setFavorite(teamId: string, userId: string, favorite: boolean): Promise<TeamMemberItem> {
    const updated = await this.members.updateItem({
      Key: { teamId, userId },
      UpdateExpression: 'SET favorite = :f',
      ExpressionAttributeValues: { ':f': favorite },
      ConditionExpression: 'attribute_exists(teamId)',
      ReturnValues: 'ALL_NEW',
    });
    return updated as unknown as TeamMemberItem;
  }
}

/**
 * Schema for scripts/setup-dynamodb.sh:
 *
 *   teams:        PK orgId, SK teamId; GSI orgId-slug-index (orgId, slug)
 *   team-members: PK teamId, SK userId; GSI userId-index (userId)
 *
 *   # TTL on `ttl` (only populated on archive — drops the row 90d later)
 */
