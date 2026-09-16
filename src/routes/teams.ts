/**
 * teamsRouter — teams and their channels, the way Teams has them.
 *
 * Mounted at `/api/teams` AFTER requireAuth. A **team** is a named group
 * of people (this file's `teams` / `team-members` tables). A **channel**
 * is a conversation inside a team, and a channel IS a hangout room
 * carrying the team's `teamId` — so every channel gets the room's chat
 * (`room:<slug>`), its membership mirror into the gateway, its calls and
 * its recordings without any of that being rebuilt here.
 *
 *   POST   /api/teams                        — create a team + its General channel
 *   GET    /api/teams                        — my teams, each with its channels (feeds the rail)
 *   GET    /api/teams/:teamId                — one team: channels + members
 *   POST   /api/teams/:teamId/channels       — add a channel
 *   POST   /api/teams/:teamId/members        — add a member (owner only)
 *   DELETE /api/teams/:teamId/members/:userId — remove a member (owner, or yourself = leave)
 *   PATCH  /api/teams/:teamId/me              — favourite/unfavourite (caller's own row only)
 *
 * Membership is the point of the whole structure: a team member belongs
 * to every standard channel's conversation, and leaving the team takes
 * them out of all of them. That is done by writing the room's member
 * rows and mirroring them into the gateway's chat membership — the same
 * path `/api/rooms/:slug/members` uses.
 */

import { Router, Request, Response } from 'express';
import {
  teamRepo,
  hangoutRoomRepo,
  hangoutRoomMemberRepo,
  profileRepo,
  RoomSlugConflictError,
  TeamNotFoundError,
  TeamAccessDeniedError,
  TeamSlugConflictError,
  type ChannelPrivacy,
  type HangoutRoom,
  type HangoutRoomVisibility,
  type TeamItem,
  type TeamMemberRole,
  type TeamVisibility,
} from '../repositories';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { mirrorRoomMembers, mirrorRoomMemberRemoval } from '../gateway/chatMembership';
import { slugify } from './hangoutRooms';

export const teamsRouter = Router();

const DEFAULT_ORG_ID = 'default-org';
const DEFAULT_SOFT_CAP = 25;
const NAME_MAX_LEN = 80;
const SLUG_MAX_LEN = 64;
const SLUG_RETRY_LIMIT = Number(process.env.TEAM_SLUG_RETRY_LIMIT ?? '5');

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Same posture as hangoutRooms.ts: dev falls back to the default org. */
function resolveOrgId(req: Request): string {
  const user = req.user as unknown as { sub: string; orgId?: string } | undefined;
  if (user?.orgId) return user.orgId;
  if (process.env.SKIP_AUTH === 'true') return DEFAULT_ORG_ID;
  throw new ValidationError('orgId is required (missing from auth context)');
}

function getUserId(req: Request): string {
  const sub = req.user?.sub;
  if (!sub) throw new ValidationError('userId is required');
  return sub;
}

/**
 * A person's name for the conversation's system line ("Eve added Carol").
 * Undefined when there is none — the line then falls back to the id,
 * which is honest, and never blocks the membership mirror.
 */
async function displayNameOf(userId: string): Promise<string | undefined> {
  try {
    const profile = await profileRepo.getProfile(userId);
    return profile?.displayName || undefined;
  } catch {
    return undefined;
  }
}

function requireName(raw: unknown, field = 'name'): string {
  const name = typeof raw === 'string' ? raw.trim() : '';
  if (!name || name.length > NAME_MAX_LEN) {
    throw new ValidationError(`${field} is required (1–${NAME_MAX_LEN} chars)`);
  }
  return name;
}

/**
 * `design` + `Weekly standup` → `design-weekly-standup`. A channel slug
 * carries its team so two teams can each have a General without
 * colliding in the org-wide room namespace.
 */
export function channelSlug(teamSlug: string, channelName: string): string {
  return `${teamSlug}-${slugify(channelName)}`.slice(0, SLUG_MAX_LEN);
}

/**
 * A channel's room is private whenever the channel is private OR its
 * team is — an org-visible room inside a private team would put the
 * team's conversation in front of the whole org, which is the one thing
 * a private team is for.
 */
function roomVisibilityFor(team: TeamItem, privacy: ChannelPrivacy): HangoutRoomVisibility {
  return privacy === 'private' || team.visibility === 'private' ? 'private' : 'org';
}

interface CreateChannelArgs {
  team: TeamItem;
  name: string;
  description?: string;
  privacy: ChannelPrivacy;
  createdBy: string;
  /** Everyone who should be in the channel's conversation, creator included. */
  memberIds: string[];
  byName?: string;
}

/**
 * Create one channel: a hangout room stamped with the team, its member
 * rows, and the gateway mirror that closes its conversation to those
 * people. Retries the slug with a `-2`, `-3` suffix on collision, the
 * same tradeoff `/api/rooms` documents.
 */
async function createChannel(args: CreateChannelArgs): Promise<HangoutRoom> {
  const { team, name, privacy, createdBy } = args;
  const baseSlug = channelSlug(team.slug, name);
  const visibility = roomVisibilityFor(team, privacy);

  let room: HangoutRoom | null = null;
  let lastErr: unknown;
  for (let attempt = 1; attempt <= SLUG_RETRY_LIMIT; attempt++) {
    const slug = attempt === 1 ? baseSlug : `${baseSlug}-${attempt}`;
    try {
      room = await hangoutRoomRepo.createRoom({
        orgId: team.orgId,
        slug,
        name,
        ...(args.description !== undefined ? { description: args.description } : {}),
        visibility,
        settings: { audioOnly: false, recordingEnabled: false, softCap: DEFAULT_SOFT_CAP },
        createdBy,
        teamId: team.teamId,
        channelPrivacy: privacy,
      });
      break;
    } catch (err) {
      if (err instanceof RoomSlugConflictError) {
        lastErr = err;
        continue;
      }
      throw err;
    }
  }
  if (!room) throw lastErr ?? new RoomSlugConflictError(baseSlug);

  const wanted = [...new Set(args.memberIds)];
  for (const userId of wanted) {
    await hangoutRoomMemberRepo.addMember(
      room.roomId,
      userId,
      userId === createdBy ? 'owner' : 'member',
      createdBy,
    );
  }

  // The channel's conversation carries exactly these people, with the
  // whole history — a team member is entitled to the channel's past.
  const members = await Promise.all(
    wanted
      .filter((id) => id !== createdBy)
      .map(async (userId) => ({ userId, displayName: await displayNameOf(userId) })),
  );
  mirrorRoomMembers({
    slug: room.slug,
    ownerId: createdBy,
    ownerName: args.byName ?? (await displayNameOf(createdBy)),
    members,
  });

  return room;
}

/** Load a team, or 404. */
async function loadTeam(orgId: string, teamId: string): Promise<TeamItem> {
  const team = await teamRepo.getTeam(orgId, teamId);
  if (!team || team.state === 'archived') throw new TeamNotFoundError(teamId);
  return team;
}

/**
 * A private team is invisible to anyone who is not in it — not just
 * unwritable, unreadable. An org team is discoverable without
 * membership, and the caller's role comes back as undefined.
 */
async function assertCanView(team: TeamItem, userId: string): Promise<TeamMemberRole | undefined> {
  const membership = await teamRepo.getMembership(team.teamId, userId);
  if (membership) return membership.role;
  if (team.visibility === 'private') throw new TeamNotFoundError(team.teamId);
  return undefined;
}

async function requireMembership(team: TeamItem, userId: string): Promise<TeamMemberRole> {
  const membership = await teamRepo.getMembership(team.teamId, userId);
  if (!membership) {
    // Same shape as the view guard: a private team does not admit it exists.
    if (team.visibility === 'private') throw new TeamNotFoundError(team.teamId);
    throw new TeamAccessDeniedError(team.teamId);
  }
  return membership.role;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

/**
 * POST /api/teams — create a team.
 *
 * A team with no channel is not a team, so this also creates its
 * General channel, exactly as Teams does. 201 `{ team, channels }`.
 */
teamsRouter.post(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const body = (req.body ?? {}) as {
      name?: string;
      description?: string;
      visibility?: TeamVisibility;
      byName?: string;
    };

    const name = requireName(body.name);
    const visibility: TeamVisibility = body.visibility === 'private' ? 'private' : 'org';
    const baseSlug = slugify(name);

    let team: TeamItem | null = null;
    let lastErr: unknown;
    for (let attempt = 1; attempt <= SLUG_RETRY_LIMIT; attempt++) {
      const slug = attempt === 1 ? baseSlug : `${baseSlug}-${attempt}`;
      try {
        team = await teamRepo.createTeam({
          orgId,
          slug,
          name,
          ...(body.description !== undefined ? { description: body.description } : {}),
          visibility,
          createdBy: userId,
        });
        break;
      } catch (err) {
        if (err instanceof TeamSlugConflictError) {
          lastErr = err;
          continue;
        }
        throw err;
      }
    }
    if (!team) throw lastErr ?? new ValidationError('could not allocate a team slug');

    await teamRepo.addMember(team.teamId, userId, 'owner', userId);

    const byName = typeof body.byName === 'string' ? body.byName.slice(0, 80) : undefined;
    const general = await createChannel({
      team,
      name: 'General',
      privacy: 'standard',
      createdBy: userId,
      memberIds: [userId],
      ...(byName ? { byName } : {}),
    });

    res.status(201).json({ team, channels: [general] });
  }),
);

/**
 * GET /api/teams — the caller's teams, each with its channels.
 *
 * One call feeds the whole rail: the frontend should not have to fan out
 * a channel request per team to paint it.
 */
teamsRouter.get(
  '/',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);

    const memberships = await teamRepo.listTeamsForUser(orgId, userId);
    const teams = await Promise.all(
      memberships.map(async ({ team, role, favorite }) => {
        const channels = await hangoutRoomRepo.listRoomsByTeam(team.teamId);
        // A private channel is only in the rail of someone who is in it.
        const visible: HangoutRoom[] = [];
        for (const c of channels) {
          if (c.channelPrivacy !== 'private' || c.createdBy === userId) {
            visible.push(c);
            continue;
          }
          if (await hangoutRoomMemberRepo.isMember(c.roomId, userId)) visible.push(c);
        }
        return { ...team, role, favorite, channels: visible };
      }),
    );

    teams.sort((a, b) => (b.updatedAt ?? '').localeCompare(a.updatedAt ?? ''));
    res.status(200).json({ teams });
  }),
);

/**
 * GET /api/teams/:teamId — one team, its channels and its roster.
 */
teamsRouter.get(
  '/:teamId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const team = await loadTeam(orgId, req.params.teamId);
    const role = await assertCanView(team, userId);

    const all = await hangoutRoomRepo.listRoomsByTeam(team.teamId);
    const channels: HangoutRoom[] = [];
    for (const c of all) {
      if (c.channelPrivacy !== 'private' || c.createdBy === userId) {
        channels.push(c);
        continue;
      }
      if (await hangoutRoomMemberRepo.isMember(c.roomId, userId)) channels.push(c);
    }
    const members = await teamRepo.listMembers(team.teamId);

    res.status(200).json({ team, ...(role ? { role } : {}), channels, members });
  }),
);

/**
 * POST /api/teams/:teamId/channels — add a channel.
 *
 * Any member may create one, as in Teams. A standard channel starts with
 * the whole team in it; a private one starts with only its creator.
 */
teamsRouter.post(
  '/:teamId/channels',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const body = (req.body ?? {}) as {
      name?: string;
      description?: string;
      privacy?: ChannelPrivacy;
      byName?: string;
    };

    const team = await loadTeam(orgId, req.params.teamId);
    await requireMembership(team, userId);

    const name = requireName(body.name);
    const privacy: ChannelPrivacy = body.privacy === 'private' ? 'private' : 'standard';

    const memberIds =
      privacy === 'private'
        ? [userId]
        : (await teamRepo.listMembers(team.teamId)).map((m) => m.userId);

    const byName = typeof body.byName === 'string' ? body.byName.slice(0, 80) : undefined;
    const channel = await createChannel({
      team,
      name,
      ...(body.description !== undefined ? { description: body.description } : {}),
      privacy,
      createdBy: userId,
      memberIds: [...new Set([userId, ...memberIds])],
      ...(byName ? { byName } : {}),
    });

    res.status(201).json({ channel });
  }),
);

/**
 * POST /api/teams/:teamId/members — add someone to the team.
 *
 * Owner only, and the point of it: they land in every standard channel's
 * conversation at the same moment, with its history.
 */
teamsRouter.post(
  '/:teamId/members',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const callerId = getUserId(req);
    const body = (req.body ?? {}) as { userId?: string; displayName?: string; byName?: string };

    const targetId = typeof body.userId === 'string' ? body.userId.trim() : '';
    if (!targetId) throw new ValidationError('userId is required');

    const team = await loadTeam(orgId, req.params.teamId);
    const role = await requireMembership(team, callerId);
    if (role !== 'owner') throw new TeamAccessDeniedError(team.teamId);

    const member = await teamRepo.addMember(team.teamId, targetId, 'member', callerId);

    const targetName =
      (typeof body.displayName === 'string' && body.displayName.slice(0, 80)) ||
      (await displayNameOf(targetId));
    const byName =
      (typeof body.byName === 'string' && body.byName.slice(0, 80)) ||
      (await displayNameOf(callerId));

    // Standard channels follow the team; a private channel does not —
    // being on the team is not an invitation into a private room.
    const channels = await hangoutRoomRepo.listRoomsByTeam(team.teamId);
    for (const channel of channels) {
      if (channel.channelPrivacy === 'private') continue;
      await hangoutRoomMemberRepo.addMember(channel.roomId, targetId, 'member', callerId);
      mirrorRoomMembers({
        slug: channel.slug,
        ownerId: channel.createdBy,
        ...(byName ? { ownerName: byName } : {}),
        members: [{ userId: targetId, ...(targetName ? { displayName: targetName } : {}) }],
      });
    }

    res.status(201).json({ member });
  }),
);

/**
 * DELETE /api/teams/:teamId/members/:userId — remove someone, or leave.
 *
 * Out of the team is out of all its channels, private ones included: the
 * membership came from the team, so it goes with it.
 */
teamsRouter.delete(
  '/:teamId/members/:userId',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const callerId = getUserId(req);
    const targetId = req.params.userId;

    const team = await loadTeam(orgId, req.params.teamId);
    const role = await requireMembership(team, callerId);
    if (targetId !== callerId && role !== 'owner') {
      throw new TeamAccessDeniedError(team.teamId);
    }

    await teamRepo.removeMember(team.teamId, targetId);

    const targetName =
      (typeof req.query.name === 'string' && req.query.name.slice(0, 80)) ||
      (await displayNameOf(targetId));
    const byName =
      (typeof req.query.byName === 'string' && req.query.byName.slice(0, 80)) ||
      (await displayNameOf(callerId));

    const channels = await hangoutRoomRepo.listRoomsByTeam(team.teamId);
    for (const channel of channels) {
      await hangoutRoomMemberRepo.removeMember(channel.roomId, targetId);
      mirrorRoomMemberRemoval({
        slug: channel.slug,
        userId: targetId,
        ...(targetName ? { name: targetName } : {}),
        byOwnerId: channel.createdBy,
        ...(byName ? { byOwnerName: byName } : {}),
      });
    }

    res.status(204).end();
  }),
);

/**
 * PATCH /api/teams/:teamId/me — favourite or unfavourite a team.
 *
 * A person's own preference, not a change to the team: it only ever
 * touches the CALLER's own membership row (never `:userId` from the URL,
 * unlike the members routes above), and 404s the same way a private
 * team does for a non-member rather than admitting the team exists.
 */
teamsRouter.patch(
  '/:teamId/me',
  asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const orgId = resolveOrgId(req);
    const userId = getUserId(req);
    const body = (req.body ?? {}) as { favorite?: unknown };

    if (typeof body.favorite !== 'boolean') {
      throw new ValidationError('favorite is required (boolean)');
    }

    const team = await loadTeam(orgId, req.params.teamId);
    await requireMembership(team, userId);

    const member = await teamRepo.setFavorite(team.teamId, userId, body.favorite);

    res.status(200).json({ member });
  }),
);
