import { Router, Request, Response } from 'express';
import { QueryCommand, GetCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../lib/aws-clients';
import { getCachedProfile, setCachedProfile, invalidateProfileCache } from '../lib/cache';
import { profileRepo } from '../repositories';
import type { ProfileItem } from '../repositories';
import {
  asyncHandler,
  ValidationError,
  NotFoundError,
  ForbiddenError,
  ConflictError,
} from '../middleware/error-handler';
import { tableName } from '../lib/ddb-table-name';

const REL_TABLE = tableName('social-relationships');

export const profilesRouter = Router();

// GET /api/profiles?q=<search>&limit=<n> — case-insensitive substring search
// over displayName (and userId). Phase 1 uses a Scan — see ProfileRepository
// for scalability caveats. Results are visibility-gated: private profiles are
// only returned when the requester is the owner OR a mutual follow.
profilesRouter.get('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const rawQuery = typeof req.query['q'] === 'string' ? req.query['q'] : '';
  const q = rawQuery.trim();

  if (!q) {
    throw new ValidationError('Query parameter q is required');
  }
  if (q.length > 100) {
    throw new ValidationError('Query parameter q must be 100 chars or fewer');
  }

  const rawLimit = typeof req.query['limit'] === 'string' ? parseInt(req.query['limit'], 10) : NaN;
  let limit = Number.isFinite(rawLimit) ? rawLimit : 20;
  if (limit < 1) limit = 1;
  if (limit > 50) limit = 50;

  const requesterId = req.user!.sub;

  // Pull matches from the repository. We fetch a bit more than `limit` so
  // visibility-gating that drops private profiles still fills the page.
  const candidates = await profileRepo.searchProfiles(q, limit * 2);

  // Partition public vs. private-not-owned so we only compute mutual-follow
  // sets when we actually need to gate something.
  const privateOthers = candidates.filter(
    (p) => p.visibility === 'private' && p.userId !== requesterId,
  );

  let mutualFollowIds = new Set<string>();
  if (privateOthers.length > 0) {
    // Users the requester follows (PK query — no scan).
    const followingResult = await docClient.send(
      new QueryCommand({
        TableName: REL_TABLE,
        KeyConditionExpression: 'followerId = :fid',
        ExpressionAttributeValues: { ':fid': requesterId },
      }),
    );
    const followeeSet = new Set(
      (followingResult.Items ?? []).map((i) => i['followeeId'] as string),
    );

    // For each private candidate the requester follows, confirm the reverse
    // relationship via a point-get (O(1)).
    const reverseChecks = await Promise.all(
      privateOthers
        .filter((p) => followeeSet.has(p.userId))
        .map(async (p) => {
          const reverse = await docClient.send(
            new GetCommand({
              TableName: REL_TABLE,
              Key: { followerId: p.userId, followeeId: requesterId },
            }),
          );
          return reverse.Item ? p.userId : null;
        }),
    );
    mutualFollowIds = new Set(reverseChecks.filter((id): id is string => id !== null));
  }

  const visible = candidates.filter((p) => {
    if (p.visibility !== 'private') return true;
    if (p.userId === requesterId) return true;
    return mutualFollowIds.has(p.userId);
  });

  // Shape to ProfileSummary. Note: email is not persisted in the profiles
  // table (it lives in Cognito), so it is intentionally omitted here.
  const profiles = visible.slice(0, limit).map((p) => ({
    userId: p.userId,
    displayName: p.displayName,
    ...(p.avatarUrl ? { avatarUrl: p.avatarUrl } : {}),
  }));

  res.status(200).json({ profiles });
}));

// POST /api/profiles — create own profile (PROF-01)
profilesRouter.post('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { displayName, bio, avatarUrl, visibility } = req.body as {
    displayName?: string;
    bio?: string;
    avatarUrl?: string;
    visibility?: string;
  };

  // Validation
  if (!displayName || displayName.length < 1 || displayName.length > 50) {
    throw new ValidationError('displayName is required (max 50 chars)');
  }
  if (bio !== undefined && bio.length > 160) {
    throw new ValidationError('bio must be 160 chars or fewer');
  }
  if (visibility !== undefined && visibility !== 'public' && visibility !== 'private') {
    throw new ValidationError('visibility must be public or private');
  }

  const userId = req.user!.sub;

  // Check if profile already exists
  const existing = await profileRepo.getProfile(userId);
  if (existing) {
    throw new ConflictError('Profile already exists. Use PUT to update.');
  }

  const now = new Date().toISOString();
  const item: ProfileItem = {
    userId,
    displayName,
    bio: bio ?? '',
    avatarUrl: avatarUrl ?? '',
    visibility: (visibility as 'public' | 'private') ?? 'public',
    createdAt: now,
    updatedAt: now,
  };

  await profileRepo.createProfile(item);

  // Populate cache with newly created profile
  void setCachedProfile(userId, item);

  res.status(201).json(item);
}));

// GET /api/profiles/:userId — read profile with visibility gating (PROF-03, PROF-04, PROF-05)
profilesRouter.get('/:userId', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { userId } = req.params;

  // Check Redis cache first
  let item = await getCachedProfile<ProfileItem>(userId);

  if (!item) {
    item = await profileRepo.getProfile(userId);

    if (!item) {
      throw new NotFoundError('Profile not found');
    }

    // Populate cache on miss
    void setCachedProfile(userId, item);
  }

  // Visibility gate: private profile only visible to owner
  if (item.visibility === 'private' && req.user!.sub !== item.userId) {
    throw new ForbiddenError('This profile is private');
  }

  res.status(200).json(item);
}));

// PUT /api/profiles — update own profile (PROF-02)
profilesRouter.put('/', asyncHandler(async (req: Request, res: Response): Promise<void> => {
  const { displayName, bio, avatarUrl, visibility } = req.body as {
    displayName?: string;
    bio?: string;
    avatarUrl?: string;
    visibility?: string;
  };

  // Validation for provided fields
  if (displayName !== undefined && (displayName.length < 1 || displayName.length > 50)) {
    throw new ValidationError('displayName is required (max 50 chars)');
  }
  if (bio !== undefined && bio.length > 160) {
    throw new ValidationError('bio must be 160 chars or fewer');
  }
  if (visibility !== undefined && visibility !== 'public' && visibility !== 'private') {
    throw new ValidationError('visibility must be public or private');
  }

  const userId = req.user!.sub;

  // Check profile exists
  const existing = await profileRepo.getProfile(userId);
  if (!existing) {
    throw new NotFoundError('Profile not found. Use POST to create.');
  }

  const updated = await profileRepo.updateProfile(userId, {
    ...(displayName !== undefined ? { displayName } : {}),
    ...(bio !== undefined ? { bio } : {}),
    ...(avatarUrl !== undefined ? { avatarUrl } : {}),
    ...(visibility !== undefined ? { visibility: visibility as 'public' | 'private' } : {}),
  });

  // Invalidate stale cache; next GET will re-populate
  void invalidateProfileCache(userId);

  res.status(200).json(updated);
}));
