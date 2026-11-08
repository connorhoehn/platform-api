// `meeting:create` — the step behind a planned `kind: 'meeting'`.
//
// "Create a meeting with the platform team" used to be a generic agent step
// with nothing behind it: the loop said it had done it, and no meeting existed
// anywhere. This creates the real thing the platform already has — a hangout
// room, the place a call happens — with the team's own members on it.
//
// What it does NOT do, and says so:
//   - there is no calendar integration here, so the time is a PROPOSAL
//     (`calendar: 'none'`) and nobody is invited anywhere;
//   - a team name that matches nothing in the directory leaves the attendee
//     list EMPTY with a notice, rather than inventing a plausible one;
//   - the work graph's meeting source admits attendance, recording and
//     transcript facts — a room nobody has joined is none of those — so the
//     room is declared as the run's own artifact (`artifacts[]`, the same
//     declaration the deck step makes) instead of a fabricated attendance.

import { skipAuthEnabled } from '../lib/insecureDevMode';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { hangoutRoomRepo, hangoutRoomMemberRepo, profileRepo, teamRepo } from '../repositories';
import { readPath } from '../pipeline/image/ScatterStepHandler';
import log from '../lib/logger';

export const MEETING_STEP_KIND = 'meeting:create';

export interface MeetingNodeData {
  type: typeof MEETING_STEP_KIND;
  /** The team to invite, when the planner already knows it. Else read from the text. */
  team?: string;
  /** The meeting's title. Defaults to the step label, then the instruction. */
  title?: string;
  /** How far ahead the proposed time is, in minutes. Default: the next day. */
  proposeInMinutes?: number;
  /** An explicit start (ISO), when something already decided when. */
  startsAt?: string;
  /** How long the meeting is meant to be. Default 30 minutes. */
  durationMinutes?: number;
  label?: string;
}

export interface MeetingAttendee {
  userId: string;
  role: string;
}

const TITLE_PATHS = ['$.title', '$.instruction', '$.prompt'];
const TEAM_PATHS = ['$.team', '$.teamName'];
const ORG_PATHS = ['$.organizationId', '$.orgId', '$.trigger.organizationId'];
const REQUESTER_PATHS = ['$.requestedBy', '$.trigger.requestedBy'];
const DEFAULT_PROPOSAL_MINUTES = 24 * 60;
const DEFAULT_DURATION_MINUTES = 30;
const MAX_ATTENDEES = 50;
/** The org a local deployment puts everything in — the same default the rooms route uses. */
const DEV_ORG_ID = 'default-org';

/**
 * The organization a room will belong to.
 *
 * A chat command carries one; a run started another way may not, and a loop's
 * first version carried none at all — which failed the step outright with
 * "no organizationId" and dead-lettered the run. Local development has a
 * standing answer (the same `default-org` the rooms route falls back to), and
 * taking it is said out loud rather than assumed silently.
 */
export function resolveOrganization(context: Record<string, unknown>): { organizationId?: string; notice?: string } {
  const declared = firstOf(ORG_PATHS, context);
  if (declared) return { organizationId: declared };
  const configured = str(process.env.WORK_GRAPH_ORG) ?? str(process.env.DEFAULT_ORG_ID);
  if (configured) return { organizationId: configured, notice: `The run named no organization, so the room was made in ${configured}.` };
  if (skipAuthEnabled()) {
    return { organizationId: DEV_ORG_ID, notice: `The run named no organization, so the room was made in ${DEV_ORG_ID}.` };
  }
  return {};
}

function str(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value.trim() : undefined;
}

function firstOf(paths: readonly string[], context: Record<string, unknown>): string | undefined {
  for (const path of paths) {
    const value = str(readPath(context, path));
    if (value) return value;
  }
  return undefined;
}

/**
 * The team a sentence names: "with the platform team", "the Platform team's
 * review". Only the words actually written — no fuzzy matching, because an
 * attendee list is a list of real people who will be told they are expected.
 */
export function teamNameFrom(text: string | undefined): string | undefined {
  if (!text) return undefined;
  // At most three words, because "meet the Data Platform team" names a team
  // and "meet the" does not — a greedy match would put the whole sentence in
  // an attendee lookup.
  const named = /(?:with|for|of)\s+(?:the\s+)?((?:[A-Za-z0-9][\w&.-]*\s+){0,2}[A-Za-z0-9][\w&.-]*)\s+team\b/i.exec(text);
  const bare = /(?:^|\s)((?:[A-Za-z0-9][\w&.-]*\s+){0,1}[A-Za-z0-9][\w&.-]*)\s+team\b/i.exec(text);
  const match = named ?? bare;
  const name = match?.[1]?.trim().replace(/\s+/g, ' ');
  return name && name.toLowerCase() !== 'the' ? name : undefined;
}

/** A room slug from a title: lowercase words, hyphens, nothing else. */
export function meetingSlug(title: string, stamp: string): string {
  const base = title.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/^-|-$/g, '').slice(0, 40) || 'meeting';
  return `${base}-${stamp}`;
}

/** The proposed start: a whole hour, this far ahead. Nothing is booked. */
export function proposedStart(now: Date, minutesAhead: number): string {
  const at = new Date(now.getTime() + minutesAhead * 60_000);
  at.setUTCMinutes(0, 0, 0);
  return at.toISOString();
}

export interface MeetingStepHandlerOpts {
  rooms?: Pick<typeof hangoutRoomRepo, 'createRoom'>;
  members?: Pick<typeof hangoutRoomMemberRepo, 'addMember'>;
  teams?: Pick<typeof teamRepo, 'listTeamsForUser' | 'getTeamBySlug' | 'listMembers'>;
  profiles?: Pick<typeof profileRepo, 'searchProfiles'>;
  now?: () => Date;
}

/** The directory lookup: a team by its slug, then by its name among the requester's. */
async function resolveTeam(
  teams: NonNullable<MeetingStepHandlerOpts['teams']>,
  orgId: string,
  requestedBy: string,
  name: string,
): Promise<{ teamId: string; name: string } | null> {
  const slug = name.toLowerCase().replace(/[^a-z0-9]+/g, '-');
  const bySlug = await teams.getTeamBySlug(orgId, slug).catch(() => null);
  if (bySlug) return { teamId: bySlug.teamId, name: bySlug.name };
  const mine = await teams.listTeamsForUser(orgId, requestedBy).catch(() => []);
  const match = mine.find(
    (entry) => entry.team.name.trim().toLowerCase() === name.toLowerCase() || entry.team.slug === slug,
  );
  return match ? { teamId: match.team.teamId, name: match.team.name } : null;
}

export function createMeetingStepHandler(opts: MeetingStepHandlerOpts = {}): DescribedStepHandler {
  const rooms = opts.rooms ?? hangoutRoomRepo;
  const members = opts.members ?? hangoutRoomMemberRepo;
  const teams = opts.teams ?? teamRepo;
  const profiles = opts.profiles ?? profileRepo;
  const now = opts.now ?? (() => new Date());

  return {
    kind: MEETING_STEP_KIND,
    description:
      'Creates the meeting a loop step asked for: a real room, with the named team\'s members on it and a proposed time. It books no calendar and invites nobody.',
    reads: 'The team named in the step (or the node\'s `team`), the title, and who asked.',
    emits:
      'meetingId, title, attendees and the proposed startsAt — with `calendar: \'none\'`, and a notice when the team matched nothing.',
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const data = (node.data ?? {}) as unknown as MeetingNodeData;
      const requestedBy = firstOf(REQUESTER_PATHS, context) ?? 'agent:loop';
      const organization = resolveOrganization(context);
      const source = str(data.title) ?? str(data.label) ?? firstOf(TITLE_PATHS, context) ?? 'Meeting';
      const title = source.slice(0, 120);
      const teamName = str(data.team) ?? firstOf(TEAM_PATHS, context) ?? teamNameFrom(source) ?? teamNameFrom(firstOf(TITLE_PATHS, context));
      // An explicit time wins; otherwise the proposal. Either way it is stored
      // as a real field on the room, not as prose in its description.
      const explicit = str(data.startsAt);
      const given = explicit && Number.isFinite(Date.parse(explicit)) ? new Date(Date.parse(explicit)).toISOString() : undefined;
      const startsAt = given ?? proposedStart(now(), data.proposeInMinutes ?? DEFAULT_PROPOSAL_MINUTES);
      const durationMinutes = typeof data.durationMinutes === 'number' && data.durationMinutes > 0
        ? Math.round(data.durationMinutes)
        : DEFAULT_DURATION_MINUTES;
      const notices: string[] = [];

      if (!organization.organizationId) {
        return {
          status: 'failed',
          error: 'meeting: no organization — the run declared none and this deployment configures no default (WORK_GRAPH_ORG)',
        };
      }
      const organizationId = organization.organizationId;
      if (organization.notice) notices.push(organization.notice);

      // ── The directory ────────────────────────────────────────────────────
      let attendees: MeetingAttendee[] = [];
      let team: { teamId: string; name: string } | null = null;
      if (teamName) {
        team = await resolveTeam(teams, organizationId, requestedBy, teamName);
        if (!team) {
          // No such team. The name may still be a person's — "a meeting with
          // Frank" — so the directory is asked once, and whatever it says is
          // reported rather than left to look like a full invitation.
          const people = await profiles.searchProfiles(teamName, 5).catch(() => []);
          const matched = people.filter((profile) => profile.displayName?.trim().toLowerCase() === teamName.toLowerCase());
          if (matched.length) {
            attendees = matched.map((profile) => ({ userId: profile.userId, role: 'member' }));
            notices.push(`No team named "${teamName}", so the ${matched.length === 1 ? 'person' : 'people'} of that name in the directory were added instead.`);
          } else {
            notices.push(`No team or person named "${teamName}" in the directory, so nobody was added.`);
          }
        } else {
          const roster = await teams.listMembers(team.teamId).catch(() => []);
          attendees = (roster as Array<{ userId: string; role: string }>)
            .slice(0, MAX_ATTENDEES)
            .map((member) => ({ userId: member.userId, role: member.role }));
          if (!attendees.length) notices.push(`The team "${team.name}" has no members, so nobody was added.`);
        }
      } else {
        notices.push('The step named no team, so nobody was added.');
      }

      // ── The room ─────────────────────────────────────────────────────────
      try {
        const room = await rooms.createRoom({
          orgId: organizationId,
          slug: meetingSlug(title, String(Date.parse(startsAt)).slice(-6)),
          name: title,
          description: 'Proposed by an agent loop',
          startsAt,
          durationMinutes,
          visibility: 'org',
          settings: { audioOnly: false, recordingEnabled: true, softCap: 25 },
          createdBy: requestedBy,
        } as Parameters<typeof rooms.createRoom>[0]);

        const invited: string[] = [];
        for (const attendee of attendees) {
          if (attendee.userId === requestedBy) continue;
          const added = await members.addMember(room.roomId, attendee.userId, 'member', requestedBy).then(() => true).catch((err) => {
            log.warn({ roomId: room.roomId, userId: attendee.userId, err: String(err) }, 'meeting: an attendee could not be added');
            return false;
          });
          if (added) invited.push(attendee.userId);
        }
        await members.addMember(room.roomId, requestedBy, 'owner', requestedBy).catch(() => undefined);

        return {
          status: 'completed',
          output: {
            meetingId: room.roomId,
            slug: room.slug,
            title,
            attendees: invited,
            attendeeCount: invited.length,
            startsAt,
            durationMinutes,
            // An explicit time was decided elsewhere; a proposal was not.
            timeSource: given ? 'given' : 'proposed',
            // No calendar is wired to this deployment: the time is a proposal,
            // and saying so is the difference between a meeting and a plan.
            calendar: 'none',
            ...(team ? { team: { teamId: team.teamId, name: team.name } } : {}),
            ...(notices.length ? { notices } : {}),
            createdByRunId: helpers.runId,
            // The run declares what it made. The work graph's meeting source
            // admits attendance/recording/transcript facts, and a room nobody
            // has joined is none of them, so this is the honest projection.
            artifacts: [{ artifactId: room.roomId, kind: 'meeting', title, startsAt, durationMinutes }],
            result: { kind: 'room', roomId: room.roomId, slug: room.slug },
          },
        };
      } catch (err) {
        return { status: 'failed', error: `meeting: ${err instanceof Error ? err.message : String(err)}` };
      }
    },
  };
}
