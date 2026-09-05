// The policy decision point.
//
// The two properties that matter here are the ones that make this SAFER than
// the scattered inline checks it replaces: default-deny for unknown actions,
// and framework-enforced tenant isolation that a policy cannot forget.

import {
  ALLOW,
  authorize,
  deny,
  dmLobbyParticipants,
  principalOf,
  registerPolicy,
  registeredActions,
  requireAuthorized,
  AuthorizationError,
} from '../authorize';
import { TENANTLESS, tenantOf } from '../tenant';

const acme = tenantOf('acme');
const globex = tenantOf('globex');
const hank = principalOf({ sub: 'dev-hank' }, acme);

describe('authorize — framework guarantees', () => {
  it('DEFAULT DENIES an action with no registered policy', () => {
    // A typo'd or not-yet-written action must never fail open. This is the
    // failure mode the whole module exists to make impossible.
    return authorize({
      principal: hank,
      action: 'totally:invented',
      resource: { type: 'thing' },
    }).then((d) => {
      expect(d.allow).toBe(false);
      expect(d.allow === false && d.reason).toMatch(/no policy registered/);
    });
  });

  it('denies cross-tenant BEFORE any policy runs — the policy cannot forget it', async () => {
    let policyRan = false;
    registerPolicy('test:crosstenant', () => {
      policyRan = true;
      return ALLOW; // a permissive policy that would have allowed it
    });

    const d = await authorize({
      principal: hank, // tenant acme
      action: 'test:crosstenant',
      resource: { type: 'thing', id: 'x', tenant: globex },
    });

    expect(d.allow).toBe(false);
    expect(policyRan).toBe(false);
    // 404, not 403: "that exists, in another tenant" is itself a disclosure.
    expect(d.allow === false && d.status).toBe(404);
  });

  it('allows same-tenant through to the policy', async () => {
    registerPolicy('test:sametenant', () => ALLOW);
    const d = await authorize({
      principal: hank,
      action: 'test:sametenant',
      resource: { type: 'thing', tenant: acme },
    });
    expect(d.allow).toBe(true);
  });

  it('denies an unauthenticated principal', async () => {
    registerPolicy('test:open', () => ALLOW);
    const d = await authorize({
      principal: principalOf(undefined, TENANTLESS),
      action: 'test:open',
      resource: { type: 'thing' },
    });
    expect(d.allow).toBe(false);
  });

  it('requireAuthorized throws with the decision status', async () => {
    registerPolicy('test:nope', () => deny('because', 404));
    await expect(
      requireAuthorized({ principal: hank, action: 'test:nope', resource: { type: 'x' } }),
    ).rejects.toThrow(AuthorizationError);
    await requireAuthorized({
      principal: hank,
      action: 'test:nope',
      resource: { type: 'x' },
    }).catch((e: AuthorizationError) => {
      expect(e.status).toBe(404);
    });
  });

  it('exposes the registered actions so the rule set is auditable', () => {
    expect(registeredActions()).toContain('recording:listByLobby');
  });
});

describe('dmLobbyParticipants', () => {
  it('reads both participants out of a dm lobby name', () => {
    expect(dmLobbyParticipants('dm:dev-bob:dev-hank')).toEqual(['dev-bob', 'dev-hank']);
  });
  it('returns null for lobbies that do not encode membership', () => {
    expect(dmLobbyParticipants('room:lounge')).toBeNull();
    expect(dmLobbyParticipants('global-hangout')).toBeNull();
    expect(dmLobbyParticipants('dm:onlyone')).toBeNull();
  });
});

describe('recording:listByLobby — the hole this closed', () => {
  const listByLobby = (principal: ReturnType<typeof principalOf>, id: string, attrs?: Record<string, unknown>) =>
    authorize({ principal, action: 'recording:listByLobby', resource: { type: 'lobby', id, ...(attrs ? { attrs } : {}) } });

  // An org-visible room is readable by the org — that is what the field
  // means. Membership alone denied EVERYONE here, including the room's own
  // creator: an org room has no member rows, because nobody has to join what
  // is already open to them, so the member list came back empty and matched
  // nobody. Observed live: every recording in an org room was unreachable.
  it('allows an org-visible room even with no members', async () => {
    const grace = principalOf({ sub: 'dev-grace' }, acme);
    expect((await listByLobby(grace, 'room:asdf', { visibility: 'org', memberIds: [] })).allow).toBe(true);
  });

  it('allows an org-visible room when membership could not be established', async () => {
    const grace = principalOf({ sub: 'dev-grace' }, acme);
    expect((await listByLobby(grace, 'room:asdf', { visibility: 'org' })).allow).toBe(true);
  });

  // Visibility widens nothing for a room that did not declare it.
  it('still requires membership for a room that is not org-visible', async () => {
    const grace = principalOf({ sub: 'dev-grace' }, acme);
    expect((await listByLobby(grace, 'room:secret', { visibility: 'private', memberIds: ['dev-bob'] })).allow).toBe(false);
    expect((await listByLobby(grace, 'room:secret', { memberIds: ['dev-bob'] })).allow).toBe(false);
  });

  // A DM is private no matter what a caller claims about visibility.
  it('does not let a visibility claim open somebody else DM', async () => {
    const grace = principalOf({ sub: 'dev-grace' }, acme);
    expect((await listByLobby(grace, 'dm:dev-bob:dev-hank', { visibility: 'org' })).allow).toBe(false);
  });

  it('lets a DM participant list their own conversation', async () => {
    expect((await listByLobby(hank, 'dm:dev-bob:dev-hank')).allow).toBe(true);
  });

  it("REFUSES a stranger listing someone else's private DM", async () => {
    // This is the exact request that used to succeed: any authenticated
    // user could list any lobby, and the route's own comment leaned on the
    // UI hiding the link.
    const dave = principalOf({ sub: 'dev-dave' }, acme);
    const d = await listByLobby(dave, 'dm:dev-bob:dev-hank');
    expect(d.allow).toBe(false);
    // 404 — confirming that DM exists would itself leak.
    expect(d.allow === false && d.status).toBe(404);
  });

  it('allows a room member and refuses a non-member', async () => {
    const members = { memberIds: ['dev-hank', 'dev-carol'] };
    expect((await listByLobby(hank, 'room:lounge', members)).allow).toBe(true);
    const dave = principalOf({ sub: 'dev-dave' }, acme);
    expect((await listByLobby(dave, 'room:lounge', members)).allow).toBe(false);
  });

  it('DENIES when membership could not be established — never falls open', async () => {
    // A room lookup that failed (deleted room, DDB blip) must not become
    // "allow"; that is how a lookup failure turns into a data leak.
    const d = await listByLobby(hank, 'room:lounge');
    expect(d.allow).toBe(false);
    expect(d.allow === false && d.reason).toMatch(/could not be established/);
  });

  it('refuses an empty lobby name', async () => {
    expect((await listByLobby(hank, '')).allow).toBe(false);
  });
});

// ---------------------------------------------------------------------------
// `recording:read` exists because the lookup behind it got wider.
//
// It used to be `listByUser(caller, { limit: 100 }).find(...)` — the
// caller's own index served as both the lookup and, by accident, the access
// check. Replacing that with a by-id point-query (so a recording past the
// caller's 100th stops 404ing) removed the accident, so the check had to
// become deliberate. These are the properties it must hold.
// ---------------------------------------------------------------------------
describe('recording:read', () => {
  const read = (principal: ReturnType<typeof principalOf>, attrs?: Record<string, unknown>) =>
    authorize({
      principal,
      action: 'recording:read',
      resource: { type: 'recording', id: 'rec-abc', ...(attrs ? { attrs } : {}) },
    });

  it('allows a participant', async () => {
    expect((await read(hank, { isParticipant: true })).allow).toBe(true);
  });

  it('refuses a non-participant with 404, not 403', async () => {
    const d = await read(hank, { isParticipant: false });
    expect(d.allow).toBe(false);
    // Recordings are made of DMs between named people. A 403 would confirm
    // the recording exists, which is the same disclosure listByLobby
    // refuses above.
    expect(d.allow === false && d.status).toBe(404);
  });

  it('DENIES when membership was never established — never falls open', async () => {
    // A failed or skipped fan-out lookup arrives as a missing attr. Anything
    // other than an explicit `true` has to refuse.
    expect((await read(hank)).allow).toBe(false);
    expect((await read(hank, { isParticipant: 'true' })).allow).toBe(false);
    expect((await read(hank, { isParticipant: undefined })).allow).toBe(false);
  });

  it('is registered, so it is not silently default-denied', async () => {
    expect(registeredActions()).toContain('recording:read');
  });
});
