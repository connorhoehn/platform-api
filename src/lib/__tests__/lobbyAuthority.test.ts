// Per-resource lobby admission for OrgIQ tokens (lib/lobbyAuthority.ts +
// lib/lobbyAccess.ts `orgiqLobbyAuthority` / `governedTenant`). With
// LOBBY_AUTHORITY_REQUIRED=true an OrgIQ caller's lobby must ALSO be admitted
// by the OrgIQ BFF (POST LOBBY_AUTHORITY_URL with the caller's bearer); every
// failure refuses. Off keeps the prefix rule alone.
import type { Request } from 'express';
import { makeLobbyAccess } from '../lobbyAccess';
import { askLobbyAuthority, lobbyAuthorityRequired, lobbyAuthorityTenants } from '../lobbyAuthority';

const URL_ = 'http://bff.test/api/realtime/lobby-access';

/**
 * A stand-in for the BFF route: 401 unless the bearer is 'valid', 200
 * `{ allowed: true }` only for the exact lobbies it admits, 403 otherwise.
 */
function bff(admitted: string[]) {
  const calls: Array<{ auth: string | undefined; lobbyName: unknown }> = [];
  const fetchImpl = (async (_url: string, init?: RequestInit) => {
    const headers = init?.headers as Record<string, string>;
    const { lobbyName } = JSON.parse(String(init?.body ?? '{}')) as { lobbyName?: unknown };
    calls.push({ auth: headers.authorization, lobbyName });
    const json = (status: number, body: unknown) => new Response(JSON.stringify(body), { status, headers: { 'content-type': 'application/json' } });
    if (headers.authorization !== 'Bearer valid') return json(401, { error: 'invalid or expired realtime token' });
    return admitted.includes(String(lobbyName)) ? json(200, { allowed: true }) : json(403, { allowed: false });
  }) as unknown as typeof fetch;
  return { fetchImpl, calls };
}

const orgiq = (sub: string, bearer = 'valid', over: Record<string, unknown> = {}) =>
  ({ user: { sub, orgId: 'orgiq', tenants: ['orgiq', 'social', 'assessment'], authKind: 'orgiq-hs256', ...over }, headers: { authorization: `Bearer ${bearer}` } }) as unknown as Request;
const cognito = (sub: string, orgId: string) => ({ user: { sub, orgId }, headers: { authorization: 'Bearer cognito' } }) as unknown as Request;

function access(required: boolean, admitted: string[]) {
  const server = bff(admitted);
  const a = makeLobbyAccess({
    roomBySlug: async () => null,
    isRoomMember: async () => false,
    isChannelMember: async () => false,
    documentExists: async () => false,
    assertCanReadDocument: async () => { throw new Error('404'); },
    orgiqLobbyAuthority: async (req, lobbyName) => {
      if (!required) return true;
      const h = req.headers.authorization;
      return askLobbyAuthority(h?.startsWith('Bearer ') ? h.slice(7) : '', lobbyName, { url: URL_, fetchImpl: server.fetchImpl });
    },
    governedTenant: (t) => required && ['orgiq', 'assessment', 'social', 'pipelines'].includes(t),
  });
  return { a, calls: server.calls };
}

describe('LOBBY_AUTHORITY_REQUIRED on', () => {
  it('grant present: the BFF admits this exact lobby → allowed, asked with the caller\'s own bearer', async () => {
    const { a, calls } = access(true, ['orgiq:initiative:p-auth-migration']);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:initiative:p-auth-migration')).toBe(true);
    expect(calls).toEqual([{ auth: 'Bearer valid', lobbyName: 'orgiq:initiative:p-auth-migration' }]);
  });
  it('expired/forged token at the BFF (401) → refused 404, even though platform-api accepted the token', async () => {
    const { a } = access(true, ['orgiq:initiative:p-auth-migration']);
    expect(await a.canUseLobby(orgiq('u-vera', 'expired'), 'orgiq:initiative:p-auth-migration')).toBe(false);
    await expect(a.assertLobbyAccess(orgiq('u-vera', 'expired'), 'orgiq:initiative:p-auth-migration')).rejects.toMatchObject({ status: 404 });
  });
  it('wrong lobby: an admission for one lobby is not one for another (no prefix or wildcard carry-over)', async () => {
    const { a } = access(true, ['orgiq:initiative:p-auth-migration']);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:initiative:p-soc2')).toBe(false);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:initiative:p-auth-migration:x')).toBe(false);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:unlisted-family')).toBe(false);
    expect(await a.canUseLobby(orgiq('u-vera'), 'social:ch:private-design')).toBe(false);
  });
  it('forged tenant: a lobby outside the token\'s tenants is refused before the BFF is asked', async () => {
    const { a, calls } = access(true, ['pipelines:run:r-1']);
    expect(await a.canUseLobby(orgiq('u-vera'), 'pipelines:run:r-1')).toBe(false);
    // A DM the caller is not in: refused by the prefix rule, never asked.
    expect(await a.canUseLobby(orgiq('u-theo'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
    expect(calls).toEqual([]);
  });
  it('a governed tenant is unreachable to every non-OrgIQ caller (Cognito / SKIP_AUTH dev bearer)', async () => {
    const { a, calls } = access(true, ['orgiq:initiative:p-auth-migration']);
    expect(await a.canUseLobby(cognito('u-vera', 'orgiq'), 'orgiq:initiative:p-auth-migration')).toBe(false);
    expect(await a.canUseLobby(cognito('u-vera', 'orgiq'), 'orgiq:dm:u-nora:u-vera')).toBe(false);
    // Other partitions and unprefixed lobbies keep their rules.
    expect(await a.canUseLobby(cognito('alice', 'org-1'), 'org-1:dm:alice:bob')).toBe(true);
    expect(await a.canUseLobby(cognito('alice', 'org-1'), 'dm:alice:bob')).toBe(true);
    expect(await a.canUseLobby(cognito('alice', 'org-1'), 'global-hangout')).toBe(true);
    expect(calls).toEqual([]);
  });
  it('one BFF question per request and lobby (assert + filter)', async () => {
    const { a, calls } = access(true, ['orgiq:initiative:p-auth-migration']);
    const r = orgiq('u-vera');
    await a.canUseLobby(r, 'orgiq:initiative:p-auth-migration');
    await a.assertLobbyAccess(r, 'orgiq:initiative:p-auth-migration');
    expect(calls).toHaveLength(1);
  });
});

describe('LOBBY_AUTHORITY_REQUIRED off (default) keeps today\'s prefix rule', () => {
  it('admits any well-formed lobby in the token\'s tenants, never asking the BFF', async () => {
    const { a, calls } = access(false, []);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:initiative:p-soc2')).toBe(true);
    expect(await a.canUseLobby(orgiq('u-vera'), 'orgiq:anything')).toBe(true);
    expect(await a.canUseLobby(orgiq('u-vera'), 'pipelines:run:r-1')).toBe(false);
    expect(await a.canUseLobby(cognito('u-vera', 'orgiq'), 'orgiq:dm:u-nora:u-vera')).toBe(true);
    expect(calls).toEqual([]);
  });
});

describe('askLobbyAuthority fails closed', () => {
  const lobby = 'orgiq:initiative:p-auth-migration';
  const respond = (status: number, body: unknown) => (async () => new Response(typeof body === 'string' ? body : JSON.stringify(body), { status })) as unknown as typeof fetch;
  it.each([
    ['no URL configured', { url: '' }],
    ['a BFF without the route (404)', { url: URL_, fetchImpl: respond(404, { error: 'not found' }) }],
    ['5xx', { url: URL_, fetchImpl: respond(503, { error: 'down' }) }],
    ['200 without allowed:true', { url: URL_, fetchImpl: respond(200, { allowed: 'yes' }) }],
    ['200 with a non-JSON body', { url: URL_, fetchImpl: respond(200, 'ok') }],
    ['a network error', { url: URL_, fetchImpl: (async () => { throw new TypeError('fetch failed'); }) as unknown as typeof fetch }],
  ])('%s → refused', async (_label, opts) => {
    expect(await askLobbyAuthority('valid', lobby, opts)).toBe(false);
  });
  it('a hung BFF → refused at the timeout', async () => {
    const hang = ((_u: string, init?: RequestInit) => new Promise((_res, rej) => {
      init?.signal?.addEventListener('abort', () => rej(new Error('aborted')));
    })) as unknown as typeof fetch;
    expect(await askLobbyAuthority('valid', lobby, { url: URL_, fetchImpl: hang, timeoutMs: 20 })).toBe(false);
  });
  it('an empty bearer is refused without a request', async () => {
    const { fetchImpl, calls } = bff([lobby]);
    expect(await askLobbyAuthority('', lobby, { url: URL_, fetchImpl })).toBe(false);
    expect(calls).toEqual([]);
  });
});

describe('configuration', () => {
  it('the flag is on only for "true"; tenants default to the BFF\'s four', () => {
    expect(lobbyAuthorityRequired({} as NodeJS.ProcessEnv)).toBe(false);
    expect(lobbyAuthorityRequired({ LOBBY_AUTHORITY_REQUIRED: 'TRUE' } as NodeJS.ProcessEnv)).toBe(true);
    expect(lobbyAuthorityRequired({ LOBBY_AUTHORITY_REQUIRED: '1' } as NodeJS.ProcessEnv)).toBe(false);
    expect(lobbyAuthorityTenants({} as NodeJS.ProcessEnv)).toEqual(['orgiq', 'assessment', 'social', 'pipelines']);
    expect(lobbyAuthorityTenants({ LOBBY_AUTHORITY_TENANTS: 'orgiq, social' } as NodeJS.ProcessEnv)).toEqual(['orgiq', 'social']);
  });
});
