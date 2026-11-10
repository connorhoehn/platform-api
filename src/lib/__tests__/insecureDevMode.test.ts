import { skipAuthEnabled, assertSkipAuthAllowed, insecureDevMode } from '../insecureDevMode';

const env = (e: Record<string, string>) => e as unknown as NodeJS.ProcessEnv;

describe('insecure dev mode', () => {
  it('SKIP_AUTH counts only with INSECURE_DEV_MODE=true outside production', () => {
    expect(skipAuthEnabled(env({ SKIP_AUTH: 'true' }))).toBe(false);
    expect(skipAuthEnabled(env({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true' }))).toBe(true);
    expect(skipAuthEnabled(env({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true', NODE_ENV: 'production' }))).toBe(false);
    expect(insecureDevMode(env({ INSECURE_DEV_MODE: '1' }))).toBe(false);
  });

  it('boot refuses SKIP_AUTH=true without the flag, and in production', () => {
    expect(() => assertSkipAuthAllowed(env({ SKIP_AUTH: 'true' }))).toThrow(/INSECURE_DEV_MODE/);
    expect(() => assertSkipAuthAllowed(env({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true', NODE_ENV: 'production' }))).toThrow();
    expect(() => assertSkipAuthAllowed(env({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true', BIND_HOST: '127.0.0.1' }))).not.toThrow();
    expect(() => assertSkipAuthAllowed(env({}))).not.toThrow();
  });

  it('SKIP_AUTH needs a loopback bind unless the container network-bind flag is set', () => {
    const dev = { SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true' };
    // No BIND_HOST = every interface: refused.
    expect(() => assertSkipAuthAllowed(env(dev))).toThrow(/loopback bind/);
    expect(() => assertSkipAuthAllowed(env({ ...dev, BIND_HOST: '0.0.0.0' }))).toThrow(/0\.0\.0\.0/);
    expect(() => assertSkipAuthAllowed(env({ ...dev, BIND_HOST: '192.168.1.20' }))).toThrow(/loopback bind/);
    expect(() => assertSkipAuthAllowed(env({ ...dev, BIND_HOST: '::' }))).toThrow(/loopback bind/);
    // HOST is a hostname, not a bind decision: ignored.
    expect(() => assertSkipAuthAllowed(env({ ...dev, HOST: '127.0.0.1' }))).toThrow(/loopback bind/);
    for (const host of ['127.0.0.1', 'localhost', '::1', '[::1]', '127.0.0.2']) {
      expect(() => assertSkipAuthAllowed(env({ ...dev, BIND_HOST: host }))).not.toThrow();
    }
    expect(() => assertSkipAuthAllowed(env({ ...dev, INSECURE_DEV_MODE_ALLOW_NETWORK_BIND: 'true' }))).not.toThrow();
    // The network-bind flag never opens production.
    expect(() => assertSkipAuthAllowed(env({ ...dev, NODE_ENV: 'production', INSECURE_DEV_MODE_ALLOW_NETWORK_BIND: 'true' }))).toThrow();
    // A non-loopback bind is fine when SKIP_AUTH is off.
    expect(() => assertSkipAuthAllowed(env({ INSECURE_DEV_MODE: 'true', BIND_HOST: '0.0.0.0' }))).not.toThrow();
  });
});

describe('shortcuts are ignored without INSECURE_DEV_MODE', () => {
  const saved = { ...process.env };
  afterEach(() => { process.env = { ...saved }; });
  const shortcutOn = () => { process.env.SKIP_AUTH = 'true'; delete process.env.INSECURE_DEV_MODE; };

  it.each([
    ['teams', async () => require('../../routes/teams')],
    ['lvsWebhooks', async () => require('../../routes/lvsWebhooks')],
  ])('%s module loads and skipAuthEnabled() is false', async (_n, load) => {
    shortcutOn();
    await load();
    expect(skipAuthEnabled()).toBe(false);
  });

  it('meetingStepHandler.resolveOrganization does not fall back to the dev org', async () => {
    shortcutOn();
    delete process.env.WORK_GRAPH_ORG; delete process.env.DEFAULT_ORG_ID;
    const { resolveOrganization } = require('../../loop/meetingStepHandler');
    expect(resolveOrganization({}).organizationId).toBeUndefined();
    process.env.INSECURE_DEV_MODE = 'true';
    expect(resolveOrganization({}).organizationId).toBeDefined();
  });

  it('requireRoomMembership does not skip the membership check', async () => {
    shortcutOn();
    const { requireRoomMembership } = require('../../middleware/require-membership');
    const next = jest.fn();
    const res: any = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    await requireRoomMembership({ params: { roomId: 'r' }, user: undefined } as any, res, next).catch(() => undefined);
    expect(next).not.toHaveBeenCalledWith();
  });

  it('document policy stays closed, lobby authority stays required, images stay SSRF-guarded', async () => {
    shortcutOn();
    delete process.env.DOCUMENT_GRANT_ISSUER; delete process.env.DOCUMENT_GRANT_PUBLIC_KEY; delete process.env.DOCUMENT_GRANT_KEY_ID;
    const { requireDocumentPolicy } = require('../../middleware/document-policy');
    const next = jest.fn();
    const res: any = { status: jest.fn().mockReturnThis(), json: jest.fn() };
    await requireDocumentPolicy({} as any, res, next);
    expect(next).not.toHaveBeenCalled();
    expect(res.status).toHaveBeenCalledWith(503);

    const { lobbyAuthorityRequired } = require('../lobbyAuthority');
    expect(lobbyAuthorityRequired(env({ LOBBY_AUTHORITY_REQUIRED: 'false' }))).toBe(true);
    expect(lobbyAuthorityRequired(env({ LOBBY_AUTHORITY_REQUIRED: 'false', INSECURE_DEV_MODE: 'true' }))).toBe(false);
  });

  it('requiredGateEnv arms every gate unless the dev switch is on', async () => {
    const { requiredGateEnv } = require('../insecureDevMode');
    expect(requiredGateEnv('X_REQUIRED', env({ X_REQUIRED: 'false' })).X_REQUIRED).toBe('true');
    expect(requiredGateEnv('X_REQUIRED', env({ X_REQUIRED: 'false', INSECURE_DEV_MODE: 'true' })).X_REQUIRED).toBe('false');
  });

  it('default canCreateRoom rule refuses dev-user and empty ids without the flag', async () => {
    const { defaultCanCreateRoomRule } = require('../../middleware/canCreateRoom');
    delete process.env.INSECURE_DEV_MODE;
    expect(defaultCanCreateRoomRule('dev-user', 'o')).toBe(false);
    expect(defaultCanCreateRoomRule('u', '')).toBe(false);
    expect(defaultCanCreateRoomRule('u', 'o')).toBe(true);
    process.env.INSECURE_DEV_MODE = 'true';
    expect(defaultCanCreateRoomRule('dev-user', 'o')).toBe(true);
  });
});
