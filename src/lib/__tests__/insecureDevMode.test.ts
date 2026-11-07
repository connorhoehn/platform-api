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
    expect(() => assertSkipAuthAllowed(env({ SKIP_AUTH: 'true', INSECURE_DEV_MODE: 'true' }))).not.toThrow();
    expect(() => assertSkipAuthAllowed(env({}))).not.toThrow();
  });
});
