// platform-api/src/lib/insecureDevMode.ts
//
// ONE explicit switch for the insecure local-development shortcuts. The
// switch itself lives in @connorhoehn/service-runtime (shared by every repo);
// this file keeps platform-api's existing names on top of it.
//
// SKIP_AUTH used to be honoured whenever NODE_ENV was not 'production' — and
// the image never set NODE_ENV, so a deployment that inherited SKIP_AUTH=true
// ran every request as an unverified, decoded JWT (security review, 6 November
// 2026). Now SKIP_AUTH counts only with INSECURE_DEV_MODE=true as well, never
// in production, and the process refuses to start when SKIP_AUTH is set
// without it (assertSkipAuthAllowed, called from index.ts).

import { insecureDevMode, assertDevShortcutAllowed } from '@connorhoehn/service-runtime';

export { insecureDevMode };
export const INSECURE_DEV_FLAG = 'INSECURE_DEV_MODE';

/** Is the SKIP_AUTH dev fallback in effect? */
export function skipAuthEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.SKIP_AUTH === 'true' && insecureDevMode(env);
}

/**
 * The address the HTTP server listens on: BIND_HOST; undefined = every
 * interface. HOST is deliberately not read: shells and platforms set it to a
 * hostname, which is not a bind decision. Same rule as the realtime gateway.
 */
export function resolveBindHost(env: NodeJS.ProcessEnv = process.env): string | undefined {
  const raw = (env.BIND_HOST ?? '').trim();
  return raw === '' ? undefined : raw;
}

export function isLoopbackHost(host: string | undefined | null): boolean {
  if (!host) return false;
  const h = host.trim().replace(/^\[|\]$/g, '').toLowerCase();
  if (h === 'localhost' || h === '::1') return true;
  return /^127\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(h);
}

/**
 * Boot gate: SKIP_AUTH=true without the dev flag (or in production) is a
 * misconfiguration. With the flag it also needs a loopback bind
 * (BIND_HOST=127.0.0.1): a dev server on the network would accept unsigned
 * tokens from anyone who can reach it. A container dev stack cannot listen on
 * loopback (the host and sibling containers reach the container's address), so
 * it may set a second, equally explicit INSECURE_DEV_MODE_ALLOW_NETWORK_BIND=true.
 */
export function assertSkipAuthAllowed(env: NodeJS.ProcessEnv = process.env): void {
  assertDevShortcutAllowed('SKIP_AUTH', env);
  if (!skipAuthEnabled(env)) return;
  const host = resolveBindHost(env);
  if (!isLoopbackHost(host) && env.INSECURE_DEV_MODE_ALLOW_NETWORK_BIND !== 'true') {
    throw new Error(
      `SKIP_AUTH=true requires a loopback bind (BIND_HOST=127.0.0.1); the server would listen on ${host ?? 'every interface'}. ` +
        'A container dev stack may set INSECURE_DEV_MODE_ALLOW_NETWORK_BIND=true instead.',
    );
  }
}

/**
 * Env snapshot for `assertRequiredInProd`: the gate is armed (`<name>=true`)
 * unless the insecure dev switch is on, in which case the operator's own
 * value (or its absence) stands.
 */
export function requiredGateEnv(name: string, env: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  return insecureDevMode(env) ? env : { ...env, [name]: 'true' };
}
