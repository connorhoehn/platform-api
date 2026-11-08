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

/** Boot gate: SKIP_AUTH=true without the dev flag (or in production) is a misconfiguration. */
export function assertSkipAuthAllowed(env: NodeJS.ProcessEnv = process.env): void {
  assertDevShortcutAllowed('SKIP_AUTH', env);
}

/**
 * Env snapshot for `assertRequiredInProd`: the gate is armed (`<name>=true`)
 * unless the insecure dev switch is on, in which case the operator's own
 * value (or its absence) stands.
 */
export function requiredGateEnv(name: string, env: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  return insecureDevMode(env) ? env : { ...env, [name]: 'true' };
}
