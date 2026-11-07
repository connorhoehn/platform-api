// platform-api/src/lib/insecureDevMode.ts
//
// ONE explicit switch for the insecure local-development shortcuts.
//
// SKIP_AUTH used to be honoured whenever NODE_ENV was not 'production' — and
// the image never set NODE_ENV, so a deployment that inherited SKIP_AUTH=true
// ran every request as an unverified, decoded JWT (security review, 6 November
// 2026). Now SKIP_AUTH counts only with INSECURE_DEV_MODE=true as well, never
// in production, and the process refuses to start when SKIP_AUTH is set
// without it (assertSkipAuthAllowed, called from index.ts).

export const INSECURE_DEV_FLAG = 'INSECURE_DEV_MODE';

/** The explicit dev flag is on, and this is not production. */
export function insecureDevMode(env: NodeJS.ProcessEnv = process.env): boolean {
  return (env[INSECURE_DEV_FLAG] ?? '').trim().toLowerCase() === 'true' && env.NODE_ENV !== 'production';
}

/** Is the SKIP_AUTH dev fallback in effect? */
export function skipAuthEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return env.SKIP_AUTH === 'true' && insecureDevMode(env);
}

/** Boot gate: SKIP_AUTH=true without the dev flag (or in production) is a misconfiguration. */
export function assertSkipAuthAllowed(env: NodeJS.ProcessEnv = process.env): void {
  if (env.SKIP_AUTH !== 'true') return;
  if (env.NODE_ENV === 'production') {
    throw new Error('SKIP_AUTH=true is not allowed in production');
  }
  if (!insecureDevMode(env)) {
    throw new Error(`SKIP_AUTH=true requires ${INSECURE_DEV_FLAG}=true (local development only)`);
  }
}
