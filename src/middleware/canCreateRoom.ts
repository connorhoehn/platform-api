/**
 * canCreateRoom — pluggable permission hook for hangout-room creation.
 *
 * Why a hook (not a hard-coded policy)? Different deployments enforce
 * different rules: SaaS may gate on a "rooms_create" capability, an
 * internal install may gate on group membership, dev installs should
 * default-allow. We expose a single function the host process can
 * override via `setCanCreateRoomHook(fn)` at bootstrap.
 *
 * Default rule (installed at module load, so every process has it): the
 * caller must have a user id and an org id, and the anonymous `dev-user`
 * identity counts only under INSECURE_DEV_MODE.
 *
 * Failure mode: if the hook throws (e.g. transient downstream lookup
 * error), we fail CLOSED (return false). This is the safer default for
 * a write endpoint — operators can wire telemetry on the hook to spot
 * silent denials.
 */

import { insecureDevMode } from '../lib/insecureDevMode';

export type CanCreateRoomHook = (
  userId: string,
  orgId: string,
) => boolean | Promise<boolean>;

export const defaultCanCreateRoomRule: CanCreateRoomHook = (userId, orgId) => {
  if (!userId || !orgId) return false;
  if (userId === 'dev-user') return insecureDevMode();
  return true;
};

let hook: CanCreateRoomHook = defaultCanCreateRoomRule;

/**
 * Replace the active permission hook. Call this once at process bootstrap
 * (typically from the entrypoint that wires capability/feature-flag
 * lookups). Tests use this to install temporary deny/allow hooks.
 */
export function setCanCreateRoomHook(fn: CanCreateRoomHook): void {
  hook = fn;
}

/**
 * Reset to the default rule. Test-only convenience so suites can
 * isolate from each other without leaking hook state.
 */
export function resetCanCreateRoomHook(): void {
  hook = defaultCanCreateRoomRule;
}

/**
 * Evaluate the hook. Always returns a boolean — never throws. On hook
 * errors, fails closed (returns false) so a flaky downstream doesn't
 * silently grant access.
 */
export async function canCreateRoom(userId: string, orgId: string): Promise<boolean> {
  try {
    return !!(await Promise.resolve(hook(userId, orgId)));
  } catch {
    return false;
  }
}
