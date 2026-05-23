/**
 * canCreateRoom — pluggable permission hook for hangout-room creation.
 *
 * Why a hook (not a hard-coded policy)? Different deployments enforce
 * different rules: SaaS may gate on a "rooms_create" capability, an
 * internal install may gate on group membership, dev installs should
 * default-allow. We expose a single function the host process can
 * override via `setCanCreateRoomHook(fn)` at bootstrap.
 *
 * Default: open (returns true) — matches dev (SKIP_AUTH=true) and any
 * production deployment that hasn't wired a stricter policy yet.
 *
 * Failure mode: if the hook throws (e.g. transient downstream lookup
 * error), we fail CLOSED (return false). This is the safer default for
 * a write endpoint — operators can wire telemetry on the hook to spot
 * silent denials.
 */

export type CanCreateRoomHook = (
  userId: string,
  orgId: string,
) => boolean | Promise<boolean>;

let hook: CanCreateRoomHook = () => true; // default open

/**
 * Replace the active permission hook. Call this once at process bootstrap
 * (typically from the entrypoint that wires capability/feature-flag
 * lookups). Tests use this to install temporary deny/allow hooks.
 */
export function setCanCreateRoomHook(fn: CanCreateRoomHook): void {
  hook = fn;
}

/**
 * Reset to the default open policy. Test-only convenience so suites can
 * isolate from each other without leaking hook state.
 */
export function resetCanCreateRoomHook(): void {
  hook = () => true;
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
