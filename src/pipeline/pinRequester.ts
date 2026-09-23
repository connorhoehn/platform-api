// A run's context is its trigger payload (distributed-core seeds
// `context = { ...triggerPayload, steps: {} }`), and step handlers act AS
// `context.requestedBy`: the deck steps write revisions under that name and
// check edit rights against it (NFR #41, #47). Wherever the payload is JSON a
// caller supplied — the manual trigger route, a replay override, a webhook
// body, an automation's saved binding — the requester is pinned to the
// identity the server verified, never taken from the payload.

/**
 * The payload with `requestedBy` (and `trigger.requestedBy`, the other path the
 * deck steps read) set to the verified caller. `overridden` names what the
 * caller claimed instead, for the log.
 */
export function pinRequester(
  payload: Record<string, unknown> | undefined,
  userId: string,
): { payload: Record<string, unknown>; overridden?: string } {
  const base = payload && typeof payload === 'object' && !Array.isArray(payload) ? payload : {};
  const claims = [base.requestedBy, (base.trigger as Record<string, unknown> | undefined)?.requestedBy]
    .filter((value): value is string => typeof value === 'string' && value !== userId);
  const trigger = base.trigger && typeof base.trigger === 'object' && !Array.isArray(base.trigger)
    ? { trigger: { ...(base.trigger as Record<string, unknown>), ...('requestedBy' in (base.trigger as Record<string, unknown>) ? { requestedBy: userId } : {}) } }
    : {};
  return { payload: { ...base, ...trigger, requestedBy: userId }, ...(claims.length ? { overridden: claims[0] } : {}) };
}

