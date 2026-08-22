// lib/org-channel.ts
//
// GAP B.2 (cell architecture): org-prefixed channel names.
//
// platform-api owns channel NAMING; the gateway's authz gate
// (realtime-examples/src/middleware/authz-middleware.ts deriveChannelOrg)
// already enforces the `org:<orgId>:<name>` shape: a user token carrying an
// org claim may only touch channels whose name carries the SAME org.
// Unprefixed channels fail OPEN at that gate (legacy compatibility), which is
// what makes this rollout incremental: existing rooms keep their bare-UUID
// channelIds and behave exactly as before; rooms minted by org-carrying
// tokens get the prefix and the tenant gate starts to bite.
//
// Side effect (by design, see CELL-ARCHITECTURE-DESIGN "2b→B.2"): the
// gateway's per-channel seq counters key on the channel string, so an
// org-prefixed name makes channel:seq org-scoped for free — no rekey of the
// hot broadcast path.

const ORG_PREFIX = 'org:';

/**
 * Build a channel name, org-prefixed when the caller's org is known.
 *
 *   orgChannel('acme', 'a1b2…')  → 'org:acme:a1b2…'
 *   orgChannel(undefined, 'a1b2…') → 'a1b2…'   (org-less → unprefixed, fails
 *                                               open at the gateway gate —
 *                                               preserves dev/SKIP_AUTH and
 *                                               legacy-token behavior)
 *
 * NEVER pass the cell-router's `default-org` placement fallback as `orgId` —
 * an org-less caller stays org-less (see org-claim.ts).
 */
export function orgChannel(orgId: string | null | undefined, name: string): string {
  const org = typeof orgId === 'string' ? orgId.trim() : '';
  if (org === '') return name;
  return `${ORG_PREFIX}${org}:${name}`;
}

/**
 * Parse the owning org out of a channel name. Mirrors the gateway's
 * deriveChannelOrg: `null` for unprefixed/legacy names.
 */
export function parseChannelOrg(channelId: string): string | null {
  if (!channelId.startsWith(ORG_PREFIX)) return null;
  const rest = channelId.slice(ORG_PREFIX.length);
  const sep = rest.indexOf(':');
  const org = sep === -1 ? rest : rest.slice(0, sep);
  return org.trim() !== '' ? org.trim() : null;
}
