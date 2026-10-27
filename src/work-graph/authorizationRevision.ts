import { createHash } from 'crypto';
import type { AuthorizedWorkGraph, ViewerWorkEdge, ViewerWorkNode } from './policy';

/**
 * Bind a snapshot/stream to what the source ACLs actually permit. Grant and
 * membership revisions alone cannot detect a document becoming private.
 * Lifecycle, content and heartbeat timestamps deliberately stay out: ordinary
 * updates use the delta stream rather than invalidating its authorization.
 *
 * Shape: `wa2.<base>.<set>`.
 *  - `base` hashes the grant/membership revision and every source that is
 *    not `available`. A healthy source and an absent one read the same: an
 *    empty day lists no sources, and its first activity adds one, which is an
 *    addition the set below accounts for, not a permission change. A source
 *    whose authorization is unavailable still moves the base (fails closed).
 *  - `set` is the SUM (mod 2^256) of one SHA-256 digest per disclosed node and
 *    edge — (kind, id, disclosure, capabilities). Any permission change, any
 *    removal and any addition still changes it, exactly as the old single hash
 *    did; equality is still the only thing a gateway compares.
 *
 * The sum is what makes it checkable without the old entity list (NFR #22):
 * if the only difference between two revisions is entities CREATED in a
 * replay window, `set(new) − Σ digest(created) = set(old)`. So a new node on
 * the day — the common case, one per document change — no longer has to be
 * treated like a revocation: `revisionExplainedByAdditions` proves it, and
 * anything it cannot prove (a permission changed, something vanished, a
 * placeholder renumbered) still fails closed.
 */
const MODULUS = 1n << 256n;
const PREFIX = 'wa2';

type Entity = Pick<ViewerWorkNode | ViewerWorkEdge, 'id' | 'disclosure' | 'capabilities'>;

export function authorizationEntityDigest(kind: 'node' | 'edge', entity: Entity): bigint {
  const tuple = [kind, entity.id, entity.disclosure, [...new Set(entity.capabilities)].sort()];
  return BigInt(`0x${createHash('sha256').update(JSON.stringify(tuple)).digest('hex')}`);
}

function baseDigest(
  policyRevision: string,
  sources: AuthorizedWorkGraph['sources'],
): string {
  return createHash('sha256').update(JSON.stringify({
    policyRevision,
    sources: sources.filter(({ health }) => health !== 'available')
      .map(({ source, health }) => [source, health])
      .sort((left, right) => String(left[0]).localeCompare(String(right[0]))),
  })).digest('hex').slice(0, 32);
}

export function workGraphAuthorizationRevision(
  policyRevision: string,
  authorization: Pick<AuthorizedWorkGraph, 'nodes' | 'edges' | 'sources'>,
): string {
  let sum = 0n;
  for (const node of authorization.nodes) sum = (sum + authorizationEntityDigest('node', node)) % MODULUS;
  for (const edge of authorization.edges) sum = (sum + authorizationEntityDigest('edge', edge)) % MODULUS;
  return `${PREFIX}.${baseDigest(policyRevision, authorization.sources)}.${sum.toString(16).padStart(64, '0')}`;
}

function parse(revision: string): { base: string; sum: bigint } | null {
  const match = /^wa2\.([0-9a-f]{32})\.([0-9a-f]{64})$/.exec(revision);
  return match ? { base: match[1]!, sum: BigInt(`0x${match[2]}`) } : null;
}

/**
 * True when `to` differs from `from` by exactly the given created entities'
 * digests and nothing else (same grants, memberships and source health).
 */
export function revisionExplainedByAdditions(from: string, to: string, created: bigint[]): boolean {
  if (from === to) return true;
  const before = parse(from);
  const after = parse(to);
  if (!before || !after || before.base !== after.base || created.length === 0) return false;
  let sum = after.sum;
  for (const digest of created) sum = (sum - digest + MODULUS) % MODULUS;
  return sum === before.sum;
}
