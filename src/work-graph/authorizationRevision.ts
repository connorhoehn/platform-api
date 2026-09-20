import { createHash } from 'crypto';
import type { AuthorizedWorkGraph } from './policy';

/**
 * Bind a snapshot/stream to what the source ACLs actually permit. Grant and
 * membership revisions alone cannot detect a document becoming private.
 * Lifecycle, content and heartbeat timestamps deliberately stay out: ordinary
 * updates use the delta stream rather than invalidating its authorization.
 */
export function workGraphAuthorizationRevision(
  policyRevision: string,
  authorization: Pick<AuthorizedWorkGraph, 'nodes' | 'edges' | 'sources'>,
): string {
  const permissions = (entities: AuthorizedWorkGraph['nodes'] | AuthorizedWorkGraph['edges']) => entities
    .map((entity) => [entity.id, entity.disclosure, [...new Set(entity.capabilities)].sort()])
    .sort((left, right) => String(left[0]).localeCompare(String(right[0])));
  return createHash('sha256').update(JSON.stringify({
    policyRevision,
    nodes: permissions(authorization.nodes),
    edges: permissions(authorization.edges),
    sources: authorization.sources.map(({ source, health }) => [source, health])
      .sort((left, right) => left[0].localeCompare(right[0])),
  })).digest('base64url');
}
