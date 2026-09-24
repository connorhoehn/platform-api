import type { ViewerWorkNode } from '@connorhoehn/realtime-modules/work-graph';

/**
 * Which of a day's authorized nodes a snapshot carries (NFR #221).
 *
 * The contract holds 250 nodes and a busy day authorizes far more: connor's
 * 2026-09-24 held ~240 pipeline runs from one hour of another loop's probes,
 * plus seeded rows dated hours AHEAD of now. The old cut was "newest 250 by
 * `updatedAt`", so the future-dated seeds always ranked first and a deck saved
 * a few minutes earlier fell out under the next burst of runs.
 *
 * The cut is a pure function of the nodes and the horizon (the build's `now`):
 * the same input answers the same set in the same order on every replica, which
 * the build memo and the view hash (NFR #16, #85) rely on. Ranked, first kept:
 *
 *   1. live work — a run or anything else `running` or `waiting`;
 *   2. what the person made or met in the last two hours — documents (decks
 *      included), their revisions, meetings, transcripts, conversations,
 *      projects and tasks — at most half the cap, so a day of edits can never
 *      starve the runs out entirely;
 *   3. everything else in the day, newest first.
 *
 * "Newest" reads a row dated after the horizon (the seeded scenario writes
 * rows hours ahead) AS the horizon: it is current, but it cannot outrank the
 * work of the last minute by being dated in the future. Ties break on the id.
 */
export const SNAPSHOT_NODE_CAP = 250;
const RECENT_MS = 2 * 60 * 60 * 1_000;
const MADE_OR_MET = new Set<ViewerWorkNode['kind']>([
  'document', 'change', 'meeting', 'transcript', 'conversation', 'project', 'task',
]);

export interface SnapshotCut {
  kept: ViewerWorkNode[];
  /** Held back, newest first. The "N more earlier" group. */
  omitted: ViewerWorkNode[];
}

export function cutSnapshotNodes(
  nodes: readonly ViewerWorkNode[],
  horizon: string,
  cap = SNAPSHOT_NODE_CAP,
): SnapshotCut {
  const now = Date.parse(horizon);
  // Parsed once per node, not once per comparison.
  const times = new Map(nodes.map((node) => [node.id, effectiveAt(node, now)]));
  const at = (node: ViewerWorkNode) => times.get(node.id) ?? 0;
  const byRecency = (a: ViewerWorkNode, b: ViewerWorkNode) => at(b) - at(a) || a.id.localeCompare(b.id);
  const ordered = nodes.slice().sort(byRecency);
  if (ordered.length <= cap) return { kept: ordered, omitted: [] };

  const taken = new Set<string>();
  const kept: ViewerWorkNode[] = [];
  const take = (node: ViewerWorkNode) => {
    if (kept.length >= cap || taken.has(node.id)) return;
    taken.add(node.id);
    kept.push(node);
  };
  for (const node of ordered) {
    if (node.status === 'running' || node.status === 'waiting') take(node);
  }
  const madeBudget = Math.floor(cap / 2);
  let made = 0;
  for (const node of ordered) {
    if (made >= madeBudget) break;
    if (taken.has(node.id) || !MADE_OR_MET.has(node.kind) || now - at(node) > RECENT_MS) continue;
    take(node);
    made += 1;
  }
  for (const node of ordered) take(node);
  // The kept set goes out in one fixed order (newest first), whatever tier
  // admitted each node, exactly as the old cut did.
  kept.sort(byRecency);
  return { kept, omitted: ordered.filter((node) => !taken.has(node.id)) };
}

/** `updatedAt` as a number, never later than the horizon. */
function effectiveAt(node: ViewerWorkNode, now: number): number {
  const value = Date.parse(node.updatedAt);
  return Number.isFinite(value) ? Math.min(value, now) : 0;
}

/**
 * A page of the held-back nodes: `after` is the previous page's `next`. The
 * key is the last node's place in the omitted order (time, id), not an offset:
 * work that lands between two pages ranks ahead of the key and never shifts
 * what the next page holds.
 */
export function pageOmittedNodes(
  omitted: readonly ViewerWorkNode[],
  horizon: string,
  after: string | undefined,
  limit: number,
): { nodes: ViewerWorkNode[]; next?: string } {
  const now = Date.parse(horizon);
  const keyOf = (node: ViewerWorkNode): PageKey => [effectiveAt(node, now), node.id];
  let start = 0;
  if (after) {
    const [at, id] = decodePageKey(after);
    const index = omitted.findIndex((node) => {
      const [nodeAt, nodeId] = keyOf(node);
      return nodeAt < at || (nodeAt === at && nodeId > id);
    });
    start = index < 0 ? omitted.length : index;
  }
  const nodes = omitted.slice(start, start + limit);
  const last = nodes[nodes.length - 1];
  return start + limit < omitted.length && last
    ? { nodes, next: Buffer.from(JSON.stringify(keyOf(last))).toString('base64url') }
    : { nodes };
}

type PageKey = [number, string];

export class PageKeyError extends Error {}

function decodePageKey(value: string): PageKey {
  try {
    const parsed = JSON.parse(Buffer.from(value, 'base64url').toString('utf8')) as unknown;
    if (Array.isArray(parsed) && parsed.length === 2 && Number.isSafeInteger(parsed[0]) && typeof parsed[1] === 'string') {
      return parsed as PageKey;
    }
  } catch { /* fall through */ }
  throw new PageKeyError('Page key is invalid.');
}
