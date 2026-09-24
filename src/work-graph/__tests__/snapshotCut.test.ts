import { describe, expect, it } from '@jest/globals';
import type { ViewerWorkNode } from '@connorhoehn/realtime-modules/work-graph';
import { cutSnapshotNodes, pageOmittedNodes } from '../snapshotCut';

const NOW = '2026-09-24T11:30:00.000Z';
const minutesAgo = (minutes: number) => new Date(Date.parse(NOW) - minutes * 60_000).toISOString();
function node(id: string, kind: ViewerWorkNode['kind'], updatedAt: string, status: ViewerWorkNode['status'] = 'completed'): ViewerWorkNode {
  return { id, kind, title: id, status, disclosure: 'details', updatedAt, capabilities: [] };
}
/** connor's day: a deck saved minutes ago, then a burst of runs, seeded rows dated ahead, old live runs. */
function busyDay(runs: number, deckAgeMinutes = 5): ViewerWorkNode[] {
  return [
    node('deck', 'document', minutesAgo(deckAgeMinutes)),
    node('deck-v1', 'change', minutesAgo(deckAgeMinutes)),
    ...Array.from({ length: runs }, (_, i) => node(`run-${String(i).padStart(4, '0')}`, 'run', minutesAgo(i * 0.05))),
    node('seed-doc', 'document', '2026-09-24T13:39:00.000Z'),
    node('seed-meeting', 'meeting', '2026-09-24T13:39:00.000Z'),
    node('stuck-run', 'run', minutesAgo(9 * 60), 'running'),
    node('waiting-run', 'run', minutesAgo(8 * 60), 'waiting'),
  ];
}

describe('NFR #221 — which nodes a capped snapshot keeps', () => {
  it('keeps a deck saved minutes ago through a burst of newer runs', () => {
    for (const age of [1, 5, 15, 60]) {
      const { kept, omitted } = cutSnapshotNodes(busyDay(1_500, age), NOW);
      expect(kept).toHaveLength(250);
      expect(kept.map((n) => n.id)).toEqual(expect.arrayContaining(['deck', 'deck-v1']));
      expect(omitted).toHaveLength(1_506 - 250);
    }
  });

  it('always keeps live work, however old', () => {
    const ids = cutSnapshotNodes(busyDay(1_500), NOW).kept.map((n) => n.id);
    expect(ids).toEqual(expect.arrayContaining(['stuck-run', 'waiting-run']));
  });

  it('reads a row dated ahead as the horizon: kept, but not ahead of real work', () => {
    const { kept } = cutSnapshotNodes(busyDay(1_500), NOW);
    const ids = kept.map((n) => n.id);
    expect(ids).toEqual(expect.arrayContaining(['seed-doc', 'seed-meeting']));
    // Ties at the horizon break on id, so the newest real run is not pushed down by the seeds' dates.
    expect(ids.indexOf('run-0000')).toBeLessThan(ids.indexOf('seed-doc'));
  });

  it('gives made-or-met work at most half the cap', () => {
    const docs = Array.from({ length: 400 }, (_, i) => node(`doc-${i}`, 'change', minutesAgo(10 + i * 0.1)));
    const runs = Array.from({ length: 400 }, (_, i) => node(`run-${i}`, 'run', minutesAgo(i * 0.01)));
    const { kept } = cutSnapshotNodes([...docs, ...runs], NOW);
    expect(kept.filter((n) => n.kind === 'change')).toHaveLength(125);
    expect(kept.filter((n) => n.kind === 'run')).toHaveLength(125);
  });

  it('is deterministic: any input order gives the same cut, in the same order', () => {
    const day = busyDay(900);
    const first = cutSnapshotNodes(day, NOW);
    for (let round = 0; round < 20; round += 1) {
      const shuffled = day.slice().sort(() => Math.random() - 0.5);
      const next = cutSnapshotNodes(shuffled, NOW);
      expect(next.kept.map((n) => n.id)).toEqual(first.kept.map((n) => n.id));
      expect(next.omitted.map((n) => n.id)).toEqual(first.omitted.map((n) => n.id));
    }
  });

  it('keeps everything under the cap, newest first', () => {
    const { kept, omitted } = cutSnapshotNodes(busyDay(10), NOW);
    expect(omitted).toEqual([]);
    expect(kept).toHaveLength(16);
  });
});

describe('NFR #221 — paging the held-back nodes', () => {
  it('walks every omitted node once, newest first, and ignores work that lands between pages', () => {
    const { omitted } = cutSnapshotNodes(busyDay(600), NOW);
    const seen: string[] = [];
    let after: string | undefined;
    let pages = 0;
    do {
      // New work arriving between pages ranks ahead of the key.
      const current = pages === 1 ? [node('late', 'run', NOW), ...omitted] : omitted;
      const page = pageOmittedNodes(current, NOW, after, 100);
      seen.push(...page.nodes.map((n) => n.id));
      after = page.next;
      pages += 1;
    } while (after);
    expect(seen).toEqual(omitted.map((n) => n.id));
    expect(pages).toBe(Math.ceil(omitted.length / 100));
  });

  it('refuses a key it did not issue', () => {
    expect(() => pageOmittedNodes([], NOW, 'bm90LWEta2V5', 10)).toThrow('Page key is invalid.');
  });
});
