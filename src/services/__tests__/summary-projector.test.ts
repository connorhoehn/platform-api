// Wave A (2026-08-25): summary → document projection.
//
// parseActionItems: the summary prompt mandates an exact markdown table
// shape — the parser must survive the separator row, TBD deadlines, and
// a missing section. projectSummary: owner labels resolve to userIds via
// the participant roster, redelivery dedupes on existing item text, and
// non-document lobbies (plain hangouts) skip projection entirely.

import { describe, test, expect, jest } from '@jest/globals';
import { parseActionItems, projectSummary } from '../summary-projector';

const SUMMARY = `## Summary
We aligned on the Q3 rollout.

## Key Decisions
- Ship it.

## Action Items
| Owner | Action | Deadline |
|-------|--------|----------|
| Hank | Draft the rollout doc | 2026-09-01 |
| Carol | Review capacity plan | TBD |

## Open Questions
- None.
`;

describe('parseActionItems', () => {
  test('parses the mandated table, skipping header/separator, TBD → no dueDate', () => {
    expect(parseActionItems(SUMMARY)).toEqual([
      { owner: 'Hank', action: 'Draft the rollout doc', deadline: '2026-09-01' },
      { owner: 'Carol', action: 'Review capacity plan' },
    ]);
  });

  test('missing section → empty', () => {
    expect(parseActionItems('## Summary\nShort call, no commitments.')).toEqual([]);
  });

  test('stops at the next section heading', () => {
    const md = `## Action Items
| Owner | Action | Deadline |
|---|---|---|
| A | Do a thing | TBD |

## Speaking Time
- A: 5 min`;
    expect(parseActionItems(md)).toHaveLength(1);
  });
});

interface CreatedItem {
  documentId: string;
  sectionId: string;
  text: string;
  assignee?: string;
  dueDate?: string;
  category?: string;
}

function makeDeps(over: {
  summaryMarkdown?: string | undefined;
  lobbyName?: string;
  sections?: Array<{ sectionId: string; title?: string }>;
  existingItems?: Array<{ text: string }>;
  participants?: Array<{ userId: string; displayName?: string }>;
}) {
  const created: CreatedItem[] = [];
  const dispatch = jest.fn(async () => ({ fired: [], failed: [] }));
  const deps = {
    recordings: {
      getRecording: jest.fn(async () => ({
        callId: 'call-1',
        recordingId: 'rec-1',
        lobbyName: over.lobbyName ?? 'doc-42',
        summaryMarkdown: 'summaryMarkdown' in over ? over.summaryMarkdown : SUMMARY,
        participants: over.participants ?? [
          { userId: 'dev-hank', displayName: 'Hank' },
          { userId: 'dev-carol', displayName: 'Carol' },
        ],
      })),
    },
    sections: {
      getSectionsForDocument: jest.fn(async () =>
        over.sections ?? [{ sectionId: 'sec-1', title: 'Overview' }],
      ),
    },
    items: {
      getItemsForSection: jest.fn(async () => over.existingItems ?? []),
      createItem: jest.fn(async (input: CreatedItem) => {
        created.push(input);
        return { ...input, itemId: `item-${created.length}` };
      }),
    },
    dispatch,
  };
  return { deps: deps as never as Parameters<typeof projectSummary>[1], created, raw: deps };
}

describe('projectSummary', () => {
  test('projects action items with roster-resolved assignees', async () => {
    const { deps, created } = makeDeps({});
    const out = await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);

    expect(out).toMatchObject({ projected: 2, skipped: 0, documentId: 'doc-42' });
    expect(created).toEqual([
      expect.objectContaining({
        documentId: 'doc-42',
        sectionId: 'action-items',
        text: 'Draft the rollout doc',
        assignee: 'dev-hank',
        dueDate: '2026-09-01',
        category: 'action-item',
      }),
      expect.objectContaining({
        text: 'Review capacity plan',
        assignee: 'dev-carol',
      }),
    ]);
    expect(created[1].dueDate).toBeUndefined();
  });

  test('unresolvable owner keeps the raw label as assignee', async () => {
    const { deps, created } = makeDeps({ participants: [] });
    await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);
    expect(created[0].assignee).toBe('Hank');
  });

  test('prefers a real section titled "Action Items"', async () => {
    const { deps, created } = makeDeps({
      sections: [
        { sectionId: 'sec-1', title: 'Overview' },
        { sectionId: 'sec-9', title: 'Action Items' },
      ],
    });
    await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);
    expect(created[0].sectionId).toBe('sec-9');
  });

  test('redelivery dedupes on existing item text', async () => {
    const { deps, created } = makeDeps({
      existingItems: [{ text: 'Draft the rollout doc' }],
    });
    const out = await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);
    expect(out).toMatchObject({ projected: 1, skipped: 1 });
    expect(created).toHaveLength(1);
    expect(created[0].text).toBe('Review capacity plan');
  });

  test('plain hangout lobby (no sections) skips projection', async () => {
    const { deps, created } = makeDeps({ sections: [] });
    const out = await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);
    expect(out.reason).toBe('lobby is not a document');
    expect(created).toHaveLength(0);
  });

  test('missing summary skips projection', async () => {
    const { deps, created } = makeDeps({ summaryMarkdown: undefined });
    const out = await projectSummary({ recordingId: 'rec-1', callId: 'call-1' }, deps);
    expect(out.reason).toBe('recording or summary missing');
    expect(created).toHaveLength(0);
  });
});
