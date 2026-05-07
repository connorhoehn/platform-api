/**
 * Tests for social-api DocumentExporter — pure builders, no DB.
 *
 * Covers:
 *  - buildJsonExport minimal shape
 *  - buildJsonExport nested sections + items + comments + reviews
 *  - buildMarkdownExport structural output (headings, lists, quotes)
 *  - Round-trip: JSON export → parseJsonImport → same section titles + items
 */
import {
  buildJsonExport,
  buildMarkdownExport,
  type DocumentExportData,
  type DocumentMeta,
} from '../src/services/document-exporter';
import { parseJsonImport } from '../src/services/document-importer';

const baseMeta: DocumentMeta = {
  id: 'doc-1',
  title: 'Quarterly Plan',
  type: 'plan',
  status: 'draft',
  createdBy: 'alice',
  createdAt: '2026-04-18T10:00:00.000Z',
  updatedAt: '2026-04-18T11:00:00.000Z',
};

function baseData(): DocumentExportData {
  return {
    meta: baseMeta,
    sections: [],
    comments: [],
    reviews: [],
    items: [],
  };
}

describe('buildJsonExport', () => {
  test('minimal document produces expected envelope', () => {
    const out = buildJsonExport(baseData());
    expect(out).toEqual({
      document: {
        meta: {
          id: 'doc-1',
          title: 'Quarterly Plan',
          type: 'plan',
          status: 'draft',
          createdBy: 'alice',
          createdAt: '2026-04-18T10:00:00.000Z',
          updatedAt: '2026-04-18T11:00:00.000Z',
        },
        sections: [],
      },
    });
  });

  test('optional meta fields passed through only when present', () => {
    const out = buildJsonExport({
      ...baseData(),
      meta: { ...baseMeta, icon: 'rocket', description: 'Plan for Q2' },
    });
    expect(out.document.meta).toMatchObject({
      icon: 'rocket',
      description: 'Plan for Q2',
    });
  });

  test('nested sections preserve items, comments, reviews grouped by sectionId', () => {
    const data: DocumentExportData = {
      meta: baseMeta,
      sections: [
        {
          documentId: 'doc-1',
          sectionId: 'sec-a',
          type: 'text',
          title: 'Goals',
          sortOrder: 0,
          createdAt: 't1',
          updatedAt: 't2',
        },
        {
          documentId: 'doc-1',
          sectionId: 'sec-b',
          type: 'text',
          title: 'Actions',
          sectionType: 'checklist',
          sortOrder: 1,
          createdAt: 't1',
          updatedAt: 't2',
        },
      ],
      items: [
        {
          sectionKey: 'doc-1:sec-b',
          itemId: 'item-1',
          documentId: 'doc-1',
          sectionId: 'sec-b',
          text: 'Ship MVP',
          status: 'open',
          assignee: 'bob',
          priority: 'high',
          createdAt: 't1',
          updatedAt: 't1',
        },
      ],
      comments: [
        {
          documentId: 'doc-1',
          commentId: 'c-1',
          sectionId: 'sec-a',
          text: 'Looks good',
          userId: 'u-1',
          displayName: 'Alice',
          color: '#fff',
          timestamp: '2026-04-18T10:00:00.000Z',
        },
      ],
      reviews: [
        {
          documentId: 'doc-1',
          reviewKey: 'sec-a:u-2',
          sectionId: 'sec-a',
          userId: 'u-2',
          displayName: 'Carol',
          status: 'approved',
          timestamp: '2026-04-18T10:30:00.000Z',
        },
      ],
    };

    const out = buildJsonExport(data);
    expect(out.document.sections).toHaveLength(2);

    const [goals, actions] = out.document.sections;
    expect(goals.id).toBe('sec-a');
    expect(goals.items).toEqual([]);
    expect(goals.comments).toHaveLength(1);
    expect(goals.comments[0]).toMatchObject({ text: 'Looks good', displayName: 'Alice' });
    expect(goals.reviews).toHaveLength(1);
    expect(goals.reviews[0]).toMatchObject({ status: 'approved' });

    expect(actions.id).toBe('sec-b');
    expect(actions.sectionType).toBe('checklist');
    expect(actions.items).toHaveLength(1);
    expect(actions.items[0]).toMatchObject({
      id: 'item-1',
      text: 'Ship MVP',
      assignee: 'bob',
      priority: 'high',
    });
  });
});

describe('buildMarkdownExport', () => {
  test('produces valid markdown with headings, list items, quotes', () => {
    const md = buildMarkdownExport({
      meta: baseMeta,
      sections: [
        {
          documentId: 'doc-1',
          sectionId: 'sec-a',
          type: 'text',
          title: 'Goals',
          sortOrder: 0,
          createdAt: 't1',
          updatedAt: 't2',
        },
      ],
      items: [
        {
          sectionKey: 'doc-1:sec-a',
          itemId: 'i-1',
          documentId: 'doc-1',
          sectionId: 'sec-a',
          text: 'Write docs',
          status: 'done',
          assignee: 'bob',
          priority: 'high',
          createdAt: 't',
          updatedAt: 't',
        },
        {
          sectionKey: 'doc-1:sec-a',
          itemId: 'i-2',
          documentId: 'doc-1',
          sectionId: 'sec-a',
          text: 'Ship it',
          status: 'open',
          createdAt: 't',
          updatedAt: 't',
        },
      ],
      comments: [
        {
          documentId: 'doc-1',
          commentId: 'c-1',
          sectionId: 'sec-a',
          text: 'Nice',
          userId: 'u-1',
          displayName: 'Alice',
          color: '#fff',
          timestamp: '2026-04-18T10:00:00.000Z',
        },
      ],
      reviews: [],
    });

    expect(md).toContain('# Quarterly Plan');
    expect(md).toContain('Status: draft | Created by: alice | Date: 2026-04-18');
    expect(md).toContain('## Goals');
    expect(md).toContain('### Action Items');
    expect(md).toContain('- [x] Write docs (@bob, priority: high)');
    expect(md).toContain('- [ ] Ship it');
    expect(md).toContain('### Comments');
    expect(md).toContain('> **Alice** (2026-04-18): Nice');
  });

  test('omits sections with no items/comments/reviews beyond the heading', () => {
    const md = buildMarkdownExport({
      meta: baseMeta,
      sections: [
        {
          documentId: 'doc-1',
          sectionId: 'empty',
          type: 'text',
          title: 'Empty',
          sortOrder: 0,
          createdAt: 't',
          updatedAt: 't',
        },
      ],
      items: [],
      comments: [],
      reviews: [],
    });
    expect(md).toContain('## Empty');
    expect(md).not.toContain('### Action Items');
    expect(md).not.toContain('### Comments');
    expect(md).not.toContain('### Reviews');
  });
});

describe('exporter → importer round trip', () => {
  test('JSON export can be re-imported and yields the same section titles + item texts', () => {
    const data: DocumentExportData = {
      meta: baseMeta,
      sections: [
        {
          documentId: 'doc-1',
          sectionId: 'sec-a',
          type: 'text',
          title: 'Goals',
          sortOrder: 0,
          createdAt: 't',
          updatedAt: 't',
        },
        {
          documentId: 'doc-1',
          sectionId: 'sec-b',
          type: 'text',
          title: 'Actions',
          sortOrder: 1,
          createdAt: 't',
          updatedAt: 't',
        },
      ],
      items: [
        {
          sectionKey: 'doc-1:sec-b',
          itemId: 'i-1',
          documentId: 'doc-1',
          sectionId: 'sec-b',
          text: 'Ship MVP',
          status: 'open',
          priority: 'high',
          createdAt: 't',
          updatedAt: 't',
        },
      ],
      comments: [],
      reviews: [],
    };

    const json = buildJsonExport(data);
    const reimported = parseJsonImport(JSON.stringify(json));

    expect(reimported.map((s) => s.title)).toEqual(['Goals', 'Actions']);
    expect(reimported[1].items.map((i) => i.text)).toEqual(['Ship MVP']);
    expect(reimported[1].items[0]).toMatchObject({ status: 'open', priority: 'high' });
  });
});
