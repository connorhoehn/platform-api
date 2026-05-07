// Pure-function tests for document-exporter.
//
// No mocks needed — buildJsonExport and buildMarkdownExport are pure
// functions that transform data into export shapes.

import { buildJsonExport, buildMarkdownExport, type DocumentExportData } from '../document-exporter';

const META = {
  id: 'doc-1',
  title: 'Test Doc',
  type: 'custom',
  status: 'draft',
  createdBy: 'alice',
  createdAt: '2026-01-15T10:00:00Z',
  updatedAt: '2026-01-16T12:00:00Z',
};

const SECTION = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  title: 'Overview',
  type: 'text',
  sortOrder: 0,
  createdAt: '2026-01-15T10:00:00Z',
  updatedAt: '2026-01-15T10:00:00Z',
};

const ITEM = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  itemId: 'item-1',
  text: 'Fix the bug',
  status: 'open',
  assignee: 'bob',
  priority: 'high',
  createdAt: '2026-01-15T11:00:00Z',
  updatedAt: '2026-01-15T11:00:00Z',
};

const DONE_ITEM = {
  ...ITEM,
  itemId: 'item-2',
  text: 'Write tests',
  status: 'done',
  assignee: undefined,
  priority: undefined,
};

const COMMENT = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  commentId: 'c-1',
  text: 'Looks good',
  userId: 'bob',
  displayName: 'Bob',
  timestamp: '2026-01-15T12:00:00Z',
};

const REPLY = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  commentId: 'c-2',
  parentCommentId: 'c-1',
  text: 'Thanks!',
  userId: 'alice',
  displayName: 'Alice',
  timestamp: '2026-01-15T13:00:00Z',
};

const REVIEW = {
  documentId: 'doc-1',
  sectionId: 'sec-1',
  userId: 'charlie',
  displayName: 'Charlie',
  status: 'approved',
  timestamp: '2026-01-15T14:00:00Z',
};

function makeData(overrides: Partial<DocumentExportData> = {}): DocumentExportData {
  return {
    meta: META,
    sections: [SECTION as any],
    comments: [],
    reviews: [],
    items: [],
    ...overrides,
  };
}

describe('buildJsonExport', () => {
  it('returns document envelope with meta and sections', () => {
    const result = buildJsonExport(makeData());
    expect(result.document.meta.id).toBe('doc-1');
    expect(result.document.meta.title).toBe('Test Doc');
    expect(result.document.sections).toHaveLength(1);
    expect(result.document.sections[0].id).toBe('sec-1');
  });

  it('groups items, comments, and reviews by section', () => {
    const result = buildJsonExport(makeData({
      items: [ITEM as any],
      comments: [COMMENT as any],
      reviews: [REVIEW as any],
    }));
    const sec = result.document.sections[0];
    expect(sec.items).toHaveLength(1);
    expect(sec.items[0].text).toBe('Fix the bug');
    expect(sec.comments).toHaveLength(1);
    expect(sec.comments[0].text).toBe('Looks good');
    expect(sec.reviews).toHaveLength(1);
    expect(sec.reviews[0].status).toBe('approved');
  });

  it('includes optional meta fields when present', () => {
    const result = buildJsonExport(makeData({
      meta: { ...META, icon: '📄', description: 'A test doc' },
    }));
    expect(result.document.meta.icon).toBe('📄');
    expect(result.document.meta.description).toBe('A test doc');
  });

  it('omits optional meta fields when absent', () => {
    const result = buildJsonExport(makeData());
    expect(result.document.meta).not.toHaveProperty('icon');
    expect(result.document.meta).not.toHaveProperty('description');
  });

  it('omits optional item fields when absent', () => {
    const cleanItem = { ...ITEM, assignee: undefined, priority: undefined, dueDate: undefined, category: undefined, notes: undefined };
    const result = buildJsonExport(makeData({ items: [cleanItem as any] }));
    const exported = result.document.sections[0].items[0];
    expect(exported).not.toHaveProperty('assignee');
    expect(exported).not.toHaveProperty('priority');
  });

  it('handles empty sections gracefully', () => {
    const result = buildJsonExport(makeData({ sections: [] }));
    expect(result.document.sections).toEqual([]);
  });
});

describe('buildMarkdownExport', () => {
  it('starts with title and meta line', () => {
    const md = buildMarkdownExport(makeData());
    expect(md).toContain('# Test Doc');
    expect(md).toContain('Status: draft');
    expect(md).toContain('Created by: alice');
    expect(md).toContain('2026-01-15');
  });

  it('renders section headings', () => {
    const md = buildMarkdownExport(makeData());
    expect(md).toContain('## Overview');
  });

  it('renders action items with checkboxes', () => {
    const md = buildMarkdownExport(makeData({ items: [ITEM as any, DONE_ITEM as any] }));
    expect(md).toContain('- [ ] Fix the bug (@bob, priority: high)');
    expect(md).toContain('- [x] Write tests');
  });

  it('renders comments as blockquotes', () => {
    const md = buildMarkdownExport(makeData({ comments: [COMMENT as any] }));
    expect(md).toContain('> **Bob** (2026-01-15): Looks good');
  });

  it('renders threaded replies indented', () => {
    const md = buildMarkdownExport(makeData({
      comments: [COMMENT as any, REPLY as any],
    }));
    expect(md).toContain('>   > **Alice** (2026-01-15): Thanks!');
  });

  it('renders reviews', () => {
    const md = buildMarkdownExport(makeData({ reviews: [REVIEW as any] }));
    expect(md).toContain('- Approved by Charlie (2026-01-15)');
  });

  it('omits empty subsections', () => {
    const md = buildMarkdownExport(makeData());
    expect(md).not.toContain('### Action Items');
    expect(md).not.toContain('### Comments');
    expect(md).not.toContain('### Reviews');
  });
});
