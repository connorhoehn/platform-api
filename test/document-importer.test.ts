/**
 * Tests for social-api DocumentImporter.
 *
 * Covers:
 *  - parseMarkdownSections on a simple doc
 *  - parseMarkdownSections on malformed input → ValidationError
 *  - parseJsonImport happy path
 *  - parseJsonImport missing-field errors
 *  - applyImport issues correct repo.createSection / repo.createItem calls
 */
import {
  parseMarkdownSections,
  parseJsonImport,
  applyImport,
} from '../src/services/document-importer';
import { ValidationError } from '../src/middleware/error-handler';

describe('parseMarkdownSections', () => {
  test('parses section headings and checkbox list items', () => {
    const md = [
      '# Title',
      '',
      '## Goals',
      '- [ ] Write spec',
      '- [x] Review spec (@alice, priority: high)',
      '',
      '## Actions',
      '- [ ] Ship it',
    ].join('\n');

    const out = parseMarkdownSections(md);

    expect(out).toHaveLength(2);
    expect(out[0].title).toBe('Goals');
    expect(out[0].items).toHaveLength(2);
    expect(out[0].items[0]).toMatchObject({ text: 'Write spec', status: 'open' });
    expect(out[0].items[1]).toMatchObject({
      text: 'Review spec',
      status: 'done',
      assignee: 'alice',
      priority: 'high',
    });

    expect(out[1].title).toBe('Actions');
    expect(out[1].items).toEqual([{ text: 'Ship it', status: 'open' }]);
  });

  test('throws ValidationError on empty string', () => {
    expect(() => parseMarkdownSections('')).toThrow(ValidationError);
    expect(() => parseMarkdownSections('   \n  ')).toThrow(ValidationError);
  });

  test('throws ValidationError on non-string input', () => {
    expect(() => parseMarkdownSections(null)).toThrow(ValidationError);
    expect(() => parseMarkdownSections(42)).toThrow(ValidationError);
  });

  test('throws ValidationError when no section headings are present', () => {
    // Content exists but no ## heading → caller almost certainly sent the wrong
    // format. Surface that as ValidationError instead of silently importing 0
    // sections.
    expect(() => parseMarkdownSections('just a paragraph')).toThrow(ValidationError);
  });
});

describe('parseJsonImport', () => {
  test('returns sections + items from a valid JSON export', () => {
    const body = {
      document: {
        meta: { id: 'd', title: 't', type: 'x', status: 'draft', createdBy: 'u', createdAt: '', updatedAt: '' },
        sections: [
          {
            id: 'sec-1',
            title: 'Goals',
            type: 'text',
            sortOrder: 0,
            items: [
              { id: 'i-1', text: 'Write spec', status: 'open', priority: 'high' },
              { id: 'i-2', text: 'Done thing', status: 'done', assignee: 'alice' },
            ],
          },
        ],
      },
    };

    const out = parseJsonImport(JSON.stringify(body));
    expect(out).toHaveLength(1);
    expect(out[0].title).toBe('Goals');
    expect(out[0].type).toBe('text');
    expect(out[0].items).toHaveLength(2);
    expect(out[0].items[0]).toMatchObject({ text: 'Write spec', priority: 'high' });
    expect(out[0].items[1]).toMatchObject({ text: 'Done thing', status: 'done', assignee: 'alice' });
  });

  test('accepts a pre-parsed object', () => {
    const out = parseJsonImport({ document: { sections: [{ title: 'x', items: [] }] } });
    expect(out).toHaveLength(1);
    expect(out[0].title).toBe('x');
  });

  test('throws ValidationError on invalid JSON text', () => {
    expect(() => parseJsonImport('{ not json')).toThrow(ValidationError);
  });

  test('throws ValidationError when document field is missing', () => {
    expect(() => parseJsonImport(JSON.stringify({ foo: 'bar' }))).toThrow(
      ValidationError,
    );
  });

  test('throws ValidationError when document.sections is missing/not an array', () => {
    expect(() => parseJsonImport(JSON.stringify({ document: {} }))).toThrow(
      ValidationError,
    );
    expect(() =>
      parseJsonImport(JSON.stringify({ document: { sections: 'nope' } })),
    ).toThrow(ValidationError);
  });

  test('throws ValidationError on empty/missing content', () => {
    expect(() => parseJsonImport('')).toThrow(ValidationError);
    expect(() => parseJsonImport(null)).toThrow(ValidationError);
  });
});

describe('applyImport', () => {
  function makeDeps(existingSections: Array<{ sortOrder: number }> = []) {
    let itemCounter = 0;
    let sectionCounter = 0;

    const createSection = jest.fn(async (input: any) => ({
      documentId: input.documentId,
      sectionId: `sec-${++sectionCounter}`,
      type: input.type,
      title: input.title,
      sortOrder: input.sortOrder,
      createdAt: 't',
      updatedAt: 't',
      ...(input.sectionType ? { sectionType: input.sectionType } : {}),
    }));

    const createItem = jest.fn(async (input: any) => ({
      sectionKey: `${input.documentId}:${input.sectionId}`,
      itemId: `i-${++itemCounter}`,
      documentId: input.documentId,
      sectionId: input.sectionId,
      text: input.text,
      status: input.status,
      createdAt: 't',
      updatedAt: 't',
      ...(input.assignee ? { assignee: input.assignee } : {}),
      ...(input.priority ? { priority: input.priority } : {}),
    }));

    const getSectionsForDocument = jest.fn(async () => existingSections as any);

    return {
      deps: {
        sectionRepo: { createSection, getSectionsForDocument } as any,
        itemRepo: { createItem } as any,
      },
      createSection,
      createItem,
      getSectionsForDocument,
    };
  }

  test('creates sections and items; starts sortOrder at 0 when no existing sections', async () => {
    const { deps, createSection, createItem } = makeDeps([]);

    const result = await applyImport(
      'doc-1',
      [
        { title: 'A', items: [{ text: 'a1', status: 'open' }] },
        { title: 'B', items: [] },
      ],
      deps,
    );

    expect(result).toEqual({ sectionsCreated: 2, itemsCreated: 1 });

    expect(createSection).toHaveBeenCalledTimes(2);
    expect(createSection).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ documentId: 'doc-1', title: 'A', sortOrder: 0, type: 'text' }),
    );
    expect(createSection).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ title: 'B', sortOrder: 1 }),
    );

    expect(createItem).toHaveBeenCalledTimes(1);
    expect(createItem).toHaveBeenCalledWith(
      expect.objectContaining({
        documentId: 'doc-1',
        sectionId: 'sec-1',
        text: 'a1',
        status: 'open',
      }),
    );
  });

  test('continues sortOrder after the highest existing section', async () => {
    const { deps, createSection } = makeDeps([{ sortOrder: 0 }, { sortOrder: 7 }]);

    await applyImport('doc-1', [{ title: 'C', items: [] }], deps);

    expect(createSection).toHaveBeenCalledWith(
      expect.objectContaining({ sortOrder: 8 }),
    );
  });

  test('forwards item metadata (assignee, priority) to createItem', async () => {
    const { deps, createItem } = makeDeps([]);

    await applyImport(
      'doc-1',
      [
        {
          title: 'Actions',
          items: [
            { text: 'x', status: 'done', assignee: 'bob', priority: 'high' },
          ],
        },
      ],
      deps,
    );

    expect(createItem).toHaveBeenCalledWith(
      expect.objectContaining({
        text: 'x',
        status: 'done',
        assignee: 'bob',
        priority: 'high',
      }),
    );
  });

  test('honours per-section type when provided (JSON import path)', async () => {
    const { deps, createSection } = makeDeps([]);

    await applyImport(
      'doc-1',
      [{ title: 'S', type: 'checklist', sectionType: 'task', items: [] }],
      deps,
    );

    expect(createSection).toHaveBeenCalledWith(
      expect.objectContaining({ type: 'checklist', sectionType: 'task' }),
    );
  });
});
