// Tests for document-importer: parseMarkdownSections, parseJsonImport, applyImport.
//
// Parser tests are pure (no mocks). applyImport uses injected deps so
// we pass mock repos without jest.mock.

import { ValidationError } from '../../middleware/error-handler';
import {
  parseMarkdownSections,
  parseJsonImport,
  applyImport,
  type ParsedSection,
} from '../document-importer';

describe('parseMarkdownSections', () => {
  it('parses sections from ## headings', () => {
    const result = parseMarkdownSections('## Intro\n\n## Details\n');
    expect(result).toHaveLength(2);
    expect(result[0].title).toBe('Intro');
    expect(result[1].title).toBe('Details');
  });

  it('parses action items with checkboxes', () => {
    const md = '## Tasks\n- [ ] Do this\n- [x] Done that\n';
    const result = parseMarkdownSections(md);
    expect(result[0].items).toHaveLength(2);
    expect(result[0].items[0]).toEqual({ text: 'Do this', status: 'open' });
    expect(result[0].items[1]).toEqual({ text: 'Done that', status: 'done' });
  });

  it('extracts assignee and priority from trailing metadata', () => {
    const md = '## Tasks\n- [ ] Fix bug (@bob, priority: high)\n';
    const result = parseMarkdownSections(md);
    expect(result[0].items[0]).toEqual(expect.objectContaining({
      text: 'Fix bug',
      assignee: 'bob',
      priority: 'high',
    }));
  });

  it('throws on empty input', () => {
    expect(() => parseMarkdownSections('')).toThrow(ValidationError);
    expect(() => parseMarkdownSections('   ')).toThrow(ValidationError);
  });

  it('throws on non-string input', () => {
    expect(() => parseMarkdownSections(null)).toThrow(ValidationError);
    expect(() => parseMarkdownSections(42)).toThrow(ValidationError);
  });

  it('throws when no section headings found', () => {
    expect(() => parseMarkdownSections('just plain text')).toThrow(
      /section headings/,
    );
  });

  it('ignores items before first section heading', () => {
    const md = '- [ ] orphan item\n## Section\n- [ ] real item\n';
    const result = parseMarkdownSections(md);
    expect(result).toHaveLength(1);
    expect(result[0].items).toHaveLength(1);
    expect(result[0].items[0].text).toBe('real item');
  });
});

describe('parseJsonImport', () => {
  it('parses valid JSON string', () => {
    const json = JSON.stringify({
      document: {
        sections: [{ title: 'S1', items: [{ text: 'Do it', status: 'open' }] }],
      },
    });
    const result = parseJsonImport(json);
    expect(result).toHaveLength(1);
    expect(result[0].title).toBe('S1');
    expect(result[0].items[0].text).toBe('Do it');
  });

  it('accepts pre-parsed object', () => {
    const result = parseJsonImport({
      document: { sections: [{ title: 'A' }] },
    });
    expect(result).toHaveLength(1);
    expect(result[0].items).toEqual([]);
  });

  it('throws on empty string', () => {
    expect(() => parseJsonImport('')).toThrow(ValidationError);
  });

  it('throws on invalid JSON', () => {
    expect(() => parseJsonImport('{not json')).toThrow(/Invalid JSON/);
  });

  it('throws on null input', () => {
    expect(() => parseJsonImport(null)).toThrow(ValidationError);
  });

  it('throws when document field missing', () => {
    expect(() => parseJsonImport(JSON.stringify({ foo: 'bar' }))).toThrow(
      /missing required "document"/,
    );
  });

  it('throws when sections is not an array', () => {
    expect(() => parseJsonImport(JSON.stringify({ document: { sections: 'nope' } }))).toThrow(
      /missing required "document.sections"/,
    );
  });

  it('defaults title to Untitled and type to text', () => {
    const result = parseJsonImport({ document: { sections: [{}] } });
    expect(result[0].title).toBe('Untitled');
    expect(result[0].type).toBe('text');
  });

  it('preserves optional item fields', () => {
    const result = parseJsonImport({
      document: {
        sections: [{
          title: 'S',
          items: [{ text: 'T', status: 'done', assignee: 'a', priority: 'low', dueDate: '2026-06-01', category: 'bug' }],
        }],
      },
    });
    const item = result[0].items[0];
    expect(item.assignee).toBe('a');
    expect(item.priority).toBe('low');
    expect(item.dueDate).toBe('2026-06-01');
    expect(item.category).toBe('bug');
  });
});

describe('applyImport', () => {
  const mockCreateSection = jest.fn();
  const mockGetSections = jest.fn();
  const mockCreateItem = jest.fn();

  const deps = {
    sectionRepo: {
      createSection: mockCreateSection,
      getSectionsForDocument: mockGetSections,
    },
    itemRepo: { createItem: mockCreateItem },
  };

  beforeEach(() => {
    mockCreateSection.mockReset();
    mockGetSections.mockReset();
    mockCreateItem.mockReset();
  });

  it('creates sections and items, returns counts', async () => {
    mockGetSections.mockResolvedValue([]);
    mockCreateSection.mockResolvedValue({ sectionId: 'new-sec' });
    mockCreateItem.mockResolvedValue(undefined);

    const parsed: ParsedSection[] = [
      { title: 'S1', items: [{ text: 'T1', status: 'open' }, { text: 'T2', status: 'done' }] },
    ];
    const result = await applyImport('doc-1', parsed, deps);
    expect(result).toEqual({ sectionsCreated: 1, itemsCreated: 2 });
    expect(mockCreateSection).toHaveBeenCalledWith(
      expect.objectContaining({ documentId: 'doc-1', title: 'S1', sortOrder: 0 }),
    );
  });

  it('continues sortOrder from existing sections', async () => {
    mockGetSections.mockResolvedValue([{ sortOrder: 3 }, { sortOrder: 5 }]);
    mockCreateSection.mockResolvedValue({ sectionId: 'new-sec' });

    await applyImport('doc-1', [{ title: 'New', items: [] }], deps);
    expect(mockCreateSection).toHaveBeenCalledWith(
      expect.objectContaining({ sortOrder: 6 }),
    );
  });

  it('handles empty parsed input', async () => {
    mockGetSections.mockResolvedValue([]);
    const result = await applyImport('doc-1', [], deps);
    expect(result).toEqual({ sectionsCreated: 0, itemsCreated: 0 });
  });

  it('passes assignee and priority through to createItem', async () => {
    mockGetSections.mockResolvedValue([]);
    mockCreateSection.mockResolvedValue({ sectionId: 'sec-new' });
    mockCreateItem.mockResolvedValue(undefined);

    await applyImport('doc-1', [{
      title: 'S',
      items: [{ text: 'Fix', status: 'open', assignee: 'alice', priority: 'high' }],
    }], deps);

    expect(mockCreateItem).toHaveBeenCalledWith(
      expect.objectContaining({ assignee: 'alice', priority: 'high' }),
    );
  });
});

describe('applyImport — performance with large datasets', () => {
  const mockCreateSection = jest.fn();
  const mockGetSections = jest.fn();
  const mockCreateItem = jest.fn();

  const deps = {
    sectionRepo: {
      createSection: mockCreateSection,
      getSectionsForDocument: mockGetSections,
    },
    itemRepo: { createItem: mockCreateItem },
  };

  beforeEach(() => {
    mockCreateSection.mockReset();
    mockGetSections.mockReset();
    mockCreateItem.mockReset();
    mockGetSections.mockResolvedValue([]);
  });

  function generateLargeParsedDataset(sectionCount: number, itemsPerSection: number): ParsedSection[] {
    const sections: ParsedSection[] = [];
    for (let i = 0; i < sectionCount; i++) {
      const items = [];
      for (let j = 0; j < itemsPerSection; j++) {
        items.push({
          text: `Item ${i}-${j}: sample text for validation`,
          status: j % 3 === 0 ? 'done' : 'open',
          ...(j % 5 === 0 ? { assignee: `user-${j % 10}` } : {}),
          ...(j % 7 === 0 ? { priority: ['low', 'medium', 'high'][j % 3] } : {}),
          ...(j % 11 === 0 ? { dueDate: `2026-${String((j % 12) + 1).padStart(2, '0')}-15` } : {}),
        });
      }
      sections.push({
        title: `Section ${i}`,
        type: ['text', 'checklist', 'progress'][i % 3],
        items,
      });
    }
    return sections;
  }

  it('completes 1000-row import in under 5 seconds', async () => {
    mockCreateSection.mockImplementation(async (data) => ({
      sectionId: `sec-${data.title}`,
    }));
    mockCreateItem.mockResolvedValue(undefined);

    // 20 sections × 50 items = 1000 total items
    const parsed = generateLargeParsedDataset(20, 50);
    expect(parsed.reduce((sum, s) => sum + s.items.length, 0)).toBe(1000);

    const startTime = Date.now();
    const result = await applyImport('perf-doc', parsed, deps);
    const elapsed = Date.now() - startTime;

    expect(result.sectionsCreated).toBe(20);
    expect(result.itemsCreated).toBe(1000);
    expect(elapsed).toBeLessThan(5000);
  });

  it('handles 1000-row import with varied field types', async () => {
    mockCreateSection.mockImplementation(async (data) => ({
      sectionId: `sec-${data.title}`,
    }));
    mockCreateItem.mockResolvedValue(undefined);

    const parsed = generateLargeParsedDataset(25, 40);
    const result = await applyImport('perf-doc-2', parsed, deps);

    expect(result.itemsCreated).toBe(1000);
    // Verify mock was called with optional fields distributed correctly
    const calls = mockCreateItem.mock.calls;
    const withAssignee = calls.filter((c) => c[0].assignee).length;
    const withPriority = calls.filter((c) => c[0].priority).length;
    const withDueDate = calls.filter((c) => c[0].dueDate).length;
    expect(withAssignee).toBeGreaterThan(0);
    expect(withPriority).toBeGreaterThan(0);
    expect(withDueDate).toBeGreaterThan(0);
  });

  it('handles partial failure — 50% validation errors during bulk import', async () => {
    let sectionCallCount = 0;
    mockCreateSection.mockImplementation(async (data) => ({
      sectionId: `sec-${data.title}`,
    }));

    // Fail every other item creation to simulate validation errors
    let itemCallCount = 0;
    mockCreateItem.mockImplementation(async () => {
      itemCallCount++;
      if (itemCallCount % 2 === 0) {
        throw new ValidationError('simulated validation failure');
      }
    });

    const parsed = generateLargeParsedDataset(10, 100); // 1000 items
    let thrownError: unknown = null;

    try {
      await applyImport('partial-fail-doc', parsed, deps);
    } catch (err) {
      thrownError = err;
    }

    // applyImport does NOT catch per-item errors — it fails fast
    expect(thrownError).toBeInstanceOf(ValidationError);
    expect((thrownError as ValidationError).message).toMatch(/simulated validation/);

    // First section's first item succeeded, second item failed → stopped there
    expect(mockCreateSection).toHaveBeenCalledTimes(1);
    expect(mockCreateItem).toHaveBeenCalledTimes(2);
  });

  it('measures memory stability for 2000-row import', async () => {
    mockCreateSection.mockImplementation(async (data) => ({
      sectionId: `sec-${data.title}`,
    }));
    mockCreateItem.mockResolvedValue(undefined);

    const memBefore = process.memoryUsage().heapUsed;
    const parsed = generateLargeParsedDataset(40, 50); // 2000 items
    const result = await applyImport('mem-doc', parsed, deps);
    const memAfter = process.memoryUsage().heapUsed;

    expect(result.itemsCreated).toBe(2000);

    // Memory delta should be < 50MB for 2000 in-memory mock calls
    const deltaMB = (memAfter - memBefore) / (1024 * 1024);
    expect(deltaMB).toBeLessThan(50);
  });

  it('handles empty items in large section without error', async () => {
    mockCreateSection.mockResolvedValue({ sectionId: 'sec-empty' });
    mockCreateItem.mockResolvedValue(undefined);

    const parsed: ParsedSection[] = [
      { title: 'Empty Section', items: [] },
      ...generateLargeParsedDataset(10, 100),
    ];

    const result = await applyImport('mixed-doc', parsed, deps);
    expect(result.sectionsCreated).toBe(11);
    expect(result.itemsCreated).toBe(1000);
  });
});
