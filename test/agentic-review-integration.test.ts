// test/agentic-review-integration.test.ts
//
// Integration test: tag-based trigger → file detection → type mapping.
// Scenario: A document in MockDriveAdapter gets tagged "needs-review", the adapter
// filters files by tag, and the mapping system maps it to the correct document type.

import { MockDriveAdapter } from '../src/ingest/adapters/mock-drive';
import type { FileDescriptor } from '../src/ingest/types';
import { matchByRules } from '../src/ingest/mapping/rule-matcher';

describe('Agentic Review Integration', () => {
  let adapter: MockDriveAdapter;

  beforeEach(() => {
    adapter = new MockDriveAdapter();
  });

  it('should detect tagged file via adapter and map to document type', async () => {
    // Seed adapter with a document that has "needs-review" tag
    adapter.seed([
      {
        path: 'design-docs/feature-spec.md',
        content: '# Feature Spec\n\nThis is a draft specification for the new feature.',
        tags: ['needs-review', 'design-doc'],
        owner: 'engineer@example.com',
        lastModified: new Date().toISOString(),
      },
    ]);

    // List files with tag filter
    const files: FileDescriptor[] = [];
    for await (const file of adapter.list(
      { tagFilter: 'needs-review', adapterClass: 'MockDriveAdapter' },
      { lastSyncAt: null, checkpoint: undefined },
    )) {
      files.push(file);
    }

    expect(files.length).toBe(1);
    expect(files[0].path).toBe('design-docs/feature-spec.md');
    expect(files[0].tags).toContain('needs-review');

    // Map file to document type using mapping rules
    const mappingRules = [
      {
        ruleId: 'rule-1',
        condition: { field: 'mimeType' as const, op: 'eq' as const, value: 'text/markdown' },
        targetTypeId: 'design-document',
        fieldMapping: {
          name: 'title',
          path: 'filePath',
          tags: 'tags',
        },
        priority: 1,
      },
    ];

    const mappingResult = matchByRules(files[0], mappingRules);
    expect(mappingResult).toBeTruthy();
    expect(mappingResult?.targetTypeId).toBe('design-document');
    expect(mappingResult?.method).toBe('rule');
  });

  it('should filter out files without matching tag', async () => {
    // Seed adapter with files, only one has the target tag
    adapter.seed([
      {
        path: 'docs/proposal.md',
        content: '# Proposal Draft',
        tags: ['draft'],
        lastModified: '2026-05-01T10:00:00Z',
      },
      {
        path: 'docs/approved-design.md',
        content: '# Approved Design',
        tags: ['needs-review', 'approved'],
        lastModified: '2026-05-01T11:00:00Z',
      },
    ]);

    // List files with tag filter
    const files: FileDescriptor[] = [];
    for await (const file of adapter.list(
      { tagFilter: 'needs-review', adapterClass: 'MockDriveAdapter' },
      { lastSyncAt: null, checkpoint: undefined },
    )) {
      files.push(file);
    }

    expect(files.length).toBe(1);
    expect(files[0].path).toBe('docs/approved-design.md');
  });

  it('should detect tag changes between syncs', async () => {
    // Initial seed without tag
    adapter.seed([
      {
        path: 'docs/proposal.md',
        content: '# Proposal Draft',
        tags: [],
        lastModified: '2026-05-01T10:00:00Z',
      },
    ]);

    // First sync — file has no matching tag, should be filtered out
    let files: FileDescriptor[] = [];
    for await (const file of adapter.list(
      { tagFilter: 'needs-review', adapterClass: 'MockDriveAdapter' },
      { lastSyncAt: null, checkpoint: undefined },
    )) {
      files.push(file);
    }

    expect(files.length).toBe(0);

    // Now tag the file
    adapter.tagFile('docs/proposal.md', 'needs-review');

    // Second sync — file now matches tag filter
    files = [];
    for await (const file of adapter.list(
      { tagFilter: 'needs-review', adapterClass: 'MockDriveAdapter' },
      { lastSyncAt: null, checkpoint: undefined },
    )) {
      files.push(file);
    }

    expect(files.length).toBe(1);
    expect(files[0].path).toBe('docs/proposal.md');
    expect(files[0].tags).toContain('needs-review');
  });

  it('should support bidirectional sync via push method', async () => {
    // Seed adapter with initial file
    adapter.seed([
      {
        path: 'docs/original.md',
        content: '# Original Content',
        tags: ['needs-review'],
        lastModified: '2026-05-01T10:00:00Z',
      },
    ]);

    // Fetch file
    const files: FileDescriptor[] = [];
    for await (const file of adapter.list(
      { tagFilter: 'needs-review', adapterClass: 'MockDriveAdapter' },
      { lastSyncAt: null, checkpoint: undefined },
    )) {
      files.push(file);
    }

    const fileContent = await adapter.fetch(
      { adapterClass: 'MockDriveAdapter' },
      files[0],
    );
    const originalText = fileContent.data.toString(fileContent.encoding);
    expect(originalText).toContain('Original Content');

    // Push updated content back (simulating pipeline edit + sync-back)
    const updatedBuffer = Buffer.from('# Updated Content\n\nEdited by agentic review pipeline.', 'utf-8');
    await adapter.push(
      { adapterClass: 'MockDriveAdapter', pushPath: 'docs/original.md' },
      'doc-123',
      {
        data: updatedBuffer,
        encoding: 'utf-8' as const,
      },
    );

    // Verify file was updated in adapter
    const updatedContent = await adapter.fetch(
      { adapterClass: 'MockDriveAdapter' },
      files[0],
    );
    const updatedText = updatedContent.data.toString(updatedContent.encoding);
    expect(updatedText).toContain('Updated Content');
    expect(updatedText).toContain('agentic review pipeline');
  });
});
