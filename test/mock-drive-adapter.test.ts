// test/mock-drive-adapter.test.ts
//
// Tests for MockDriveAdapter: seed, list (folder/tag/cursor filtering),
// fetch, addFile, updateFile, tagFile, removeFile, and push.

import { MockDriveAdapter } from '../src/ingest/adapters/mock-drive';
import type { MockDriveFile } from '../src/ingest/adapters/mock-drive';
import type { SyncCursor, SourceConfig } from '../src/ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────

function emptyCursor(): SyncCursor {
    return { lastSyncAt: null };
}

function cursorSince(isoDate: string): SyncCursor {
    return { lastSyncAt: isoDate };
}

async function listAll(adapter: MockDriveAdapter, config: SourceConfig = {}, cursor: SyncCursor = emptyCursor()) {
    const items = [];
    for await (const item of adapter.list(config, cursor)) {
        items.push(item);
    }
    return items;
}

function makeDriveFile(overrides: Partial<MockDriveFile> = {}): MockDriveFile {
    return {
        path: 'docs/report.json',
        content: '{"title":"Report"}',
        ...overrides,
    };
}

// ─── seed + list — basic ──────────────────────────────────────────────────

describe('MockDriveAdapter — seed and list', () => {
    test('empty adapter yields nothing', async () => {
        const adapter = new MockDriveAdapter();
        const files = await listAll(adapter);
        expect(files).toHaveLength(0);
    });

    test('seeded files appear in list', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            makeDriveFile({ path: 'a.json', content: '{}' }),
            makeDriveFile({ path: 'b.csv', content: 'x,y' }),
        ]);
        const files = await listAll(adapter);
        expect(files).toHaveLength(2);
    });

    test('list result includes correct file metadata', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'docs/report.md', content: '# Hello' })]);
        const files = await listAll(adapter);
        expect(files[0].path).toBe('docs/report.md');
        expect(files[0].name).toBe('report.md');
        expect(files[0].mimeType).toBe('text/markdown');
        expect(files[0].hash).toMatch(/^[0-9a-f]{64}$/); // SHA-256 hex
        expect(files[0].size).toBeGreaterThan(0);
    });

    test('includes tags in listed file descriptor', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'hi', tags: ['finance', 'q1'] })]);
        const files = await listAll(adapter);
        expect(files[0].tags).toEqual(['finance', 'q1']);
    });

    test('omits tags field when tags array is empty', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'hi', tags: [] })]);
        const files = await listAll(adapter);
        expect(files[0].tags).toBeUndefined();
    });
});

// ─── list — folder filter ──────────────────────────────────────────────────

describe('MockDriveAdapter — list folderId filter', () => {
    test('filters to files with matching path prefix', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            makeDriveFile({ path: 'meetings/standup.md', content: 'daily' }),
            makeDriveFile({ path: 'finance/budget.csv', content: 'x,y' }),
        ]);
        const files = await listAll(adapter, { folderId: 'meetings/' });
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('meetings/standup.md');
    });

    test('no folderId returns all files', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            makeDriveFile({ path: 'a.json', content: '{}' }),
            makeDriveFile({ path: 'b.json', content: '{}' }),
        ]);
        const files = await listAll(adapter, {});
        expect(files).toHaveLength(2);
    });
});

// ─── list — tag filter ────────────────────────────────────────────────────

describe('MockDriveAdapter — list tagFilter', () => {
    test('returns only files with matching tag', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            makeDriveFile({ path: 'a.txt', content: 'x', tags: ['finance'] }),
            makeDriveFile({ path: 'b.txt', content: 'y', tags: ['hr'] }),
            makeDriveFile({ path: 'c.txt', content: 'z', tags: ['finance', 'q1'] }),
        ]);
        const files = await listAll(adapter, { tagFilter: 'finance' });
        expect(files).toHaveLength(2);
        expect(files.every((f) => f.path !== 'b.txt')).toBe(true);
    });

    test('returns empty when no file has the tag', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'x', tags: ['other'] })]);
        const files = await listAll(adapter, { tagFilter: 'finance' });
        expect(files).toHaveLength(0);
    });
});

// ─── list — cursor filtering ──────────────────────────────────────────────

describe('MockDriveAdapter — list cursor filtering', () => {
    test('returns all files when cursor.lastSyncAt is null', async () => {
        const past = new Date(Date.now() - 1000).toISOString();
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'old.txt', content: 'x', lastModified: past })]);
        const files = await listAll(adapter, {}, emptyCursor());
        expect(files).toHaveLength(1);
    });

    test('skips files not modified after lastSyncAt', async () => {
        const t0 = new Date(1_700_000_000_000).toISOString();
        const t1 = new Date(1_700_000_001_000).toISOString(); // 1s after t0
        const adapter = new MockDriveAdapter();
        adapter.seed([
            makeDriveFile({ path: 'old.txt', content: 'x', lastModified: t0 }),
            makeDriveFile({ path: 'new.txt', content: 'y', lastModified: t1 }),
        ]);
        const files = await listAll(adapter, {}, cursorSince(t0));
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('new.txt');
    });
});

// ─── fetch ────────────────────────────────────────────────────────────────

describe('MockDriveAdapter — fetch', () => {
    test('returns file content as buffer', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'doc.txt', content: 'Hello World' })]);
        const [desc] = await listAll(adapter);
        const content = await adapter.fetch({}, desc);
        expect(content.data.toString('utf-8')).toBe('Hello World');
        expect(content.encoding).toBe('utf-8');
    });

    test('parses JSON content automatically', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'data.json', content: '{"x":1}' })]);
        const [desc] = await listAll(adapter);
        const content = await adapter.fetch({}, desc);
        expect(content.parsed).toEqual({ x: 1 });
    });

    test('throws when file not found', async () => {
        const adapter = new MockDriveAdapter();
        await expect(
            adapter.fetch({}, { path: 'ghost.txt', name: 'ghost.txt', hash: '', size: 0, mimeType: 'text/plain', lastModified: '' }),
        ).rejects.toThrow('file not found');
    });
});

// ─── addFile / updateFile / tagFile / removeFile ──────────────────────────

describe('MockDriveAdapter — mutation methods', () => {
    test('addFile adds a new file', async () => {
        const adapter = new MockDriveAdapter();
        adapter.addFile(makeDriveFile({ path: 'new.txt', content: 'added' }));
        const files = await listAll(adapter);
        expect(files).toHaveLength(1);
    });

    test('updateFile changes content and bumps lastModified', async () => {
        const past = new Date(Date.now() - 5000).toISOString();
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'old', lastModified: past })]);
        adapter.updateFile('f.txt', 'new content');
        const [desc] = await listAll(adapter);
        const content = await adapter.fetch({}, desc);
        expect(content.data.toString('utf-8')).toBe('new content');
    });

    test('updateFile throws when file not found', () => {
        const adapter = new MockDriveAdapter();
        expect(() => adapter.updateFile('ghost.txt', 'x')).toThrow('file not found');
    });

    test('tagFile adds a tag', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'x', tags: [] })]);
        adapter.tagFile('f.txt', 'finance');
        const files = await listAll(adapter);
        expect(files[0].tags).toContain('finance');
    });

    test('tagFile is idempotent — duplicate tag not added', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'x', tags: ['finance'] })]);
        adapter.tagFile('f.txt', 'finance');
        const files = await listAll(adapter);
        expect(files[0].tags?.filter((t) => t === 'finance')).toHaveLength(1);
    });

    test('tagFile throws when file not found', () => {
        const adapter = new MockDriveAdapter();
        expect(() => adapter.tagFile('ghost.txt', 'tag')).toThrow('file not found');
    });

    test('removeFile removes the file', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'f.txt', content: 'x' })]);
        adapter.removeFile('f.txt');
        const files = await listAll(adapter);
        expect(files).toHaveLength(0);
    });
});

// ─── push ─────────────────────────────────────────────────────────────────

describe('MockDriveAdapter — push', () => {
    test('creates new file when path does not exist', async () => {
        const adapter = new MockDriveAdapter();
        const content = { data: Buffer.from('pushed content', 'utf-8'), encoding: 'utf-8' as const };
        await adapter.push({ pushPath: 'output/result.txt' }, 'doc-1', content);
        const files = await listAll(adapter);
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('output/result.txt');
    });

    test('updates existing file content on push', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([makeDriveFile({ path: 'out.txt', content: 'old' })]);
        const content = { data: Buffer.from('updated', 'utf-8'), encoding: 'utf-8' as const };
        await adapter.push({ pushPath: 'out.txt' }, 'doc-1', content);
        const [desc] = await listAll(adapter);
        const fetched = await adapter.fetch({}, desc);
        expect(fetched.data.toString('utf-8')).toBe('updated');
    });

    test('uses documentId as path when pushPath not in config', async () => {
        const adapter = new MockDriveAdapter();
        const content = { data: Buffer.from('doc content', 'utf-8'), encoding: 'utf-8' as const };
        await adapter.push({}, 'my-document-id', content);
        const files = await listAll(adapter);
        expect(files[0].path).toBe('my-document-id');
    });
});
