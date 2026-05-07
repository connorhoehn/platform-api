// social-api/src/ingest/adapters/__tests__/mock-drive.test.ts
//
// Tests for MockDriveAdapter: list filtering (folder, tag, cursor),
// fetch (text/JSON), push, mutation helpers, error cases.

import { MockDriveAdapter } from '../mock-drive';
import type { MockDriveFile } from '../mock-drive';
import type { SyncCursor } from '../../types';

function emptyCursor(): SyncCursor {
    return { lastSyncAt: null };
}

function makeCursor(lastSyncAt: string): SyncCursor {
    return { lastSyncAt };
}

async function collectList(
    adapter: MockDriveAdapter,
    config: Record<string, unknown> = {},
    cursor: SyncCursor = emptyCursor(),
) {
    const results = [];
    for await (const fd of adapter.list(config as never, cursor)) {
        results.push(fd);
    }
    return results;
}

const baseFile: MockDriveFile = {
    path: 'docs/report.md',
    content: '# Report',
    tags: ['published'],
    owner: 'alice',
    lastModified: '2026-01-01T00:00:00.000Z',
};

// ─── list — basic ─────────────────────────────────────────────────────────

describe('MockDriveAdapter — list basic', () => {
    test('empty drive yields no files', async () => {
        const adapter = new MockDriveAdapter();
        const files = await collectList(adapter);
        expect(files).toHaveLength(0);
    });

    test('seeded file appears in list', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([baseFile]);
        const files = await collectList(adapter);
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('docs/report.md');
        expect(files[0].name).toBe('report.md');
    });

    test('descriptor has hash, size, mimeType, lastModified', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'x.json', content: '{"a":1}' }]);
        const [fd] = await collectList(adapter);
        expect(typeof fd.hash).toBe('string');
        expect(fd.hash).toHaveLength(64);
        expect(fd.size).toBe(7);
        expect(fd.mimeType).toBe('application/json');
        expect(typeof fd.lastModified).toBe('string');
    });

    test('descriptor includes tags when present', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ ...baseFile, tags: ['reviewed', 'approved'] }]);
        const [fd] = await collectList(adapter);
        expect(fd.tags).toEqual(['reviewed', 'approved']);
    });

    test('descriptor tags is undefined when file has no tags', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'x.txt', content: 'hello', tags: [] }]);
        const [fd] = await collectList(adapter);
        expect(fd.tags).toBeUndefined();
    });

    test('mimeType defaults to octet-stream for unknown extension', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'file.xyz', content: 'data' }]);
        const [fd] = await collectList(adapter);
        expect(fd.mimeType).toBe('application/octet-stream');
    });
});

// ─── list — folder filter ──────────────────────────────────────────────────

describe('MockDriveAdapter — list folderId filter', () => {
    test('folderId prefix filters out files outside the folder', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            { path: 'meetings/standup.md', content: 'hello' },
            { path: 'reports/q1.md', content: 'world' },
        ]);
        const files = await collectList(adapter, { folderId: 'meetings/' });
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('meetings/standup.md');
    });

    test('no folderId returns all files', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            { path: 'a.txt', content: 'a' },
            { path: 'b.txt', content: 'b' },
        ]);
        const files = await collectList(adapter, {});
        expect(files).toHaveLength(2);
    });
});

// ─── list — tag filter ─────────────────────────────────────────────────────

describe('MockDriveAdapter — list tagFilter', () => {
    test('tagFilter keeps only files with the given tag', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            { path: 'a.txt', content: 'a', tags: ['needs-review'] },
            { path: 'b.txt', content: 'b', tags: ['published'] },
        ]);
        const files = await collectList(adapter, { tagFilter: 'needs-review' });
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('a.txt');
    });

    test('tagFilter with no matching files yields empty list', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'x.txt', content: 'x', tags: ['draft'] }]);
        const files = await collectList(adapter, { tagFilter: 'published' });
        expect(files).toHaveLength(0);
    });
});

// ─── list — cursor filter ──────────────────────────────────────────────────

describe('MockDriveAdapter — list cursor (lastSyncAt)', () => {
    test('null cursor returns all files', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'a.txt', content: 'a', lastModified: '2026-01-01T00:00:00.000Z' }]);
        const files = await collectList(adapter, {}, emptyCursor());
        expect(files).toHaveLength(1);
    });

    test('cursor skips files modified at or before lastSyncAt', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([
            { path: 'old.txt', content: 'old', lastModified: '2026-01-01T00:00:00.000Z' },
            { path: 'new.txt', content: 'new', lastModified: '2026-03-01T00:00:00.000Z' },
        ]);
        const files = await collectList(adapter, {}, makeCursor('2026-02-01T00:00:00.000Z'));
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('new.txt');
    });

    test('cursor at exact lastModified time excludes the file (modified <= lastSyncAt)', async () => {
        const adapter = new MockDriveAdapter();
        const ts = '2026-01-15T00:00:00.000Z';
        adapter.seed([{ path: 'exact.txt', content: 'x', lastModified: ts }]);
        const files = await collectList(adapter, {}, makeCursor(ts));
        expect(files).toHaveLength(0);
    });
});

// ─── fetch ─────────────────────────────────────────────────────────────────

describe('MockDriveAdapter — fetch', () => {
    test('fetch returns utf-8 Buffer for text file', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'hello.txt', content: 'hello world' }]);
        const [fd] = await collectList(adapter);
        const content = await adapter.fetch({} as never, fd);
        expect(content.encoding).toBe('utf-8');
        expect(content.data.toString('utf-8')).toBe('hello world');
    });

    test('fetch returns parsed JSON for .json file', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'data.json', content: '{"key":"val"}' }]);
        const [fd] = await collectList(adapter);
        const content = await adapter.fetch({} as never, fd);
        expect(content.parsed).toEqual({ key: 'val' });
    });

    test('fetch throws for unknown path', async () => {
        const adapter = new MockDriveAdapter();
        const fd = { path: 'ghost.txt', name: 'ghost.txt', hash: 'x', size: 0, mimeType: 'text/plain', lastModified: '' };
        await expect(adapter.fetch({} as never, fd)).rejects.toThrow('file not found');
    });
});

// ─── push ──────────────────────────────────────────────────────────────────

describe('MockDriveAdapter — push', () => {
    test('push updates existing file content', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'doc.txt', content: 'original' }]);
        const [fd] = await collectList(adapter);
        await adapter.push({} as never, 'doc.txt', { data: Buffer.from('updated', 'utf-8'), encoding: 'utf-8' });
        const content = await adapter.fetch({} as never, fd);
        expect(content.data.toString('utf-8')).toBe('updated');
    });

    test('push creates new file at pushPath when not found', async () => {
        const adapter = new MockDriveAdapter();
        await adapter.push({ pushPath: 'new-file.txt' } as never, 'doc-id', { data: Buffer.from('created', 'utf-8'), encoding: 'utf-8' });
        const files = await collectList(adapter);
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('new-file.txt');
    });

    test('push falls back to documentId when no pushPath', async () => {
        const adapter = new MockDriveAdapter();
        await adapter.push({} as never, 'fallback-path.txt', { data: Buffer.from('x', 'utf-8'), encoding: 'utf-8' });
        const files = await collectList(adapter);
        expect(files[0].path).toBe('fallback-path.txt');
    });
});

// ─── mutation helpers ──────────────────────────────────────────────────────

describe('MockDriveAdapter — mutation helpers', () => {
    test('addFile makes file appear in list', async () => {
        const adapter = new MockDriveAdapter();
        adapter.addFile({ path: 'added.txt', content: 'added' });
        const files = await collectList(adapter);
        expect(files).toHaveLength(1);
    });

    test('updateFile changes content and bumps lastModified', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'f.txt', content: 'old', lastModified: '2026-01-01T00:00:00.000Z' }]);
        adapter.updateFile('f.txt', 'new content');
        const [fd] = await collectList(adapter);
        const content = await adapter.fetch({} as never, fd);
        expect(content.data.toString('utf-8')).toBe('new content');
        expect(new Date(fd.lastModified).getTime()).toBeGreaterThan(new Date('2026-01-01T00:00:00.000Z').getTime());
    });

    test('updateFile throws for unknown path', () => {
        const adapter = new MockDriveAdapter();
        expect(() => adapter.updateFile('ghost.txt', 'x')).toThrow('file not found');
    });

    test('tagFile adds tag to file', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'f.txt', content: 'x', tags: [] }]);
        adapter.tagFile('f.txt', 'reviewed');
        const files = await collectList(adapter, { tagFilter: 'reviewed' });
        expect(files).toHaveLength(1);
    });

    test('tagFile does not duplicate existing tag', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'f.txt', content: 'x', tags: ['reviewed'] }]);
        adapter.tagFile('f.txt', 'reviewed');
        const [fd] = await collectList(adapter);
        expect(fd.tags!.filter((t) => t === 'reviewed')).toHaveLength(1);
    });

    test('tagFile throws for unknown path', () => {
        const adapter = new MockDriveAdapter();
        expect(() => adapter.tagFile('ghost.txt', 'tag')).toThrow('file not found');
    });

    test('removeFile removes the file from the drive', async () => {
        const adapter = new MockDriveAdapter();
        adapter.seed([{ path: 'rm.txt', content: 'bye' }]);
        adapter.removeFile('rm.txt');
        const files = await collectList(adapter);
        expect(files).toHaveLength(0);
    });
});
