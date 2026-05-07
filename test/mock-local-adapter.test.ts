// test/mock-local-adapter.test.ts
//
// Tests for MockLocalAdapter: list (cursor filtering, nested dirs, mimeType),
// and fetch (content, encoding, JSON auto-parse).

import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { MockLocalAdapter } from '../src/ingest/adapters/mock-local';

// ─── helpers ──────────────────────────────────────────────────────────────

async function makeTempDir(): Promise<string> {
    return fs.mkdtemp(path.join(os.tmpdir(), 'mock-local-test-'));
}

async function writeFile(dir: string, relPath: string, content: string): Promise<void> {
    const fullPath = path.join(dir, relPath);
    await fs.mkdir(path.dirname(fullPath), { recursive: true });
    await fs.writeFile(fullPath, content, 'utf-8');
}

async function listAll(adapter: MockLocalAdapter, basePath: string, lastSyncAt: string | null = null) {
    const items = [];
    for await (const item of adapter.list({ basePath }, { lastSyncAt })) {
        items.push(item);
    }
    return items;
}

// ─── list — basic ──────────────────────────────────────────────────────────

describe('MockLocalAdapter — list', () => {
    let tmpDir: string;
    let adapter: MockLocalAdapter;

    beforeEach(async () => {
        tmpDir = await makeTempDir();
        adapter = new MockLocalAdapter();
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('empty directory yields no files', async () => {
        const items = await listAll(adapter, tmpDir);
        expect(items).toHaveLength(0);
    });

    test('lists a file in the root', async () => {
        await writeFile(tmpDir, 'hello.txt', 'hi');
        const items = await listAll(adapter, tmpDir);
        expect(items).toHaveLength(1);
        expect(items[0].name).toBe('hello.txt');
        expect(items[0].path).toBe('hello.txt');
    });

    test('lists files in nested directories', async () => {
        await writeFile(tmpDir, 'docs/report.pdf', 'data');
        await writeFile(tmpDir, 'data/records.csv', 'x,y');
        const items = await listAll(adapter, tmpDir);
        expect(items).toHaveLength(2);
        const paths = items.map((f) => f.path).sort();
        expect(paths).toContain('data/records.csv');
        expect(paths).toContain('docs/report.pdf');
    });

    test('file descriptor includes correct metadata', async () => {
        await writeFile(tmpDir, 'doc.json', '{"key":"val"}');
        const items = await listAll(adapter, tmpDir);
        expect(items[0].name).toBe('doc.json');
        expect(items[0].mimeType).toBe('application/json');
        expect(items[0].size).toBeGreaterThan(0);
        expect(items[0].hash).toMatch(/^[0-9a-f]{64}$/);
        expect(items[0].lastModified).toBeTruthy();
    });

    test('uses application/octet-stream for unknown extension', async () => {
        await writeFile(tmpDir, 'archive.zip', 'data');
        const items = await listAll(adapter, tmpDir);
        expect(items[0].mimeType).toBe('application/octet-stream');
    });

    test('infers correct mimeType for each known extension', async () => {
        const cases: [string, string][] = [
            ['f.json', 'application/json'],
            ['f.csv', 'text/csv'],
            ['f.md', 'text/markdown'],
            ['f.txt', 'text/plain'],
        ];
        for (const [name, mime] of cases) {
            await writeFile(tmpDir, name, 'content');
        }
        const items = await listAll(adapter, tmpDir);
        for (const [name, mime] of cases) {
            const item = items.find((f) => f.name === name);
            expect(item?.mimeType).toBe(mime);
        }
    });
});

// ─── list — cursor filtering ──────────────────────────────────────────────

describe('MockLocalAdapter — list cursor filtering', () => {
    let tmpDir: string;
    let adapter: MockLocalAdapter;

    beforeEach(async () => {
        tmpDir = await makeTempDir();
        adapter = new MockLocalAdapter();
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('returns all files when cursor.lastSyncAt is null', async () => {
        await writeFile(tmpDir, 'file.txt', 'data');
        const items = await listAll(adapter, tmpDir, null);
        expect(items).toHaveLength(1);
    });

    test('skips files not modified after lastSyncAt', async () => {
        await writeFile(tmpDir, 'old.txt', 'old');
        const items = await listAll(adapter, tmpDir);
        const oldTime = items[0].lastModified;

        // Wait 1ms to ensure new file has a later mtime
        await new Promise((r) => setTimeout(r, 5));
        await writeFile(tmpDir, 'new.txt', 'new');

        const filtered = await listAll(adapter, tmpDir, oldTime);
        // Should only get the file modified strictly after oldTime
        expect(filtered.every((f) => f.name !== 'old.txt')).toBe(true);
        expect(filtered.some((f) => f.name === 'new.txt')).toBe(true);
    });
});

// ─── fetch ────────────────────────────────────────────────────────────────

describe('MockLocalAdapter — fetch', () => {
    let tmpDir: string;
    let adapter: MockLocalAdapter;

    beforeEach(async () => {
        tmpDir = await makeTempDir();
        adapter = new MockLocalAdapter();
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('returns text content as utf-8', async () => {
        await writeFile(tmpDir, 'note.txt', 'Hello World');
        const [desc] = await listAll(adapter, tmpDir);
        const content = await adapter.fetch({ basePath: tmpDir }, desc);
        expect(content.data.toString('utf-8')).toBe('Hello World');
        expect(content.encoding).toBe('utf-8');
    });

    test('returns markdown content as utf-8', async () => {
        await writeFile(tmpDir, 'README.md', '# Title');
        const [desc] = await listAll(adapter, tmpDir);
        const content = await adapter.fetch({ basePath: tmpDir }, desc);
        expect(content.encoding).toBe('utf-8');
        expect(content.data.toString('utf-8')).toBe('# Title');
    });

    test('parses JSON content into content.parsed', async () => {
        await writeFile(tmpDir, 'data.json', '{"score":42}');
        const [desc] = await listAll(adapter, tmpDir);
        const content = await adapter.fetch({ basePath: tmpDir }, desc);
        expect(content.parsed).toEqual({ score: 42 });
    });

    test('does not parse non-JSON content', async () => {
        await writeFile(tmpDir, 'data.csv', 'a,b\n1,2');
        const [desc] = await listAll(adapter, tmpDir);
        const content = await adapter.fetch({ basePath: tmpDir }, desc);
        expect(content.parsed).toBeUndefined();
    });

    test('fetch from nested path works correctly', async () => {
        await writeFile(tmpDir, 'nested/dir/file.txt', 'nested content');
        const items = await listAll(adapter, tmpDir);
        const desc = items.find((f) => f.name === 'file.txt')!;
        const content = await adapter.fetch({ basePath: tmpDir }, desc);
        expect(content.data.toString('utf-8')).toBe('nested content');
    });
});
