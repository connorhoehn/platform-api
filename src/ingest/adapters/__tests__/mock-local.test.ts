// social-api/src/ingest/adapters/__tests__/mock-local.test.ts
//
// Tests for MockLocalAdapter: list (all files, cursor filter, recursive), fetch (mime types, JSON parse).

import * as fs from 'fs/promises';
import * as os from 'os';
import * as path from 'path';
import { MockLocalAdapter } from '../mock-local';
import type { SyncCursor, SourceConfig } from '../../types';

// ─── Helpers ───────────────────────────────────────────────────────────────

async function makeTmpDir(): Promise<string> {
    return fs.mkdtemp(path.join(os.tmpdir(), 'mock-local-test-'));
}

async function writeFile(dir: string, relPath: string, content: string): Promise<void> {
    const full = path.join(dir, relPath);
    await fs.mkdir(path.dirname(full), { recursive: true });
    await fs.writeFile(full, content, 'utf-8');
}

function config(basePath: string): SourceConfig {
    return { basePath };
}

const noCursor: SyncCursor = { lastSyncAt: null };

async function listAll(adapter: MockLocalAdapter, basePath: string, cursor = noCursor) {
    const files = [];
    for await (const f of adapter.list(config(basePath), cursor)) {
        files.push(f);
    }
    return files;
}

// ─── list ─────────────────────────────────────────────────────────────────

describe('MockLocalAdapter — list', () => {
    let tmpDir: string;
    let adapter: MockLocalAdapter;

    beforeEach(async () => {
        tmpDir = await makeTmpDir();
        adapter = new MockLocalAdapter();
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('lists a single file', async () => {
        await writeFile(tmpDir, 'report.txt', 'hello');
        const files = await listAll(adapter, tmpDir);
        expect(files).toHaveLength(1);
        expect(files[0].name).toBe('report.txt');
    });

    test('file descriptor has correct path relative to basePath', async () => {
        await writeFile(tmpDir, 'docs/notes.md', '# Notes');
        const files = await listAll(adapter, tmpDir);
        expect(files[0].path).toBe(path.join('docs', 'notes.md'));
    });

    test('file descriptor has non-empty hash', async () => {
        await writeFile(tmpDir, 'a.txt', 'content');
        const files = await listAll(adapter, tmpDir);
        expect(files[0].hash).toHaveLength(64); // sha256 hex
    });

    test('file descriptor size matches content', async () => {
        const content = 'hello world';
        await writeFile(tmpDir, 'test.txt', content);
        const files = await listAll(adapter, tmpDir);
        expect(files[0].size).toBe(Buffer.byteLength(content, 'utf-8'));
    });

    test('infers mimeType from extension', async () => {
        await writeFile(tmpDir, 'data.json', '{}');
        await writeFile(tmpDir, 'sheet.csv', 'a,b\n1,2');
        await writeFile(tmpDir, 'doc.md', '# Hi');
        await writeFile(tmpDir, 'note.txt', 'text');
        await writeFile(tmpDir, 'binary.pdf', '%PDF');
        const files = await listAll(adapter, tmpDir);
        const byName = Object.fromEntries(files.map((f) => [f.name, f.mimeType]));
        expect(byName['data.json']).toBe('application/json');
        expect(byName['sheet.csv']).toBe('text/csv');
        expect(byName['doc.md']).toBe('text/markdown');
        expect(byName['note.txt']).toBe('text/plain');
        expect(byName['binary.pdf']).toBe('application/pdf');
    });

    test('unknown extension falls back to application/octet-stream', async () => {
        await writeFile(tmpDir, 'archive.xyz', 'binary');
        const files = await listAll(adapter, tmpDir);
        expect(files[0].mimeType).toBe('application/octet-stream');
    });

    test('scans recursively into subdirectories', async () => {
        await writeFile(tmpDir, 'a.txt', 'a');
        await writeFile(tmpDir, 'sub/b.txt', 'b');
        await writeFile(tmpDir, 'sub/deep/c.txt', 'c');
        const files = await listAll(adapter, tmpDir);
        expect(files).toHaveLength(3);
    });

    test('empty directory yields no files', async () => {
        const files = await listAll(adapter, tmpDir);
        expect(files).toHaveLength(0);
    });

    test('cursor with lastSyncAt filters out files not modified since', async () => {
        await writeFile(tmpDir, 'old.txt', 'old');
        // Use a future cursor time — all files should be filtered out
        const futureTs = new Date(Date.now() + 60_000).toISOString();
        const files = await listAll(adapter, tmpDir, { lastSyncAt: futureTs });
        expect(files).toHaveLength(0);
    });

    test('cursor with past lastSyncAt still yields recently modified files', async () => {
        await writeFile(tmpDir, 'new.txt', 'new');
        const pastTs = new Date(Date.now() - 60_000).toISOString();
        const files = await listAll(adapter, tmpDir, { lastSyncAt: pastTs });
        expect(files).toHaveLength(1);
    });

    test('different file contents produce different hashes', async () => {
        await writeFile(tmpDir, 'a.txt', 'content-a');
        await writeFile(tmpDir, 'b.txt', 'content-b');
        const files = await listAll(adapter, tmpDir);
        expect(files[0].hash).not.toBe(files[1].hash);
    });
});

// ─── fetch ────────────────────────────────────────────────────────────────

describe('MockLocalAdapter — fetch', () => {
    let tmpDir: string;
    let adapter: MockLocalAdapter;

    beforeEach(async () => {
        tmpDir = await makeTmpDir();
        adapter = new MockLocalAdapter();
    });

    afterEach(async () => {
        await fs.rm(tmpDir, { recursive: true, force: true });
    });

    test('fetches text file with utf-8 encoding', async () => {
        await writeFile(tmpDir, 'note.txt', 'hello world');
        const files = await listAll(adapter, tmpDir);
        const content = await adapter.fetch(config(tmpDir), files[0]);
        expect(content.encoding).toBe('utf-8');
        expect(content.data.toString('utf-8')).toBe('hello world');
    });

    test('fetches JSON file with parsed content', async () => {
        const obj = { name: 'Alice', age: 30 };
        await writeFile(tmpDir, 'data.json', JSON.stringify(obj));
        const files = await listAll(adapter, tmpDir);
        const content = await adapter.fetch(config(tmpDir), files[0]);
        expect(content.parsed).toEqual(obj);
        expect(content.encoding).toBe('utf-8');
    });

    test('fetches markdown file without parsed content', async () => {
        await writeFile(tmpDir, 'doc.md', '# Title');
        const files = await listAll(adapter, tmpDir);
        const content = await adapter.fetch(config(tmpDir), files[0]);
        expect(content.parsed).toBeUndefined();
        expect(content.encoding).toBe('utf-8');
    });

    test('fetches CSV file with utf-8 encoding and no parsed content', async () => {
        await writeFile(tmpDir, 'data.csv', 'a,b\n1,2');
        const files = await listAll(adapter, tmpDir);
        const content = await adapter.fetch(config(tmpDir), files[0]);
        expect(content.encoding).toBe('utf-8');
        expect(content.parsed).toBeUndefined();
    });

    test('fetches nested file via relative path', async () => {
        await writeFile(tmpDir, 'sub/report.txt', 'nested content');
        const files = await listAll(adapter, tmpDir);
        const content = await adapter.fetch(config(tmpDir), files[0]);
        expect(content.data.toString('utf-8')).toBe('nested content');
    });
});
