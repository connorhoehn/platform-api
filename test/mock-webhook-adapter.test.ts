// test/mock-webhook-adapter.test.ts
//
// Tests for MockWebhookAdapter: receive, list (drains buffer), and fetch.

import { MockWebhookAdapter } from '../src/ingest/adapters/mock-webhook';
import type { FileDescriptor, FileContent } from '../src/ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeDescriptor(path: string): FileDescriptor {
    return {
        path,
        name: path.split('/').pop()!,
        hash: 'abc',
        size: 10,
        mimeType: 'text/plain',
        lastModified: '2026-01-01T00:00:00Z',
    };
}

function makeContent(text: string): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

async function drainList(adapter: MockWebhookAdapter): Promise<FileDescriptor[]> {
    const items: FileDescriptor[] = [];
    for await (const item of adapter.list({}, { lastSyncAt: null })) {
        items.push(item);
    }
    return items;
}

// ─── receive + list ───────────────────────────────────────────────────────

describe('MockWebhookAdapter — receive and list', () => {
    test('empty adapter yields no files', async () => {
        const adapter = new MockWebhookAdapter();
        const items = await drainList(adapter);
        expect(items).toHaveLength(0);
    });

    test('received file appears in list', async () => {
        const adapter = new MockWebhookAdapter();
        const desc = makeDescriptor('docs/file.txt');
        adapter.receive(desc, makeContent('hello'));
        const items = await drainList(adapter);
        expect(items).toHaveLength(1);
        expect(items[0].path).toBe('docs/file.txt');
    });

    test('multiple received files all appear in list', async () => {
        const adapter = new MockWebhookAdapter();
        adapter.receive(makeDescriptor('a.txt'), makeContent('a'));
        adapter.receive(makeDescriptor('b.txt'), makeContent('b'));
        const items = await drainList(adapter);
        expect(items).toHaveLength(2);
    });

    test('list drains the buffer — second list returns nothing', async () => {
        const adapter = new MockWebhookAdapter();
        adapter.receive(makeDescriptor('f.txt'), makeContent('x'));
        await drainList(adapter); // first drain
        const second = await drainList(adapter);
        expect(second).toHaveLength(0);
    });
});

// ─── fetch ────────────────────────────────────────────────────────────────

describe('MockWebhookAdapter — fetch', () => {
    test('returns content for received file', async () => {
        const adapter = new MockWebhookAdapter();
        const desc = makeDescriptor('docs/data.txt');
        adapter.receive(desc, makeContent('payload text'));
        const content = await adapter.fetch({}, desc);
        expect(content.data.toString('utf-8')).toBe('payload text');
    });

    test('fetch removes content from map (one-time consume)', async () => {
        const adapter = new MockWebhookAdapter();
        const desc = makeDescriptor('f.txt');
        adapter.receive(desc, makeContent('data'));
        await adapter.fetch({}, desc);
        await expect(adapter.fetch({}, desc)).rejects.toThrow('No content found');
    });

    test('throws when no content registered for file', async () => {
        const adapter = new MockWebhookAdapter();
        const desc = makeDescriptor('ghost.txt');
        await expect(adapter.fetch({}, desc)).rejects.toThrow('No content found');
    });
});
