// social-api/src/ingest/adapters/__tests__/mock-webhook.test.ts
//
// Tests for MockWebhookAdapter: receive buffering, list drains buffer,
// fetch returns and deletes content, error on unknown file.

import { MockWebhookAdapter } from '../mock-webhook';
import type { FileDescriptor, FileContent } from '../../types';

function makeDescriptor(p: string): FileDescriptor {
    return {
        path: p,
        name: p.split('/').pop()!,
        hash: 'abc',
        size: 10,
        mimeType: 'text/plain',
        lastModified: new Date().toISOString(),
    };
}

function makeContent(text: string): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

async function drainList(adapter: MockWebhookAdapter): Promise<FileDescriptor[]> {
    const results: FileDescriptor[] = [];
    for await (const fd of adapter.list({} as never, {} as never)) {
        results.push(fd);
    }
    return results;
}

describe('MockWebhookAdapter — receive and list', () => {
    test('empty buffer yields no files', async () => {
        const adapter = new MockWebhookAdapter();
        const files = await drainList(adapter);
        expect(files).toHaveLength(0);
    });

    test('received file appears in list', async () => {
        const adapter = new MockWebhookAdapter();
        const fd = makeDescriptor('incoming/file.txt');
        adapter.receive(fd, makeContent('hello'));
        const files = await drainList(adapter);
        expect(files).toHaveLength(1);
        expect(files[0].path).toBe('incoming/file.txt');
    });

    test('list drains the buffer — second call yields empty', async () => {
        const adapter = new MockWebhookAdapter();
        adapter.receive(makeDescriptor('a.txt'), makeContent('a'));
        await drainList(adapter);
        const second = await drainList(adapter);
        expect(second).toHaveLength(0);
    });

    test('multiple received files all appear in list', async () => {
        const adapter = new MockWebhookAdapter();
        adapter.receive(makeDescriptor('x.txt'), makeContent('x'));
        adapter.receive(makeDescriptor('y.txt'), makeContent('y'));
        adapter.receive(makeDescriptor('z.txt'), makeContent('z'));
        const files = await drainList(adapter);
        expect(files).toHaveLength(3);
    });
});

describe('MockWebhookAdapter — fetch', () => {
    test('fetch returns the content for a received file', async () => {
        const adapter = new MockWebhookAdapter();
        const fd = makeDescriptor('data.txt');
        adapter.receive(fd, makeContent('payload'));
        const content = await adapter.fetch({} as never, fd);
        expect(content.data.toString('utf-8')).toBe('payload');
    });

    test('fetch removes content from map (consumed once)', async () => {
        const adapter = new MockWebhookAdapter();
        const fd = makeDescriptor('once.txt');
        adapter.receive(fd, makeContent('once'));
        await adapter.fetch({} as never, fd);
        await expect(adapter.fetch({} as never, fd)).rejects.toThrow('No content found');
    });

    test('fetch throws for unknown path', async () => {
        const adapter = new MockWebhookAdapter();
        const fd = makeDescriptor('ghost.txt');
        await expect(adapter.fetch({} as never, fd)).rejects.toThrow('No content found');
    });
});
