// social-api/src/ingest/adapters/__tests__/confluence.test.ts
//
// Unit tests for ConfluenceSourceAdapter + ConfluenceWriteBackDriver.
// All Confluence HTTP calls are mocked — no real network access.

import {
  ConfluenceSourceAdapter,
  ConfluenceWriteBackDriver,
  type ConfluenceConfig,
  type WriteBackResult,
} from '../confluence';
import type { SyncCursor } from '../../types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeConfig(overrides: Partial<ConfluenceConfig> = {}): ConfluenceConfig {
  return {
    baseUrl: 'https://acme.atlassian.net/wiki',
    email: 'agent@acme.com',
    apiToken: 'tok-abc',
    spaceKey: 'ENG',
    ...overrides,
  };
}

function makeCursor(overrides: Partial<SyncCursor> = {}): SyncCursor {
  return { lastSyncAt: null, ...overrides };
}

interface MockResp {
  status: number;
  ok: boolean;
  statusText?: string;
  body?: unknown;
}

function jsonResponse(r: MockResp): Response {
  return {
    ok: r.ok,
    status: r.status,
    statusText: r.statusText ?? 'OK',
    json: async () => r.body,
  } as unknown as Response;
}

// ─── happy-path fetch ─────────────────────────────────────────────────────

describe('ConfluenceSourceAdapter — happy fetch', () => {
  test('list yields page descriptors and follows _links.next', async () => {
    const fetchMock = jest.fn();
    fetchMock
      .mockResolvedValueOnce(jsonResponse({
        ok: true, status: 200,
        body: {
          results: [
            { id: '111', title: 'Page A', version: { number: 3 }, body: { storage: { value: '<p>a</p>' } } },
            { id: '222', title: 'Page B', version: { number: 1 }, body: { storage: { value: '<p>b</p>' } } },
          ],
          _links: { next: '/rest/api/content?cursor=abc', base: 'https://acme.atlassian.net/wiki' },
        },
      }))
      .mockResolvedValueOnce(jsonResponse({
        ok: true, status: 200,
        body: {
          results: [
            { id: '333', title: 'Page C', version: { number: 5 }, body: { storage: { value: '<p>c</p>' } } },
          ],
          _links: { base: 'https://acme.atlassian.net/wiki' }, // no next → stop
        },
      }));

    const adapter = new ConfluenceSourceAdapter(fetchMock as unknown as typeof fetch);
    const results = [];
    for await (const f of adapter.list(makeConfig(), makeCursor())) results.push(f);

    expect(results).toHaveLength(3);
    expect(results[0].path).toBe('111');
    expect(results[0].name).toBe('Page A');
    expect(results[0].mimeType).toBe('text/html');
    expect(results[0].hash).toBe('v3:111');
    expect(results[0].metadata?.versionNumber).toBe(3);
    expect(results[2].path).toBe('333');

    // Two HTTP calls: initial + follow-up
    expect(fetchMock).toHaveBeenCalledTimes(2);
    const firstUrl = fetchMock.mock.calls[0][0] as string;
    expect(firstUrl).toContain('/rest/api/content');
    expect(firstUrl).toContain('spaceKey=ENG');
    const secondUrl = fetchMock.mock.calls[1][0] as string;
    expect(secondUrl).toBe('https://acme.atlassian.net/wiki/rest/api/content?cursor=abc');

    // Auth header is basic with base64(email:token)
    const expectedAuth =
      'Basic ' + Buffer.from('agent@acme.com:tok-abc').toString('base64');
    const opts = fetchMock.mock.calls[0][1] as { headers: Record<string, string> };
    expect(opts.headers.Authorization).toBe(expectedAuth);
  });

  test('list resumes from cursor.checkpoint', async () => {
    const fetchMock = jest.fn().mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200,
      body: { results: [], _links: {} },
    }));
    const adapter = new ConfluenceSourceAdapter(fetchMock as unknown as typeof fetch);
    const cursor = makeCursor({ checkpoint: 'https://acme.atlassian.net/wiki/rest/api/content?cursor=resume' });
    for await (const _ of adapter.list(makeConfig(), cursor)) { /* drain */ }
    expect(fetchMock.mock.calls[0][0]).toBe('https://acme.atlassian.net/wiki/rest/api/content?cursor=resume');
  });

  test('list under rootPageId hits the descendant/page endpoint', async () => {
    const fetchMock = jest.fn().mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200,
      body: { results: [], _links: {} },
    }));
    const adapter = new ConfluenceSourceAdapter(fetchMock as unknown as typeof fetch);
    for await (const _ of adapter.list(makeConfig({ spaceKey: undefined, rootPageId: '99' }), makeCursor())) { /* drain */ }
    const url = fetchMock.mock.calls[0][0] as string;
    expect(url).toContain('/rest/api/content/99/descendant/page');
  });

  test('fetch returns body.storage.value as utf-8 buffer + parsed metadata', async () => {
    const fetchMock = jest.fn().mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200,
      body: {
        id: '555', title: 'Spec', version: { number: 7 },
        body: { storage: { value: '<h1>hello</h1>' } },
      },
    }));
    const adapter = new ConfluenceSourceAdapter(fetchMock as unknown as typeof fetch);
    const content = await adapter.fetch(makeConfig(), {
      path: '555',
      name: 'Spec',
      hash: 'v7:555',
      size: 14,
      mimeType: 'text/html',
      lastModified: new Date().toISOString(),
      metadata: { pageId: '555', versionNumber: 7 },
    });

    expect(content.encoding).toBe('utf-8');
    expect(content.data.toString('utf-8')).toBe('<h1>hello</h1>');
    expect((content.parsed as { versionNumber: number }).versionNumber).toBe(7);
    expect((content.parsed as { pageId: string }).pageId).toBe('555');
    const url = fetchMock.mock.calls[0][0] as string;
    expect(url).toContain('/rest/api/content/555');
    expect(url).toContain('expand=version,body.storage');
  });

  test('list propagates HTTP errors', async () => {
    const fetchMock = jest.fn().mockResolvedValueOnce({
      ok: false, status: 401, statusText: 'Unauthorized', json: async () => ({}),
    });
    const adapter = new ConfluenceSourceAdapter(fetchMock as unknown as typeof fetch);
    const iter = adapter.list(makeConfig(), makeCursor());
    await expect(iter.next()).rejects.toThrow(/401/);
  });
});

// ─── happy push ───────────────────────────────────────────────────────────

describe('ConfluenceWriteBackDriver — happy push', () => {
  test('sends PUT with version + 1 and returns ack', async () => {
    const fetchMock = jest.fn();
    // First call: peekTitle GET
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '777', title: 'Old Title', version: { number: 4 } },
    }));
    // Second call: PUT
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '777', title: 'Old Title', version: { number: 5 } },
    }));

    const driver = new ConfluenceWriteBackDriver(fetchMock as unknown as typeof fetch);
    const result = await driver.push(makeConfig(), '777', {
      body: '<p>updated</p>',
      baseVersion: 4,
    });

    expect(result.ok).toBe(true);
    if (result.ok) {
      expect(result.documentId).toBe('777');
      expect(result.newVersion).toBe(5);
    }

    // Inspect the PUT call
    const putCall = fetchMock.mock.calls[1];
    expect(putCall[1].method).toBe('PUT');
    const sentBody = JSON.parse(putCall[1].body);
    expect(sentBody.id).toBe('777');
    expect(sentBody.version.number).toBe(5);
    expect(sentBody.body.storage.value).toBe('<p>updated</p>');
    expect(sentBody.body.storage.representation).toBe('storage');
    expect(sentBody.title).toBe('Old Title');
  });

  test('uses caller-supplied title when provided (no extra GET needed for title)', async () => {
    const fetchMock = jest.fn();
    // peekTitle still happens (driver doesn't short-circuit) but we test
    // that the supplied title wins.
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '777', title: 'Old', version: { number: 4 } },
    }));
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '777', title: 'Brand New', version: { number: 5 } },
    }));

    const driver = new ConfluenceWriteBackDriver(fetchMock as unknown as typeof fetch);
    await driver.push(makeConfig(), '777', {
      body: '<p>x</p>',
      title: 'Brand New',
      baseVersion: 4,
    });
    const sentBody = JSON.parse(fetchMock.mock.calls[1][1].body);
    expect(sentBody.title).toBe('Brand New');
  });
});

// ─── version conflict ─────────────────────────────────────────────────────

describe('ConfluenceWriteBackDriver — version conflict', () => {
  test('returns WriteBackConflict on 409 with remote body + version', async () => {
    const fetchMock = jest.fn();
    // peekTitle GET (before PUT)
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '888', title: 'Stale', version: { number: 6 } },
    }));
    // PUT → 409
    fetchMock.mockResolvedValueOnce({
      ok: false, status: 409, statusText: 'Conflict', json: async () => ({}),
    });
    // Re-read current page after conflict
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200,
      body: {
        id: '888', title: 'Newer Title',
        version: { number: 7 },
        body: { storage: { value: '<p>remote-version</p>' } },
      },
    }));

    const driver = new ConfluenceWriteBackDriver(fetchMock as unknown as typeof fetch);
    const result: WriteBackResult = await driver.push(makeConfig(), '888', {
      body: '<p>local-edit</p>',
      baseVersion: 4, // editor was looking at v4, but server is at v7 — conflict
    });

    expect(result.ok).toBe(false);
    if (!result.ok) {
      expect(result.reason).toBe('version-conflict');
      expect(result.documentId).toBe('888');
      expect(result.baseVersion).toBe(4);
      expect(result.remoteVersion).toBe(7);
      expect(result.remoteTitle).toBe('Newer Title');
      expect(result.remoteBody).toBe('<p>remote-version</p>');
    }
    // Driver did NOT auto-merge — confirmed by no further PUT after the 409.
    const putCalls = fetchMock.mock.calls.filter((c) => c[1]?.method === 'PUT');
    expect(putCalls).toHaveLength(1);
  });

  test('non-409 errors throw', async () => {
    const fetchMock = jest.fn();
    fetchMock.mockResolvedValueOnce(jsonResponse({
      ok: true, status: 200, body: { id: '999', title: 'X', version: { number: 1 } },
    }));
    fetchMock.mockResolvedValueOnce({
      ok: false, status: 500, statusText: 'Server Error', json: async () => ({}),
    });
    const driver = new ConfluenceWriteBackDriver(fetchMock as unknown as typeof fetch);
    await expect(
      driver.push(makeConfig(), '999', { body: '<p>x</p>', baseVersion: 1 }),
    ).rejects.toThrow(/500/);
  });
});
