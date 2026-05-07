// social-api/src/ingest/adapters/__tests__/jira.test.ts
//
// Unit tests for JiraSourceAdapter + JiraWriteBackDriver. Mocks the global
// `fetch` so no real Jira API calls are made.

import {
  JiraSourceAdapter,
  JiraWriteBackDriver,
  mergeStatusUpdate,
  isTerminalStatus,
  issuePath,
  commentPath,
  parseJiraPath,
  type WriteBackChange,
} from '../jira';
import type { SourceConfig, SyncCursor, FileDescriptor } from '../../types';

// ─── fetch mocking helpers ────────────────────────────────────────────────

interface FetchCall {
  url: string;
  init: RequestInit | undefined;
}

let fetchCalls: FetchCall[] = [];
let fetchQueue: Array<() => Response> = [];
const realFetch = global.fetch;

function queueResponse(body: unknown, status = 200): void {
  fetchQueue.push(() => makeResponse(body, status));
}

function queueRaw(producer: () => Response): void {
  fetchQueue.push(producer);
}

function makeResponse(body: unknown, status = 200): Response {
  const text = typeof body === 'string' ? body : JSON.stringify(body);
  return {
    ok: status >= 200 && status < 300,
    status,
    json: async () => (typeof body === 'string' ? JSON.parse(body) : body),
    text: async () => text,
  } as unknown as Response;
}

beforeEach(() => {
  fetchCalls = [];
  fetchQueue = [];
  (global as { fetch: typeof fetch }).fetch = (async (
    url: string | URL | Request,
    init?: RequestInit,
  ) => {
    fetchCalls.push({ url: String(url), init });
    const next = fetchQueue.shift();
    if (!next) {
      throw new Error(`Unexpected fetch — no queued response (url=${String(url)})`);
    }
    return next();
  }) as typeof fetch;
});

afterAll(() => {
  global.fetch = realFetch;
});

// ─── helpers ──────────────────────────────────────────────────────────────

function makeConfig(overrides: Partial<SourceConfig> = {}): SourceConfig {
  return {
    baseUrl: 'https://acme.atlassian.net',
    email: 'bot@acme.io',
    apiToken: 'token123',
    projectKey: 'ENG',
    ...overrides,
  };
}

function makeCursor(overrides: Partial<SyncCursor> = {}): SyncCursor {
  return { lastSyncAt: null, ...overrides };
}

// ─── JiraSourceAdapter.list ───────────────────────────────────────────────

describe('JiraSourceAdapter.list — fetch issues + comments', () => {
  test('yields issue + embedded comments under the project key', async () => {
    queueResponse({
      issues: [
        {
          id: '10000',
          key: 'ENG-123',
          fields: {
            summary: 'fix the build',
            description: 'something is broken',
            updated: '2026-05-01T12:00:00.000Z',
            created: '2026-04-30T09:00:00.000Z',
            status: { name: 'In Progress', statusCategory: { key: 'indeterminate' } },
            project: { key: 'ENG' },
            comment: {
              total: 2,
              comments: [
                { id: 'c1', body: 'looking into it', updated: '2026-05-01T12:30:00.000Z' },
                { id: 'c2', body: 'fixed in PR', updated: '2026-05-01T13:00:00.000Z' },
              ],
            },
          },
        },
      ],
      startAt: 0,
      maxResults: 50,
      total: 1,
    });

    const adapter = new JiraSourceAdapter();
    const results: FileDescriptor[] = [];
    for await (const f of adapter.list(makeConfig(), makeCursor())) {
      results.push(f);
    }

    expect(results).toHaveLength(3);

    // First yield is the issue
    expect(results[0].path).toBe('ENG/ENG-123');
    expect(results[0].mimeType).toBe('application/vnd.jira.issue+json');
    expect(results[0].metadata?.kind).toBe('issue');
    expect(results[0].metadata?.issueKey).toBe('ENG-123');
    expect(results[0].metadata?.status).toBe('In Progress');

    // Then comments
    expect(results[1].path).toBe('ENG/ENG-123/comment/c1');
    expect(results[1].metadata?.kind).toBe('comment');
    expect(results[1].metadata?.commentId).toBe('c1');
    expect(results[2].path).toBe('ENG/ENG-123/comment/c2');
  });

  test('passes JQL with project + updated >= clause when cursor.lastSyncAt set', async () => {
    queueResponse({ issues: [], startAt: 0, maxResults: 50, total: 0 });

    const adapter = new JiraSourceAdapter();
    for await (const _ of adapter.list(
      makeConfig(),
      makeCursor({ lastSyncAt: '2026-05-01T10:30:00.000Z' }),
    )) { /* drain */ }

    expect(fetchCalls).toHaveLength(1);
    const url = fetchCalls[0].url;
    expect(url).toContain('/rest/api/3/search');
    // URLSearchParams encodes spaces as `+`. Use it to decode for fidelity.
    const params = new URLSearchParams(url.split('?')[1]);
    const jql = params.get('jql') ?? '';
    expect(jql).toContain('project = "ENG"');
    expect(jql).toContain('updated >= "2026-05-01 10:30"');
    expect(jql).toContain('ORDER BY updated ASC');
  });

  test('sends Basic auth header from email + apiToken', async () => {
    queueResponse({ issues: [], startAt: 0, maxResults: 50, total: 0 });

    const adapter = new JiraSourceAdapter();
    for await (const _ of adapter.list(makeConfig(), makeCursor())) { /* drain */ }

    const auth = (fetchCalls[0].init?.headers as Record<string, string>).Authorization;
    expect(auth).toMatch(/^Basic /);
    const decoded = Buffer.from(auth.replace('Basic ', ''), 'base64').toString();
    expect(decoded).toBe('bot@acme.io:token123');
  });

  test('pages additional comments when total > embedded length', async () => {
    queueResponse({
      issues: [
        {
          id: '1',
          key: 'ENG-7',
          fields: {
            updated: '2026-05-01T00:00:00.000Z',
            project: { key: 'ENG' },
            comment: {
              total: 3,
              comments: [{ id: 'c1', updated: '2026-05-01T00:00:00.000Z' }],
            },
          },
        },
      ],
      startAt: 0,
      maxResults: 50,
      total: 1,
    });
    // Extra-comment paginated call
    queueResponse({
      comments: [
        { id: 'c2', updated: '2026-05-01T00:01:00.000Z' },
        { id: 'c3', updated: '2026-05-01T00:02:00.000Z' },
      ],
      total: 3,
    });

    const adapter = new JiraSourceAdapter();
    const results: FileDescriptor[] = [];
    for await (const f of adapter.list(makeConfig(), makeCursor())) {
      results.push(f);
    }

    const commentPaths = results
      .filter((r) => r.metadata?.kind === 'comment')
      .map((r) => r.metadata?.commentId);
    expect(commentPaths).toEqual(['c1', 'c2', 'c3']);
  });
});

// ─── JiraSourceAdapter.fetch ──────────────────────────────────────────────

describe('JiraSourceAdapter.fetch', () => {
  test('fetches an issue by key and returns parsed JSON', async () => {
    queueResponse({ id: '10000', key: 'ENG-123', fields: { summary: 'x' } });

    const adapter = new JiraSourceAdapter();
    const file: FileDescriptor = {
      path: 'ENG/ENG-123',
      name: 'ENG-123',
      hash: 'h',
      size: 0,
      mimeType: 'application/vnd.jira.issue+json',
      lastModified: '2026-05-01T00:00:00.000Z',
    };

    const content = await adapter.fetch(makeConfig(), file);
    expect((content.parsed as { key: string }).key).toBe('ENG-123');
    expect(content.encoding).toBe('utf-8');
    expect(fetchCalls[0].url).toContain('/rest/api/3/issue/ENG-123');
  });

  test('fetches a comment by id', async () => {
    queueResponse({ id: 'c1', body: 'looking into it' });

    const adapter = new JiraSourceAdapter();
    const file: FileDescriptor = {
      path: 'ENG/ENG-123/comment/c1',
      name: 'c1',
      hash: 'h',
      size: 0,
      mimeType: 'application/vnd.jira.comment+json',
      lastModified: '2026-05-01T00:00:00.000Z',
    };

    const content = await adapter.fetch(makeConfig(), file);
    expect((content.parsed as { id: string }).id).toBe('c1');
    expect(fetchCalls[0].url).toContain('/rest/api/3/issue/ENG-123/comment/c1');
  });
});

// ─── path helpers ─────────────────────────────────────────────────────────

describe('jira path helpers — issue-id ↔ documentId mapping', () => {
  test('issuePath / commentPath shape', () => {
    expect(issuePath('ENG', 'ENG-1')).toBe('ENG/ENG-1');
    expect(commentPath('ENG', 'ENG-1', 'c42')).toBe('ENG/ENG-1/comment/c42');
  });

  test('parseJiraPath round-trips both shapes', () => {
    expect(parseJiraPath('ENG/ENG-1')).toEqual({
      kind: 'issue', projectKey: 'ENG', issueKey: 'ENG-1',
    });
    expect(parseJiraPath('ENG/ENG-1/comment/c42')).toEqual({
      kind: 'comment', projectKey: 'ENG', issueKey: 'ENG-1', commentId: 'c42',
    });
    expect(parseJiraPath('something/else/here/wrong')).toBeNull();
    expect(parseJiraPath('only-one')).toBeNull();
  });
});

// ─── status merge rules ───────────────────────────────────────────────────

describe('mergeStatusUpdate — status-field rule-based merge', () => {
  test('apply when remote is non-terminal and differs from local', () => {
    expect(mergeStatusUpdate('Review', { name: 'In Progress' })).toEqual({
      kind: 'apply', target: 'Review',
    });
  });

  test('noop when remote name equals local target (case-insensitive)', () => {
    expect(mergeStatusUpdate('done', { name: 'Done' })).toEqual({ kind: 'noop' });
  });

  test('reject when remote is the literal "done"', () => {
    const result = mergeStatusUpdate('In Progress', { name: 'Done' });
    expect(result.kind).toBe('reject');
  });

  test('reject when remote is "Closed"', () => {
    const result = mergeStatusUpdate('In Progress', { name: 'Closed' });
    expect(result.kind).toBe('reject');
  });

  test('reject when remote is "Resolved"', () => {
    const result = mergeStatusUpdate('Open', { name: 'Resolved' });
    expect(result.kind).toBe('reject');
  });

  test('reject when remote category=done even with custom name', () => {
    const result = mergeStatusUpdate('Open', {
      name: 'Shipped',
      statusCategory: { key: 'done' },
    });
    expect(result.kind).toBe('reject');
  });

  test('apply when remote is null/undefined', () => {
    expect(mergeStatusUpdate('Open', null).kind).toBe('apply');
    expect(mergeStatusUpdate('Open', undefined).kind).toBe('apply');
  });

  test('isTerminalStatus matches by name and by category', () => {
    expect(isTerminalStatus({ name: 'Done' })).toBe(true);
    expect(isTerminalStatus({ name: 'closed' })).toBe(true);
    expect(isTerminalStatus({ name: 'In Progress' })).toBe(false);
    expect(isTerminalStatus({ name: 'Custom', statusCategory: { key: 'done' } })).toBe(true);
    expect(isTerminalStatus(null)).toBe(false);
  });
});

// ─── JiraWriteBackDriver — comment.add ────────────────────────────────────

describe('JiraWriteBackDriver — comment.add', () => {
  test('posts to /rest/api/3/issue/:key/comment and returns ack', async () => {
    queueResponse({ id: 'cnew', body: 'hello', created: '2026-05-06T10:00:00.000Z' }, 201);

    const driver = new JiraWriteBackDriver();
    const change: WriteBackChange = {
      documentId: 'doc-1',
      sourceRef: 'ENG-123',
      change: { op: 'comment.add', issueKey: 'ENG-123', body: 'hello' },
      timestamp: '2026-05-06T10:00:00.000Z',
    };

    const ack = await driver.push(makeConfig(), change);
    expect(ack.newVersion).toBe('cnew');
    expect(ack.acknowledgedAt).toBe('2026-05-06T10:00:00.000Z');

    expect(fetchCalls).toHaveLength(1);
    expect(fetchCalls[0].url).toContain('/rest/api/3/issue/ENG-123/comment');
    expect(fetchCalls[0].init?.method).toBe('POST');
    const body = JSON.parse(fetchCalls[0].init?.body as string);
    expect(body.body).toBe('hello');
  });

  test('falls back to change.sourceRef when op.issueKey is missing', async () => {
    queueResponse({ id: 'cx', created: '2026-05-06T10:00:00.000Z' }, 201);

    const driver = new JiraWriteBackDriver();
    const change: WriteBackChange = {
      documentId: 'doc-1',
      sourceRef: 'ENG-99',
      change: { op: 'comment.add', issueKey: '', body: 'fallback' },
      timestamp: '2026-05-06T10:00:00.000Z',
    };

    await driver.push(makeConfig(), change);
    expect(fetchCalls[0].url).toContain('/issue/ENG-99/comment');
  });

  test('throws when Jira responds non-2xx', async () => {
    queueResponse({ error: 'forbidden' }, 403);

    const driver = new JiraWriteBackDriver();
    await expect(
      driver.push(makeConfig(), {
        documentId: 'doc-1',
        sourceRef: 'ENG-1',
        change: { op: 'comment.add', issueKey: 'ENG-1', body: 'x' },
        timestamp: 'ts',
      }),
    ).rejects.toThrow(/comment.add.*403/);
  });
});

// ─── JiraWriteBackDriver — issue.fields.update ───────────────────────────

describe('JiraWriteBackDriver — issue.fields.update — happy path', () => {
  test('applies status transition when remote is non-terminal', async () => {
    // 1. read current issue
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: {
        updated: '2026-05-06T09:00:00.000Z',
        status: { name: 'Open', statusCategory: { key: 'new' } },
      },
    });
    // 2. list transitions
    queueResponse({
      transitions: [
        { id: '11', name: 'Start', to: { name: 'In Progress' } },
        { id: '21', name: 'Resolve', to: { name: 'Done' } },
      ],
    });
    // 3. POST transition
    queueResponse({}, 204);
    // 4. re-read for new version
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: {
        updated: '2026-05-06T10:00:00.000Z',
        status: { name: 'In Progress', statusCategory: { key: 'indeterminate' } },
      },
    });

    const driver = new JiraWriteBackDriver();
    const ack = await driver.push(makeConfig(), {
      documentId: 'doc-1',
      sourceRef: 'ENG-1',
      change: { op: 'issue.fields.update', issueKey: 'ENG-1', fields: { status: 'In Progress' } },
      timestamp: 'ts',
    });

    expect(ack.merged).toBeUndefined();
    expect(ack.newVersion).toBe('2026-05-06T10:00:00.000Z');

    // Verify the transition POST was made with the matched id
    const transitionPost = fetchCalls.find(
      (c) => c.url.endsWith('/transitions') && c.init?.method === 'POST',
    );
    expect(transitionPost).toBeDefined();
    const body = JSON.parse(transitionPost!.init!.body as string);
    expect(body.transition.id).toBe('11');
  });

  test('no-ops when local target equals remote (idempotent)', async () => {
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: { updated: 'v1', status: { name: 'In Progress' } },
    });
    // re-read at the end
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: { updated: 'v1', status: { name: 'In Progress' } },
    });

    const driver = new JiraWriteBackDriver();
    const ack = await driver.push(makeConfig(), {
      documentId: 'doc-1',
      sourceRef: 'ENG-1',
      change: { op: 'issue.fields.update', issueKey: 'ENG-1', fields: { status: 'In Progress' } },
      timestamp: 'ts',
    });

    // No transitions called, no PUT — only the two reads
    expect(fetchCalls).toHaveLength(2);
    expect(ack.merged).toBeUndefined();
  });
});

describe('JiraWriteBackDriver — issue.fields.update — reject path', () => {
  test('drops status update when remote is "Done" (terminal) and acks merged=true', async () => {
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: {
        updated: 'v-final',
        status: { name: 'Done', statusCategory: { key: 'done' } },
      },
    });
    // Re-read at end (no mutation happened, but driver still re-reads)
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: {
        updated: 'v-final',
        status: { name: 'Done', statusCategory: { key: 'done' } },
      },
    });

    const driver = new JiraWriteBackDriver();
    const ack = await driver.push(makeConfig(), {
      documentId: 'doc-1',
      sourceRef: 'ENG-1',
      change: { op: 'issue.fields.update', issueKey: 'ENG-1', fields: { status: 'In Progress' } },
      timestamp: 'ts',
    });

    expect(ack.merged).toBe(true);
    expect(ack.newVersion).toBe('v-final');

    // No transitions, no PUT — only the two reads.
    expect(fetchCalls.filter((c) => c.init?.method === 'POST')).toHaveLength(0);
    expect(fetchCalls.filter((c) => c.init?.method === 'PUT')).toHaveLength(0);
  });

  test('rejects non-status field update with stale baseVersion', async () => {
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: { updated: 'v2', status: { name: 'Open' } },
    });

    const driver = new JiraWriteBackDriver();
    await expect(
      driver.push(makeConfig(), {
        documentId: 'doc-1',
        sourceRef: 'ENG-1',
        baseVersion: 'v1', // stale
        change: {
          op: 'issue.fields.update',
          issueKey: 'ENG-1',
          fields: { summary: 'new summary' },
        },
        timestamp: 'ts',
      }),
    ).rejects.toThrow(/conflict on non-status fields/);
  });

  test('non-status field update with matching baseVersion is applied', async () => {
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: { updated: 'v1', status: { name: 'Open' } },
    });
    queueResponse({}, 204); // PUT
    queueResponse({
      id: '1', key: 'ENG-1',
      fields: { updated: 'v2', status: { name: 'Open' } },
    });

    const driver = new JiraWriteBackDriver();
    const ack = await driver.push(makeConfig(), {
      documentId: 'doc-1',
      sourceRef: 'ENG-1',
      baseVersion: 'v1',
      change: {
        op: 'issue.fields.update',
        issueKey: 'ENG-1',
        fields: { summary: 'new summary' },
      },
      timestamp: 'ts',
    });

    expect(ack.newVersion).toBe('v2');
    const put = fetchCalls.find((c) => c.init?.method === 'PUT');
    expect(put).toBeDefined();
    const body = JSON.parse(put!.init!.body as string);
    expect(body.fields.summary).toBe('new summary');
  });
});

describe('JiraWriteBackDriver — unknown op', () => {
  test('throws on unknown op discriminator', async () => {
    const driver = new JiraWriteBackDriver();
    await expect(
      driver.push(makeConfig(), {
        documentId: 'doc-1',
        sourceRef: 'ENG-1',
        change: { op: 'something.else' } as unknown,
        timestamp: 'ts',
      }),
    ).rejects.toThrow(/unknown op/);
  });

  test('throws when change.change is missing op', async () => {
    const driver = new JiraWriteBackDriver();
    await expect(
      driver.push(makeConfig(), {
        documentId: 'doc-1',
        sourceRef: 'ENG-1',
        change: {},
        timestamp: 'ts',
      }),
    ).rejects.toThrow(/missing.*op.*discriminator/);
  });
});
