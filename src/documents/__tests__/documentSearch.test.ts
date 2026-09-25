import { DocumentSearchIndex, meaningfulDescription, scoreDocument, tokenize, decodeCursor, encodeCursor, type DocumentRow, type DocumentSearchDeps } from '../documentSearch';

const rows: DocumentRow[] = [
  { documentId: 'rp', title: 'Release plan', type: 'page', createdBy: 'connor', updatedAt: '2026-09-24T10:00:00Z' },
  { documentId: 'sn', title: 'Sprint notes', type: 'page', createdBy: 'connor', updatedAt: '2026-09-23T10:00:00Z' },
  { documentId: 'lc', title: 'Launch checklist', type: 'page', createdBy: 'connor', description: 'Release checks for rollout.', updatedAt: '2026-09-24T11:00:00Z' },
  { documentId: 'sr', title: 'Sprint review', type: 'presentation', createdBy: 'connor', description: 'Previous release review.', updatedAt: '2026-09-24T09:00:00Z' },
  { documentId: 'secret', title: 'Release secrets', type: 'page', createdBy: 'frank', updatedAt: '2026-09-24T12:00:00Z' },
  { documentId: 'shared', title: 'Shared release memo', type: 'page', createdBy: 'frank', channel: 'chat:room:a', updatedAt: '2026-09-20T12:00:00Z' },
  { documentId: 'other', title: 'Budget', type: 'page', createdBy: 'connor', updatedAt: '2026-09-22T12:00:00Z' },
];
const bodies: Record<string, string> = { sn: 'Open questions for the release.', rp: 'Scope, readiness and rollback.' };

function deps(over: Partial<DocumentSearchDeps> = {}) {
  const calls = { scan: 0, text: 0, member: 0 };
  const d: DocumentSearchDeps = {
    scanRows: async () => { calls.scan++; return rows; },
    listFolders: async () => ({
      folders: [{ id: 'f1', name: 'Release assets', parentFolderId: 'f0' }, { id: 'f0', name: 'Sprint planning', parentFolderId: null }, { id: 'fx', name: 'Frank only', parentFolderId: null }],
      placement: new Map<string, string | null>([['rp', 'f1'], ['lc', 'f1'], ['sn', 'f0'], ['secret', 'fx']]),
    }),
    readText: async (id) => { calls.text++; return { timestamp: 1, excerpt: bodies[id] ?? null, text: bodies[id] ?? '' }; },
    memberLookup: async (channel) => { calls.member++; return channel === 'chat:room:a'; },
    now: () => 1_000,
    ...over,
  };
  return { d, calls };
}
const connor = { sub: 'connor', orgId: 'org' };
const req = (q: string, extra: object = {}) => ({ q, sort: 'relevance' as const, filters: {}, offset: 0, limit: 30, ...extra });

describe('meaningfulDescription', () => {
  it('drops generic descriptions', () => {
    expect(meaningfulDescription({ description: 'Project documents', title: 'X' })).toBeNull();
    expect(meaningfulDescription({ description: 'Sprint review presentation', title: 'Sprint review' })).toBeNull();
    expect(meaningfulDescription({ description: 'Release checks.', title: 'Launch checklist' })).toBe('Release checks.');
  });
});

describe('scoreDocument', () => {
  it('ranks title over summary over body and needs every word', () => {
    const t = tokenize('release');
    expect(scoreDocument({ title: 'Release plan', summary: '', body: '' }, t, 'release')!.match).toBe('title');
    expect(scoreDocument({ title: 'X', summary: 'the release', body: '' }, t, 'release')!.match).toBe('summary');
    expect(scoreDocument({ title: 'X', summary: '', body: 'a release' }, t, 'release')!.match).toBe('body');
    expect(scoreDocument({ title: 'Unreleased', summary: '', body: '' }, t, 'release')).toBeNull();
    expect(scoreDocument({ title: 'Release plan', summary: '', body: '' }, tokenize('release budget'), 'release budget')).toBeNull();
  });
  it('cursor round-trips', () => { expect(decodeCursor(encodeCursor(30))).toBe(30); expect(decodeCursor('junk')).toBe(0); });
});

describe('DocumentSearchIndex', () => {
  it('returns only readable documents, best first, with folders and summaries', async () => {
    const { d } = deps();
    const res = await new DocumentSearchIndex(d).search(connor, req('release'));
    expect(res.items.map((i) => i.id)).toEqual(['rp', 'shared', 'lc', 'sr', 'sn']);
    expect(res.items.find((i) => i.id === 'secret')).toBeUndefined();
    expect(res.libraryTotal).toBe(6);
    expect(res.items[0]).toMatchObject({ folderName: 'Release assets', summary: 'Scope, readiness and rollback.', match: 'title' });
    expect(res.items.find((i) => i.id === 'lc')!.summary).toBe('Release checks for rollout.');
    expect(res.items.find((i) => i.id === 'sr')).toMatchObject({ kind: 'presentation', summary: 'Previous release review.' });
    expect(res.facets.folders.map((f) => f.id)).not.toContain('fx');
    expect(res.facets.owners.map((o) => o.id).sort()).toEqual(['connor', 'frank']);
  });
  it('ranks an archived copy below the live one', async () => {
    const { d } = deps({
      scanRows: async () => [
        { documentId: 'old', title: 'Release plan', type: 'page', createdBy: 'connor', updatedAt: '2026-09-25T00:00:00Z' },
        { documentId: 'new', title: 'Release plan', type: 'page', createdBy: 'connor', updatedAt: '2026-09-20T00:00:00Z' },
        { documentId: 'memo', title: 'Release memo', type: 'page', createdBy: 'connor', updatedAt: '2026-09-19T00:00:00Z' },
      ],
      listFolders: async () => ({ folders: [{ id: 'a', name: 'Archive', parentFolderId: 'r' }, { id: 'r', name: 'Research', parentFolderId: null }], placement: new Map([['old', 'a']]) }),
    });
    const res = await new DocumentSearchIndex(d).search(connor, req('release plan'));
    expect(res.items.map((i) => i.id)).toEqual(['new', 'old']);
  });
  it('filters by kind, folder subtree and owner; sorts by name; pages', async () => {
    const { d } = deps();
    const idx = new DocumentSearchIndex(d);
    expect((await idx.search(connor, req('release', { filters: { kind: ['presentation'] } }))).items.map((i) => i.id)).toEqual(['sr']);
    expect((await idx.search(connor, req('', { sort: 'name', filters: { folder: 'f0' } }))).items.map((i) => i.id)).toEqual(['lc', 'rp', 'sn']);
    expect((await idx.search(connor, req('', { filters: { owner: ['frank'] } }))).items.map((i) => i.id)).toEqual(['shared']);
    const p1 = await idx.search(connor, req('', { sort: 'updated', limit: 2 }));
    expect(p1.items.map((i) => i.id)).toEqual(['lc', 'rp']);
    const p2 = await idx.search(connor, req('', { sort: 'updated', limit: 2, offset: decodeCursor(p1.nextCursor!) }));
    expect(p2.items.map((i) => i.id)).toEqual(['sr', 'sn']);
  });
  it('shares one scan and one membership read per channel across a burst', async () => {
    const { d, calls } = deps();
    const idx = new DocumentSearchIndex(d);
    await Promise.all([idx.search(connor, req('r')), idx.search(connor, req('re')), idx.search(connor, req('rel'))]);
    expect(calls.scan).toBe(1);
    expect(calls.member).toBe(1); // one channel, remembered across the burst
    expect(calls.text).toBe(5); // each readable page once
  });
});
