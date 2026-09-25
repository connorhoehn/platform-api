jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
const mockWarn = jest.fn();
jest.mock('../../lib/logger', () => ({ __esModule: true, default: { info: jest.fn(), warn: (...args: unknown[]) => mockWarn(...args), error: jest.fn() } }));

import { markProvisionalDeck, sweepProvisionalDecks } from '../provisionalDecks';

type Cmd = { constructor: { name: string }; input: Record<string, any> };
function client(handler: (cmd: Cmd) => unknown) {
  const sent: Cmd[] = [];
  return { sent, docClient: { send: jest.fn(async (cmd: Cmd) => { sent.push(cmd); return handler(cmd); }) } as never };
}
const conditionFailed = () => { throw Object.assign(new Error('failed'), { name: 'ConditionalCheckFailedException' }); };
const HOUR = 60 * 60 * 1000;
const T0 = Date.parse('2026-09-25T02:00:00.000Z');

type DraftRow = { status: string; updatedAt: string };
/** A run-drafts fake keyed `documentId/draftId`, recording deletes. */
function draftStore(rows: Record<string, DraftRow>, opts: { failDelete?: (documentId: string, draftId: string) => boolean } = {}) {
  const deleted: string[] = [];
  return {
    deleted,
    get: async (doc: string, id: string) => rows[`${doc}/${id}`] ?? null,
    listForDocument: async (doc: string) => Object.keys(rows).filter((k) => k.startsWith(`${doc}/`)).map((k) => ({ draftId: k.slice(doc.length + 1) })),
    delete: async (doc: string, id: string) => {
      if (opts.failDelete?.(doc, id)) throw new Error('ddb throttled');
      deleted.push(`${doc}/${id}`);
      delete rows[`${doc}/${id}`];
    },
  };
}

beforeEach(() => mockWarn.mockReset());

describe('provisional decks', () => {
  it('marks only an empty presentation the same person just created', async () => {
    const { sent, docClient } = client(() => ({}));
    expect(await markProvisionalDeck({ documentId: 'd1', draftId: 'dr1', userId: 'dev-connor', now: new Date(T0).toISOString(), docClient })).toBe(true);
    const input = sent[0]!.input;
    expect(input.ConditionExpression).toContain('createdBy = :by');
    expect(input.ConditionExpression).toContain('createdAt >= :recent');
    expect(input.ConditionExpression).toContain('attribute_not_exists(headRevision)');
    expect(input.ConditionExpression).toContain('#type = :type');
    expect(input.ExpressionAttributeValues[':mark']).toEqual({ draftId: 'dr1', by: 'dev-connor', since: new Date(T0).toISOString() });
  });

  it('an existing deck (condition fails) is not marked', async () => {
    const { docClient } = client(conditionFailed);
    expect(await markProvisionalDeck({ documentId: 'd1', draftId: 'dr1', userId: 'u', now: new Date(T0).toISOString(), docClient })).toBe(false);
  });

  it('reaps an idle abandoned draft\'s empty deck, keeps a fresh one and one mid-dispatch', async () => {
    const rows = [
      { documentId: 'old', provisional: { draftId: 'a', since: new Date(T0 - 2 * HOUR).toISOString() } },
      { documentId: 'fresh', provisional: { draftId: 'b', since: new Date(T0 - 5 * 60_000).toISOString() } },
      { documentId: 'busy', provisional: { draftId: 'c', since: new Date(T0 - 3 * HOUR).toISOString() } },
      { documentId: 'touched', provisional: { draftId: 'd', since: new Date(T0 - 3 * HOUR).toISOString() } },
    ];
    const { sent, docClient } = client((cmd) => (cmd.constructor.name === 'ScanCommand' ? { Items: rows } : {}));
    const drafts = draftStore({
      'old/a': { status: 'draft', updatedAt: new Date(T0 - 2 * HOUR).toISOString() },
      // A second, unmarked draft on the reaped deck goes with it.
      'old/a2': { status: 'cancelled', updatedAt: new Date(T0 - 3 * HOUR).toISOString() },
      'fresh/b': { status: 'draft', updatedAt: new Date(T0 - 5 * 60_000).toISOString() },
      'busy/c': { status: 'dispatching', updatedAt: new Date(T0 - 3 * HOUR).toISOString() },
      'touched/d': { status: 'draft', updatedAt: new Date(T0 - 10 * 60_000).toISOString() },
    });
    const out = await sweepProvisionalDecks({ drafts, docClient, now: () => T0, ttlMs: HOUR });
    expect(out).toEqual({ scanned: 4, reaped: ['old'], kept: 3, draftsRemoved: 2, draftsFailed: 0 });
    // The reaped deck's drafts are gone; every kept deck's draft stays.
    expect(drafts.deleted.sort()).toEqual(['old/a', 'old/a2']);
    const del = sent.find((cmd) => cmd.constructor.name === 'DeleteCommand')!.input;
    expect(del.Key).toEqual({ documentId: 'old' });
    expect(del.ConditionExpression).toContain('provisional.draftId = :draftId');
    expect(del.ConditionExpression).toContain('attribute_not_exists(headRevision)');
  });

  it('a deck that got its first revision between scan and delete is kept', async () => {
    const { docClient } = client((cmd) => {
      if (cmd.constructor.name === 'ScanCommand') return { Items: [{ documentId: 'raced', provisional: { draftId: 'a', since: new Date(T0 - 2 * HOUR).toISOString() } }] };
      return conditionFailed();
    });
    const drafts = draftStore({ 'raced/a': { status: 'draft', updatedAt: new Date(T0 - 2 * HOUR).toISOString() } });
    const out = await sweepProvisionalDecks({ drafts, docClient, now: () => T0, ttlMs: HOUR });
    expect(out).toEqual({ scanned: 1, reaped: [], kept: 1, draftsRemoved: 0, draftsFailed: 0 });
    // The document row survived, so its draft must too.
    expect(drafts.deleted).toEqual([]);
  });

  it('a draft delete that throws still reports the deck reaped, logs, and the sweep goes on', async () => {
    const since = new Date(T0 - 2 * HOUR).toISOString();
    const { docClient } = client((cmd) => (cmd.constructor.name === 'ScanCommand'
      ? { Items: [{ documentId: 'x', provisional: { draftId: 'a', since } }, { documentId: 'y', provisional: { draftId: 'b', since } }] }
      : {}));
    const drafts = draftStore({
      'x/a': { status: 'draft', updatedAt: since },
      'y/b': { status: 'draft', updatedAt: since },
    }, { failDelete: (doc) => doc === 'x' });
    const out = await sweepProvisionalDecks({ drafts, docClient, now: () => T0, ttlMs: HOUR });
    expect(out).toEqual({ scanned: 2, reaped: ['x', 'y'], kept: 0, draftsRemoved: 1, draftsFailed: 1 });
    expect(drafts.deleted).toEqual(['y/b']);
    expect(mockWarn).toHaveBeenCalledWith(expect.objectContaining({ documentId: 'x', draftId: 'a' }), expect.stringContaining('not deleted'));
  });
});
