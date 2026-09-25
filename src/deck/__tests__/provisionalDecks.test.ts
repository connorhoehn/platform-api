jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { markProvisionalDeck, sweepProvisionalDecks } from '../provisionalDecks';

type Cmd = { constructor: { name: string }; input: Record<string, any> };
function client(handler: (cmd: Cmd) => unknown) {
  const sent: Cmd[] = [];
  return { sent, docClient: { send: jest.fn(async (cmd: Cmd) => { sent.push(cmd); return handler(cmd); }) } as never };
}
const conditionFailed = () => { throw Object.assign(new Error('failed'), { name: 'ConditionalCheckFailedException' }); };
const HOUR = 60 * 60 * 1000;
const T0 = Date.parse('2026-09-25T02:00:00.000Z');

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
    const drafts = { get: async (_doc: string, id: string) => ({
      a: { status: 'draft', updatedAt: new Date(T0 - 2 * HOUR).toISOString() },
      b: { status: 'draft', updatedAt: new Date(T0 - 5 * 60_000).toISOString() },
      c: { status: 'dispatching', updatedAt: new Date(T0 - 3 * HOUR).toISOString() },
      d: { status: 'draft', updatedAt: new Date(T0 - 10 * 60_000).toISOString() },
    } as Record<string, { status: string; updatedAt: string }>)[id] ?? null };
    const out = await sweepProvisionalDecks({ drafts, docClient, now: () => T0, ttlMs: HOUR });
    expect(out).toEqual({ scanned: 4, reaped: ['old'], kept: 3 });
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
    const out = await sweepProvisionalDecks({ drafts: { get: async () => null }, docClient, now: () => T0, ttlMs: HOUR });
    expect(out).toEqual({ scanned: 1, reaped: [], kept: 1 });
  });
});
