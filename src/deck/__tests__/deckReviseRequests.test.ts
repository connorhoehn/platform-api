// Two replicas share one claim store (DynamoDB in production): the second POST
// of a requestId waits for the first's run id instead of starting its own.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn() }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));
import { claimDeckReviseRequest, DynamoClaimStore, MemoryClaimStore, setClaimStore } from '../deckReviseRequests';

describe('claimDeckReviseRequest', () => {
  beforeEach(() => setClaimStore(new MemoryClaimStore()));
  afterAll(() => setClaimStore(null));
  const args = { userId: 'frank', documentId: 'audit-deck-1', requestId: 'r-1' };

  it('a concurrent duplicate waits for the first claim\'s run id', async () => {
    const first = await claimDeckReviseRequest(args);
    expect(first.status).toBe('claimed');
    const second = claimDeckReviseRequest(args, { waitMs: 2000, pollMs: 10 });
    setTimeout(() => { if (first.status === 'claimed') void first.commit('run-7'); }, 30);
    await expect(second).resolves.toEqual({ status: 'existing', runId: 'run-7' });
  });

  it('answers in-flight when the first never gets a run id in time', async () => {
    await claimDeckReviseRequest({ ...args, requestId: 'r-2' });
    await expect(claimDeckReviseRequest({ ...args, requestId: 'r-2' }, { waitMs: 30, pollMs: 10 })).resolves.toEqual({ status: 'in-flight' });
  });

  it('is per person and document', async () => {
    await claimDeckReviseRequest({ ...args, requestId: 'r-3' });
    expect((await claimDeckReviseRequest({ ...args, requestId: 'r-3', userId: 'connor' })).status).toBe('claimed');
  });

  it('a released claim lets the retry start; a committed one is not released', async () => {
    const first = await claimDeckReviseRequest({ ...args, requestId: 'r-4' });
    if (first.status !== 'claimed') throw new Error('expected claim');
    await first.release();
    const retry = await claimDeckReviseRequest({ ...args, requestId: 'r-4' });
    expect(retry.status).toBe('claimed');
    if (retry.status !== 'claimed') return;
    await retry.commit('run-9');
    await retry.release();
    await expect(claimDeckReviseRequest({ ...args, requestId: 'r-4' })).resolves.toEqual({ status: 'existing', runId: 'run-9' });
  });
});

describe('DynamoClaimStore', () => {
  const conditional = () => Object.assign(new Error('conditional'), { name: 'ConditionalCheckFailedException' });
  const make = (send: jest.Mock) => new DynamoClaimStore({ send } as never, async () => undefined);
  const input = (send: jest.Mock, call: number) => (send.mock.calls[call][0] as { input: Record<string, unknown> }).input;

  it('creates with a conditional put that also takes over an expired row', async () => {
    const send = jest.fn(async () => ({}));
    await expect(make(send).create('k', 'pending', 60)).resolves.toBe(true);
    const put = input(send, 0);
    expect(put.TableName).toBe('test-deck-revise-claims');
    expect(put.ConditionExpression).toBe('attribute_not_exists(claimKey) OR expiresAt < :now');
    expect((put.Item as { expiresAt: number }).expiresAt).toBeGreaterThan(Math.floor(Date.now() / 1000));
  });

  it('a conditional failure means someone else holds the claim', async () => {
    const send = jest.fn(async () => { throw conditional(); });
    await expect(make(send).create('k', 'pending', 60)).resolves.toBe(false);
  });

  it('any other error propagates (fail closed)', async () => {
    const send = jest.fn(async () => { throw Object.assign(new Error('down'), { name: 'ResourceNotFoundException' }); });
    await expect(make(send).create('k', 'pending', 60)).rejects.toThrow('down');
  });

  it('reads consistently and treats an expired row as absent', async () => {
    const past = Math.floor(Date.now() / 1000) - 5;
    const send = jest.fn()
      .mockResolvedValueOnce({ Item: { claimKey: 'k', value: 'run-1', expiresAt: past + 100 } })
      .mockResolvedValueOnce({ Item: { claimKey: 'k', value: 'run-1', expiresAt: past } });
    const store = make(send);
    await expect(store.get('k')).resolves.toBe('run-1');
    expect(input(send, 0).ConsistentRead).toBe(true);
    await expect(store.get('k')).resolves.toBeNull();
  });

  it('commit and release are conditioned on pending and swallow only conditional failures', async () => {
    const send = jest.fn(async () => { throw conditional(); });
    const store = make(send);
    await expect(store.commit('k', 'run-2', 60)).resolves.toBeUndefined();
    await expect(store.release('k')).resolves.toBeUndefined();
    expect(input(send, 0).ConditionExpression).toBe('#v = :pending');
    expect(input(send, 1).ConditionExpression).toBe('#v = :pending');
  });
});
