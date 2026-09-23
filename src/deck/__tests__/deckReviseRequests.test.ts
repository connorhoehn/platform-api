// Two replicas share one claim store (Redis in production): the second POST
// of a requestId waits for the first's run id instead of starting its own.
jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
import { claimDeckReviseRequest, setClaimStore } from '../deckReviseRequests';

describe('claimDeckReviseRequest', () => {
  beforeEach(() => setClaimStore(null));
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
});
