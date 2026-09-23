import { ACCESS_CHANGED_SIGNAL_TOPIC, publishAccessChangedSignal, withAccessChangedSignal } from '../work-graph-access-signal';

const publishMock = jest.fn().mockResolvedValue(1);
jest.mock('../../lib/redis-client', () => ({
  getRedisClient: jest.fn(async () => ({ publish: publishMock })),
}));
jest.mock('../../lib/logger', () => ({ __esModule: true, default: { warn: jest.fn(), info: jest.fn(), error: jest.fn() } }));

const grant = {
  id: 'grant-1', organizationId: 'org-1', ownerId: 'owner-1', revision: 2, state: 'paused', expiresAt: '2026-09-23T19:32:20.457Z',
};

describe('work-graph access-changed signal (NFR #21)', () => {
  beforeEach(() => publishMock.mockClear());

  it('publishes the RedisPubSubManager wire shape on the access-changed topic', async () => {
    await publishAccessChangedSignal({ organizationId: 'org-1', personId: 'owner-1', reason: 'grant', grantId: 'grant-1', expiresAt: grant.expiresAt });
    expect(publishMock).toHaveBeenCalledTimes(1);
    const [channel, raw] = publishMock.mock.calls[0];
    expect(channel).toBe(ACCESS_CHANGED_SIGNAL_TOPIC);
    const wire = JSON.parse(raw);
    expect(wire.topic).toBe(ACCESS_CHANGED_SIGNAL_TOPIC);
    expect(wire.payload).toEqual({ organizationId: 'org-1', personId: 'owner-1', reason: 'grant', grantId: 'grant-1', expiresAt: grant.expiresAt });
    expect(wire.metadata.messageId).toMatch(/^pub-platform-api-/);
    expect(wire.metadata.topic).toBe(ACCESS_CHANGED_SIGNAL_TOPIC);
  });

  it('a committed grant write publishes once, naming the owner; the store is otherwise untouched', async () => {
    const publish = jest.fn().mockResolvedValue(undefined);
    const store = {
      calls: 0,
      async putGrant(next: typeof grant, expectedRevision: number | null) { this.calls += 1; return { ...next, expectedRevision }; },
      async getGrant() { return grant; },
    };
    const wrapped = withAccessChangedSignal(store, publish);
    const stored = await wrapped.putGrant(grant, 1);
    await Promise.resolve();
    expect(stored).toMatchObject({ id: 'grant-1', expectedRevision: 1 });
    expect(store.calls).toBe(1);
    expect(publish).toHaveBeenCalledTimes(1);
    expect(publish).toHaveBeenCalledWith({ organizationId: 'org-1', personId: 'owner-1', reason: 'grant', grantId: 'grant-1', expiresAt: grant.expiresAt });
    expect(await wrapped.getGrant()).toBe(grant);
  });

  it('a pair write publishes for the current grant; a failed publish never fails the write', async () => {
    const publish = jest.fn().mockRejectedValue(new Error('redis down'));
    const store = {
      async putGrant(next: typeof grant) { return next; },
      async putGrantPair(current: typeof grant, history: typeof grant) { return { current, history }; },
    };
    const wrapped = withAccessChangedSignal(store, publish);
    await expect(wrapped.putGrantPair!(grant, { ...grant, id: 'grant-h' })).resolves.toMatchObject({ current: { id: 'grant-1' } });
    await expect(wrapped.putGrant(grant, null)).resolves.toBe(grant);
    await Promise.resolve();
    expect(publish).toHaveBeenCalledTimes(2);
    expect(publish.mock.calls[0][0].grantId).toBe('grant-1');
  });

  it('two platform replicas: only the one that commits publishes', async () => {
    const backing = { async putGrant(next: typeof grant) { return next; } };
    const publishA = jest.fn().mockResolvedValue(undefined);
    const publishB = jest.fn().mockResolvedValue(undefined);
    const replicaA = withAccessChangedSignal(backing, publishA);
    withAccessChangedSignal(backing, publishB);
    await replicaA.putGrant(grant, 1);
    await Promise.resolve();
    expect(publishA).toHaveBeenCalledTimes(1);
    expect(publishB).not.toHaveBeenCalled();
  });
});
