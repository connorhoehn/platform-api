// Opt-in: the claim script and lease against a real Redis (lane P1).
//   SCHEDULER_LEASE_REDIS_URL=redis://127.0.0.1:6379/13 npx jest scheduler-fenced-leadership.redis
// Uses a unique key prefix and deletes every key it made. Never point it at db 0.

import { randomUUID } from 'node:crypto';
import { createScheduleEvaluator } from '../scheduleEvaluator';
import { createFencedSchedulerLeadership, createLeaseRedisGetter } from '../scheduler-fenced-leadership';

jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

const url = process.env.SCHEDULER_LEASE_REDIS_URL;
const run = url && !/\/0$/.test(url) && /\/\d+$/.test(url) ? describe : describe.skip;

run('scheduler fenced leadership on a real Redis', () => {
  const key = `test:p1:schedule-leader:${randomUUID()}`;
  const getterA = createLeaseRedisGetter(url!, { retryMs: 0 });
  const getterB = createLeaseRedisGetter(url!, { retryMs: 0 });

  afterAll(async () => {
    const redis = await getterA.get();
    if (redis) {
      for (const k of [key, `${key}:fence`, `${key}:fired:p-1:1`, `${key}:fired:p-1:2`]) {
        await redis.command('DEL', k);
      }
    }
    await getterA.close();
    await getterB.close();
  });

  it('one leader, one fire per minute, refused claims after the key moves', async () => {
    const a = createFencedSchedulerLeadership({ redis: getterA.get, key, leaseMs: 2_000 });
    const b = createFencedSchedulerLeadership({ redis: getterB.get, key, leaseMs: 2_000 });

    expect(await a.ensureLeader()).toBe(true);
    expect(await b.ensureLeader()).toBe(false);
    expect(await a.claimFire('p-1', 1)).toBe(true);
    expect(await a.claimFire('p-1', 1)).toBe(false); // already fired

    await a.stop();                                  // graceful hand-back
    expect(await b.ensureLeader()).toBe(true);
    expect(await b.claimFire('p-1', 1)).toBe(false); // the marker outlived A
    expect(await b.claimFire('p-1', 2)).toBe(true);
    await b.stop();
  });

  it('the evaluator fires once across two replicas', async () => {
    const fires: string[] = [];
    const mk = (name: string, get: () => Promise<never>) => createScheduleEvaluator({
      listPipelines: () => [{ id: `p-${key}`, status: 'published', triggerBinding: { event: 'schedule', schedule: '* * * * *' } }],
      trigger: async () => { fires.push(name); },
      leadership: createFencedSchedulerLeadership({ redis: get, key: `${key}:e`, leaseMs: 2_000 }),
    });
    const a = mk('a', getterA.get as never);
    const b = mk('b', getterB.get as never);
    const now = new Date();
    await Promise.all([a.runProtectedTick(now), b.runProtectedTick(now)]);
    await Promise.all([a.runProtectedTick(now), b.runProtectedTick(now)]);
    expect(fires).toHaveLength(1);
    await a.shutdown();
    await b.shutdown();
    const redis = await getterA.get();
    const minute = Math.floor(now.getTime() / 60_000);
    for (const k of [`${key}:e`, `${key}:e:fence`, `${key}:e:fired:p-${key}:${minute}`]) await redis!.command('DEL', k);
  });
});
