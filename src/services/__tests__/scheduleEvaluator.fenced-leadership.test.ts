// ROADMAP P2.1 contracts (lane P1): two schedule evaluators against one fake
// Redis, leadership on distributed-core's FencedLease plus the per-fire claim
// in scheduler-fenced-leadership.ts.
//
//   1. Exactly one fire per pipeline per minute with two replicas whose
//      timers are offset (the bug the per-tick DDB lock had).
//   2. When the leader's lease fences (connection lost), the follower takes
//      over once Redis expires the key, and keeps firing once per minute.
//   3. No double fire across a failover inside a minute — crash or graceful
//      hand-off — because the fire marker outlives the leader.
//   4. A stale leader whose local clock still says "live" is refused at the
//      side effect (the claim script checks the lease key) and fences.
//   5. Redis unreachable: nobody fires (fail closed); leadership resumes.

import { FENCED_LEASE_SCRIPTS, type LeaseRedis } from 'distributed-core/cluster/leases';
import { createScheduleEvaluator, type PipelineForSchedule } from '../scheduleEvaluator';
import {
  createFencedSchedulerLeadership,
  SCHEDULE_CLAIM_SCRIPT,
  type SchedulerLeadership,
} from '../scheduler-fenced-leadership';

jest.mock('../../lib/logger', () => ({
  __esModule: true,
  default: { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() },
}));

// ─── Fake Redis: one keyspace, one connection object per replica ──────────

interface Entry { value: string; expiresAt: number | null }

class FakeRedisServer {
  readonly data = new Map<string, Entry>();
  get(key: string): string | null {
    const e = this.data.get(key);
    if (!e) return null;
    if (e.expiresAt !== null && e.expiresAt <= Date.now()) { this.data.delete(key); return null; }
    return e.value;
  }
  setNx(key: string, value: string, px: number): boolean {
    if (this.get(key) !== null) return false;
    this.data.set(key, { value, expiresAt: Date.now() + px });
    return true;
  }
  execute(args: string[]): unknown {
    const [cmd, ...rest] = args;
    if (cmd === 'GET') return this.get(rest[0]);
    if (cmd === 'DEL') return this.data.delete(rest[0]) ? 1 : 0;
    if (cmd !== 'EVAL') throw new Error(`fake: unsupported ${cmd}`);
    const [script, n, ...tail] = rest;
    const keys = tail.slice(0, Number(n));
    const argv = tail.slice(Number(n));
    switch (script) {
      case FENCED_LEASE_SCRIPTS.ACQUIRE: {
        if (!this.setNx(keys[0], argv[0], Number(argv[1]))) return 0;
        const next = Number(this.get(keys[1]) ?? '0') + 1;
        this.data.set(keys[1], { value: String(next), expiresAt: null });
        return next;
      }
      case FENCED_LEASE_SCRIPTS.RENEW: {
        if (this.get(keys[0]) !== argv[0]) return 0;
        this.data.get(keys[0])!.expiresAt = Date.now() + Number(argv[1]);
        return 1;
      }
      case FENCED_LEASE_SCRIPTS.RELEASE:
        if (this.get(keys[0]) !== argv[0]) return 0;
        this.data.delete(keys[0]);
        return 1;
      case SCHEDULE_CLAIM_SCRIPT:
        if (this.get(keys[0]) !== argv[0]) return -1;
        return this.setNx(keys[1], argv[1], Number(argv[2])) ? 1 : 0;
      default:
        throw new Error('fake: unknown script');
    }
  }
}

class FakeConnection implements LeaseRedis {
  /** Commands reject without executing (connection down). */
  down = false;
  /** Commands execute but their replies are held until `flush()` (stalled reply). */
  stalled = false;
  private held: Array<() => void> = [];
  private readonly observers = new Set<() => void>();
  constructor(private readonly server: FakeRedisServer) {}
  async command(...args: string[]): Promise<unknown> {
    if (this.down) throw new Error('connection down');
    const reply = this.server.execute(args.map(String));
    if (this.stalled) await new Promise<void>((resolve) => this.held.push(resolve));
    return reply;
  }
  onUnavailable(handler: () => void): () => void {
    this.observers.add(handler);
    return () => { this.observers.delete(handler); };
  }
  lose(): void {
    this.down = true;
    for (const h of [...this.observers]) h();
  }
  flush(): void { const h = this.held; this.held = []; h.forEach((r) => r()); }
}

// ─── Harness ───────────────────────────────────────────────────────────────

const LEASE_MS = 3_000;
const T0 = Date.UTC(2026, 9, 14, 12, 0, 0);
const pipelines: PipelineForSchedule[] = [
  { id: 'p-every-minute', status: 'published', triggerBinding: { event: 'schedule', schedule: '* * * * *' } },
];

interface Replica {
  name: string;
  conn: FakeConnection;
  leadership: SchedulerLeadership;
  evaluator: ReturnType<typeof createScheduleEvaluator>;
}

function makeReplica(
  name: string,
  server: FakeRedisServer,
  fires: Array<{ replica: string; minute: number }>,
  now: () => number = () => Date.now(),
): Replica {
  const conn = new FakeConnection(server);
  const leadership = createFencedSchedulerLeadership({
    redis: async () => (conn.down ? null : conn),
    leaseMs: LEASE_MS,
    ownerPrefix: name,
    now,
  });
  const evaluator = createScheduleEvaluator({
    listPipelines: () => pipelines,
    trigger: async (_id, payload) => {
      fires.push({ replica: name, minute: Math.floor(Date.parse(String(payload.firedAt)) / 60_000) });
    },
    leadership,
  });
  return { name, conn, leadership, evaluator };
}

const tick = (r: Replica) => r.evaluator.runProtectedTick(new Date(Date.now()));
const minuteOf = (ms: number) => Math.floor(ms / 60_000);

/** Advance to `atMs` past T0, letting lease renewals run on the way. */
async function at(atMs: number): Promise<void> {
  const delta = T0 + atMs - Date.now();
  if (delta > 0) await jest.advanceTimersByTimeAsync(delta);
}

function firesPerMinute(fires: Array<{ minute: number }>): Map<number, number> {
  const out = new Map<number, number>();
  for (const f of fires) out.set(f.minute, (out.get(f.minute) ?? 0) + 1);
  return out;
}

beforeEach(() => {
  jest.useFakeTimers({ now: T0 });
});
afterEach(() => {
  jest.useRealTimers();
});

describe('schedule evaluator on a FencedLease — single flight across replicas', () => {
  it('fires each pipeline exactly once per minute with two replicas whose ticks are offset', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);
    const b = makeReplica('b', server, fires);

    for (let m = 0; m < 10; m++) {
      await at(m * 60_000 + 1_000);
      const ranA = await tick(a);
      await at(m * 60_000 + 6_000);
      const ranB = await tick(b);
      expect([ranA, ranB].filter(Boolean)).toHaveLength(1);
    }

    const perMinute = firesPerMinute(fires);
    expect(perMinute.size).toBe(10);
    expect([...perMinute.values()].every((n) => n === 1)).toBe(true);
    expect(fires.every((f) => f.replica === 'a')).toBe(true);
    await a.evaluator.shutdown();
    await b.evaluator.shutdown();
  });

  it('hands over when the leader fences, without a duplicate in any minute', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);
    const b = makeReplica('b', server, fires);

    for (let m = 0; m < 8; m++) {
      if (m === 3) {
        a.conn.lose(); // A fences at once; its key expires within LEASE_MS
        expect(a.leadership.isLeader()).toBe(false);
      }
      await at(m * 60_000 + 1_000);
      await tick(a);
      await at(m * 60_000 + 6_000);
      await tick(b);
    }

    const perMinute = firesPerMinute(fires);
    expect([...perMinute.values()].every((n) => n === 1)).toBe(true);
    expect(perMinute.size).toBe(8); // B leads from minute 3 on, no gap at this tick spacing
    expect(fires.filter((f) => f.minute < minuteOf(T0) + 3).every((f) => f.replica === 'a')).toBe(true);
    expect(fires.filter((f) => f.minute >= minuteOf(T0) + 3).every((f) => f.replica === 'b')).toBe(true);
    expect(b.leadership.isLeader()).toBe(true);
    await a.evaluator.shutdown();
    await b.evaluator.shutdown();
  });

  it('does not fire a minute twice when the leader crashes after firing it', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);
    const b = makeReplica('b', server, fires);

    await at(1_000);
    expect(await tick(a)).toBe(true); // A fires minute 0
    a.conn.down = true;               // crash: no renewals, no release
    a.evaluator.stop();
    await at(1_000 + LEASE_MS + 500); // key has expired in Redis
    expect(await tick(b)).toBe(true); // B leads inside minute 0 ...
    await at(60_000 + 1_000);
    expect(await tick(b)).toBe(true); // ... and fires minute 1

    expect(fires).toEqual([
      { replica: 'a', minute: minuteOf(T0) },
      { replica: 'b', minute: minuteOf(T0) + 1 },
    ]);
    await b.evaluator.shutdown();
  });

  it('does not fire a minute twice across a graceful hand-off', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);
    const b = makeReplica('b', server, fires);

    await at(1_000);
    await tick(a);
    await a.evaluator.shutdown(); // releases the key at once
    await at(2_000);
    expect(await tick(b)).toBe(true); // B leads immediately, same minute
    await at(60_000 + 2_000);
    await tick(b);

    expect(fires.map((f) => `${f.replica}:${f.minute - minuteOf(T0)}`)).toEqual(['a:0', 'b:1']);
    await b.evaluator.shutdown();
  });

  it('refuses the fire claim when the lease key names another owner (fencing at the side effect)', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);

    await at(1_000);
    expect(await tick(a)).toBe(true); // A leads, fires minute 0
    await at(60_000 + 1_000 - 600);   // A renewed at +60.0 s; next renewal at +61.0 s
    // Within one renewal period the key is overwritten by another owner. A's
    // local view is still live, so only the claim script can stop it.
    server.data.set('platform-api:schedule-evaluator:leader', { value: 'someone-else', expiresAt: Date.now() + LEASE_MS });
    expect(a.leadership.isLeader()).toBe(true);
    await at(60_000 + 1_000 - 300);   // still before A's next renewal
    expect(a.leadership.isLeader()).toBe(true);
    expect(await tick(a)).toBe(true); // A evaluates minute 1 ...

    // ... but the claim is refused, so minute 1 does not fire from A.
    expect(fires.map((f) => `${f.replica}:${f.minute - minuteOf(T0)}`)).toEqual(['a:0']);
    expect(a.leadership.isLeader()).toBe(false); // the refused claim fenced it
    await a.evaluator.shutdown();
  });

  it('fails closed while Redis is unreachable and resumes leadership when it returns', async () => {
    const server = new FakeRedisServer();
    const fires: Array<{ replica: string; minute: number }> = [];
    const a = makeReplica('a', server, fires);
    const b = makeReplica('b', server, fires);
    a.conn.down = true;
    b.conn.down = true;

    await at(1_000);
    expect(await tick(a)).toBe(false);
    expect(await tick(b)).toBe(false);
    expect(fires).toHaveLength(0);

    a.conn.down = false;
    b.conn.down = false;
    await at(60_000 + 1_000);
    await tick(b);
    await tick(a);
    expect(fires.map((f) => `${f.replica}:${f.minute - minuteOf(T0)}`)).toEqual(['b:1']);
    await a.evaluator.shutdown();
    await b.evaluator.shutdown();
  });
});
