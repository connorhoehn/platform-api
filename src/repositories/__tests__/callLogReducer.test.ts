import { applyCallEvent, matchesFilter, participantsOf, type CallEvent, type CallLogRow } from '../CallLogRepository';

const T0 = '2026-09-09T10:00:00.000Z', T1 = '2026-09-09T10:00:05.000Z', T2 = '2026-09-09T10:03:05.000Z';
const base = (over: Partial<CallEvent>): CallEvent => ({ callId: 'c1', action: 'invite', callerId: 'ana', callerName: 'Ana', targetUserIds: ['bob'], lobbyName: 'dm:ana:bob', at: T0, ...over });
const run = (events: CallEvent[], userId: string) => events.reduce<CallLogRow | null>((row, e) => applyCallEvent(row, e, userId, '2026-09-09T12:00:00.000Z'), null)!;

describe('the call log reducer', () => {
  it('reads as outgoing to the caller and incoming to the rung', () => {
    expect(run([base({})], 'ana').direction).toBe('outgoing');
    expect(run([base({})], 'bob').direction).toBe('incoming');
    expect(participantsOf(base({ targetUserIds: ['bob', 'cy', 'ana'] }))).toEqual(['ana', 'bob', 'cy']);
  });

  it('answered → ended yields a duration; the outcome is answered', () => {
    const row = run([base({}), base({ action: 'accepted', at: T1 }), base({ action: 'ended', at: T2 })], 'bob');
    expect(row).toMatchObject({ outcome: 'answered', answeredAt: T1, endedAt: T2, durationMs: 180_000 });
  });

  it('a ring that ends unanswered is missed; a caller who gives up is cancelled', () => {
    expect(run([base({}), base({ action: 'ended', at: T2 })], 'bob').outcome).toBe('missed');
    expect(run([base({}), base({ action: 'cancelled', at: T1 })], 'bob').outcome).toBe('cancelled');
    expect(run([base({}), base({ action: 'missed', at: T1 })], 'bob').outcome).toBe('missed');
  });

  it('outcomes only move forward, whatever order the reports arrive in', () => {
    // Two tabs: one reports missed after the other already reported accepted.
    const row = run([base({}), base({ action: 'accepted', at: T1 }), base({ action: 'missed', at: T2 })], 'bob');
    expect(row.outcome).toBe('answered');
    // Reporting the same accept twice changes nothing.
    const twice = run([base({}), base({ action: 'accepted', at: T1 }), base({ action: 'accepted', at: T2 })], 'bob');
    expect(twice.answeredAt).toBe(T1);
  });

  it('a late invite report never moves the start forward, and fills the caller name once', () => {
    const row = run([base({ action: 'accepted', at: T1, callerName: undefined }), base({ at: T0 })], 'bob');
    expect(row.startedAt).toBe(T0);
    expect(row.callerName).toBe('Ana');
  });

  it('filters: missed means an incoming ring nobody took', () => {
    const missedIn = run([base({}), base({ action: 'missed', at: T1 })], 'bob');
    const missedOut = run([base({}), base({ action: 'missed', at: T1 })], 'ana');
    expect(matchesFilter(missedIn, 'missed')).toBe(true);
    expect(matchesFilter(missedOut, 'missed')).toBe(false);
    expect(matchesFilter(missedOut, 'outgoing')).toBe(true);
  });
});

describe('a reused callId', () => {
  it('starts a new attempt when an invite arrives after the previous one ended', () => {
    const T3 = '2026-09-09T10:05:00.000Z', T4 = '2026-09-09T10:06:00.000Z';
    const row = run([
      base({}), base({ action: 'ended', at: T1 }), base({ action: 'cancelled', at: T1 }),
      base({ at: T3 }), base({ action: 'missed', at: T4 }),
    ], 'bob');
    expect(row).toMatchObject({ outcome: 'missed', startedAt: T3 });
    expect(row.endedAt).toBeUndefined();
  });
});
