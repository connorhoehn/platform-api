// A person's day, in their zone. The bug this prevents is a run started at
// 23:40 local being filed under tomorrow because the window was UTC-shaped.
import { isKnownTimeZone, localDayBounds, todayIn, zoneOffsetMs } from '../localDay';

describe('localDayBounds', () => {
  it('spans local midnight to local midnight, as UTC instants', () => {
    const b = localDayBounds('2026-09-21', 'America/Chicago');
    expect([b.startIso, b.endIso]).toEqual(['2026-09-21T05:00:00.000Z', '2026-09-22T05:00:00.000Z']);
    expect(b).toMatchObject({ day: '2026-09-21', timeZone: 'America/Chicago' });
  });

  it('is half-open, so a run at exactly midnight lands on one day only', () => {
    const day = localDayBounds('2026-09-21', 'UTC');
    expect(day.startIso).toBe('2026-09-21T00:00:00.000Z');
    expect(day.endIso).toBe('2026-09-22T00:00:00.000Z');
    const next = localDayBounds('2026-09-22', 'UTC');
    expect(next.startIso).toBe(day.endIso);
  });

  it('handles a zone ahead of UTC', () => {
    const b = localDayBounds('2026-09-21', 'Asia/Tokyo'); // UTC+9, no DST
    expect([b.startIso, b.endIso]).toEqual(['2026-09-20T15:00:00.000Z', '2026-09-21T15:00:00.000Z']);
  });

  // The two-pass offset correction earns its keep here: the naive guess lands
  // on the wrong side of the transition.
  it('gets a 23-hour spring-forward day right', () => {
    const b = localDayBounds('2026-03-08', 'America/Chicago'); // CST → CDT at 02:00
    expect([b.startIso, b.endIso]).toEqual(['2026-03-08T06:00:00.000Z', '2026-03-09T05:00:00.000Z']);
    expect(Date.parse(b.endIso) - Date.parse(b.startIso)).toBe(23 * 3_600_000);
  });

  it('gets a 25-hour fall-back day right', () => {
    const b = localDayBounds('2026-11-01', 'America/Chicago'); // CDT → CST at 02:00
    expect(Date.parse(b.endIso) - Date.parse(b.startIso)).toBe(25 * 3_600_000);
  });

  it('falls back to UTC for a zone this runtime does not know, rather than failing the request', () => {
    expect(localDayBounds('2026-09-21', 'Mars/Olympus').timeZone).toBe('UTC');
  });

  it('falls back to today when the day is not a date', () => {
    const now = new Date('2026-09-21T12:00:00.000Z');
    expect(localDayBounds('not-a-day', 'UTC', now).day).toBe('2026-09-21');
  });
});

describe('todayIn', () => {
  it('reads the calendar where the person is, not where the server is', () => {
    const lateUtc = new Date('2026-09-22T02:00:00.000Z'); // 21:00 on the 21st in Chicago
    expect(todayIn('America/Chicago', lateUtc)).toBe('2026-09-21');
    expect(todayIn('UTC', lateUtc)).toBe('2026-09-22');
  });
});

describe('zoneOffsetMs', () => {
  it('is signed the way a reader expects: ahead of UTC is positive', () => {
    const at = new Date('2026-09-21T12:00:00.000Z');
    expect(zoneOffsetMs(at, 'Asia/Tokyo')).toBe(9 * 3_600_000);
    expect(zoneOffsetMs(at, 'America/Chicago')).toBe(-5 * 3_600_000);
    expect(zoneOffsetMs(at, 'UTC')).toBe(0);
  });
});

describe('isKnownTimeZone', () => {
  it('knows a real zone from a made-up one', () => {
    expect([isKnownTimeZone('America/Chicago'), isKnownTimeZone('Mars/Olympus'), isKnownTimeZone('')])
      .toEqual([true, false, false]);
  });
});
