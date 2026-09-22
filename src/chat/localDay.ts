// "That person's loops on that day" needs a day in THEIR timezone, not the
// server's. A run started at 23:40 in Chicago belongs to the Chicago day the
// person was working, and a UTC-shaped window would file it under tomorrow.
//
// Node has no zoned-time arithmetic, but `Intl.DateTimeFormat` with a
// `timeZone` will tell us what the wall clock reads there for a given
// instant. Inverting that — instant FROM wall clock — is the two-pass trick
// below: guess, measure the offset at the guess, correct, then measure again
// because the correction itself can cross a DST boundary.

/** Milliseconds a zone is ahead of UTC at a given instant. */
export function zoneOffsetMs(at: Date, timeZone: string): number {
  const dtf = new Intl.DateTimeFormat('en-US', {
    timeZone, hour12: false,
    year: 'numeric', month: '2-digit', day: '2-digit',
    hour: '2-digit', minute: '2-digit', second: '2-digit',
  });
  const parts: Record<string, number> = {};
  for (const part of dtf.formatToParts(at)) {
    if (part.type !== 'literal') parts[part.type] = Number(part.value);
  }
  // `hour: '2-digit'` with hour12:false renders midnight as 24 in some ICU builds.
  const hour = parts.hour === 24 ? 0 : parts.hour;
  const asUtc = Date.UTC(parts.year, parts.month - 1, parts.day, hour, parts.minute, parts.second);
  return asUtc - at.getTime();
}

/** True for an IANA zone this runtime actually knows. */
export function isKnownTimeZone(timeZone: string): boolean {
  if (!timeZone) return false;
  try {
    new Intl.DateTimeFormat('en-US', { timeZone });
    return true;
  } catch {
    return false;
  }
}

export interface LocalDayBounds {
  /** Inclusive ISO instant of 00:00:00.000 local. */
  startIso: string;
  /** EXCLUSIVE ISO instant of the next local midnight. */
  endIso: string;
  day: string;
  timeZone: string;
}

/** `YYYY-MM-DD`, as the person's own calendar would write today. */
export function todayIn(timeZone: string, now: Date = new Date()): string {
  const dtf = new Intl.DateTimeFormat('en-CA', { timeZone, year: 'numeric', month: '2-digit', day: '2-digit' });
  return dtf.format(now);
}

/**
 * The half-open UTC window `[local 00:00, next local 00:00)` for one day.
 *
 * Half-open on purpose: a closed window either drops a run that started
 * exactly at midnight or counts it on both days, and "both" is worse.
 *
 * An unknown zone falls back to UTC rather than throwing — a bad `timezone`
 * query parameter should narrow the answer, not fail the request.
 */
export function localDayBounds(day: string, timeZone: string, now: Date = new Date()): LocalDayBounds {
  const zone = isKnownTimeZone(timeZone) ? timeZone : 'UTC';
  const valid = /^\d{4}-\d{2}-\d{2}$/.test(day) ? day : todayIn(zone, now);
  const [y, m, d] = valid.split('-').map(Number);

  const instantAtLocalMidnight = (year: number, month: number, dayOfMonth: number): Date => {
    const naive = Date.UTC(year, month - 1, dayOfMonth, 0, 0, 0, 0);
    // Pass one: offset at the naive guess. Pass two: offset at the corrected
    // instant, which is what matters when the correction crossed a DST edge.
    let guess = naive - zoneOffsetMs(new Date(naive), zone);
    guess = naive - zoneOffsetMs(new Date(guess), zone);
    return new Date(guess);
  };

  const start = instantAtLocalMidnight(y, m, d);
  const nextDay = new Date(Date.UTC(y, m - 1, d + 1));
  const end = instantAtLocalMidnight(
    nextDay.getUTCFullYear(), nextDay.getUTCMonth() + 1, nextDay.getUTCDate(),
  );

  return { startIso: start.toISOString(), endIso: end.toISOString(), day: valid, timeZone: zone };
}
