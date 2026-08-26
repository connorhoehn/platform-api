// platform-api/src/lib/__tests__/recording-truth.test.ts
//
// The three lies a recording row used to tell, one describe block each.
// Fixtures are trimmed from REAL rows scanned out of
// `video-call-recordings` on 2026-08-25 and cross-checked against ffprobe
// on the composed.mp4 files on disk — not invented shapes.

import {
  deriveMediaDurationMs,
  reconcileParticipants,
  publicizeTracks,
  ROSTER_JOIN_GRACE_MS,
} from '../recording-truth';

// ---------------------------------------------------------------------------
// 1. Duration
// ---------------------------------------------------------------------------

describe('deriveMediaDurationMs', () => {
  test('the worst measured row: 23.3s claimed for 0.5s of media', () => {
    // recordingId 2026-08-25T17-57-18-422Z-9440ef59. Session lifetime
    // 23,278ms; ffprobe on composed.mp4 said 500ms. +4556%.
    const durationMs = deriveMediaDurationMs({
      sessionDurationMs: 23_278,
      tracks: [{ legId: 'leg-001', startedAtMs: 4, endedAtMs: 512, durationMs: 508 }],
    });
    expect(durationMs).toBe(508);
  });

  test('the self-contradicting row: card said 18s, player said 0:08', () => {
    // recordingId 2026-08-25T00-52-56-468Z-c607562f — the screen where the
    // metadata card and the timeline above it disagreed.
    const durationMs = deriveMediaDurationMs({
      sessionDurationMs: 18_736,
      tracks: [
        { legId: 'leg-001', startedAtMs: 3, endedAtMs: 8_671, durationMs: 8_668 },
        { legId: 'leg-002', startedAtMs: 21, endedAtMs: 8_640, durationMs: 8_619 },
      ],
    });
    expect(durationMs).toBe(8_668); // 8s, matching the player
    expect(durationMs).toBeLessThan(18_736);
  });

  test('spans from the earliest leg open to the latest leg close', () => {
    // A late joiner must not shorten the recording, and an early leaver
    // must not shorten it either.
    expect(
      deriveMediaDurationMs({
        sessionDurationMs: 120_000,
        tracks: [
          { startedAtMs: 1_000, endedAtMs: 20_000 },
          { startedAtMs: 15_000, endedAtMs: 60_000 },
        ],
      }),
    ).toBe(59_000);
  });

  test('a probed composed duration, when a caller has one, wins over the timeline', () => {
    expect(
      deriveMediaDurationMs({
        composedDurationMs: 2_533,
        sessionDurationMs: 23_526,
        tracks: [{ startedAtMs: 4, endedAtMs: 22_720 }],
      }),
    ).toBe(2_533);
  });

  test('falls back to max(track.durationMs) when the payload has no offsets', () => {
    expect(
      deriveMediaDurationMs({
        sessionDurationMs: 90_000,
        tracks: [{ durationMs: 4_000 }, { durationMs: 12_000 }],
      }),
    ).toBe(12_000);
  });

  test('no tracks at all → the session lifetime, because some number beats 0s', () => {
    expect(deriveMediaDurationMs({ sessionDurationMs: 60_000, tracks: [] })).toBe(60_000);
    expect(deriveMediaDurationMs({ sessionDurationMs: 60_000 })).toBe(60_000);
  });

  test('never claims more media than the session that produced it', () => {
    // Defensive: a clock-skewed leg must not invent duration.
    expect(
      deriveMediaDurationMs({
        sessionDurationMs: 10_000,
        tracks: [{ startedAtMs: 0, endedAtMs: 999_999 }],
      }),
    ).toBe(10_000);
  });

  test('never negative, and survives junk offsets', () => {
    expect(
      deriveMediaDurationMs({
        sessionDurationMs: 5_000,
        tracks: [{ startedAtMs: NaN, endedAtMs: undefined, durationMs: -3 }],
      }),
    ).toBe(5_000);
    expect(deriveMediaDurationMs({ sessionDurationMs: null, tracks: [] })).toBe(0);
  });
});

// ---------------------------------------------------------------------------
// 2. Participants
// ---------------------------------------------------------------------------

describe('reconcileParticipants', () => {
  test('the "7 👥" row: 4x hank + 3x bob in a 2-person DM collapses to 2', () => {
    // recordingId 2026-08-24T22-07-23-349Z-917f1e14. Every refresh/rejoin
    // appended a roster entry; the row rendered 7 people.
    const startedAt = '2026-08-24T22:07:23.349Z';
    const endedAt = '2026-08-24T22:07:45.000Z';
    const joins = (mins: number) =>
      new Date(Date.parse(startedAt) - mins * 60_000).toISOString();

    const out = reconcileParticipants({
      startedAt,
      endedAt,
      startedBy: 'dev-hank',
      participants: [
        { userId: 'dev-hank', displayName: 'Hank Hill', joinedAt: joins(3) },
        { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: joins(3) },
        { userId: 'dev-hank', joinedAt: joins(2) },
        { userId: 'dev-bob', joinedAt: joins(2) },
        { userId: 'dev-hank', joinedAt: joins(1) },
        { userId: 'dev-bob', joinedAt: joins(1) },
        { userId: 'dev-hank', joinedAt: joins(0) },
      ],
      tracks: [
        { legId: 'leg-001', participantId: 'p-1' },
        { legId: 'leg-002', participantId: 'p-2' },
      ],
    });

    expect(out.participantCount).toBe(2);
    expect(out.participants.map((p) => p.userId).sort()).toEqual(['dev-bob', 'dev-hank']);
    // Display names survive dedup even though the freshest entry lacked one.
    expect(out.participants.find((p) => p.userId === 'dev-hank')?.displayName).toBe('Hank Hill');
  });

  test('the "1 👥 for a two-person call" row: the initiator is counted', () => {
    // recordingId 2026-08-26T02-23-24-593Z-0d14eca7 — hank rang carol, hank
    // had already hung up when the roster was snapshotted.
    const out = reconcileParticipants({
      startedAt: '2026-08-26T02:23:24.679Z',
      endedAt: '2026-08-26T02:23:42.558Z',
      startedBy: 'dev-e2e-hank-9knf9e',
      participants: [
        {
          userId: 'dev-e2e-carol-9knf9e',
          displayName: 'Carol Chen',
          joinedAt: '2026-08-26T02:22:42.704Z',
        },
      ],
      tracks: [{ legId: 'leg-001', participantId: 'p-1' }],
    });

    expect(out.participantCount).toBe(2);
    expect(out.participants.map((p) => p.userId)).toContain('dev-e2e-hank-9knf9e');
  });

  test('drops a roster entry from a previous call in the same lobby', () => {
    const startedAt = '2026-08-23T21:26:40.268Z';
    const out = reconcileParticipants({
      startedAt,
      endedAt: '2026-08-23T21:27:10.000Z',
      startedBy: 'dev-hank',
      participants: [
        { userId: 'dev-hank', joinedAt: '2026-08-23T21:26:30.000Z' },
        { userId: 'dev-bob', joinedAt: '2026-08-23T21:26:31.000Z' },
        // 46 minutes before this recording even started — the channel's
        // lifetime roster, not this call's.
        { userId: 'dev-alice', joinedAt: '2026-08-23T20:40:47.142Z' },
      ],
      tracks: [{ legId: 'leg-001' }, { legId: 'leg-002' }],
    });

    expect(out.participants.map((p) => p.userId).sort()).toEqual(['dev-bob', 'dev-hank']);
    expect(out.participantCount).toBe(2);
  });

  test('drops someone who joined after the recording ended', () => {
    const out = reconcileParticipants({
      startedAt: '2026-08-25T10:00:00.000Z',
      endedAt: '2026-08-25T10:00:30.000Z',
      startedBy: 'dev-hank',
      participants: [
        { userId: 'dev-hank', joinedAt: '2026-08-25T10:00:01.000Z' },
        { userId: 'dev-late', joinedAt: '2026-08-25T10:05:00.000Z' },
      ],
    });
    expect(out.participants.map((p) => p.userId)).toEqual(['dev-hank']);
  });

  test('scoping never under-reports: an all-stale roster is kept, not emptied', () => {
    // recordingId 2026-08-25T00-57-55-251Z-274b03bf — a room recording
    // started 10.6 HOURS after everyone joined. Narrowing the window would
    // wipe the chip strip and replace it with just the initiator; keeping a
    // stale name is the smaller lie.
    const out = reconcileParticipants({
      startedAt: '2026-08-25T00:57:55.251Z',
      endedAt: '2026-08-25T00:58:40.000Z',
      startedBy: 'dev-hank',
      participants: [
        { userId: 'dev-hank', displayName: 'Hank Hill', joinedAt: '2026-08-24T14:20:32.000Z' },
        { userId: 'dev-bob', displayName: 'Bob Martinez', joinedAt: '2026-08-24T14:21:00.000Z' },
        { userId: 'dev-alice', displayName: 'Alice Ng', joinedAt: '2026-08-24T14:22:00.000Z' },
      ],
      tracks: [{ legId: 'leg-001' }, { legId: 'leg-002' }, { legId: 'leg-003' }],
    });
    expect(out.participantCount).toBe(3);
    expect(out.participants.map((p) => p.displayName).sort()).toEqual([
      'Alice Ng', 'Bob Martinez', 'Hank Hill',
    ]);
  });

  test('a leg with a userId re-adds someone the live roster already dropped', () => {
    const out = reconcileParticipants({
      startedAt: '2026-08-25T10:00:00.000Z',
      endedAt: '2026-08-25T10:01:00.000Z',
      startedBy: 'dev-hank',
      participants: [{ userId: 'dev-hank', joinedAt: '2026-08-25T10:00:00.500Z' }],
      tracks: [
        { legId: 'leg-001', userId: 'dev-hank' },
        { legId: 'leg-002', userId: 'dev-earlyleaver', participantId: 'p-2' },
      ],
    });
    expect(out.participants.map((p) => p.userId).sort()).toEqual(['dev-earlyleaver', 'dev-hank']);
    expect(out.participantCount).toBe(2);
  });

  test('count is floored at the recorded-leg count (participantId-only legs are people too)', () => {
    const out = reconcileParticipants({
      startedAt: '2026-08-25T10:00:00.000Z',
      endedAt: '2026-08-25T10:01:00.000Z',
      startedBy: 'dev-hank',
      participants: [{ userId: 'dev-hank' }],
      tracks: [{ participantId: 'p-1' }, { participantId: 'p-2' }, { participantId: 'p-3' }],
    });
    expect(out.participantCount).toBe(3);
  });

  test('an entry with no joinedAt is never disqualified by the window', () => {
    const out = reconcileParticipants({
      startedAt: '2026-08-25T10:00:00.000Z',
      endedAt: '2026-08-25T10:01:00.000Z',
      startedBy: 'dev-hank',
      participants: [{ userId: 'dev-bob' }],
    });
    expect(out.participants.map((p) => p.userId).sort()).toEqual(['dev-bob', 'dev-hank']);
  });

  test('empty everything yields an empty answer, not a phantom participant', () => {
    const out = reconcileParticipants({});
    expect(out.participants).toEqual([]);
    expect(out.participantCount).toBe(0);
  });

  test('the grace window is a real bound, not effectively infinite', () => {
    expect(ROSTER_JOIN_GRACE_MS).toBeGreaterThan(60_000);
    expect(ROSTER_JOIN_GRACE_MS).toBeLessThan(60 * 60_000);
  });
});

// ---------------------------------------------------------------------------
// 3. Track URLs
// ---------------------------------------------------------------------------

describe('publicizeTracks', () => {
  const opts = {
    channelArn: 'arn:local:ivs:channel/fe74b6a1-06b0-45d1-8e08-554a2d50c9f4',
    recordingId: '2026-08-26T02-35-09-652Z-546cf1e9',
    publicBaseUrl: 'http://lvs.example:3001',
  };

  test('a file:// path with the developer home directory never reaches the client', () => {
    const [track] = publicizeTracks(
      [
        {
          legId: 'leg-001',
          key: 'arn_local_ivs_channel_fe74b6a1/2026-08-26T02-35-09-652Z-546cf1e9/peer_leg-001.mp4',
          file: '/Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
          url: 'file:///Users/connorhoehn/Projects/realtime-examples/.tilt-lvs-recordings/x/peer_leg-001.mp4',
          size: 796,
        },
      ],
      opts,
    );

    const serialized = JSON.stringify(track);
    expect(serialized).not.toContain('file://');
    expect(serialized).not.toContain('/Users/');
    expect(track).not.toHaveProperty('file');
    expect(track.url).toBe(
      'http://lvs.example:3001/api/channels/'
      + encodeURIComponent(opts.channelArn)
      + '/recordings/'
      + encodeURIComponent(opts.recordingId)
      + '/segments/peer_leg-001.mp4',
    );
  });

  test('non-url fields are preserved so the downloads list still renders', () => {
    const [track] = publicizeTracks(
      [{ legId: 'leg-002', userId: 'dev-bob', key: 'a/b/peer_leg-002.mp4', size: 4242, kind: 'av' }],
      opts,
    );
    expect(track.legId).toBe('leg-002');
    expect(track.userId).toBe('dev-bob');
    expect(track.size).toBe(4242);
    expect(track.kind).toBe('av');
  });

  test('an unrecoverable key drops the url rather than leaking the raw path', () => {
    const [track] = publicizeTracks(
      [{ legId: 'leg-003', url: 'file:///Users/connorhoehn/secret-dir' }],
      opts,
    );
    expect(track.url).toBeUndefined();
    expect(JSON.stringify(track)).not.toContain('/Users/');
  });

  test('empty / missing input is an empty list, not a throw', () => {
    expect(publicizeTracks(undefined, opts)).toEqual([]);
    expect(publicizeTracks(null, opts)).toEqual([]);
    expect(publicizeTracks([], opts)).toEqual([]);
  });
});
