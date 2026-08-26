// scripts/backfill-recording-truth.ts
//
// One-shot repair for recording rows written before 2026-08-25, when the
// UI's three claims about a recording were all wrong (see
// src/lib/recording-truth.ts for the measurements):
//
//   durationMs        held the SFU channel-session lifetime, not the media
//   participants[]    held the channel's lifetime roster: undeduped,
//                     unscoped, and missing the person who placed the call
//   participantCount  counted sessions, not people
//
// WHY BACKFILL AND NOT JUST DERIVE-ON-READ. Both, actually — but the
// backfill is not optional, because the two tables are not equally
// repairable:
//
//   video-call-recordings              has tracks[] and the raw roster, so
//                                      a read can re-derive everything.
//                                      The repository does exactly that as
//                                      a safety net.
//
//   video-call-recording-participants  is a denormalized fan-out index with
//                                      NO tracks[] and NO roster. Nothing
//                                      can be re-derived from it, and it is
//                                      the table the list view reads. A
//                                      pure derive-on-read fix would leave
//                                      every "Past Calls" card still
//                                      claiming +10.2s.
//
// And a MISSING fan-out row can't be fixed on read at all: if the initiator
// has no index row, their query returns nothing to correct. Those rows have
// to be written.
//
// Idempotent: re-running rewrites the same values. Safe to interrupt.
//
// Usage (host, against the local DDB the pod reads):
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 \
//     npx tsx scripts/backfill-recording-truth.ts --dry-run
//   LOCAL_DDB_ENDPOINT=http://localhost:18001 \
//     npx tsx scripts/backfill-recording-truth.ts --apply

import { ScanCommand, PutCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { docClient } from '../src/lib/aws-clients';
import {
  CALL_RECORDINGS_TABLE,
  CALL_RECORDING_PARTICIPANTS_TABLE,
  type CallRecording,
} from '../src/repositories/CallRecordingRepository';
import {
  deriveMediaDurationMs,
  reconcileParticipants,
} from '../src/lib/recording-truth';

const APPLY = process.argv.includes('--apply');
const RETENTION_DAYS = Number(process.env.RECORDING_RETENTION_DAYS ?? 30);

interface Stats {
  scanned: number;
  durationsFixed: number;
  durationMsSaved: number;
  countsFixed: number;
  rostersDeduped: number;
  fanoutRowsWritten: number;
  fanoutRowsUpdated: number;
  initiatorRowsCreated: number;
}

async function scanAll(table: string): Promise<Array<Record<string, unknown>>> {
  const out: Array<Record<string, unknown>> = [];
  let key: Record<string, unknown> | undefined;
  do {
    const page = await docClient.send(
      new ScanCommand({ TableName: table, ...(key ? { ExclusiveStartKey: key } : {}) }),
    );
    out.push(...((page.Items ?? []) as Array<Record<string, unknown>>));
    key = page.LastEvaluatedKey as Record<string, unknown> | undefined;
  } while (key);
  return out;
}

async function main(): Promise<void> {
  const stats: Stats = {
    scanned: 0,
    durationsFixed: 0,
    durationMsSaved: 0,
    countsFixed: 0,
    rostersDeduped: 0,
    fanoutRowsWritten: 0,
    fanoutRowsUpdated: 0,
    initiatorRowsCreated: 0,
  };

  const recordings = (await scanAll(CALL_RECORDINGS_TABLE)) as unknown as CallRecording[];
  const fanout = await scanAll(CALL_RECORDING_PARTICIPANTS_TABLE);

  // recordingId -> the userIds that already have an index row.
  const existingFanout = new Map<string, Set<string>>();
  for (const row of fanout) {
    const rid = row.recordingId as string;
    if (!rid) continue;
    if (!existingFanout.has(rid)) existingFanout.set(rid, new Set());
    existingFanout.get(rid)!.add(row.userId as string);
  }

  const ttl = Math.floor(Date.now() / 1000) + RETENTION_DAYS * 86400;

  for (const rec of recordings) {
    stats.scanned += 1;

    // --- FINDING 1 -------------------------------------------------------
    // Rows already carrying sessionDurationMs were written by the corrected
    // path (or a previous run of this script) — leave their duration alone.
    const alreadyCorrected = rec.sessionDurationMs !== undefined;
    const sessionDurationMs = alreadyCorrected
      ? (rec.sessionDurationMs as number)
      : (rec.durationMs ?? 0);
    const mediaDurationMs = alreadyCorrected
      ? rec.durationMs
      : deriveMediaDurationMs({ tracks: rec.tracks, sessionDurationMs });
    if (mediaDurationMs !== rec.durationMs) {
      stats.durationsFixed += 1;
      stats.durationMsSaved += rec.durationMs - mediaDurationMs;
    }

    // --- FINDINGS 2 + 3 --------------------------------------------------
    const people = reconcileParticipants({
      participants: rec.participants,
      tracks: rec.tracks,
      startedBy: rec.startedBy,
      startedAt: rec.startedAt,
      endedAt: rec.endedAt,
    });
    if ((rec.participants?.length ?? 0) !== people.participants.length) {
      stats.rostersDeduped += 1;
    }
    if (rec.participantCount !== people.participantCount) {
      stats.countsFixed += 1;
    }

    if (APPLY) {
      await docClient.send(
        new UpdateCommand({
          TableName: CALL_RECORDINGS_TABLE,
          Key: { callId: rec.callId, recordingId: rec.recordingId },
          UpdateExpression:
            'SET durationMs = :d, sessionDurationMs = :s, participants = :p, participantCount = :pc',
          ExpressionAttributeValues: {
            ':d': mediaDurationMs,
            ':s': sessionDurationMs,
            ':p': people.participants,
            ':pc': people.participantCount,
          },
          // Never resurrect a row that a concurrent delete removed.
          ConditionExpression: 'attribute_exists(callId)',
        }),
      );
    }

    // --- Fan-out repair ---------------------------------------------------
    // Rewrite every index row the recording SHOULD have. Missing rows are
    // created (that's the invisible-to-the-initiator case); existing rows
    // are refreshed so the list view stops rendering the old duration and
    // the old session count.
    const have = existingFanout.get(rec.recordingId) ?? new Set<string>();
    for (const p of people.participants) {
      const isNew = !have.has(p.userId);
      if (isNew) {
        stats.fanoutRowsWritten += 1;
        if (rec.startedBy === p.userId) stats.initiatorRowsCreated += 1;
      } else {
        stats.fanoutRowsUpdated += 1;
      }
      if (!APPLY) continue;
      await docClient.send(
        new PutCommand({
          TableName: CALL_RECORDING_PARTICIPANTS_TABLE,
          Item: {
            userId: p.userId,
            startedAtRecordingId: `${rec.startedAt}#${rec.recordingId}`,
            callId: rec.callId,
            recordingId: rec.recordingId,
            channelArn: rec.channelArn,
            lobbyName: rec.lobbyName,
            startedAt: rec.startedAt,
            endedAt: rec.endedAt,
            durationMs: mediaDurationMs,
            sessionDurationMs,
            thumbnailUrl: rec.thumbnailUrl,
            composedUrl: rec.composedUrl,
            participantCount: people.participantCount,
            participants: people.participants
              .slice(0, 12)
              .map((q) => ({ userId: q.userId, displayName: q.displayName })),
            // Preserve the original expiry when the row already had one —
            // a backfill must not silently extend retention.
            ttl: (fanout.find(
              (f) => f.recordingId === rec.recordingId && f.userId === p.userId,
            )?.ttl as number | undefined) ?? (rec.ttl ?? ttl),
          },
        }),
      );
    }
  }

  console.log(JSON.stringify({ mode: APPLY ? 'apply' : 'dry-run', ...stats }, null, 2));
  if (!APPLY) console.log('\nDry run — nothing written. Re-run with --apply.');
}

main().then(
  () => process.exit(0),
  (err) => {
    console.error(err);
    process.exit(1);
  },
);
