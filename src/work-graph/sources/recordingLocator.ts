/**
 * Finds `video-call-recordings` rows by the ids the work graph names them by
 * (realtime-examples NFR #148).
 *
 * The table is keyed `(callId, recordingId)` with a `recordingId-index` GSI.
 * The graph addresses a meeting `meeting:<callId>`, a recording
 * `recording:<recordingId>` and a transcript `transcript:<transcriptId>` (or
 * `transcript:<recordingId>` when the recording has no transcript id). Every
 * build used to find those rows with a filtered Scan of the whole table —
 * three per build, growing with every recording ever made. Now:
 *
 *   meeting:<id>     → Query on the hash key `callId`
 *   recording:<id>   → Query on `recordingId-index`
 *   transcript:<id>  → Query on `recordingId-index`; when that finds nothing,
 *                      the transcript id's remembered `(callId, recordingId)`
 *                      in the cache Redis and one GetItem. Only a cold id (or
 *                      one with no row) still Scans, and fills the memory.
 *
 * Rows are always read fresh; the cache holds only which key a transcript id
 * lives under, and a remembered key is believed only when the row it names
 * still carries that transcript id. More than one row for an id is ambiguous
 * and resolves to null, as the Scan did. A bare id (no prefix) is looked up
 * as all three.
 */
import { GetCommand, QueryCommand, ScanCommand, type DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import type { RedisClientType } from 'redis';

import { getRedisCacheClient } from '../../lib/redis-client';
import { memoOnce } from '../buildMemo';

type Row = Record<string, unknown>;
type CacheClient = Pick<RedisClientType, 'get' | 'set' | 'del'>;

export const RECORDINGS_RECORDING_ID_INDEX = 'recordingId-index';
/** A transcript id's remembered key; ids never move, so this only bounds writes that bypass the repository. */
export const TRANSCRIPT_KEY_TTL_S = 60 * 60;
/** "No row carries this transcript id" — short, so a row written later shows within a minute. */
export const TRANSCRIPT_MISS_TTL_S = 60;
const MISS = '-';
const MAX_SCAN_PAGES = 50;

const transcriptKey = (table: string, id: string): string => `wg:rec-tid:${table}:${id}`;

export interface RecordingLocatorCounters { queries: number; gets: number; scans: number; keyHits: number; missHits: number }
export const recordingLocatorCounters: RecordingLocatorCounters = { queries: 0, gets: 0, scans: 0, keyHits: 0, missHits: 0 };

/** `meeting:x` → `{ kind: 'meeting', id: 'x' }`; a bare id has no kind. */
export function parseRecordingResourceId(resourceId: string): { kind?: 'meeting' | 'recording' | 'transcript'; id: string } {
  const match = /^(meeting|recording|transcript):(.*)$/.exec(resourceId);
  return match ? { kind: match[1] as 'meeting' | 'recording' | 'transcript', id: match[2]! } : { id: resourceId };
}

const rowKey = (row: Row): string => `${String(row.callId)}\u0000${String(row.recordingId)}`;

export class RecordingLocator {
  constructor(private readonly cache: () => Promise<CacheClient | null> = getRedisCacheClient) {}

  /** The one row a graph resource id names; null when none — or more than one. Read once per build. */
  find(client: DynamoDBDocumentClient, table: string, resourceId: string): Promise<Row | null> {
    return memoOnce(`recording\u0000${table}\u0000${resourceId}`, () => this.read(client, table, resourceId));
  }

  private async read(client: DynamoDBDocumentClient, table: string, resourceId: string): Promise<Row | null> {
    const { kind, id } = parseRecordingResourceId(resourceId);
    if (!id) return null;
    const rows = new Map<string, Row>();
    const add = (found: Row[]): void => { for (const row of found) rows.set(rowKey(row), row); };
    if (kind === 'meeting') add(await this.byCallId(client, table, id));
    else if (kind === 'recording') add(await this.byRecordingId(client, table, id));
    else if (kind === 'transcript') {
      // A recording's own transcript is addressed by the recording id; a
      // transcript with its own id is found through the remembered key.
      add(await this.byRecordingId(client, table, id));
      if (rows.size === 0) add(await this.byTranscriptId(client, table, id));
    } else {
      const [byCall, byRecording, byTranscript] = await Promise.all([
        this.byCallId(client, table, id),
        this.byRecordingId(client, table, id),
        this.byTranscriptId(client, table, id),
      ]);
      add(byCall); add(byRecording); add(byTranscript);
    }
    return rows.size === 1 ? [...rows.values()][0]! : null;
  }

  /** Up to two rows: enough to tell one from ambiguous. */
  private async query(client: DynamoDBDocumentClient, input: ConstructorParameters<typeof QueryCommand>[0]): Promise<Row[]> {
    const rows: Row[] = [];
    let startKey: Record<string, unknown> | undefined;
    do {
      recordingLocatorCounters.queries += 1;
      const result = await client.send(new QueryCommand({ ...input, ...(startKey ? { ExclusiveStartKey: startKey } : {}) }));
      rows.push(...((result?.Items ?? []) as Row[]));
      startKey = result?.LastEvaluatedKey as Record<string, unknown> | undefined;
    } while (startKey && rows.length < 2);
    return rows;
  }

  private byCallId(client: DynamoDBDocumentClient, table: string, callId: string): Promise<Row[]> {
    return this.query(client, {
      TableName: table,
      KeyConditionExpression: 'callId = :id',
      ExpressionAttributeValues: { ':id': callId },
    });
  }

  private async byRecordingId(client: DynamoDBDocumentClient, table: string, recordingId: string): Promise<Row[]> {
    const rows = await this.query(client, {
      TableName: table,
      IndexName: RECORDINGS_RECORDING_ID_INDEX,
      KeyConditionExpression: 'recordingId = :id',
      ExpressionAttributeValues: { ':id': recordingId },
    });
    return rows.filter((row) => row.recordingId === recordingId);
  }

  private async byTranscriptId(client: DynamoDBDocumentClient, table: string, transcriptId: string): Promise<Row[]> {
    const cache = await this.cache().catch(() => null);
    const remembered = cache ? await cache.get(transcriptKey(table, transcriptId)).catch(() => null) : null;
    if (remembered === MISS) {
      recordingLocatorCounters.missHits += 1;
      return [];
    }
    if (remembered) {
      try {
        const key = JSON.parse(remembered) as { callId?: unknown; recordingId?: unknown };
        if (typeof key.callId === 'string' && typeof key.recordingId === 'string') {
          recordingLocatorCounters.gets += 1;
          const row = (await client.send(new GetCommand({ TableName: table, Key: { callId: key.callId, recordingId: key.recordingId } })))?.Item as Row | undefined;
          if (row?.transcriptId === transcriptId) {
            recordingLocatorCounters.keyHits += 1;
            return [row];
          }
        }
      } catch {
        // Unreadable memory: fall through to the Scan, which rewrites it.
      }
    }
    const rows: Row[] = [];
    let startKey: Record<string, unknown> | undefined;
    let pages = 0;
    do {
      recordingLocatorCounters.scans += 1;
      const result = await client.send(new ScanCommand({
        TableName: table,
        FilterExpression: 'transcriptId = :id',
        ExpressionAttributeValues: { ':id': transcriptId },
        ...(startKey ? { ExclusiveStartKey: startKey } : {}),
      }));
      rows.push(...((result?.Items ?? []) as Row[]));
      startKey = result?.LastEvaluatedKey as Record<string, unknown> | undefined;
      pages += 1;
    } while (startKey && rows.length < 2 && pages < MAX_SCAN_PAGES);
    if (cache && !startKey) {
      // Remember only a complete answer: one row's key, or "none". Two rows
      // stay unremembered, so the ambiguity is found again every time.
      const value = rows.length === 1 ? JSON.stringify({ callId: rows[0]!.callId, recordingId: rows[0]!.recordingId })
        : rows.length === 0 ? MISS : undefined;
      if (value) {
        await cache.set(transcriptKey(table, transcriptId), value, { EX: value === MISS ? TRANSCRIPT_MISS_TTL_S : TRANSCRIPT_KEY_TTL_S })
          .catch(() => undefined);
      }
    }
    return rows;
  }

  /** A recording row was written or deleted: forget what its transcript id pointed at. */
  async noteWrite(table: string, row: { transcriptId?: unknown; recordingId?: unknown }): Promise<void> {
    const ids = [row.transcriptId, row.recordingId].filter((value): value is string => typeof value === 'string' && !!value);
    if (!ids.length) return;
    try {
      const cache = await this.cache().catch(() => null);
      if (cache) await cache.del(ids.map((id) => transcriptKey(table, id)));
    } catch {
      // The TTLs bound anything this misses.
    }
  }
}

export const recordingLocator = new RecordingLocator();
