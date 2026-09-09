// The call log — every call attempt, as each participant saw it.
//
// Before this, a call that nobody answered left no trace anywhere: the invite
// was a gateway broadcast, the callee's banner aged out after sixty seconds,
// and the UX audit recorded "no missed-call record on either side". This is
// that record. One row PER PARTICIPANT per call (pk `userId`, sk `callId`), so
// a person's list is one Query and the same call reads as `outgoing` to the
// caller and `incoming` to everyone rung.
//
// ## Events, not state
//
// Clients report the frames they already send and receive — `invite`,
// `accepted`, `declined`, `cancelled`, `ended` — plus `missed`, which only the
// callee can know (its ring timed out). Several tabs and both parties report
// the same frame, so `applyCallEvent` is a monotonic reducer: outcomes only
// move forward in precedence (answered beats declined beats cancelled beats
// missed beats ringing) and timestamps only fill in. Reporting twice is free.

import { DynamoDBDocumentClient, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { CreateTableCommand, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import { BaseRepository } from './BaseRepository';
import { ddbClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

export const CALL_LOG_TABLE = tableName('call-log');

export type CallOutcome = 'ringing' | 'answered' | 'missed' | 'declined' | 'cancelled';
export type CallDirection = 'incoming' | 'outgoing';
export type CallEventAction = 'invite' | 'accepted' | 'declined' | 'cancelled' | 'ended' | 'missed';

export interface CallEvent {
  callId: string;
  action: CallEventAction;
  callerId: string;
  callerName?: string;
  /** Everyone rung. Empty for an ambient (untargeted) invite. */
  targetUserIds: string[];
  lobbyName: string;
  audioOnly?: boolean;
  /** `knock` is somebody asking to be let INTO a call — not a call in itself, and not logged. */
  kind?: 'invite' | 'knock';
  sessionId?: string;
  /** ISO time the frame carried; the server's clock when absent. */
  at?: string;
  /** Who is reporting — decides which row `accepted`/`declined` name. */
  reporterId?: string;
}

export interface CallLogRow {
  userId: string;
  callId: string;
  direction: CallDirection;
  callerId: string;
  callerName?: string;
  targetUserIds: string[];
  lobbyName: string;
  audioOnly: boolean;
  outcome: CallOutcome;
  startedAt: string;
  answeredAt?: string;
  endedAt?: string;
  durationMs?: number;
  sessionId?: string;
  /** Set when the person has looked at their missed calls since this one. */
  seenAt?: string;
  updatedAt: string;
}

const PRECEDENCE: Record<CallOutcome, number> = { ringing: 0, missed: 1, cancelled: 2, declined: 3, answered: 4 };

/** Everyone a call touches: the caller and the rung. */
export function participantsOf(event: CallEvent): string[] {
  return [...new Set([event.callerId, ...event.targetUserIds].filter(Boolean))];
}

/**
 * The next state of one participant's row given one event. Pure, and the
 * whole contract: everything the routes do is fan this over `participantsOf`.
 */
export function applyCallEvent(row: CallLogRow | null, event: CallEvent, userId: string, now: string): CallLogRow {
  const at = event.at ?? now;
  const base: CallLogRow = row ?? {
    userId,
    callId: event.callId,
    direction: userId === event.callerId ? 'outgoing' : 'incoming',
    callerId: event.callerId,
    ...(event.callerName ? { callerName: event.callerName } : {}),
    targetUserIds: event.targetUserIds,
    lobbyName: event.lobbyName,
    audioOnly: Boolean(event.audioOnly),
    outcome: 'ringing',
    startedAt: at,
    updatedAt: now,
  };
  const next: CallLogRow = { ...base, updatedAt: now };
  if (event.callerName && !next.callerName) next.callerName = event.callerName;
  if (event.sessionId && !next.sessionId) next.sessionId = event.sessionId;
  // A late `invite` report must not move the start forward.
  if (event.action === 'invite' && at < next.startedAt) next.startedAt = at;

  const promote = (outcome: CallOutcome) => {
    if (PRECEDENCE[outcome] > PRECEDENCE[next.outcome]) next.outcome = outcome;
  };
  switch (event.action) {
    case 'accepted':
      promote('answered');
      if (!next.answeredAt) next.answeredAt = at;
      break;
    case 'declined':
      promote('declined');
      break;
    case 'cancelled':
      // The caller gave up before anyone answered. If someone HAD answered,
      // this is just the caller leaving — the answer stands.
      promote('cancelled');
      break;
    case 'missed':
      promote('missed');
      break;
    case 'ended':
      if (!next.endedAt) next.endedAt = at;
      // A call that ended while still ringing was never picked up.
      if (next.outcome === 'ringing') next.outcome = 'missed';
      break;
    case 'invite':
    default:
      break;
  }
  if (next.answeredAt && next.endedAt) {
    const ms = Date.parse(next.endedAt) - Date.parse(next.answeredAt);
    if (Number.isFinite(ms) && ms >= 0) next.durationMs = ms;
  }
  return next;
}

export type CallLogFilter = 'all' | 'missed' | 'incoming' | 'outgoing';

export function matchesFilter(row: CallLogRow, filter: CallLogFilter): boolean {
  switch (filter) {
    case 'missed': return row.direction === 'incoming' && row.outcome === 'missed';
    case 'incoming': return row.direction === 'incoming';
    case 'outgoing': return row.direction === 'outgoing';
    default: return true;
  }
}

export class CallLogRepository extends BaseRepository {
  private ensured: Promise<void> | null = null;

  constructor(docClient: DynamoDBDocumentClient) {
    super(CALL_LOG_TABLE, docClient);
  }

  /** Local-dev tables are created on first use; production has them already. */
  ensureTable(): Promise<void> {
    this.ensured ??= (async () => {
      try {
        await ddbClient.send(new CreateTableCommand({
          TableName: CALL_LOG_TABLE,
          AttributeDefinitions: [
            { AttributeName: 'userId', AttributeType: 'S' },
            { AttributeName: 'callId', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'userId', KeyType: 'HASH' },
            { AttributeName: 'callId', KeyType: 'RANGE' },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }));
        log.info({ table: CALL_LOG_TABLE }, 'created the call-log table');
      } catch (err) {
        if (!(err instanceof ResourceInUseException)) {
          log.warn({ event: 'call.log.create_failed', table: CALL_LOG_TABLE, err: err instanceof Error ? err.message : String(err) }, 'could not create the call-log table');
        }
      }
    })();
    return this.ensured;
  }

  async getRow(userId: string, callId: string): Promise<CallLogRow | null> {
    await this.ensureTable();
    return this.getItem<CallLogRow>({ userId, callId });
  }

  /** Applies one event to every participant's row. Returns the rows written. */
  async applyEvent(event: CallEvent): Promise<CallLogRow[]> {
    await this.ensureTable();
    if (event.kind === 'knock') return [];
    const now = new Date().toISOString();
    const rows: CallLogRow[] = [];
    for (const userId of participantsOf(event)) {
      const current = await this.getItem<CallLogRow>({ userId, callId: event.callId });
      const next = applyCallEvent(current, event, userId, now);
      await this.putItem(next as unknown as Record<string, unknown>);
      rows.push(next);
    }
    return rows;
  }

  async listForUser(userId: string, filter: CallLogFilter = 'all', limit = 100): Promise<CallLogRow[]> {
    await this.ensureTable();
    const result = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      KeyConditionExpression: 'userId = :u',
      ExpressionAttributeValues: { ':u': userId },
    }));
    const rows = ((result.Items ?? []) as CallLogRow[])
      .filter((r) => matchesFilter(r, filter))
      .sort((a, b) => b.startedAt.localeCompare(a.startedAt));
    return rows.slice(0, limit);
  }

  /** Missed incoming calls the person has not looked at. */
  async unseenMissed(userId: string): Promise<number> {
    const rows = await this.listForUser(userId, 'missed', 500);
    return rows.filter((r) => !r.seenAt).length;
  }

  async markSeen(userId: string): Promise<number> {
    const rows = (await this.listForUser(userId, 'missed', 500)).filter((r) => !r.seenAt);
    const now = new Date().toISOString();
    for (const r of rows) {
      await this.docClient.send(new UpdateCommand({
        TableName: this.tableName,
        Key: { userId, callId: r.callId },
        UpdateExpression: 'SET seenAt = :s, updatedAt = :s',
        ExpressionAttributeValues: { ':s': now },
      }));
    }
    return rows.length;
  }
}
