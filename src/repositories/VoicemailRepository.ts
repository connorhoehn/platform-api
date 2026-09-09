// The message a caller leaves when nobody picks up.
//
// Bytes live HERE, in a second table, not on the call-log row: the row is
// queried for every list and badge and must stay small, while a message is
// read once or twice. Audio is stored base64 in the item — a minute of Opus
// at 24 kbps is ~180 KB, well inside the 400 KB item limit — so there is no
// object store to run locally and no signed URL to mint; playback is one
// authenticated GET.
import { DynamoDBDocumentClient, QueryCommand, UpdateCommand } from '@aws-sdk/lib-dynamodb';
import { CreateTableCommand, ResourceInUseException } from '@aws-sdk/client-dynamodb';
import { BaseRepository } from './BaseRepository';
import { ddbClient } from '../lib/aws-clients';
import { tableName } from '../lib/ddb-table-name';
import log from '../lib/logger';

export const VOICEMAIL_TABLE = tableName('voicemail');
/** Raw audio cap: base64 inflates by 4/3 and the item still needs headroom. */
export const VOICEMAIL_MAX_BYTES = 280 * 1024;
export const VOICEMAIL_MAX_DURATION_MS = 120_000;

export interface VoicemailItem {
  /** Recipient — the partition key; a message belongs to the person it is for. */
  userId: string;
  voicemailId: string;
  fromUserId: string;
  fromName?: string;
  callId: string;
  lobbyName: string;
  mimeType: string;
  durationMs: number;
  sizeBytes: number;
  createdAt: string;
  listenedAt?: string;
  /** Not returned by list; only by `getAudio`. */
  audioB64?: string;
}

export type VoicemailSummary = Omit<VoicemailItem, 'audioB64'>;

export class VoicemailRepository extends BaseRepository {
  private ensured: Promise<void> | null = null;

  constructor(docClient: DynamoDBDocumentClient) {
    super(VOICEMAIL_TABLE, docClient);
  }

  ensureTable(): Promise<void> {
    this.ensured ??= (async () => {
      try {
        await ddbClient.send(new CreateTableCommand({
          TableName: VOICEMAIL_TABLE,
          AttributeDefinitions: [
            { AttributeName: 'userId', AttributeType: 'S' },
            { AttributeName: 'voicemailId', AttributeType: 'S' },
          ],
          KeySchema: [
            { AttributeName: 'userId', KeyType: 'HASH' },
            { AttributeName: 'voicemailId', KeyType: 'RANGE' },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }));
        log.info({ table: VOICEMAIL_TABLE }, 'created the voicemail table');
      } catch (err) {
        if (!(err instanceof ResourceInUseException)) {
          log.warn({ event: 'voicemail.create_failed', table: VOICEMAIL_TABLE, err: err instanceof Error ? err.message : String(err) }, 'could not create the voicemail table');
        }
      }
    })();
    return this.ensured;
  }

  async save(item: VoicemailItem & { audioB64: string }): Promise<void> {
    await this.ensureTable();
    await this.putItem(item as unknown as Record<string, unknown>);
  }

  async listForUser(userId: string, limit = 100): Promise<VoicemailSummary[]> {
    await this.ensureTable();
    const result = await this.docClient.send(new QueryCommand({
      TableName: this.tableName,
      KeyConditionExpression: 'userId = :u',
      ExpressionAttributeValues: { ':u': userId },
      ProjectionExpression: 'userId, voicemailId, fromUserId, fromName, callId, lobbyName, mimeType, durationMs, sizeBytes, createdAt, listenedAt',
    }));
    return ((result.Items ?? []) as VoicemailSummary[]).sort((a, b) => b.createdAt.localeCompare(a.createdAt)).slice(0, limit);
  }

  async get(userId: string, voicemailId: string): Promise<VoicemailItem | null> {
    await this.ensureTable();
    return this.getItem<VoicemailItem>({ userId, voicemailId });
  }

  async markListened(userId: string, voicemailId: string): Promise<void> {
    await this.ensureTable();
    await this.docClient.send(new UpdateCommand({
      TableName: this.tableName,
      Key: { userId, voicemailId },
      UpdateExpression: 'SET listenedAt = if_not_exists(listenedAt, :t)',
      ExpressionAttributeValues: { ':t': new Date().toISOString() },
    }));
  }
}
