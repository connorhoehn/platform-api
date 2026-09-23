import { DeleteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { localTtlSweepEnabled, sweepExpiredProcessedRows, sweepExpiredWorkGraphRows } from '../localTtlSweep';

describe('local TTL sweep (NFR #73)', () => {
  it('runs only when asked and never in production', () => {
    expect(localTtlSweepEnabled({ LOCAL_TTL_SWEEP: 'true', NODE_ENV: 'development' })).toBe(true);
    expect(localTtlSweepEnabled({ LOCAL_TTL_SWEEP: 'true', NODE_ENV: 'production' })).toBe(false);
    expect(localTtlSweepEnabled({ NODE_ENV: 'development' })).toBe(false);
  });

  it('deletes only PROCESSED rows past their ttl, re-checked in the delete condition', async () => {
    const sent: unknown[] = [];
    const docClient = {
      send: jest.fn(async (cmd: unknown) => {
        sent.push(cmd);
        if (cmd instanceof ScanCommand) {
          const first = !cmd.input.ExclusiveStartKey;
          return first
            ? { ScannedCount: 3, Items: [{ outboxId: 'a' }, { outboxId: 'b' }], LastEvaluatedKey: { outboxId: 'b' } }
            : { ScannedCount: 2, Items: [{ outboxId: 'c' }] };
        }
        if (cmd instanceof DeleteCommand && cmd.input.Key?.outboxId === 'b') {
          throw Object.assign(new Error('changed'), { name: 'ConditionalCheckFailedException' });
        }
        return {};
      }),
    };
    const out = await sweepExpiredProcessedRows({ docClient, tableName: 't', keyAttr: 'outboxId', nowSec: 1000 });
    expect(out).toEqual({ scanned: 5, expired: 3, deleted: 2, skipped: 1 });
    const scan = sent[0] as ScanCommand;
    expect(scan.input.FilterExpression).toBe('#ttl < :now AND #status = :status');
    expect(scan.input.ExpressionAttributeValues).toEqual({ ':now': 1000, ':status': 'PROCESSED' });
    const del = sent.find((c) => c instanceof DeleteCommand) as DeleteCommand;
    expect(del.input.ConditionExpression).toBe('#ttl < :now AND #status = :status');
  });

  it('stops at the delete budget', async () => {
    const docClient = { send: jest.fn(async (cmd: unknown) => (cmd instanceof ScanCommand ? { ScannedCount: 3, Items: [{ outboxId: 'a' }, { outboxId: 'b' }, { outboxId: 'c' }] } : {})) };
    const out = await sweepExpiredProcessedRows({ docClient, tableName: 't', keyAttr: 'outboxId', maxDeletes: 2 });
    expect(out.deleted).toBe(2);
  });

  it('work-graph: deletes only WG# rows past their ttl, by full PK+SK key (NFR #77)', async () => {
    const sent: any[] = [];
    const docClient = {
      send: jest.fn(async (cmd: any) => {
        sent.push(cmd);
        if (cmd instanceof ScanCommand) return { ScannedCount: 4, Items: [{ PK: 'WG#org#u', SK: 'DELTA#1' }, { PK: 'WG#org#u', SK: 'DELTA#2' }] };
        return {};
      }),
    };
    const out = await sweepExpiredWorkGraphRows({ docClient, tableName: 'work-graph', nowSec: 50 });
    expect(out).toEqual({ scanned: 4, expired: 2, deleted: 2, skipped: 0 });
    const scan = sent[0];
    expect(scan.input.FilterExpression).toBe('#ttl < :now AND begins_with(#pk, :wg)');
    expect(scan.input.ExpressionAttributeValues).toEqual({ ':now': 50, ':wg': 'WG#' });
    const dels = sent.filter((c) => c instanceof DeleteCommand);
    expect(dels.map((d) => d.input.Key)).toEqual([{ PK: 'WG#org#u', SK: 'DELTA#1' }, { PK: 'WG#org#u', SK: 'DELTA#2' }]);
    expect(dels[0].input.ConditionExpression).toBe('#ttl < :now AND begins_with(#pk, :wg)');
  });
});
