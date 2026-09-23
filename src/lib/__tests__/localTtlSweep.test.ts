import { DeleteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { localTtlSweepEnabled, sweepExpiredProcessedRows } from '../localTtlSweep';

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
    expect(scan.input.FilterExpression).toBe('#status = :status AND #ttl < :now');
    expect(scan.input.ExpressionAttributeValues).toEqual({ ':status': 'PROCESSED', ':now': 1000 });
    const del = sent.find((c) => c instanceof DeleteCommand) as DeleteCommand;
    expect(del.input.ConditionExpression).toBe('#status = :status AND #ttl < :now');
  });

  it('stops at the delete budget', async () => {
    const docClient = { send: jest.fn(async (cmd: unknown) => (cmd instanceof ScanCommand ? { ScannedCount: 3, Items: [{ outboxId: 'a' }, { outboxId: 'b' }, { outboxId: 'c' }] } : {})) };
    const out = await sweepExpiredProcessedRows({ docClient, tableName: 't', keyAttr: 'outboxId', maxDeletes: 2 });
    expect(out.deleted).toBe(2);
  });
});
