import { DeleteCommand, ScanCommand } from '@aws-sdk/lib-dynamodb';
import { DescribeTableCommand, DescribeTimeToLiveCommand, ListTablesCommand } from '@aws-sdk/client-dynamodb';
import { outboxTerminalTtlSeconds } from '../../services/outbox-drainer';
import { defaultTtlSweepOverrides, localTtlSweepEnabled, sweepExpiredProcessedRows, sweepExpiredWorkGraphRows, sweepTtlTables } from '../localTtlSweep';

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

describe('sweepTtlTables (NFR #223)', () => {
  function fakeDdb(tables: Record<string, { ttl?: { status: string; attr?: string }; keys: Array<{ AttributeName: string; KeyType: 'HASH' | 'RANGE' }> }>) {
    return {
      send: jest.fn(async (cmd: unknown) => {
        if (cmd instanceof ListTablesCommand) return { TableNames: Object.keys(tables) };
        if (cmd instanceof DescribeTimeToLiveCommand) {
          const t = tables[cmd.input.TableName!].ttl;
          return { TimeToLiveDescription: t ? { TimeToLiveStatus: t.status, AttributeName: t.attr } : { TimeToLiveStatus: 'DISABLED' } };
        }
        if (cmd instanceof DescribeTableCommand) return { Table: { KeySchema: tables[cmd.input.TableName!].keys } };
        throw new Error('unexpected');
      }),
    };
  }

  it('sweeps only tables whose own TTL is ENABLED, by that attribute and full key, with overrides', async () => {
    const ddb = fakeDdb({
      'p-social-outbox': { ttl: { status: 'ENABLED', attr: 'ttl' }, keys: [{ AttributeName: 'outboxId', KeyType: 'HASH' }] },
      'p-claims': { ttl: { status: 'ENABLED', attr: 'expiresAt' }, keys: [{ AttributeName: 'sk', KeyType: 'RANGE' }, { AttributeName: 'pk', KeyType: 'HASH' }] },
      'p-audit': { ttl: { status: 'DISABLED' }, keys: [{ AttributeName: 'id', KeyType: 'HASH' }] },
      'other-claims': { ttl: { status: 'ENABLED', attr: 'ttl' }, keys: [{ AttributeName: 'id', KeyType: 'HASH' }] },
    });
    const sent: any[] = [];
    const docClient = { send: jest.fn(async (cmd: any) => {
      sent.push(cmd);
      if (cmd instanceof ScanCommand) {
        return cmd.input.TableName === 'p-claims'
          ? { ScannedCount: 2, Items: [{ pk: 'a', sk: '1' }] }
          : { ScannedCount: 1, Items: [{ outboxId: 'o1' }] };
      }
      return {};
    }) };
    const out = await sweepTtlTables({ ddb, docClient, prefix: 'p-', overrides: defaultTtlSweepOverrides((b) => `p-${b}`), nowSec: 1000, deletesPerSecond: 0 });
    expect(out.map((t) => [t.table, t.ttlAttr, t.deleted])).toEqual([['p-social-outbox', 'ttl', 1], ['p-claims', 'expiresAt', 1]]);
    const claimDel = sent.find((c) => c instanceof DeleteCommand && c.input.TableName === 'p-claims');
    expect(claimDel.input.Key).toEqual({ pk: 'a', sk: '1' });
    expect(claimDel.input.ExpressionAttributeNames['#ttl']).toBe('expiresAt');
    const outboxDel = sent.find((c) => c instanceof DeleteCommand && c.input.TableName === 'p-social-outbox');
    expect(outboxDel.input.ConditionExpression).toBe('#ttl < :now AND #status IN (:processed, :failed)');
    expect(sent.some((c) => c.input.TableName === 'p-audit' || c.input.TableName === 'other-claims')).toBe(false);
  });

  it('dry run counts expired rows and deletes nothing; a lost lease stops the pass', async () => {
    const ddb = fakeDdb({ t: { ttl: { status: 'ENABLED', attr: 'ttl' }, keys: [{ AttributeName: 'id', KeyType: 'HASH' }] } });
    const docClient = { send: jest.fn(async (cmd: any) => (cmd instanceof ScanCommand ? { ScannedCount: 3, Items: [{ id: 'a' }, { id: 'b' }, { id: 'c' }] } : {})) };
    const dry = await sweepTtlTables({ ddb, docClient, dryRun: true });
    expect(dry[0]).toMatchObject({ expired: 3, deleted: 0 });
    expect(docClient.send.mock.calls.some(([c]) => c instanceof DeleteCommand)).toBe(false);
    let leader = true;
    const sleep = jest.fn(async () => { leader = false; });
    const lost = await sweepTtlTables({ ddb, docClient, shouldContinue: () => leader, deletesPerSecond: 50, sleep });
    expect(lost[0].deleted).toBe(1);
    expect(sleep).toHaveBeenCalledWith(20);
  });

  it('outbox terminal rows: 1 h in local dev, 24 h otherwise, env overrides', () => {
    expect(outboxTerminalTtlSeconds({ LOCAL_TTL_SWEEP: 'true', NODE_ENV: 'development' })).toBe(3600);
    expect(outboxTerminalTtlSeconds({ NODE_ENV: 'production', LOCAL_TTL_SWEEP: 'true' })).toBe(86400);
    expect(outboxTerminalTtlSeconds({ OUTBOX_TERMINAL_TTL_SECONDS: '600' })).toBe(600);
  });
});
