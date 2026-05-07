import {
  DescribeTimeToLiveCommand,
  UpdateTimeToLiveCommand,
} from '@aws-sdk/client-dynamodb';
import { ddbClient } from './aws-clients';
import { tableName } from './ddb-table-name';
import log from './logger';

const OUTBOX_TABLE = tableName('social-outbox');

export async function ensureOutboxTTL(): Promise<void> {
  const desc = await ddbClient.send(
    new DescribeTimeToLiveCommand({ TableName: OUTBOX_TABLE }),
  );
  const status = desc.TimeToLiveDescription?.TimeToLiveStatus;
  if (status === 'ENABLED' || status === 'ENABLING') return;

  await ddbClient.send(
    new UpdateTimeToLiveCommand({
      TableName: OUTBOX_TABLE,
      TimeToLiveSpecification: {
        Enabled: true,
        AttributeName: 'ttl',
      },
    }),
  );
  log.info({ table: OUTBOX_TABLE }, 'TTL enabled on outbox table');
}
