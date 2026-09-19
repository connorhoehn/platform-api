import {
  CreateTableCommand,
  DescribeTableCommand,
  DescribeTimeToLiveCommand,
  ResourceInUseException,
  UpdateTimeToLiveCommand,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { tableName } from './ddb-table-name';

export const WORK_GRAPH_TABLE = tableName('work-graph');
export const WORK_GRAPH_GRANT_LOOKUP_INDEX = 'work-graph-grant-lookup';

function missingTable(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ResourceNotFoundException';
}

/** Idempotent C1 schema provisioning for projection, grant, outbox, and TTL rows. */
export async function ensureWorkGraphTable(client: DynamoDBClient): Promise<void> {
  try {
    await client.send(new DescribeTableCommand({ TableName: WORK_GRAPH_TABLE }));
  } catch (error) {
    if (!missingTable(error)) throw error;
    try {
      await client.send(new CreateTableCommand({
        TableName: WORK_GRAPH_TABLE,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [
          { AttributeName: 'PK', AttributeType: 'S' },
          { AttributeName: 'SK', AttributeType: 'S' },
          { AttributeName: 'grantLookupPk', AttributeType: 'S' },
          { AttributeName: 'grantLookupSk', AttributeType: 'S' },
        ],
        KeySchema: [
          { AttributeName: 'PK', KeyType: 'HASH' },
          { AttributeName: 'SK', KeyType: 'RANGE' },
        ],
        GlobalSecondaryIndexes: [{
          IndexName: WORK_GRAPH_GRANT_LOOKUP_INDEX,
          KeySchema: [
            { AttributeName: 'grantLookupPk', KeyType: 'HASH' },
            { AttributeName: 'grantLookupSk', KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        }],
      }));
    } catch (error) {
      if (!(error instanceof ResourceInUseException) && (error as { name?: string }).name !== 'ResourceInUseException') throw error;
    }
  }

  const ttl = await client.send(new DescribeTimeToLiveCommand({ TableName: WORK_GRAPH_TABLE }));
  const status = ttl.TimeToLiveDescription?.TimeToLiveStatus;
  if (status !== 'ENABLED' && status !== 'ENABLING') {
    await client.send(new UpdateTimeToLiveCommand({
      TableName: WORK_GRAPH_TABLE,
      TimeToLiveSpecification: { Enabled: true, AttributeName: 'expiresAt' },
    }));
  }
}
