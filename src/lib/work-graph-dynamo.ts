import {
  CreateTableCommand,
  DescribeTableCommand,
  DescribeTimeToLiveCommand,
  ResourceInUseException,
  UpdateTableCommand,
  UpdateTimeToLiveCommand,
  waitUntilTableExists,
  type DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import { tableName } from './ddb-table-name';

export const WORK_GRAPH_TABLE = tableName('work-graph');
export const WORK_GRAPH_GRANT_LOOKUP_INDEX = 'work-graph-grant-lookup';
export const WORK_GRAPH_COLLABORATION_LOOKUP_INDEX = 'work-graph-collaboration-lookup';
export const WORK_GRAPH_TARGET_LOOKUP_INDEX = 'work-graph-target-lookup';

const INDEXES = [
  { name: WORK_GRAPH_GRANT_LOOKUP_INDEX, pk: 'grantLookupPk', sk: 'grantLookupSk' },
  { name: WORK_GRAPH_COLLABORATION_LOOKUP_INDEX, pk: 'collaborationLookupPk', sk: 'collaborationLookupSk' },
  { name: WORK_GRAPH_TARGET_LOOKUP_INDEX, pk: 'targetLookupPk', sk: 'targetLookupSk' },
] as const;

function missingTable(error: unknown): boolean {
  return (error as { name?: string })?.name === 'ResourceNotFoundException';
}

/** Idempotent C1 schema provisioning for projection, grant, outbox, and TTL rows. */
export async function ensureWorkGraphTable(client: DynamoDBClient): Promise<void> {
  let existingIndexes = new Set<string>();
  try {
    const described = await client.send(new DescribeTableCommand({ TableName: WORK_GRAPH_TABLE }));
    existingIndexes = new Set((described.Table?.GlobalSecondaryIndexes ?? []).flatMap((index) => index.IndexName ? [index.IndexName] : []));
  } catch (error) {
    if (!missingTable(error)) throw error;
    try {
      await client.send(new CreateTableCommand({
        TableName: WORK_GRAPH_TABLE,
        BillingMode: 'PAY_PER_REQUEST',
        AttributeDefinitions: [
          { AttributeName: 'PK', AttributeType: 'S' },
          { AttributeName: 'SK', AttributeType: 'S' },
          ...INDEXES.flatMap((index) => [
            { AttributeName: index.pk, AttributeType: 'S' as const },
            { AttributeName: index.sk, AttributeType: 'S' as const },
          ]),
        ],
        KeySchema: [
          { AttributeName: 'PK', KeyType: 'HASH' },
          { AttributeName: 'SK', KeyType: 'RANGE' },
        ],
        GlobalSecondaryIndexes: INDEXES.map((index) => ({
          IndexName: index.name,
          KeySchema: [
            { AttributeName: index.pk, KeyType: 'HASH' as const },
            { AttributeName: index.sk, KeyType: 'RANGE' as const },
          ],
          Projection: { ProjectionType: 'ALL' },
        })),
      }));
      existingIndexes = new Set(INDEXES.map((index) => index.name));
    } catch (error) {
      if (!(error instanceof ResourceInUseException) && (error as { name?: string }).name !== 'ResourceInUseException') throw error;
    }
  }

  // DynamoDB accepts one GSI creation per UpdateTable request. Upgrade old
  // work-graph tables in place and wait between additions instead of leaving
  // route behavior dependent on whether the table predated collaboration.
  for (const index of INDEXES) {
    if (existingIndexes.has(index.name)) continue;
    await client.send(new UpdateTableCommand({
      TableName: WORK_GRAPH_TABLE,
      AttributeDefinitions: [
        { AttributeName: index.pk, AttributeType: 'S' },
        { AttributeName: index.sk, AttributeType: 'S' },
      ],
      GlobalSecondaryIndexUpdates: [{
        Create: {
          IndexName: index.name,
          KeySchema: [
            { AttributeName: index.pk, KeyType: 'HASH' },
            { AttributeName: index.sk, KeyType: 'RANGE' },
          ],
          Projection: { ProjectionType: 'ALL' },
        },
      }],
    }));
    await waitUntilTableExists({ client, maxWaitTime: 120 }, { TableName: WORK_GRAPH_TABLE });
  }

  const ttl = await client.send(new DescribeTimeToLiveCommand({ TableName: WORK_GRAPH_TABLE }));
  const status = ttl.TimeToLiveDescription?.TimeToLiveStatus;
  if (status !== 'ENABLED' && status !== 'ENABLING') {
    await client.send(new UpdateTimeToLiveCommand({
      TableName: WORK_GRAPH_TABLE,
      TimeToLiveSpecification: { Enabled: true, AttributeName: 'ttl' },
    }));
  }
}
