import { AsyncLocalStorage } from 'node:async_hooks';
import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { SQSClient } from '@aws-sdk/client-sqs';

const endpoint =
  process.env.LOCAL_DDB_ENDPOINT ||
  process.env.DYNAMODB_ENDPOINT ||
  process.env.LOCALSTACK_ENDPOINT;
const localConfig = endpoint
  ? {
      endpoint,
      credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
    }
  : {};

const region = process.env.AWS_REGION ?? 'us-east-1';

export const ddbClient = new DynamoDBClient({ region, ...localConfig });
export const docClient = DynamoDBDocumentClient.from(ddbClient, {
  marshallOptions: { removeUndefinedValues: true },
});
export const sqsClient = new SQSClient({ region, ...localConfig });

// ─── Per-request DynamoDB accounting (NFR #16) ────────────────────────────
// A request that wants to know what it cost runs inside `countDynamoCalls`;
// every command the shared client sends in that async context is tallied by
// `Command:table`. Outside one, the middleware is a map lookup and nothing
// else. The work-graph route reports the tally in its `server-timing` header,
// which is how "DynamoDB reads per graph build" is measured on the live stack.
export type DynamoCallTally = Map<string, number>;
const dynamoTally = new AsyncLocalStorage<DynamoCallTally>();

ddbClient.middlewareStack.add(
  (next, context) => async (args) => {
    const tally = dynamoTally.getStore();
    if (tally) {
      const input = (args.input ?? {}) as { TableName?: string; RequestItems?: Record<string, unknown> };
      const table = input.TableName ?? Object.keys(input.RequestItems ?? {}).join('+') ?? '?';
      const key = `${String(context.commandName ?? 'Command').replace(/Command$/, '')}:${table}`;
      tally.set(key, (tally.get(key) ?? 0) + 1);
      const started = Date.now();
      try {
        return await next(args);
      } finally {
        // Summed call time per kind (overlapping calls add up), under `ms:`.
        tally.set(`ms:${key}`, (tally.get(`ms:${key}`) ?? 0) + (Date.now() - started));
      }
    }
    return next(args);
  },
  { step: 'initialize', name: 'nfr16DynamoCallTally' },
);

export function countDynamoCalls<T>(run: () => Promise<T>): Promise<{ result: T; tally: DynamoCallTally }> {
  const tally: DynamoCallTally = new Map();
  return dynamoTally.run(tally, async () => ({ result: await run(), tally }));
}
