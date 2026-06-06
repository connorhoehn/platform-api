import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';

let docClient: DynamoDBDocumentClient | undefined;

export function getDynamoClient(): DynamoDBDocumentClient {
  if (!docClient) {
    const endpoint =
      process.env.LOCAL_DDB_ENDPOINT ||
      process.env.DYNAMODB_ENDPOINT ||
      process.env.LOCALSTACK_ENDPOINT;
    const raw = new DynamoDBClient({
      region: process.env.AWS_REGION ?? 'us-east-1',
      ...(endpoint
        ? { endpoint, credentials: { accessKeyId: 'local', secretAccessKey: 'local' } }
        : {}),
    });
    docClient = DynamoDBDocumentClient.from(raw, {
      marshallOptions: { removeUndefinedValues: true },
    });
  }
  return docClient;
}
