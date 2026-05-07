import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { SQSClient } from '@aws-sdk/client-sqs';

const endpoint = process.env.DYNAMODB_ENDPOINT || process.env.LOCALSTACK_ENDPOINT;
const localConfig = endpoint
  ? {
      endpoint,
      credentials: { accessKeyId: 'test', secretAccessKey: 'test' },
    }
  : {};

const region = process.env.AWS_REGION ?? 'us-east-1';

export const ddbClient = new DynamoDBClient({ region, ...localConfig });
export const docClient = DynamoDBDocumentClient.from(ddbClient, {
  marshallOptions: { removeUndefinedValues: true },
});
export const sqsClient = new SQSClient({ region, ...localConfig });
