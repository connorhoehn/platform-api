import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import { EventBridgeClient, PutEventsCommand } from '@aws-sdk/client-eventbridge';
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
export const eventBridgeClient = new EventBridgeClient({ region, ...localConfig });
export const sqsClient = new SQSClient({ region, ...localConfig });

const EVENT_BUS_NAME = process.env.EVENT_BUS_NAME ?? 'social-events';

/**
 * Publish a social event to EventBridge. Log-and-continue on failure —
 * event publish errors must NOT break the HTTP response.
 */
export async function publishSocialEvent(
  detailType: string,
  detail: Record<string, unknown>,
): Promise<void> {
  try {
    const response = await eventBridgeClient.send(
      new PutEventsCommand({
        Entries: [
          {
            Source: 'social-api',
            DetailType: detailType,
            Detail: JSON.stringify({ ...detail, timestamp: new Date().toISOString() }),
            EventBusName: EVENT_BUS_NAME,
          },
        ],
      }),
    );
    if (response.FailedEntryCount && response.FailedEntryCount > 0) {
      const failed = response.Entries?.filter(e => e.ErrorCode);
      console.error(`[event-publish] EventBridge rejected ${detailType}:`, JSON.stringify(failed));
    }
  } catch (err) {
    console.error(`[event-publish] Failed to publish ${detailType}:`, err);
  }
}
