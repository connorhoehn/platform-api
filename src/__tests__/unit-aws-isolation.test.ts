import { DynamoDBClient, ListTablesCommand } from '@aws-sdk/client-dynamodb';

it('unit-test SDK defaults use dummy credentials and a closed local endpoint', async () => {
  expect(process.env.AWS_ACCESS_KEY_ID).toBe('unit-test');
  expect(process.env.AWS_SECRET_ACCESS_KEY).toBe('unit-test');
  expect(process.env.AWS_SESSION_TOKEN).toBeUndefined();
  expect(process.env.AWS_EC2_METADATA_DISABLED).toBe('true');
  let requests = 0;
  const client = new DynamoDBClient({
    region: 'us-east-1',
    maxAttempts: 1,
    requestHandler: {
      async handle(request: { hostname: string; port?: number }) {
        requests += 1;
        expect(request.hostname).toBe('127.0.0.1');
        expect(request.port).toBe(9);
        return { response: { statusCode: 200, headers: {}, body: Buffer.from('{"TableNames":[]}') } };
      },
    },
  });
  try {
    await expect(client.send(new ListTablesCommand({}))).resolves.toMatchObject({ TableNames: [] });
    expect(requests).toBe(1);
  } finally { client.destroy(); }
});
