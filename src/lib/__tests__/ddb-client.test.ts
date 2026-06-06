const mockDynamoDBClientCtor = jest.fn().mockImplementation((opts: unknown) => ({ opts }));
const mockDocClientFrom = jest.fn();

jest.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: mockDynamoDBClientCtor,
}));

jest.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: mockDocClientFrom },
}));

let getDynamoClient: typeof import('../ddb-client').getDynamoClient;

beforeEach(() => {
  jest.resetModules();
  mockDynamoDBClientCtor.mockClear();
  mockDocClientFrom.mockClear();
  delete process.env.AWS_REGION;
  delete process.env.LOCAL_DDB_ENDPOINT;
  delete process.env.DYNAMODB_ENDPOINT;
  delete process.env.LOCALSTACK_ENDPOINT;
});

afterEach(() => {
  delete process.env.AWS_REGION;
  delete process.env.LOCAL_DDB_ENDPOINT;
  delete process.env.DYNAMODB_ENDPOINT;
  delete process.env.LOCALSTACK_ENDPOINT;
});

function load() {
  getDynamoClient = require('../ddb-client').getDynamoClient;
}

describe('getDynamoClient', () => {
  test('returns a DynamoDBDocumentClient instance', () => {
    const mockDoc = { type: 'doc-client' };
    mockDocClientFrom.mockReturnValue(mockDoc);
    load();
    const result = getDynamoClient();
    expect(result).toBe(mockDoc);
  });

  test('singleton: second call returns same instance without re-creating', () => {
    const mockDoc = { type: 'doc-client' };
    mockDocClientFrom.mockReturnValue(mockDoc);
    load();
    const first = getDynamoClient();
    const second = getDynamoClient();
    expect(first).toBe(second);
    expect(mockDynamoDBClientCtor).toHaveBeenCalledTimes(1);
    expect(mockDocClientFrom).toHaveBeenCalledTimes(1);
  });

  test('defaults region to us-east-1 when AWS_REGION not set', () => {
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as { region: string };
    expect(opts.region).toBe('us-east-1');
  });

  test('uses AWS_REGION env var when set', () => {
    process.env.AWS_REGION = 'eu-west-1';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as { region: string };
    expect(opts.region).toBe('eu-west-1');
  });

  test('no endpoint or credentials when neither DYNAMODB_ENDPOINT nor LOCALSTACK_ENDPOINT is set', () => {
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as { endpoint?: string; credentials?: unknown };
    expect(opts.endpoint).toBeUndefined();
    expect(opts.credentials).toBeUndefined();
  });

  test('injects LOCAL_DDB_ENDPOINT as endpoint with local credentials', () => {
    process.env.LOCAL_DDB_ENDPOINT = 'http://localhost:18000';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as {
      endpoint: string;
      credentials: { accessKeyId: string; secretAccessKey: string };
    };
    expect(opts.endpoint).toBe('http://localhost:18000');
    expect(opts.credentials).toEqual({ accessKeyId: 'local', secretAccessKey: 'local' });
  });

  test('LOCAL_DDB_ENDPOINT takes precedence over DYNAMODB_ENDPOINT', () => {
    process.env.LOCAL_DDB_ENDPOINT = 'http://managed-nosql-store:8000';
    process.env.DYNAMODB_ENDPOINT = 'http://dynamodb-local:8000';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as { endpoint: string };
    expect(opts.endpoint).toBe('http://managed-nosql-store:8000');
  });

  test('injects DYNAMODB_ENDPOINT as endpoint with local credentials', () => {
    process.env.DYNAMODB_ENDPOINT = 'http://localhost:8000';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as {
      endpoint: string;
      credentials: { accessKeyId: string; secretAccessKey: string };
    };
    expect(opts.endpoint).toBe('http://localhost:8000');
    expect(opts.credentials).toEqual({ accessKeyId: 'local', secretAccessKey: 'local' });
  });

  test('falls back to LOCALSTACK_ENDPOINT when LOCAL_DDB_ENDPOINT and DYNAMODB_ENDPOINT are not set', () => {
    process.env.LOCALSTACK_ENDPOINT = 'http://localhost:4566';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as {
      endpoint: string;
      credentials: { accessKeyId: string; secretAccessKey: string };
    };
    expect(opts.endpoint).toBe('http://localhost:4566');
    expect(opts.credentials).toEqual({ accessKeyId: 'local', secretAccessKey: 'local' });
  });

  test('DYNAMODB_ENDPOINT takes precedence over LOCALSTACK_ENDPOINT', () => {
    process.env.DYNAMODB_ENDPOINT = 'http://localhost:8000';
    process.env.LOCALSTACK_ENDPOINT = 'http://localhost:4566';
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const opts = mockDynamoDBClientCtor.mock.calls[0][0] as { endpoint: string };
    expect(opts.endpoint).toBe('http://localhost:8000');
  });

  test('passes removeUndefinedValues: true in marshallOptions', () => {
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    const secondArg = mockDocClientFrom.mock.calls[0][1] as {
      marshallOptions: { removeUndefinedValues: boolean };
    };
    expect(secondArg.marshallOptions.removeUndefinedValues).toBe(true);
  });

  test('passes the DynamoDBClient instance as first arg to DynamoDBDocumentClient.from', () => {
    const fakeRaw = { opts: { region: 'us-east-1' } };
    mockDynamoDBClientCtor.mockReturnValue(fakeRaw);
    mockDocClientFrom.mockReturnValue({});
    load();
    getDynamoClient();
    expect(mockDocClientFrom.mock.calls[0][0]).toBe(fakeRaw);
  });
});

export {};
