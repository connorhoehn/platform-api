// platform-api/src/pipeline/judge/__tests__/RawTextStore.test.ts
//
// Tests for InMemoryRawTextStore (round-trip, key shape, reset) and
// DdbRawTextStore (PutCommand shape, GetCommand shape, key namespace
// strip, graceful return-null on get errors).

import {
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
} from '@aws-sdk/lib-dynamodb';

const mockSend = jest.fn();
const mockDocClient = { send: mockSend } as unknown as DynamoDBDocumentClient;

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));
jest.mock('../../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import {
  InMemoryRawTextStore,
  DdbRawTextStore,
  rawTextKeyFor,
} from '../RawTextStore';

beforeEach(() => {
  mockSend.mockReset();
  mockSend.mockResolvedValue({});
});

describe('rawTextKeyFor', () => {
  it('namespaces under body/', () => {
    expect(rawTextKeyFor('jdg_01H1234ABCDE')).toBe('body/jdg_01H1234ABCDE');
  });
});

describe('InMemoryRawTextStore', () => {
  it('round-trips put then get under the canonical key', async () => {
    const store = new InMemoryRawTextStore();
    const key = await store.put('jdg_1', 'hello world');
    expect(key).toBe('body/jdg_1');
    expect(await store.get(key)).toBe('hello world');
  });

  it('returns null when the key was never put', async () => {
    const store = new InMemoryRawTextStore();
    expect(await store.get('body/missing')).toBeNull();
  });

  it('overwrites on repeat put for the same judgmentId', async () => {
    const store = new InMemoryRawTextStore();
    await store.put('jdg_1', 'v1');
    await store.put('jdg_1', 'v2');
    expect(await store.get(rawTextKeyFor('jdg_1'))).toBe('v2');
  });

  it('reset() drops all stored bodies', async () => {
    const store = new InMemoryRawTextStore();
    await store.put('jdg_1', 'x');
    store.reset();
    expect(await store.get(rawTextKeyFor('jdg_1'))).toBeNull();
  });
});

describe('DdbRawTextStore', () => {
  it('put issues a PutCommand to the pipeline-judgment-rawtext table', async () => {
    const store = new DdbRawTextStore({ docClient: mockDocClient, now: () => 1_700_000_000_000 });
    const key = await store.put('jdg_1', 'the body');

    expect(key).toBe('body/jdg_1');
    const cmd = mockSend.mock.calls[0][0] as PutCommand;
    expect(cmd).toBeInstanceOf(PutCommand);
    expect(cmd.input.TableName).toBe('test-pipeline-judgment-rawtext');
    const item = cmd.input.Item as { judgmentId: string; body: string; ttlEpoch: number };
    expect(item.judgmentId).toBe('jdg_1');
    expect(item.body).toBe('the body');
  });

  it('put sets ttlEpoch derived from now + ttlDays', async () => {
    const fixedNow = 1_700_000_000_000;
    const store = new DdbRawTextStore({
      docClient: mockDocClient,
      now: () => fixedNow,
      ttlDays: 30,
    });
    await store.put('jdg_1', 'body');
    const item = (mockSend.mock.calls[0][0] as PutCommand).input.Item as { ttlEpoch: number };
    expect(item.ttlEpoch).toBe(Math.floor(fixedNow / 1000) + 30 * 24 * 60 * 60);
  });

  it('get issues a GetCommand keyed by the stripped judgmentId', async () => {
    mockSend.mockResolvedValueOnce({ Item: { judgmentId: 'jdg_1', body: 'recovered' } });
    const store = new DdbRawTextStore({ docClient: mockDocClient });
    const out = await store.get('body/jdg_1');

    expect(out).toBe('recovered');
    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd).toBeInstanceOf(GetCommand);
    expect(cmd.input.Key).toEqual({ judgmentId: 'jdg_1' });
  });

  it('get accepts a raw judgmentId without the body/ prefix (legacy / robustness)', async () => {
    mockSend.mockResolvedValueOnce({ Item: { judgmentId: 'jdg_1', body: 'x' } });
    const store = new DdbRawTextStore({ docClient: mockDocClient });
    await store.get('jdg_1'); // no namespace prefix
    const cmd = mockSend.mock.calls[0][0] as GetCommand;
    expect(cmd.input.Key).toEqual({ judgmentId: 'jdg_1' });
  });

  it('get returns null when the item is missing', async () => {
    mockSend.mockResolvedValueOnce({});
    const store = new DdbRawTextStore({ docClient: mockDocClient });
    expect(await store.get('body/missing')).toBeNull();
  });

  it('get returns null when DDB throws (network, table missing, etc.)', async () => {
    mockSend.mockRejectedValueOnce(new Error('ResourceNotFoundException'));
    const store = new DdbRawTextStore({ docClient: mockDocClient });
    expect(await store.get('body/jdg_1')).toBeNull();
  });
});
