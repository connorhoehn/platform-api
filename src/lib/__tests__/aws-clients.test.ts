// social-api/src/lib/__tests__/aws-clients.test.ts
//
// Tests for publishSocialEvent: happy path, failed entries, client errors (log-and-continue).

const mockSend = jest.fn();

jest.mock('@aws-sdk/client-eventbridge', () => {
    const PutEventsCommand = jest.fn().mockImplementation((input) => ({ input }));
    const EventBridgeClient = jest.fn().mockImplementation(() => ({ send: mockSend }));
    return { EventBridgeClient, PutEventsCommand };
});

jest.mock('@aws-sdk/client-dynamodb', () => ({
    DynamoDBClient: jest.fn().mockImplementation(() => ({})),
}));

jest.mock('@aws-sdk/lib-dynamodb', () => ({
    DynamoDBDocumentClient: { from: jest.fn().mockReturnValue({}) },
}));

jest.mock('@aws-sdk/client-sqs', () => ({
    SQSClient: jest.fn().mockImplementation(() => ({})),
}));

beforeEach(() => {
    jest.resetModules();
    mockSend.mockReset();
    delete process.env.EVENT_BUS_NAME;
});

function load() {
    return require('../aws-clients') as typeof import('../aws-clients');
}

// ─── happy path ───────────────────────────────────────────────────────────

describe('publishSocialEvent — happy path', () => {
    test('resolves without throwing when EventBridge responds successfully', async () => {
        mockSend.mockResolvedValue({ FailedEntryCount: 0, Entries: [] });
        const { publishSocialEvent } = load();
        await expect(publishSocialEvent('UserFollowed', { followerId: 'u-1' })).resolves.toBeUndefined();
    });

    test('calls send with correct Source and DetailType', async () => {
        mockSend.mockResolvedValue({ FailedEntryCount: 0 });
        const { publishSocialEvent } = load();
        await publishSocialEvent('CommentPosted', { commentId: 'c-1' });
        const cmd = (mockSend.mock.calls[0][0] as { input: { Entries: Array<{ Source: string; DetailType: string }> } });
        expect(cmd.input.Entries[0].Source).toBe('social-api');
        expect(cmd.input.Entries[0].DetailType).toBe('CommentPosted');
    });

    test('detail JSON includes the passed detail fields', async () => {
        mockSend.mockResolvedValue({ FailedEntryCount: 0 });
        const { publishSocialEvent } = load();
        await publishSocialEvent('PostLiked', { postId: 'p-1', userId: 'u-2' });
        const cmd = (mockSend.mock.calls[0][0] as { input: { Entries: Array<{ Detail: string }> } });
        const detail = JSON.parse(cmd.input.Entries[0].Detail);
        expect(detail.postId).toBe('p-1');
        expect(detail.userId).toBe('u-2');
    });

    test('detail JSON always includes a timestamp', async () => {
        mockSend.mockResolvedValue({ FailedEntryCount: 0 });
        const { publishSocialEvent } = load();
        await publishSocialEvent('Test', {});
        const cmd = (mockSend.mock.calls[0][0] as { input: { Entries: Array<{ Detail: string }> } });
        const detail = JSON.parse(cmd.input.Entries[0].Detail);
        expect(typeof detail.timestamp).toBe('string');
        expect(new Date(detail.timestamp).getTime()).not.toBeNaN();
    });

    test('uses EVENT_BUS_NAME env var when set', async () => {
        process.env.EVENT_BUS_NAME = 'custom-bus';
        mockSend.mockResolvedValue({ FailedEntryCount: 0 });
        const { publishSocialEvent } = load();
        await publishSocialEvent('Test', {});
        const cmd = (mockSend.mock.calls[0][0] as { input: { Entries: Array<{ EventBusName: string }> } });
        expect(cmd.input.Entries[0].EventBusName).toBe('custom-bus');
    });

    test('defaults EventBusName to social-events when env var not set', async () => {
        mockSend.mockResolvedValue({ FailedEntryCount: 0 });
        const { publishSocialEvent } = load();
        await publishSocialEvent('Test', {});
        const cmd = (mockSend.mock.calls[0][0] as { input: { Entries: Array<{ EventBusName: string }> } });
        expect(cmd.input.Entries[0].EventBusName).toBe('social-events');
    });
});

// ─── error handling ───────────────────────────────────────────────────────

describe('publishSocialEvent — error handling', () => {
    test('does not throw when EventBridge rejects (log-and-continue)', async () => {
        mockSend.mockRejectedValue(new Error('network timeout'));
        const { publishSocialEvent } = load();
        await expect(publishSocialEvent('Test', {})).resolves.toBeUndefined();
    });

    test('does not throw when FailedEntryCount > 0', async () => {
        mockSend.mockResolvedValue({
            FailedEntryCount: 1,
            Entries: [{ ErrorCode: 'InternalFailure', ErrorMessage: 'service error' }],
        });
        const { publishSocialEvent } = load();
        await expect(publishSocialEvent('Test', {})).resolves.toBeUndefined();
    });
});

export {};
