// postRunCardToChannel: posts the card with the run metadata; steps down when
// the run context says the client posted the card (clientPostsCard: true) but
// still gives the metadata back; keeps posting for runs with no client.

const mockPostChatMessage = jest.fn();
jest.mock('../../gateway/GatewayServiceClient', () => ({
  ...jest.requireActual('../../gateway/GatewayServiceClient'),
  postChatMessage: (...args: unknown[]) => mockPostChatMessage(...args),
  PIPELINE_IDENTITY: { userId: 'agent:pipeline', displayName: 'Pipeline', color: '#0ea5e9' },
}));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { postRunCard, postRunCardToChannel, runCardMetadata, clientPostsCard, runCardIdentity } from '../chatRunCard';

const input = { channel: 'room:r', runId: 'run-1', pipelineId: 'people-search', command: '/search', instruction: '2 people match "k8s"', extra: { matched: 2 } };
const metadata = { kind: 'agent-run', runId: 'run-1', pipelineId: 'people-search', command: '/search', instruction: '2 people match "k8s"', agentName: 'Pipeline', agentUserId: 'agent:pipeline', matched: 2 };

beforeEach(() => mockPostChatMessage.mockReset().mockResolvedValue(undefined));

test('posts the card with the run metadata', async () => {
  await expect(postRunCardToChannel(input)).resolves.toBe(true);
  expect(mockPostChatMessage).toHaveBeenCalledWith({ channel: 'room:r', message: '/search 2 people match "k8s"', metadata });
  expect(runCardMetadata(input)).toEqual(metadata);
});

test('a run whose context carries clientPostsCard: true leaves the card to the client — nothing posted, metadata still available', async () => {
  await expect(postRunCardToChannel({ ...input, context: { clientPostsCard: true, query: 'k8s' } })).resolves.toBe(false);
  expect(mockPostChatMessage).not.toHaveBeenCalled();
  expect(runCardMetadata({ ...input, context: { clientPostsCard: true } })).toEqual(metadata);
});

test('a run started with no client (flag absent or false) keeps posting', async () => {
  await expect(postRunCardToChannel({ ...input, context: { query: 'k8s' } })).resolves.toBe(true);
  await expect(postRunCardToChannel({ ...input, context: { clientPostsCard: false } })).resolves.toBe(true);
  await expect(postRunCardToChannel({ ...input, context: null })).resolves.toBe(true);
  expect(mockPostChatMessage).toHaveBeenCalledTimes(3);
  expect(clientPostsCard({ clientPostsCard: 'true' })).toBe(false);
  expect(clientPostsCard(undefined)).toBe(false);
});

test('no channel or a gateway failure is false, never a throw', async () => {
  await expect(postRunCardToChannel({ ...input, channel: null })).resolves.toBe(false);
  mockPostChatMessage.mockRejectedValue(new Error('gateway down'));
  await expect(postRunCardToChannel(input)).resolves.toBe(false);
});

// NFR #42: the gateway admits only a DM's two members, so the card goes in as the requester there.
describe('direct messages', () => {
  const DM = 'chat:dm:dev-connor:dev-frank';
  const ctx = { requestedBy: 'dev-connor', requestedByName: 'Connor Hoehn', clientPostsCard: false };

  test('a DM card is posted as the requester (kind person); metadata still names the pipeline', async () => {
    await expect(postRunCard({ ...input, channel: DM, context: ctx })).resolves.toEqual({ posted: true, as: 'person' });
    expect(mockPostChatMessage).toHaveBeenCalledWith(expect.objectContaining({
      channel: DM, identity: { userId: 'dev-connor', displayName: 'Connor Hoehn', kind: 'person' },
      metadata: expect.objectContaining({ agentUserId: 'agent:pipeline' }),
    }));
  });

  test('an explicit requester wins; an agent requester or a non-DM channel keeps the pipeline identity', () => {
    expect(runCardIdentity({ ...input, channel: DM, requester: { userId: 'dev-frank' } })).toEqual({ userId: 'dev-frank', displayName: 'dev-frank', kind: 'person' });
    expect(runCardIdentity({ ...input, channel: DM, context: { requestedBy: 'agent:loop' } })).toBeUndefined();
    expect(runCardIdentity({ ...input, channel: 'room:r', context: ctx })).toBeUndefined();
    expect(runCardIdentity({ ...input, channel: 'chat:dmg:abc', context: ctx })).toBeUndefined();
  });

  test('a refusal comes back with the gateway reason and code, logged, never thrown', async () => {
    const { ChatRefusedError } = jest.requireActual('../../gateway/GatewayServiceClient');
    mockPostChatMessage.mockRejectedValue(new ChatRefusedError(DM, 'CHAT_DM_FORBIDDEN', 'You are not a member of this direct-message channel', 'post to'));
    const out = await postRunCard({ ...input, channel: DM });
    expect(out).toMatchObject({ posted: false, as: 'pipeline', code: 'CHAT_DM_FORBIDDEN' });
    expect(out.error).toMatch(/refused to post to chat:dm:dev-connor:dev-frank/);
  });
});
