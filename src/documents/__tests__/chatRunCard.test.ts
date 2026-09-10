// postRunCardToChannel: posts the card with the run metadata; steps down when
// the run context says the client posted the card (clientPostsCard: true) but
// still gives the metadata back; keeps posting for runs with no client.

const mockPostChatMessage = jest.fn();
jest.mock('../../gateway/GatewayServiceClient', () => ({
  postChatMessage: (...args: unknown[]) => mockPostChatMessage(...args),
  PIPELINE_IDENTITY: { userId: 'agent:pipeline', displayName: 'Pipeline', color: '#0ea5e9' },
}));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l, withContext: () => l };
});

import { postRunCardToChannel, runCardMetadata, clientPostsCard } from '../chatRunCard';

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
