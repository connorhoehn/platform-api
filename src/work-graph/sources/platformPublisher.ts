/** Concrete platform-api lifecycle publisher used by pipeline and recording hooks. */
import { publishDurable } from '../../lib/event-catalog';
import { workSourceAccountRepo } from '../../repositories';
import { PlatformWorkGraphSourcePublisher } from './publisher';

const credentialId = process.env.WORK_GRAPH_PLATFORM_CREDENTIAL_ID ?? 'platform-api-runtime';

export const platformWorkGraphSourcePublisher = new PlatformWorkGraphSourcePublisher({
  accounts: workSourceAccountRepo,
  publisher: {
    publish: async (eventName, payload) => publishDurable(eventName, payload as unknown as Record<string, unknown>),
  },
  credentialId,
});
