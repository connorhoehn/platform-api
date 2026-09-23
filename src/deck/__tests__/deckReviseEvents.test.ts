// The revise progress events: one ordered stream per request, ended once, and
// a Redis failure never reaching the revise.

jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l };
});

import { createDeckReviseEmitter, deckReviseChannelKey } from '../deckReviseEvents';

const settle = () => new Promise((resolve) => setImmediate(resolve));

describe('createDeckReviseEmitter', () => {
  it('sends nothing after the terminal event, and counts versions up', async () => {
    const sent: Array<{ channel: string; type: string; version: number }> = [];
    const events = createDeckReviseEmitter({
      documentId: 'doc-1', requestId: 'r1', userId: 'u', now: () => 0,
      publish: async (channel, message) => { const e = JSON.parse(message); sent.push({ channel, type: e.type, version: e.version }); },
    });
    events.started();
    events.phase('asking-model');
    events.failed(503, 'down');
    events.completed(['x']);
    events.phase('checking');
    await settle();
    expect(sent).toEqual([
      { channel: 'dcore:topic:pipeline.events.deck.revise.started', type: 'pipeline:deck:revise:started', version: 1 },
      { channel: 'dcore:topic:pipeline.events.deck.revise.phase', type: 'pipeline:deck:revise:phase', version: 2 },
      { channel: 'dcore:topic:pipeline.events.deck.revise.failed', type: 'pipeline:deck:revise:failed', version: 3 },
    ]);
    expect(deckReviseChannelKey('doc-1')).toBe('deck-revise:doc-1');
  });

  it('swallows a publish that fails, and is a no-op with Redis down', async () => {
    const failing = createDeckReviseEmitter({ documentId: 'd', requestId: 'r', userId: 'u', publish: async () => { throw new Error('redis gone'); } });
    expect(() => { failing.started(); failing.completed([]); }).not.toThrow();
    const noRedis = createDeckReviseEmitter({ documentId: 'd', requestId: 'r', userId: 'u' });
    expect(() => noRedis.started()).not.toThrow();
    await settle();
  });
});
