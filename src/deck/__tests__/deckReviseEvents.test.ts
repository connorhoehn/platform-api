// The revise progress events: one ordered stream per request, ended once, and
// a Redis failure never reaching the revise.

jest.mock('../../lib/redis-client', () => ({ getRedisClient: async () => null }));
jest.mock('../../lib/logger', () => {
  const l = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn(), child: () => l };
  return { __esModule: true, default: l };
});

import { createDeckReviseEmitter, deckReviseChannelKey, publishDeckRevisionWritten, setDeckReviseLiveSink, DECK_REVISION_WRITTEN_EVENT_TYPE } from '../deckReviseEvents';
import { isPipelineMqLaneEvent } from '@connorhoehn/event-catalog';

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

describe('publishDeckRevisionWritten (NFR #15)', () => {
  it('names the revision on the document channel, rides both lanes, and carries no deck text', async () => {
    const sunk: Array<{ id: unknown; type: unknown }> = [];
    setDeckReviseLiveSink((event) => { sunk.push({ id: event.id, type: event.type }); });
    const sent: Array<{ channel: string; event: { id: string; type: string; payload: Record<string, unknown> } }> = [];
    try {
      const event = publishDeckRevisionWritten(
        { documentId: 'doc-9', revisionId: 'rev-3', slideCount: 7, createdBy: 'u1', createdAt: '2026-09-23T00:00:00.000Z' },
        { now: () => 0, publish: async (channel, message) => { sent.push({ channel, event: JSON.parse(message) }); } },
      );
      await settle();
      expect(event?.type).toBe(DECK_REVISION_WRITTEN_EVENT_TYPE);
      expect(isPipelineMqLaneEvent(DECK_REVISION_WRITTEN_EVENT_TYPE)).toBe(true);
      expect(sent).toHaveLength(1);
      expect(sent[0].channel).toBe('dcore:topic:pipeline.events.deck.revise.revision-written');
      expect(sunk).toEqual([{ id: sent[0].event.id, type: DECK_REVISION_WRITTEN_EVENT_TYPE }]);
      expect(Object.keys(sent[0].event.payload).sort()).toEqual(
        ['createdAt', 'createdBy', 'documentId', 'occurredAt', 'revisionId', 'runId', 'slideCount'],
      );
      expect(sent[0].event.payload.runId).toBe('deck-revise:doc-9');
    } finally {
      setDeckReviseLiveSink(null);
    }
  });
});
