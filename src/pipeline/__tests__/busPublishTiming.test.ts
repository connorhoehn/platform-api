import { timeBusPublish, busPublishTimingSummary } from '../busPublishTiming';

describe('timeBusPublish (NFR #197)', () => {
  it('times total, wal and pubsub phases per event and passes results through', async () => {
    const sent: unknown[] = [];
    const bus = {
      _versionCounter: 0,
      config: { topic: 'pipeline.events.n1' },
      pubsub: { publish: async (_t: string, p: unknown) => { sent.push(p); } },
      async publish(type: string, payload: unknown) {
        this._versionCounter += 1;
        const event = { type, payload, version: this._versionCounter };
        await new Promise((r) => setTimeout(r, 5));
        await this.pubsub.publish(this.config.topic, event);
        return event;
      },
    };
    busPublishTimingSummary(true);
    timeBusPublish(bus);
    timeBusPublish(bus); // idempotent
    const events = await Promise.all([bus.publish('a', {}), bus.publish('b', {})]);
    expect(events.map((e) => (e as { version: number }).version)).toEqual([1, 2]);
    expect(sent).toHaveLength(2);
    const s = busPublishTimingSummary();
    expect(s.total.n).toBe(2);
    expect(s.wal.n).toBe(2);
    expect(s.pubsub.n).toBe(2);
    expect(s.wal.p50!).toBeGreaterThanOrEqual(4);
  });
});
