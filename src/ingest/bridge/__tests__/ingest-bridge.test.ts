// social-api/src/ingest/bridge/__tests__/ingest-bridge.test.ts
//
// Tests for createIngestBridge: event forwarding, error isolation, stop() cleanup.

import { createIngestBridge } from '../ingest-bridge';
import type { IngestEvent } from '../../types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeEventBus() {
    const handlers: Array<(event: IngestEvent) => void> = [];
    return {
        on: jest.fn((handler: (event: IngestEvent) => void) => { handlers.push(handler); }),
        off: jest.fn((handler: (event: IngestEvent) => void) => {
            const idx = handlers.indexOf(handler);
            if (idx >= 0) handlers.splice(idx, 1);
        }),
        emit: (event: IngestEvent) => { handlers.forEach((h) => h(event)); },
        handlerCount: () => handlers.length,
    };
}

function makeBroadcast() {
    const calls: Array<{ channel: string; message: unknown }> = [];
    return {
        calls,
        fn: jest.fn((channel: string, message: unknown) => { calls.push({ channel, message }); }),
    };
}

function makeEvent(overrides: Partial<IngestEvent> = {}): IngestEvent {
    return { type: 'sync.started', sourceId: 'src-1', timestamp: new Date().toISOString(), ...overrides } as IngestEvent;
}

// ─── createIngestBridge ───────────────────────────────────────────────────

describe('createIngestBridge — event forwarding', () => {
    test('subscribes to event bus on creation', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        expect(bus.on).toHaveBeenCalledTimes(1);
    });

    test('forwards events to ingest:progress channel', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        bus.emit(makeEvent({ type: 'sync.started' }));
        expect(broadcast.calls).toHaveLength(1);
        expect(broadcast.calls[0].channel).toBe('ingest:progress');
    });

    test('passes full event object to broadcast', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        const event = makeEvent({ sourceId: 'src-99' });
        bus.emit(event);
        expect(broadcast.calls[0].message).toBe(event);
    });

    test('forwards multiple events independently', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        bus.emit(makeEvent({ type: 'sync.started' }));
        bus.emit(makeEvent());
        bus.emit(makeEvent());
        expect(broadcast.calls).toHaveLength(3);
    });
});

describe('createIngestBridge — stop()', () => {
    test('stop() calls eventBus.off', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        const bridge = createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        bridge.stop();
        expect(bus.off).toHaveBeenCalledTimes(1);
    });

    test('stop() unsubscribes the same handler that was registered', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        const bridge = createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        bridge.stop();
        bus.emit(makeEvent());
        expect(broadcast.calls).toHaveLength(0);
    });

    test('stop() is idempotent when called twice', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        const bridge = createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        bridge.stop();
        expect(() => bridge.stop()).not.toThrow();
    });
});

describe('createIngestBridge — error isolation', () => {
    test('broadcast error does not propagate to event bus caller', () => {
        const bus = makeEventBus();
        const broadcast = makeBroadcast();
        broadcast.fn.mockImplementation(() => { throw new Error('send failed'); });
        createIngestBridge({ eventBus: bus as never, broadcast: broadcast.fn });
        expect(() => bus.emit(makeEvent())).not.toThrow();
    });
});
