// test/ingest-bridge.test.ts
//
// Tests for createIngestBridge: event forwarding, channel constant,
// broadcast error isolation, and stop() teardown.

import { createIngestBridge } from '../src/ingest/bridge/ingest-bridge';
import type { IngestEvent } from '../src/ingest/types';

function makeEventBus() {
    let _handler: ((event: IngestEvent) => void) | null = null;
    return {
        on: jest.fn((h: (event: IngestEvent) => void) => { _handler = h; }),
        off: jest.fn((h: (event: IngestEvent) => void) => { if (_handler === h) _handler = null; }),
        emit: (event: IngestEvent) => { if (_handler) _handler(event); },
    };
}

function makeEvent(): IngestEvent {
    return {
        type: 'sync.started',
        sourceId: 'src-1',
        sourceName: 'My Source',
        fileCount: 5,
        timestamp: '2026-01-01T00:00:00Z',
    };
}

describe('createIngestBridge — event forwarding', () => {
    test('subscribes to eventBus.on during construction', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn();
        createIngestBridge({ eventBus: bus, broadcast });
        expect(bus.on).toHaveBeenCalledTimes(1);
    });

    test('forwards events to broadcast on the ingest:progress channel', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn();
        createIngestBridge({ eventBus: bus, broadcast });
        const event = makeEvent();
        bus.emit(event);
        expect(broadcast).toHaveBeenCalledWith('ingest:progress', event);
    });

    test('forwards multiple event types without crashing', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn();
        createIngestBridge({ eventBus: bus, broadcast });
        bus.emit(makeEvent());
        bus.emit({ type: 'sync.completed', sourceId: 'src-1', stats: { filesFound: 1, filesNew: 1, filesUpdated: 0, filesSkipped: 0, errors: 0, durationMs: 100 }, timestamp: new Date().toISOString() });
        expect(broadcast).toHaveBeenCalledTimes(2);
    });

    test('broadcast error is caught and does not propagate to event bus caller', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn(() => { throw new Error('ws error'); });
        createIngestBridge({ eventBus: bus, broadcast });
        expect(() => bus.emit(makeEvent())).not.toThrow();
    });
});

describe('createIngestBridge — stop()', () => {
    test('stop() calls eventBus.off with the same handler', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn();
        const bridge = createIngestBridge({ eventBus: bus, broadcast });
        bridge.stop();
        expect(bus.off).toHaveBeenCalledTimes(1);
        // The handler passed to off must be the same reference passed to on
        expect(bus.off.mock.calls[0][0]).toBe(bus.on.mock.calls[0][0]);
    });

    test('no more events forwarded after stop()', () => {
        const bus = makeEventBus();
        const broadcast = jest.fn();
        const bridge = createIngestBridge({ eventBus: bus, broadcast });
        bridge.stop();
        bus.emit(makeEvent());
        expect(broadcast).not.toHaveBeenCalled();
    });

    test('stop() is idempotent — second call does not throw', () => {
        const bus = makeEventBus();
        const bridge = createIngestBridge({ eventBus: bus, broadcast: jest.fn() });
        bridge.stop();
        expect(() => bridge.stop()).not.toThrow();
    });
});
