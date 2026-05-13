// src/ingest/__tests__/catalog-event-bus.test.ts
//
// Tests for CatalogIngestEventBus: local handler dispatch, signal transport
// publishing, error resilience, and on/off handler management.

const mockSignalPublish = jest.fn<Promise<void>, [string, unknown]>();
const mockValidate = jest.fn<{ valid: boolean; errors?: Array<{ message: string }> }, [string, unknown]>();

jest.mock('../../lib/event-catalog', () => ({
  catalog: {
    getSignalTransport: jest.fn(() => ({
      publish: mockSignalPublish,
    })),
    getRegistry: jest.fn(() => ({
      validate: mockValidate,
    })),
  },
}));

jest.mock('ulid', () => ({
  ulid: jest.fn(() => 'mock-ulid-001'),
}));

import { CatalogIngestEventBus } from '../catalog-event-bus';
import type { IngestEvent } from '../types';

beforeEach(() => {
  jest.clearAllMocks();
  mockSignalPublish.mockResolvedValue(undefined);
  mockValidate.mockReturnValue({ valid: true });
});

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

function makeEvent(overrides: Partial<IngestEvent & { type: string }> = {}): IngestEvent {
  return {
    type: 'sync.started',
    sourceId: 'src-1',
    sourceName: 'Test Source',
    fileCount: 10,
    timestamp: '2026-05-07T00:00:00.000Z',
    ...overrides,
  } as IngestEvent;
}

// ---------------------------------------------------------------------------
// emit — local handlers
// ---------------------------------------------------------------------------

describe('emit', () => {
  it('calls all registered handlers', () => {
    const bus = new CatalogIngestEventBus();
    const handler1 = jest.fn();
    const handler2 = jest.fn();

    bus.on(handler1);
    bus.on(handler2);

    const event = makeEvent();
    bus.emit(event);

    expect(handler1).toHaveBeenCalledWith(event);
    expect(handler2).toHaveBeenCalledWith(event);
  });

  it('publishes to signal transport with channel ingest:events', () => {
    const bus = new CatalogIngestEventBus();
    const event = makeEvent();

    bus.emit(event);

    expect(mockSignalPublish).toHaveBeenCalledTimes(1);

    const [channel, envelope] = mockSignalPublish.mock.calls[0];
    expect(channel).toBe('ingest:events');
    expect(envelope).toMatchObject({
      eventName: 'ingest:progress',
      payload: event,
      metadata: {
        id: 'mock-ulid-001',
        producer: 'platform-api',
      },
    });
    expect((envelope as any).metadata.timestamp).toBeDefined();
  });

  it('does not throw if signal transport rejects', () => {
    mockSignalPublish.mockRejectedValue(new Error('Redis down'));

    const bus = new CatalogIngestEventBus();
    const event = makeEvent();

    // emit is synchronous — it should not throw even if publish rejects
    expect(() => bus.emit(event)).not.toThrow();
  });

  it('catches handler errors without stopping other handlers', () => {
    const bus = new CatalogIngestEventBus();
    const handler1 = jest.fn(() => { throw new Error('handler1 exploded'); });
    const handler2 = jest.fn();

    bus.on(handler1);
    bus.on(handler2);

    const event = makeEvent();
    bus.emit(event);

    expect(handler1).toHaveBeenCalledWith(event);
    expect(handler2).toHaveBeenCalledWith(event);
  });
});

// ---------------------------------------------------------------------------
// on / off — handler management
// ---------------------------------------------------------------------------

describe('on/off', () => {
  it('manages handlers correctly', () => {
    const bus = new CatalogIngestEventBus();
    const handler = jest.fn();

    bus.on(handler);

    const event1 = makeEvent({ sourceId: 'src-a' });
    bus.emit(event1);
    expect(handler).toHaveBeenCalledTimes(1);
    expect(handler).toHaveBeenCalledWith(event1);

    bus.off(handler);

    const event2 = makeEvent({ sourceId: 'src-b' });
    bus.emit(event2);
    // Should not have been called again
    expect(handler).toHaveBeenCalledTimes(1);
  });

  it('off with unregistered handler does not throw', () => {
    const bus = new CatalogIngestEventBus();
    const handler = jest.fn();

    expect(() => bus.off(handler)).not.toThrow();
  });
});

// ---------------------------------------------------------------------------
// Phase P3.2 — discriminator validation (registry-backed)
// ---------------------------------------------------------------------------

describe('emit — discriminator validation (Phase P3.2)', () => {
  it('calls registry.validate with ingest:progress and the event payload', () => {
    const bus = new CatalogIngestEventBus();
    const event = makeEvent({ type: 'sync.started' });
    bus.emit(event);
    expect(mockValidate).toHaveBeenCalledTimes(1);
    expect(mockValidate).toHaveBeenCalledWith('ingest:progress', event);
  });

  it('warns when validation fails (e.g. unknown discriminator value)', () => {
    mockValidate.mockReturnValue({
      valid: false,
      errors: [{ message: 'must be equal to one of the allowed values' }],
    });
    const bus = new CatalogIngestEventBus();
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
    bus.emit(makeEvent({ type: 'sync.invented' as unknown as 'sync.started' }));
    expect(warn).toHaveBeenCalledWith(expect.stringContaining('ingest:progress validation failed'));
    warn.mockRestore();
  });

  it('valid sub-type does NOT log a warning', () => {
    mockValidate.mockReturnValue({ valid: true });
    const bus = new CatalogIngestEventBus();
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
    bus.emit(makeEvent({ type: 'sync.started' }));
    expect(warn).not.toHaveBeenCalled();
    warn.mockRestore();
  });

  it('still publishes to signal transport even when validation fails', () => {
    mockValidate.mockReturnValue({
      valid: false,
      errors: [{ message: 'discriminator mismatch' }],
    });
    const bus = new CatalogIngestEventBus();
    const warn = jest.spyOn(console, 'warn').mockImplementation(() => {});
    bus.emit(makeEvent({ type: 'sync.invented' as unknown as 'sync.started' }));
    expect(mockSignalPublish).toHaveBeenCalledTimes(1);
    warn.mockRestore();
  });
});
