// social-api/src/ingest/__tests__/engine-singleton.test.ts
//
// Tests for get/setIngestEngine singleton helpers.

// Each test resets the module to start with null state.
beforeEach(() => {
    jest.resetModules();
});

describe('getIngestEngine / setIngestEngine', () => {
    test('initial value is null', () => {
        const { getIngestEngine } = require('../engine-singleton');
        expect(getIngestEngine()).toBeNull();
    });

    test('set then get returns the same instance', () => {
        const { getIngestEngine, setIngestEngine } = require('../engine-singleton');
        const fakeEngine = { start: jest.fn(), stop: jest.fn() };
        setIngestEngine(fakeEngine as never);
        expect(getIngestEngine()).toBe(fakeEngine);
    });

    test('set null clears the engine', () => {
        const { getIngestEngine, setIngestEngine } = require('../engine-singleton');
        setIngestEngine({ start: jest.fn() } as never);
        setIngestEngine(null);
        expect(getIngestEngine()).toBeNull();
    });

    test('second set replaces first', () => {
        const { getIngestEngine, setIngestEngine } = require('../engine-singleton');
        const first = { id: 'first' };
        const second = { id: 'second' };
        setIngestEngine(first as never);
        setIngestEngine(second as never);
        expect(getIngestEngine()).toBe(second);
    });
});
