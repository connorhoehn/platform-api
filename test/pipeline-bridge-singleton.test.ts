// test/pipeline-bridge-singleton.test.ts
//
// Tests for bridge-types.ts: setPipelineBridge / getPipelineBridge singleton.

// Reset module state between tests so the singleton is fresh.
let setPipelineBridge: (b: unknown) => void;
let getPipelineBridge: () => unknown;

beforeEach(() => {
    jest.resetModules();
    ({ setPipelineBridge, getPipelineBridge } = require('../src/pipeline/bridge-types'));
});

describe('bridge-types singleton', () => {
    test('getPipelineBridge returns null before any set', () => {
        expect(getPipelineBridge()).toBeNull();
    });

    test('set then get returns the bridge', () => {
        const bridge = { trigger: jest.fn(), getRun: jest.fn(), getHistory: jest.fn(), resolveApproval: jest.fn() };
        setPipelineBridge(bridge);
        expect(getPipelineBridge()).toBe(bridge);
    });

    test('setPipelineBridge(null) clears the bridge', () => {
        const bridge = { trigger: jest.fn(), getRun: jest.fn(), getHistory: jest.fn(), resolveApproval: jest.fn() };
        setPipelineBridge(bridge);
        setPipelineBridge(null);
        expect(getPipelineBridge()).toBeNull();
    });

    test('second set replaces the bridge', () => {
        const a = { id: 'a', trigger: jest.fn(), getRun: jest.fn(), getHistory: jest.fn(), resolveApproval: jest.fn() };
        const b = { id: 'b', trigger: jest.fn(), getRun: jest.fn(), getHistory: jest.fn(), resolveApproval: jest.fn() };
        setPipelineBridge(a);
        setPipelineBridge(b);
        expect(getPipelineBridge()).toBe(b);
    });
});
