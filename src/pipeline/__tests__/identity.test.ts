// social-api/src/pipeline/__tests__/identity.test.ts
//
// Tests for config/identity.ts: resolveStableNodeId with and without identity file.

jest.mock('distributed-core', () => ({
    loadOrCreateNodeId: jest.fn(),
}));

import { resolveStableNodeId } from '../config/identity';
import { loadOrCreateNodeId } from 'distributed-core';

const mockLoad = loadOrCreateNodeId as jest.MockedFunction<typeof loadOrCreateNodeId>;

beforeEach(() => {
    mockLoad.mockReset();
});

describe('resolveStableNodeId — ephemeral (no identity file)', () => {
    test('returns a nodeId string when identityFilePath is undefined', async () => {
        const result = await resolveStableNodeId(undefined);
        expect(typeof result.nodeId).toBe('string');
        expect(result.nodeId.length).toBeGreaterThan(0);
    });

    test('persistentNodeId is undefined when no file configured', async () => {
        const result = await resolveStableNodeId(undefined);
        expect(result.persistentNodeId).toBeUndefined();
    });

    test('nodeId matches node-<alphanum>-<timestamp> format', async () => {
        const result = await resolveStableNodeId(undefined);
        expect(result.nodeId).toMatch(/^node-[a-z0-9]+-\d+$/);
    });

    test('two calls produce different ephemeral nodeIds', async () => {
        const a = await resolveStableNodeId(undefined);
        const b = await resolveStableNodeId(undefined);
        expect(a.nodeId).not.toBe(b.nodeId);
    });

    test('does not call loadOrCreateNodeId when identityFilePath is undefined', async () => {
        await resolveStableNodeId(undefined);
        expect(mockLoad).not.toHaveBeenCalled();
    });
});

describe('resolveStableNodeId — persistent (identity file configured)', () => {
    test('returns the nodeId from the identity file', async () => {
        mockLoad.mockResolvedValue('node-persisted-abc123');
        const result = await resolveStableNodeId('/var/data/node.id');
        expect(result.nodeId).toBe('node-persisted-abc123');
        expect(result.persistentNodeId).toBe('node-persisted-abc123');
    });

    test('calls loadOrCreateNodeId with the provided path', async () => {
        mockLoad.mockResolvedValue('node-x');
        await resolveStableNodeId('/run/identity/node.id');
        expect(mockLoad).toHaveBeenCalledWith('/run/identity/node.id');
    });

    test('throws a descriptive error when loadOrCreateNodeId rejects', async () => {
        mockLoad.mockRejectedValue(new Error('EACCES: permission denied'));
        await expect(resolveStableNodeId('/locked/node.id')).rejects.toThrow(
            'PIPELINE_IDENTITY_FILE',
        );
    });

    test('error message includes the file path', async () => {
        mockLoad.mockRejectedValue(new Error('read error'));
        await expect(resolveStableNodeId('/some/path/node.id')).rejects.toThrow(
            '/some/path/node.id',
        );
    });

    test('error message includes the underlying error text', async () => {
        mockLoad.mockRejectedValue(new Error('disk full'));
        await expect(resolveStableNodeId('/path/node.id')).rejects.toThrow('disk full');
    });
});
