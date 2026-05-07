// social-api/src/pipeline/config/__tests__/identity.test.ts
//
// Tests for resolveStableNodeId: ephemeral path, persistent path via
// loadOrCreateNodeId, and the operator-facing error message on failure.

const mockLoadOrCreateNodeId = jest.fn<Promise<string>, [string]>();

jest.mock('distributed-core', () => ({
  ...jest.requireActual<typeof import('distributed-core')>('distributed-core'),
  loadOrCreateNodeId: (...args: [string]) => mockLoadOrCreateNodeId(...args),
}));

import { resolveStableNodeId } from '../identity';

afterEach(() => {
  mockLoadOrCreateNodeId.mockReset();
});

describe('resolveStableNodeId', () => {
  describe('no identity file path (ephemeral)', () => {
    it('returns a node-prefixed nodeId', async () => {
      const { nodeId } = await resolveStableNodeId(undefined);
      expect(nodeId).toMatch(/^node-/);
    });

    it('returns persistentNodeId=undefined', async () => {
      const { persistentNodeId } = await resolveStableNodeId(undefined);
      expect(persistentNodeId).toBeUndefined();
    });

    it('does not call loadOrCreateNodeId', async () => {
      await resolveStableNodeId(undefined);
      expect(mockLoadOrCreateNodeId).not.toHaveBeenCalled();
    });

    it('generates unique ids on repeated calls', async () => {
      const a = await resolveStableNodeId(undefined);
      const b = await resolveStableNodeId(undefined);
      expect(a.nodeId).not.toBe(b.nodeId);
    });
  });

  describe('with identity file path', () => {
    it('calls loadOrCreateNodeId with the provided path', async () => {
      mockLoadOrCreateNodeId.mockResolvedValue('node-abc123');
      await resolveStableNodeId('/var/lib/node-id');
      expect(mockLoadOrCreateNodeId).toHaveBeenCalledWith('/var/lib/node-id');
    });

    it('returns loaded id as both nodeId and persistentNodeId', async () => {
      mockLoadOrCreateNodeId.mockResolvedValue('node-persistent-99');
      const { nodeId, persistentNodeId } = await resolveStableNodeId('/etc/node-id');
      expect(nodeId).toBe('node-persistent-99');
      expect(persistentNodeId).toBe('node-persistent-99');
    });
  });

  describe('error wrapping', () => {
    it('re-throws with PIPELINE_IDENTITY_FILE in the message', async () => {
      mockLoadOrCreateNodeId.mockRejectedValue(new Error('ENOENT: no such file'));
      await expect(resolveStableNodeId('/bad/path/node-id')).rejects.toThrow(
        'PIPELINE_IDENTITY_FILE',
      );
    });

    it('includes the identity file path in the error message', async () => {
      mockLoadOrCreateNodeId.mockRejectedValue(new Error('EACCES: permission denied'));
      await expect(resolveStableNodeId('/read-only/node-id')).rejects.toThrow(
        '/read-only/node-id',
      );
    });

    it('includes the original error message', async () => {
      mockLoadOrCreateNodeId.mockRejectedValue(new Error('ENOENT: no such file'));
      await expect(resolveStableNodeId('/bad/path')).rejects.toThrow('ENOENT');
    });

    it('includes disabled opt-out hint in the error message', async () => {
      mockLoadOrCreateNodeId.mockRejectedValue(new Error('EACCES'));
      await expect(resolveStableNodeId('/locked/path')).rejects.toThrow(
        'PIPELINE_IDENTITY_FILE=disabled',
      );
    });

    it('wraps non-Error throws as strings', async () => {
      mockLoadOrCreateNodeId.mockRejectedValue('string-rejection');
      await expect(resolveStableNodeId('/any/path')).rejects.toThrow('string-rejection');
    });
  });
});
