// Unit tests for the VIDEO_SESSIONS_STORE feature flag factory. Exercises
// the flag-resolution logic + verifies the right concrete class is returned.

import { DynamoDBDocumentClient } from '@aws-sdk/lib-dynamodb';
import {
  createVideoSessionStore,
  resolveVideoSessionStoreMode,
} from '../videoSessionStoreFactory';
import { VideoSessionRepository } from '../VideoSessionRepository';
import { MemoryVideoSessionStore } from '../MemoryVideoSessionStore';

// Stub docClient; never called by either store in these tests so its
// internals don't matter — `as unknown as` to satisfy the type signature.
const stubClient = { send: jest.fn() } as unknown as DynamoDBDocumentClient;

describe('resolveVideoSessionStoreMode', () => {
  it('returns "ddb" by default when VIDEO_SESSIONS_STORE is unset', () => {
    expect(resolveVideoSessionStoreMode({})).toBe('ddb');
  });

  it('returns "memory" when explicitly set', () => {
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: 'memory' })).toBe('memory');
  });

  it('returns "ddb" when explicitly set', () => {
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: 'ddb' })).toBe('ddb');
  });

  it('is case-insensitive and trims whitespace', () => {
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: '  Memory ' })).toBe('memory');
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: 'DDB' })).toBe('ddb');
  });

  it('falls through to "ddb" on unknown values (no silent demotion to memory)', () => {
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: 'redis' })).toBe('ddb');
    expect(resolveVideoSessionStoreMode({ VIDEO_SESSIONS_STORE: '' })).toBe('ddb');
  });
});

describe('createVideoSessionStore', () => {
  it('returns a VideoSessionRepository by default', () => {
    const store = createVideoSessionStore(stubClient, {});
    expect(store).toBeInstanceOf(VideoSessionRepository);
  });

  it('returns a MemoryVideoSessionStore when flag = memory', () => {
    const store = createVideoSessionStore(stubClient, { VIDEO_SESSIONS_STORE: 'memory' });
    expect(store).toBeInstanceOf(MemoryVideoSessionStore);
  });

  it('returns a fresh instance on each call', () => {
    const a = createVideoSessionStore(stubClient, { VIDEO_SESSIONS_STORE: 'memory' });
    const b = createVideoSessionStore(stubClient, { VIDEO_SESSIONS_STORE: 'memory' });
    expect(a).not.toBe(b);
  });
});
