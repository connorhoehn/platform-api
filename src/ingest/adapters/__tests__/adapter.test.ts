// social-api/src/ingest/adapters/__tests__/adapter.test.ts
//
// Tests for createAdapter factory: correct adapter class per source type,
// error on unsupported type, and required interface methods.

import { createAdapter } from '../adapter';
import { MockLocalAdapter } from '../mock-local';
import { MockWebhookAdapter } from '../mock-webhook';
import { MockDriveAdapter } from '../mock-drive';
import type { IngestSource } from '../../types';

function makeSource(type: string): IngestSource {
  return {
    sourceId: 'src-test-1',
    name: 'Test Source',
    type,
    config: {},
    syncSchedule: null,
    mappingRules: [],
    defaultPipelineId: null,
    status: 'active',
    lastSyncAt: null,
    lastSyncStats: null,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
  } as IngestSource;
}

describe('createAdapter', () => {
  test('returns MockLocalAdapter instance for type "local"', () => {
    const adapter = createAdapter(makeSource('local'));
    expect(adapter).toBeInstanceOf(MockLocalAdapter);
  });

  test('returns MockWebhookAdapter instance for type "webhook"', () => {
    const adapter = createAdapter(makeSource('webhook'));
    expect(adapter).toBeInstanceOf(MockWebhookAdapter);
  });

  test('returns MockDriveAdapter instance for type "google-drive"', () => {
    const adapter = createAdapter(makeSource('google-drive'));
    expect(adapter).toBeInstanceOf(MockDriveAdapter);
  });

  test('throws for unknown type and includes the type string in the message', () => {
    expect(() => createAdapter(makeSource('confluence'))).toThrow('confluence');
    expect(() => createAdapter(makeSource('jira'))).toThrow('jira');
    expect(() => createAdapter(makeSource('unknown-xyz'))).toThrow('unknown-xyz');
  });

  test('returned local adapter has required list and fetch methods', () => {
    const adapter = createAdapter(makeSource('local'));
    expect(typeof adapter.list).toBe('function');
    expect(typeof adapter.fetch).toBe('function');
  });

  test('returned webhook adapter has required list and fetch methods', () => {
    const adapter = createAdapter(makeSource('webhook'));
    expect(typeof adapter.list).toBe('function');
    expect(typeof adapter.fetch).toBe('function');
  });

  test('returned drive adapter has required list and fetch methods', () => {
    const adapter = createAdapter(makeSource('google-drive'));
    expect(typeof adapter.list).toBe('function');
    expect(typeof adapter.fetch).toBe('function');
  });
});
