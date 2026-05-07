// social-api/src/lib/__tests__/ddbTableName.test.ts
//
// Tests for tableName(): prefix from DDB_TABLE_PREFIX env var prepended to
// base name; returns bare name when env var is unset.

const origEnv = process.env;

afterEach(() => {
  process.env = { ...origEnv };
});

afterAll(() => {
  process.env = origEnv;
});

// Re-import after each env change is NOT needed — tableName reads
// process.env at call time (not at module load time), so we can simply
// set the env var before calling the function.

import { tableName } from '../ddb-table-name';

describe('tableName', () => {
  it('returns the bare name when DDB_TABLE_PREFIX is unset', () => {
    delete process.env.DDB_TABLE_PREFIX;
    expect(tableName('social-profiles')).toBe('social-profiles');
  });

  it('prepends the prefix when DDB_TABLE_PREFIX is set', () => {
    process.env.DDB_TABLE_PREFIX = 'gateway-';
    expect(tableName('social-profiles')).toBe('gateway-social-profiles');
  });

  it('returns just the prefix when base is empty string', () => {
    process.env.DDB_TABLE_PREFIX = 'pfx-';
    expect(tableName('')).toBe('pfx-');
  });

  it('returns the bare name unchanged when prefix is empty string', () => {
    process.env.DDB_TABLE_PREFIX = '';
    expect(tableName('pipeline-dlq')).toBe('pipeline-dlq');
  });
});
