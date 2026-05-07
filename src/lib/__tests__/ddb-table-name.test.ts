// social-api/src/lib/__tests__/ddb-table-name.test.ts
//
// Tests for tableName helper: prefix applied/absent based on env var.

beforeEach(() => {
    jest.resetModules();
    delete process.env.DDB_TABLE_PREFIX;
});

afterEach(() => {
    delete process.env.DDB_TABLE_PREFIX;
});

function load() {
    return require('../ddb-table-name') as typeof import('../ddb-table-name');
}

describe('tableName', () => {
    test('returns bare name when DDB_TABLE_PREFIX is not set', () => {
        const { tableName } = load();
        expect(tableName('my-table')).toBe('my-table');
    });

    test('prepends DDB_TABLE_PREFIX when set', () => {
        process.env.DDB_TABLE_PREFIX = 'wsg-';
        const { tableName } = load();
        expect(tableName('my-table')).toBe('wsg-my-table');
    });

    test('prepends empty string prefix (no-op)', () => {
        process.env.DDB_TABLE_PREFIX = '';
        const { tableName } = load();
        expect(tableName('users')).toBe('users');
    });

    test('different base names all get the same prefix', () => {
        process.env.DDB_TABLE_PREFIX = 'dev-';
        const { tableName } = load();
        expect(tableName('documents')).toBe('dev-documents');
        expect(tableName('sessions')).toBe('dev-sessions');
        expect(tableName('events')).toBe('dev-events');
    });
});

export {};
