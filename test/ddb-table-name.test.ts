// test/ddb-table-name.test.ts
//
// Tests for tableName: prefix from DDB_TABLE_PREFIX env var.

// Reset modules before each test so env-var changes take effect on re-require.
let tableName: (base: string) => string;

beforeEach(() => {
    jest.resetModules();
    ({ tableName } = require('../src/lib/ddb-table-name'));
});

afterEach(() => {
    delete process.env.DDB_TABLE_PREFIX;
});

describe('tableName', () => {
    test('returns bare name when DDB_TABLE_PREFIX is unset', () => {
        delete process.env.DDB_TABLE_PREFIX;
        jest.resetModules();
        ({ tableName } = require('../src/lib/ddb-table-name'));
        expect(tableName('Users')).toBe('Users');
    });

    test('prepends prefix when DDB_TABLE_PREFIX is set', () => {
        process.env.DDB_TABLE_PREFIX = 'dev-';
        jest.resetModules();
        ({ tableName } = require('../src/lib/ddb-table-name'));
        expect(tableName('Users')).toBe('dev-Users');
    });

    test('works with any base name', () => {
        process.env.DDB_TABLE_PREFIX = 'proj-';
        jest.resetModules();
        ({ tableName } = require('../src/lib/ddb-table-name'));
        expect(tableName('PipelineRuns')).toBe('proj-PipelineRuns');
    });

    test('prefix of empty string returns bare name', () => {
        process.env.DDB_TABLE_PREFIX = '';
        jest.resetModules();
        ({ tableName } = require('../src/lib/ddb-table-name'));
        expect(tableName('Widgets')).toBe('Widgets');
    });
});
