// test/ingest-mapping.test.ts
//
// Tests for ingest/mapping/rule-matcher and ingest/materialization/field-extractor.
// Both are pure functions with no external dependencies.

import { matchByRules } from '../src/ingest/mapping/rule-matcher';
import { extractFields } from '../src/ingest/materialization/field-extractor';
import type { FileDescriptor, MappingRule, FileContent } from '../src/ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
    return {
        path: '/docs/report.pdf',
        name: 'report.pdf',
        hash: 'abc123',
        size: 1024,
        mimeType: 'application/pdf',
        lastModified: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeRule(overrides: Partial<MappingRule> = {}): MappingRule {
    return {
        ruleId: 'r1',
        condition: { field: 'extension', op: 'eq', value: 'pdf' },
        targetTypeId: 'type-pdf',
        fieldMapping: { title: 'f-title' },
        priority: 10,
        ...overrides,
    };
}

function makeContent(data: string, parsed?: unknown): FileContent {
    return { data: Buffer.from(data, 'utf-8'), encoding: 'utf-8', parsed };
}

// ─── matchByRules ─────────────────────────────────────────────────────────────

describe('matchByRules — no rules / no match', () => {
    test('returns null when rules array is empty', () => {
        expect(matchByRules(makeFile(), [])).toBeNull();
    });

    test('returns null when no rule matches', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'csv' } });
        expect(matchByRules(makeFile({ name: 'report.pdf' }), [rule])).toBeNull();
    });
});

describe('matchByRules — field selectors', () => {
    test('extension field: extracts lowercased extension', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        expect(matchByRules(makeFile({ name: 'REPORT.PDF' }), [rule])).not.toBeNull();
    });

    test('extension field: returns null for file with no dot', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        expect(matchByRules(makeFile({ name: 'nodotfile' }), [rule])).toBeNull();
    });

    test('path field: matches against full path', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'contains', value: '/docs/' } });
        expect(matchByRules(makeFile({ path: '/docs/report.pdf' }), [rule])).not.toBeNull();
    });

    test('mimeType field: matches exact mime type', () => {
        const rule = makeRule({ condition: { field: 'mimeType', op: 'eq', value: 'text/csv' } });
        expect(matchByRules(makeFile({ mimeType: 'text/csv' }), [rule])).not.toBeNull();
    });

    test('name field: matches file name', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'startsWith', value: 'report' } });
        expect(matchByRules(makeFile({ name: 'report.pdf' }), [rule])).not.toBeNull();
    });

    test('tag field: joins tags with comma for contains checks', () => {
        const rule = makeRule({ condition: { field: 'tag', op: 'contains', value: 'finance' } });
        expect(matchByRules(makeFile({ tags: ['finance', 'q1'] }), [rule])).not.toBeNull();
    });

    test('tag field: returns null when tags array is absent', () => {
        const rule = makeRule({ condition: { field: 'tag', op: 'contains', value: 'finance' } });
        const file = makeFile({ tags: undefined });
        expect(matchByRules(file, [rule])).toBeNull();
    });
});

describe('matchByRules — operators', () => {
    test('eq: exact match', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        expect(matchByRules(makeFile({ name: 'a.pdf' }), [rule])).not.toBeNull();
        expect(matchByRules(makeFile({ name: 'a.csv' }), [rule])).toBeNull();
    });

    test('contains: substring match', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'contains', value: 'report' } });
        expect(matchByRules(makeFile({ name: 'annual-report-2025.pdf' }), [rule])).not.toBeNull();
        expect(matchByRules(makeFile({ name: 'invoice.pdf' }), [rule])).toBeNull();
    });

    test('startsWith: prefix match', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'startsWith', value: '/archive/' } });
        expect(matchByRules(makeFile({ path: '/archive/2025/doc.pdf' }), [rule])).not.toBeNull();
        expect(matchByRules(makeFile({ path: '/docs/doc.pdf' }), [rule])).toBeNull();
    });

    test('regex: pattern match', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'regex', value: '^report-\\d{4}' } });
        expect(matchByRules(makeFile({ name: 'report-2025.pdf' }), [rule])).not.toBeNull();
        expect(matchByRules(makeFile({ name: 'report-abc.pdf' }), [rule])).toBeNull();
    });

    test('unknown op returns no match', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'unknown' as never, value: 'pdf' } });
        expect(matchByRules(makeFile(), [rule])).toBeNull();
    });
});

describe('matchByRules — priority ordering', () => {
    test('lower priority number wins when both rules match', () => {
        const hi = makeRule({ ruleId: 'hi', priority: 1, targetTypeId: 'type-hi', condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        const lo = makeRule({ ruleId: 'lo', priority: 5, targetTypeId: 'type-lo', condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        const result = matchByRules(makeFile({ name: 'doc.pdf' }), [lo, hi]); // pass lo first
        expect(result?.targetTypeId).toBe('type-hi');
        expect(result?.ruleId).toBe('hi');
    });

    test('returns the correct MappingResult shape on match', () => {
        const rule = makeRule({ ruleId: 'r-test', targetTypeId: 'type-x', fieldMapping: { a: 'b' } });
        const result = matchByRules(makeFile({ name: 'doc.pdf' }), [rule]);
        expect(result).toEqual({
            targetTypeId: 'type-x',
            fieldMapping: { a: 'b' },
            method: 'rule',
            confidence: 1.0,
            ruleId: 'r-test',
        });
    });
});

// ─── extractFields — JSON ─────────────────────────────────────────────────────

describe('extractFields — JSON', () => {
    test('extracts top-level field by key', () => {
        const content = makeContent('{"title":"Hello","author":"Alice"}');
        const result = extractFields(content, { title: 'f-title' }, 'application/json');
        expect(result.fields['f-title']).toBe('Hello');
        expect(result.warnings).toHaveLength(0);
    });

    test('resolves dot-path for nested fields', () => {
        const content = makeContent('{"meta":{"category":"News"}}');
        const result = extractFields(content, { 'meta.category': 'f-cat' }, 'application/json');
        expect(result.fields['f-cat']).toBe('News');
    });

    test('warns when dot-path field is missing', () => {
        const content = makeContent('{"a":1}');
        const result = extractFields(content, { 'b.c': 'f-x' }, 'application/json');
        expect(result.warnings[0]).toContain('"b.c"');
        expect(result.fields['f-x']).toBeUndefined();
    });

    test('uses pre-parsed content when provided', () => {
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8', parsed: { val: 42 } };
        const result = extractFields(content, { val: 'f-val' }, 'application/json');
        expect(result.fields['f-val']).toBe(42);
    });

    test('returns warning on unparseable JSON with no pre-parsed', () => {
        const content = makeContent('not-json');
        const result = extractFields(content, { x: 'f-x' }, 'application/json');
        expect(result.warnings[0]).toContain('Failed to parse JSON');
        expect(result.fields).toEqual({});
    });

    test('json variant mime type also routes to JSON extractor', () => {
        const content = makeContent('{"k":"v"}');
        const result = extractFields(content, { k: 'f-k' }, 'application/json; charset=utf-8');
        expect(result.fields['f-k']).toBe('v');
    });
});

// ─── extractFields — CSV ──────────────────────────────────────────────────────

describe('extractFields — CSV', () => {
    test('extracts column from first parsed row', () => {
        const rows = [{ Title: 'My Doc', Author: 'Bob' }];
        const content = makeContent('', rows);
        const result = extractFields(content, { Title: 'f-title', Author: 'f-author' }, 'text/csv');
        expect(result.fields['f-title']).toBe('My Doc');
        expect(result.fields['f-author']).toBe('Bob');
    });

    test('warns when column is not in the row', () => {
        const rows = [{ Name: 'X' }];
        const content = makeContent('', rows);
        const result = extractFields(content, { Missing: 'f-m' }, 'text/csv');
        expect(result.warnings[0]).toContain('"Missing"');
    });

    test('returns warning when no rows', () => {
        const content = makeContent('', []);
        const result = extractFields(content, { x: 'f-x' }, 'text/csv');
        expect(result.warnings[0]).toContain('no parsed rows');
        expect(result.fields).toEqual({});
    });

    test('returns warning when parsed is not an array', () => {
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8', parsed: { notArray: true } };
        const result = extractFields(content, { x: 'f-x' }, 'text/csv');
        expect(result.warnings[0]).toContain('no parsed rows');
    });
});

// ─── extractFields — text / markdown ─────────────────────────────────────────

describe('extractFields — text/plain and text/markdown', () => {
    test('content field maps entire text', () => {
        const content = makeContent('Hello world\nSecond line');
        const result = extractFields(content, { content: 'f-body' }, 'text/plain');
        expect(result.fields['f-body']).toBe('Hello world\nSecond line');
    });

    test('title field extracts markdown heading', () => {
        const content = makeContent('# My Heading\nSome text');
        const result = extractFields(content, { title: 'f-title' }, 'text/markdown');
        expect(result.fields['f-title']).toBe('My Heading');
    });

    test('title field falls back to first non-empty line when no heading', () => {
        const content = makeContent('\nFirst real line\nSecond line');
        const result = extractFields(content, { title: 'f-title' }, 'text/plain');
        expect(result.fields['f-title']).toBe('First real line');
    });

    test('title warns when content is all blank lines', () => {
        const content = makeContent('   \n\n   ');
        const result = extractFields(content, { title: 'f-title' }, 'text/plain');
        expect(result.warnings[0]).toContain('No title found');
    });

    test('warns on unsupported source field for text', () => {
        const content = makeContent('some text');
        const result = extractFields(content, { unknown_field: 'f-x' }, 'text/plain');
        expect(result.warnings[0]).toContain('"unknown_field"');
        expect(result.warnings[0]).toContain('only "content" and "title"');
    });
});

// ─── extractFields — unsupported mime type ────────────────────────────────────

describe('extractFields — unsupported mime type', () => {
    test('returns warning for application/pdf and empty fields', () => {
        const content = makeContent('binary data');
        const result = extractFields(content, { x: 'f-x' }, 'application/pdf');
        expect(result.warnings[0]).toContain('Unsupported mimeType');
        expect(result.warnings[0]).toContain('application/pdf');
        expect(result.fields).toEqual({});
    });
});
