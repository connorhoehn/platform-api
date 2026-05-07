// test/ingest-field-extractor.test.ts
//
// Tests for extractFields: JSON dot-path, CSV column, text/markdown extraction,
// unsupported mimeType fallback, and warning messages.

import { extractFields } from '../src/ingest/materialization/field-extractor';
import type { FileContent } from '../src/ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────

function jsonContent(obj: unknown, parsed?: unknown): FileContent {
    return {
        data: Buffer.from(JSON.stringify(obj), 'utf-8'),
        encoding: 'utf-8',
        parsed: parsed ?? obj,
    };
}

function textContent(text: string): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

function csvContent(rows: Record<string, unknown>[]): FileContent {
    return { data: Buffer.from('', 'utf-8'), encoding: 'utf-8', parsed: rows };
}

// ─── JSON extraction ──────────────────────────────────────────────────────

describe('extractFields — JSON', () => {
    test('extracts top-level field', () => {
        const content = jsonContent({ title: 'My Doc', author: 'Alice' });
        const { fields, warnings } = extractFields(content, { title: 'docTitle' }, 'application/json');
        expect(fields.docTitle).toBe('My Doc');
        expect(warnings).toHaveLength(0);
    });

    test('extracts nested field via dot-path', () => {
        const content = jsonContent({ metadata: { title: 'Nested' } });
        const { fields } = extractFields(content, { 'metadata.title': 'docTitle' }, 'application/json');
        expect(fields.docTitle).toBe('Nested');
    });

    test('warns when dot-path field is missing', () => {
        const content = jsonContent({ other: 'x' });
        const { fields, warnings } = extractFields(content, { 'missing.path': 'docField' }, 'application/json');
        expect(fields.docField).toBeUndefined();
        expect(warnings[0]).toContain('"missing.path"');
    });

    test('falls back to parsing raw data when parsed is undefined', () => {
        const raw = Buffer.from('{"score":42}', 'utf-8');
        const content: FileContent = { data: raw, encoding: 'utf-8' };
        const { fields } = extractFields(content, { score: 'numericField' }, 'application/json');
        expect(fields.numericField).toBe(42);
    });

    test('warns and returns empty fields when JSON is malformed', () => {
        const content: FileContent = { data: Buffer.from('not json', 'utf-8'), encoding: 'utf-8' };
        const { fields, warnings } = extractFields(content, { x: 'y' }, 'application/json');
        expect(fields).toEqual({});
        expect(warnings[0]).toContain('Failed to parse JSON');
    });

    test('handles json-variant mimeType via fallback', () => {
        const content = jsonContent({ val: 'ok' });
        const { fields } = extractFields(content, { val: 'result' }, 'application/vnd.api+json');
        expect(fields.result).toBe('ok');
    });
});

// ─── CSV extraction ───────────────────────────────────────────────────────

describe('extractFields — CSV', () => {
    test('extracts column value from first row', () => {
        const content = csvContent([{ amount: '1000', currency: 'USD' }]);
        const { fields, warnings } = extractFields(content, { amount: 'invoiceAmount' }, 'text/csv');
        expect(fields.invoiceAmount).toBe('1000');
        expect(warnings).toHaveLength(0);
    });

    test('warns when column is missing from row', () => {
        const content = csvContent([{ name: 'Alice' }]);
        const { fields, warnings } = extractFields(content, { email: 'contactEmail' }, 'text/csv');
        expect(fields.contactEmail).toBeUndefined();
        expect(warnings[0]).toContain('"email"');
    });

    test('returns warning when parsed is undefined (no rows)', () => {
        const content: FileContent = { data: Buffer.from('', 'utf-8'), encoding: 'utf-8' };
        const { fields, warnings } = extractFields(content, { col: 'field' }, 'text/csv');
        expect(fields).toEqual({});
        expect(warnings[0]).toContain('no parsed rows');
    });

    test('returns warning for empty parsed array', () => {
        const content = csvContent([]);
        const { fields, warnings } = extractFields(content, { col: 'field' }, 'text/csv');
        expect(fields).toEqual({});
        expect(warnings[0]).toContain('no parsed rows');
    });
});

// ─── text / markdown extraction ───────────────────────────────────────────

describe('extractFields — text/plain', () => {
    test('extracts full text via content field', () => {
        const content = textContent('Hello World');
        const { fields } = extractFields(content, { content: 'bodyText' }, 'text/plain');
        expect(fields.bodyText).toBe('Hello World');
    });

    test('extracts title from first non-empty line when no heading', () => {
        const content = textContent('My Title\nBody text here');
        const { fields } = extractFields(content, { title: 'docTitle' }, 'text/plain');
        expect(fields.docTitle).toBe('My Title');
    });

    test('warns when unsupported source field used', () => {
        const content = textContent('text');
        const { warnings } = extractFields(content, { unknown_field: 'target' }, 'text/plain');
        expect(warnings[0]).toContain('"unknown_field"');
        expect(warnings[0]).toContain('Unsupported source field');
    });
});

describe('extractFields — text/markdown', () => {
    test('extracts markdown heading as title (strips # prefix)', () => {
        const content = textContent('# My Heading\nBody content');
        const { fields } = extractFields(content, { title: 'docTitle' }, 'text/markdown');
        expect(fields.docTitle).toBe('My Heading');
    });

    test('extracts h2 heading', () => {
        const content = textContent('## Section Title\nMore text');
        const { fields } = extractFields(content, { title: 'docTitle' }, 'text/markdown');
        expect(fields.docTitle).toBe('Section Title');
    });

    test('warns when text has no non-empty lines for title', () => {
        const content = textContent('   \n\n   ');
        const { warnings } = extractFields(content, { title: 'docTitle' }, 'text/markdown');
        expect(warnings[0]).toContain('No title found');
    });

    test('extracts full content for content field', () => {
        const text = '# Title\n\nBody';
        const content = textContent(text);
        const { fields } = extractFields(content, { content: 'body' }, 'text/markdown');
        expect(fields.body).toBe(text);
    });
});

// ─── unsupported mimeType ────────────────────────────────────────────────

describe('extractFields — unsupported mimeType', () => {
    test('returns empty fields and warning for unknown mimeType', () => {
        const content = textContent('data');
        const { fields, warnings } = extractFields(content, { x: 'y' }, 'image/png');
        expect(fields).toEqual({});
        expect(warnings[0]).toContain('Unsupported mimeType');
        expect(warnings[0]).toContain('"image/png"');
    });
});
