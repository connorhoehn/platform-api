// social-api/src/ingest/materialization/__tests__/field-extractor.test.ts
//
// Tests for extractFields: JSON dot-path, CSV first-row, text/markdown, fallbacks.

import { extractFields } from '../field-extractor';
import type { FileContent } from '../../types';

function textContent(text: string): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

function jsonContent(obj: unknown): FileContent {
    return { data: Buffer.from(JSON.stringify(obj), 'utf-8'), encoding: 'utf-8' };
}

function parsedJsonContent(obj: unknown): FileContent {
    return { data: Buffer.from(JSON.stringify(obj), 'utf-8'), encoding: 'utf-8', parsed: obj };
}

// ─── JSON extraction ──────────────────────────────────────────────────────

describe('extractFields — application/json', () => {
    test('extracts top-level field via dot-path', () => {
        const content = jsonContent({ title: 'Hello World', body: 'content here' });
        const result = extractFields(content, { title: 'name' }, 'application/json');
        expect(result.fields.name).toBe('Hello World');
        expect(result.warnings).toHaveLength(0);
    });

    test('extracts nested field via dot-path', () => {
        const content = jsonContent({ metadata: { author: 'Alice' } });
        const result = extractFields(content, { 'metadata.author': 'authorId' }, 'application/json');
        expect(result.fields.authorId).toBe('Alice');
    });

    test('warns on missing field', () => {
        const content = jsonContent({ title: 'Hello' });
        const result = extractFields(content, { missing: 'target' }, 'application/json');
        expect(result.warnings).toHaveLength(1);
        expect(result.warnings[0]).toContain('"missing"');
    });

    test('uses pre-parsed content when provided', () => {
        const obj = { id: 42 };
        const content = parsedJsonContent(obj);
        const result = extractFields(content, { id: 'docId' }, 'application/json');
        expect(result.fields.docId).toBe(42);
    });

    test('warns and returns empty fields on invalid JSON', () => {
        const content: FileContent = { data: Buffer.from('not json'), encoding: 'utf-8' };
        const result = extractFields(content, { title: 'name' }, 'application/json');
        expect(result.fields).toEqual({});
        expect(result.warnings[0]).toContain('Failed to parse JSON');
    });

    test('handles array values in JSON', () => {
        const content = jsonContent({ tags: ['a', 'b', 'c'] });
        const result = extractFields(content, { tags: 'labels' }, 'application/json');
        expect(result.fields.labels).toEqual(['a', 'b', 'c']);
    });
});

// ─── CSV extraction ───────────────────────────────────────────────────────

describe('extractFields — text/csv', () => {
    test('extracts column from first parsed row', () => {
        const rows = [{ name: 'Alice', age: '30' }, { name: 'Bob', age: '25' }];
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8', parsed: rows };
        const result = extractFields(content, { name: 'fullName' }, 'text/csv');
        expect(result.fields.fullName).toBe('Alice');
    });

    test('warns for missing column in CSV row', () => {
        const rows = [{ name: 'Alice' }];
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8', parsed: rows };
        const result = extractFields(content, { title: 'docTitle' }, 'text/csv');
        expect(result.warnings[0]).toContain('"title"');
    });

    test('returns warning when parsed is undefined', () => {
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8' };
        const result = extractFields(content, { name: 'n' }, 'text/csv');
        expect(result.warnings[0]).toContain('no parsed rows');
        expect(result.fields).toEqual({});
    });

    test('returns warning when parsed rows array is empty', () => {
        const content: FileContent = { data: Buffer.alloc(0), encoding: 'utf-8', parsed: [] };
        const result = extractFields(content, { name: 'n' }, 'text/csv');
        expect(result.warnings[0]).toContain('no parsed rows');
    });
});

// ─── text/plain extraction ────────────────────────────────────────────────

describe('extractFields — text/plain', () => {
    test('content field maps entire text', () => {
        const text = 'Hello, world!';
        const result = extractFields(textContent(text), { content: 'body' }, 'text/plain');
        expect(result.fields.body).toBe(text);
    });

    test('title field extracts first non-empty line when no markdown heading', () => {
        const text = '\nFirst real line\nSecond line';
        const result = extractFields(textContent(text), { title: 'name' }, 'text/plain');
        expect(result.fields.name).toBe('First real line');
    });

    test('warns for unsupported source field in text', () => {
        const result = extractFields(textContent('hello'), { unknown: 'target' }, 'text/plain');
        expect(result.warnings[0]).toContain('"unknown"');
    });
});

// ─── text/markdown extraction ─────────────────────────────────────────────

describe('extractFields — text/markdown', () => {
    test('title field extracts h1 heading text', () => {
        const text = '# My Document\nSome body text';
        const result = extractFields(textContent(text), { title: 'name' }, 'text/markdown');
        expect(result.fields.name).toBe('My Document');
    });

    test('title field extracts h2 heading text', () => {
        const text = '## Section Title\nContent';
        const result = extractFields(textContent(text), { title: 'name' }, 'text/markdown');
        expect(result.fields.name).toBe('Section Title');
    });

    test('warns when text is completely empty (no title found)', () => {
        const result = extractFields(textContent('\n\n'), { title: 'name' }, 'text/markdown');
        expect(result.warnings[0]).toContain('No title found');
    });

    test('content field maps entire markdown text', () => {
        const text = '# Heading\nBody';
        const result = extractFields(textContent(text), { content: 'rawText' }, 'text/markdown');
        expect(result.fields.rawText).toBe(text);
    });
});

// ─── mimeType fallbacks ───────────────────────────────────────────────────

describe('extractFields — mimeType fallbacks', () => {
    test('application/json variant falls through to JSON extraction', () => {
        const content = jsonContent({ key: 'val' });
        const result = extractFields(content, { key: 'k' }, 'application/ld+json');
        expect(result.fields.k).toBe('val');
    });

    test('unsupported mimeType returns warning and empty fields', () => {
        const content: FileContent = { data: Buffer.from('data'), encoding: 'utf-8' };
        const result = extractFields(content, { x: 'y' }, 'image/png');
        expect(result.fields).toEqual({});
        expect(result.warnings[0]).toContain('image/png');
    });
});
