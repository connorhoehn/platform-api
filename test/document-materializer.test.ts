// test/document-materializer.test.ts
//
// Tests for DocumentMaterializer.materialize: create path, update path,
// fallthrough-to-create when existing doc gone, event emission, warning
// passthrough, and no-eventEmitter path.

import { DocumentMaterializer } from '../src/ingest/materialization/materializer';
import type { MaterializeOptions } from '../src/ingest/materialization/materializer';
import type { IngestItem, FileContent, MappingResult } from '../src/ingest/types';
import type { TypedDocumentItem } from '../src/repositories/TypedDocumentRepository';

// ─── helpers ──────────────────────────────────────────────────────────────────

function makeRepo(existing: TypedDocumentItem | null = null) {
    return {
        created: [] as TypedDocumentItem[],
        get: jest.fn(async () => existing),
        create: jest.fn(async (item: TypedDocumentItem) => { makeRepo.prototype; }),
    };
}

function makeMockRepo(existing: TypedDocumentItem | null = null) {
    const store: TypedDocumentItem[] = [];
    return {
        get: jest.fn(async (_id: string) => existing),
        create: jest.fn(async (item: TypedDocumentItem) => { store.push(item); }),
        _store: store,
    };
}

function makeItem(overrides: Partial<IngestItem> = {}): IngestItem {
    return {
        ingestId: 'ing-1',
        sourceId: 'src-1',
        filePath: '/docs/report.json',
        fileHash: 'abc',
        mimeType: 'application/json',
        fileSize: 100,
        status: 'materializing',
        mappedTypeId: 'type-1',
        mappingConfidence: 1.0,
        mappingMethod: 'rule',
        documentId: null,
        pipelineRunId: null,
        error: null,
        attempts: 1,
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeContent(jsonObj: unknown): FileContent {
    return {
        data: Buffer.from(JSON.stringify(jsonObj), 'utf-8'),
        encoding: 'utf-8',
    };
}

function makeMapping(overrides: Partial<MappingResult> = {}): MappingResult {
    return {
        targetTypeId: 'type-1',
        fieldMapping: { title: 'f-title' },
        method: 'rule',
        confidence: 1.0,
        ruleId: 'r1',
        ...overrides,
    };
}

function makeOptions(overrides: Partial<MaterializeOptions> = {}): MaterializeOptions {
    return {
        existingDocumentId: null,
        createdBy: 'system',
        ...overrides,
    };
}

function makeExisting(documentId: string): TypedDocumentItem {
    return {
        documentId,
        typeId: 'type-1',
        values: { 'f-title': 'Old Title' },
        createdBy: 'system',
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
    };
}

// ─── create path ──────────────────────────────────────────────────────────────

describe('DocumentMaterializer — create path', () => {
    test('returns created:true and a non-empty documentId', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(), makeContent({ title: 'Hello' }), makeMapping(), makeOptions(),
        );
        expect(result.created).toBe(true);
        expect(result.documentId).toBeTruthy();
        expect(typeof result.documentId).toBe('string');
    });

    test('calls repo.create with extracted field values', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        await mat.materialize(
            makeItem(), makeContent({ title: 'My Doc' }),
            makeMapping({ fieldMapping: { title: 'f-title' } }),
            makeOptions({ createdBy: 'tester' }),
        );
        const saved = repo._store[0];
        expect(saved.values['f-title']).toBe('My Doc');
        expect(saved.createdBy).toBe('tester');
        expect(saved.typeId).toBe('type-1');
    });

    test('each call generates a different documentId', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        const r1 = await mat.materialize(makeItem(), makeContent({ title: 'A' }), makeMapping(), makeOptions());
        const r2 = await mat.materialize(makeItem(), makeContent({ title: 'B' }), makeMapping(), makeOptions());
        expect(r1.documentId).not.toBe(r2.documentId);
    });

    test('emits document.created event with correct typeId', async () => {
        const repo = makeMockRepo(null);
        const emitter = { emit: jest.fn() };
        const mat = new DocumentMaterializer(repo as never, emitter);
        const result = await mat.materialize(
            makeItem(), makeContent({ title: 'Hi' }), makeMapping({ targetTypeId: 'type-x' }), makeOptions(),
        );
        expect(emitter.emit).toHaveBeenCalledTimes(1);
        const evt = emitter.emit.mock.calls[0][0];
        expect(evt.type).toBe('document.created');
        expect(evt.documentId).toBe(result.documentId);
        expect(evt.typeId).toBe('type-x');
    });

    test('works without an eventEmitter (no crash)', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        await expect(
            mat.materialize(makeItem(), makeContent({ title: 'Hi' }), makeMapping(), makeOptions()),
        ).resolves.toBeDefined();
    });
});

// ─── update path ──────────────────────────────────────────────────────────────

describe('DocumentMaterializer — update path', () => {
    test('returns created:false when existing document found', async () => {
        const existing = makeExisting('doc-existing');
        const repo = makeMockRepo(existing);
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(), makeContent({ title: 'New Title' }), makeMapping(),
            makeOptions({ existingDocumentId: 'doc-existing' }),
        );
        expect(result.created).toBe(false);
        expect(result.documentId).toBe('doc-existing');
    });

    test('merges new field values into existing document on update', async () => {
        const existing = makeExisting('doc-e');
        const repo = makeMockRepo(existing);
        const mat = new DocumentMaterializer(repo as never);
        await mat.materialize(
            makeItem(), makeContent({ title: 'Updated' }), makeMapping(),
            makeOptions({ existingDocumentId: 'doc-e' }),
        );
        const saved = repo._store[0];
        expect(saved.values['f-title']).toBe('Updated');
    });

    test('emits document.updated event on update', async () => {
        const existing = makeExisting('doc-upd');
        const repo = makeMockRepo(existing);
        const emitter = { emit: jest.fn() };
        const mat = new DocumentMaterializer(repo as never, emitter);
        await mat.materialize(
            makeItem(), makeContent({ title: 'X' }), makeMapping(),
            makeOptions({ existingDocumentId: 'doc-upd' }),
        );
        const evt = emitter.emit.mock.calls[0][0];
        expect(evt.type).toBe('document.updated');
        expect(evt.documentId).toBe('doc-upd');
    });

    test('falls through to create when existingDocumentId provided but doc not found', async () => {
        const repo = makeMockRepo(null); // get returns null → doc gone
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(), makeContent({ title: 'Hi' }), makeMapping(),
            makeOptions({ existingDocumentId: 'doc-gone' }),
        );
        expect(result.created).toBe(true);
        expect(result.documentId).not.toBe('doc-gone');
    });
});

// ─── warnings passthrough ─────────────────────────────────────────────────────

describe('DocumentMaterializer — warnings passthrough', () => {
    test('forwards extraction warnings in result', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        // JSON with missing field → field-extractor warns
        const result = await mat.materialize(
            makeItem(), makeContent({ other: 'x' }),
            makeMapping({ fieldMapping: { missing_key: 'f-m' } }),
            makeOptions(),
        );
        expect(result.warnings.length).toBeGreaterThan(0);
        expect(result.warnings[0]).toContain('"missing_key"');
    });

    test('returns empty warnings array when extraction is clean', async () => {
        const repo = makeMockRepo(null);
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(), makeContent({ title: 'Clean' }), makeMapping(), makeOptions(),
        );
        expect(result.warnings).toEqual([]);
    });
});
