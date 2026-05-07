// test/ingest-materializer.test.ts
//
// Tests for DocumentMaterializer: create new document, update existing,
// deleted-document fallthrough to create, field extraction wiring,
// event emitter calls, and warning passthrough.

import { DocumentMaterializer } from '../src/ingest/materialization/materializer';
import type { MaterializeOptions } from '../src/ingest/materialization/materializer';
import type { IngestItem, FileContent, MappingResult } from '../src/ingest/types';
import type { TypedDocumentItem } from '../src/repositories/TypedDocumentRepository';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeItem(overrides: Partial<IngestItem> = {}): IngestItem {
    return {
        ingestId: 'ingest-1',
        sourceId: 'src-1',
        filePath: '/docs/report.txt',
        fileHash: 'abc',
        mimeType: 'text/plain',
        fileSize: 100,
        status: 'queued',
        mappedTypeId: null,
        mappingConfidence: null,
        mappingMethod: null,
        documentId: null,
        pipelineRunId: null,
        error: null,
        attempts: 0,
        createdAt: '2026-01-01T00:00:00Z',
        updatedAt: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeContent(text: string): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

function makeMapping(overrides: Partial<MappingResult> = {}): MappingResult {
    return {
        targetTypeId: 'type-report',
        fieldMapping: { content: 'body' },
        method: 'rule',
        confidence: 1.0,
        ...overrides,
    };
}

function makeOptions(overrides: Partial<MaterializeOptions> = {}): MaterializeOptions {
    return { existingDocumentId: null, createdBy: 'ingest:src-1', ...overrides };
}

function makeRepo(existing: TypedDocumentItem | null = null) {
    const store = new Map<string, TypedDocumentItem>();
    if (existing) store.set(existing.documentId, existing);
    return {
        store,
        get: jest.fn(async (id: string) => store.get(id) ?? null),
        create: jest.fn(async (doc: TypedDocumentItem) => { store.set(doc.documentId, doc); }),
    };
}

function makeEmitter() {
    const events: unknown[] = [];
    return {
        events,
        emit: jest.fn((ev: unknown) => { events.push(ev); }),
    };
}

// ─── create new document ──────────────────────────────────────────────────

describe('DocumentMaterializer — create', () => {
    test('returns created=true and a documentId', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(makeItem(), makeContent('hello'), makeMapping(), makeOptions());
        expect(result.created).toBe(true);
        expect(typeof result.documentId).toBe('string');
        expect(result.documentId.length).toBeGreaterThan(0);
    });

    test('calls typedDocumentRepo.create with correct typeId', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        await mat.materialize(makeItem(), makeContent('text'), makeMapping({ targetTypeId: 'my-type' }), makeOptions());
        const created = repo.create.mock.calls[0][0] as TypedDocumentItem;
        expect(created.typeId).toBe('my-type');
    });

    test('stamps createdBy from options', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        await mat.materialize(makeItem(), makeContent('x'), makeMapping(), makeOptions({ createdBy: 'ingest:src-99' }));
        const created = repo.create.mock.calls[0][0] as TypedDocumentItem;
        expect(created.createdBy).toBe('ingest:src-99');
    });

    test('emits document.created event', async () => {
        const repo = makeRepo();
        const emitter = makeEmitter();
        const mat = new DocumentMaterializer(repo as never, emitter);
        const result = await mat.materialize(makeItem(), makeContent('text'), makeMapping(), makeOptions());
        expect(emitter.emit).toHaveBeenCalledTimes(1);
        const ev = emitter.events[0] as { type: string; documentId: string; typeId: string };
        expect(ev.type).toBe('document.created');
        expect(ev.documentId).toBe(result.documentId);
        expect(ev.typeId).toBe('type-report');
    });

    test('does not call emitter when eventEmitter is not provided', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        await expect(mat.materialize(makeItem(), makeContent('text'), makeMapping(), makeOptions())).resolves.toBeDefined();
    });
});

// ─── update existing document ─────────────────────────────────────────────

describe('DocumentMaterializer — update', () => {
    function makeExisting(docId: string): TypedDocumentItem {
        return {
            documentId: docId,
            typeId: 'type-report',
            values: { oldField: 'old value' },
            createdBy: 'ingest:src-1',
            createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-01-01T00:00:00Z',
        };
    }

    test('returns created=false when existing document is found', async () => {
        const existing = makeExisting('doc-existing');
        const repo = makeRepo(existing);
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(),
            makeContent('new text'),
            makeMapping(),
            makeOptions({ existingDocumentId: 'doc-existing' }),
        );
        expect(result.created).toBe(false);
        expect(result.documentId).toBe('doc-existing');
    });

    test('merges new values with existing values', async () => {
        const existing = makeExisting('doc-merge');
        const repo = makeRepo(existing);
        const mat = new DocumentMaterializer(repo as never);
        await mat.materialize(
            makeItem(),
            makeContent('updated text'),
            makeMapping({ fieldMapping: { content: 'body' } }),
            makeOptions({ existingDocumentId: 'doc-merge' }),
        );
        const saved = repo.store.get('doc-merge')!;
        expect(saved.values.oldField).toBe('old value'); // preserved
        expect(saved.values.body).toBe('updated text'); // added
    });

    test('emits document.updated event', async () => {
        const existing = makeExisting('doc-update');
        const repo = makeRepo(existing);
        const emitter = makeEmitter();
        const mat = new DocumentMaterializer(repo as never, emitter);
        await mat.materialize(makeItem(), makeContent('x'), makeMapping(), makeOptions({ existingDocumentId: 'doc-update' }));
        const ev = emitter.events[0] as { type: string };
        expect(ev.type).toBe('document.updated');
    });
});

// ─── deleted-document fallthrough ────────────────────────────────────────

describe('DocumentMaterializer — deleted document fallthrough', () => {
    test('creates new document when existingDocumentId not found in repo', async () => {
        const repo = makeRepo(null); // repo is empty
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(
            makeItem(),
            makeContent('text'),
            makeMapping(),
            makeOptions({ existingDocumentId: 'doc-deleted' }),
        );
        expect(result.created).toBe(true);
        expect(result.documentId).not.toBe('doc-deleted'); // new UUID
    });
});

// ─── extraction warnings ──────────────────────────────────────────────────

describe('DocumentMaterializer — warnings', () => {
    test('passes through extraction warnings', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        // JSON mimeType with non-existent field → extractFields emits warning
        const content: FileContent = { data: Buffer.from('{"x":1}', 'utf-8'), encoding: 'utf-8', parsed: { x: 1 } };
        const result = await mat.materialize(
            makeItem({ mimeType: 'application/json' }),
            content,
            makeMapping({ fieldMapping: { 'missing.path': 'docField' } }),
            makeOptions(),
        );
        expect(result.warnings).toHaveLength(1);
        expect(result.warnings[0]).toContain('"missing.path"');
    });

    test('returns empty warnings array when extraction is clean', async () => {
        const repo = makeRepo();
        const mat = new DocumentMaterializer(repo as never);
        const result = await mat.materialize(makeItem(), makeContent('clean'), makeMapping(), makeOptions());
        expect(result.warnings).toEqual([]);
    });
});
