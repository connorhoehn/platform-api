// social-api/src/ingest/materialization/__tests__/materializer.test.ts
//
// Tests for DocumentMaterializer: create, update, event emission, field pass-through.

import { DocumentMaterializer, type MaterializeOptions, type DocumentEventEmitter } from '../materializer';
import type { TypedDocumentRepository, TypedDocumentItem } from '../../../repositories/TypedDocumentRepository';
import type { IngestItem, FileContent, MappingResult } from '../../types';

// ─── Helpers ───────────────────────────────────────────────────────────────

function makeDocRepo(existing: TypedDocumentItem | null = null) {
    return {
        store: new Map<string, TypedDocumentItem>(),
        get: jest.fn(async (id: string) => {
            if (existing && existing.documentId === id) return existing;
            return null;
        }),
        create: jest.fn(async (doc: TypedDocumentItem) => {}),
    } as unknown as TypedDocumentRepository;
}

function makeEventEmitter(): DocumentEventEmitter & { events: unknown[] } {
    const events: unknown[] = [];
    return {
        events,
        emit: jest.fn((event) => { events.push(event); }),
    };
}

function makeItem(overrides: Partial<IngestItem> = {}): IngestItem {
    return {
        ingestId: 'item-1',
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

function makeContent(text = 'Hello world'): FileContent {
    return { data: Buffer.from(text, 'utf-8'), encoding: 'utf-8' };
}

function makeMapping(overrides: Partial<MappingResult> = {}): MappingResult {
    return {
        targetTypeId: 'doc-type',
        fieldMapping: { content: 'body' },
        method: 'rule',
        confidence: 1.0,
        ruleId: 'r1',
        ...overrides,
    };
}

const defaultOpts: MaterializeOptions = {
    existingDocumentId: null,
    createdBy: 'user-1',
};

// ─── create path ──────────────────────────────────────────────────────────

describe('DocumentMaterializer — create', () => {
    test('creates a new document and returns created=true', async () => {
        const repo = makeDocRepo();
        const mat = new DocumentMaterializer(repo);
        const result = await mat.materialize(makeItem(), makeContent(), makeMapping(), defaultOpts);
        expect(result.created).toBe(true);
        expect(typeof result.documentId).toBe('string');
        expect(result.documentId).toHaveLength(36); // UUID
    });

    test('calls typedDocumentRepo.create on new document', async () => {
        const repo = makeDocRepo();
        const mat = new DocumentMaterializer(repo);
        await mat.materialize(makeItem(), makeContent(), makeMapping(), defaultOpts);
        expect(repo.create).toHaveBeenCalledTimes(1);
        const doc = (repo.create as jest.Mock).mock.calls[0][0] as TypedDocumentItem;
        expect(doc.typeId).toBe('doc-type');
        expect(doc.createdBy).toBe('user-1');
    });

    test('emits document.created event on new document', async () => {
        const repo = makeDocRepo();
        const emitter = makeEventEmitter();
        const mat = new DocumentMaterializer(repo, emitter);
        const result = await mat.materialize(makeItem(), makeContent(), makeMapping(), defaultOpts);
        expect(emitter.events).toHaveLength(1);
        const event = emitter.events[0] as { type: string; documentId: string; typeId: string };
        expect(event.type).toBe('document.created');
        expect(event.documentId).toBe(result.documentId);
        expect(event.typeId).toBe('doc-type');
    });

    test('does not throw when eventEmitter is not provided', async () => {
        const repo = makeDocRepo();
        const mat = new DocumentMaterializer(repo); // no emitter
        await expect(mat.materialize(makeItem(), makeContent(), makeMapping(), defaultOpts)).resolves.toBeDefined();
    });

    test('passes extraction warnings through in result', async () => {
        const repo = makeDocRepo();
        const mat = new DocumentMaterializer(repo);
        // Use an unsupported mimeType to trigger a warning
        const item = makeItem({ mimeType: 'image/png' });
        const result = await mat.materialize(item, makeContent(), makeMapping({ fieldMapping: { x: 'y' } }), defaultOpts);
        expect(result.warnings.length).toBeGreaterThan(0);
    });
});

// ─── update path ──────────────────────────────────────────────────────────

describe('DocumentMaterializer — update', () => {
    test('updates existing document and returns created=false', async () => {
        const existing: TypedDocumentItem = {
            documentId: 'doc-existing',
            typeId: 'doc-type',
            values: { body: 'old content' },
            createdBy: 'user-1',
            createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-01-01T00:00:00Z',
        };
        const repo = makeDocRepo(existing);
        const mat = new DocumentMaterializer(repo);
        const opts: MaterializeOptions = { existingDocumentId: 'doc-existing', createdBy: 'user-1' };
        const result = await mat.materialize(makeItem(), makeContent('Updated'), makeMapping(), opts);
        expect(result.created).toBe(false);
        expect(result.documentId).toBe('doc-existing');
    });

    test('merges new extracted values with existing values', async () => {
        const existing: TypedDocumentItem = {
            documentId: 'doc-existing',
            typeId: 'doc-type',
            values: { other: 'preserved' },
            createdBy: 'user-1',
            createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-01-01T00:00:00Z',
        };
        const repo = makeDocRepo(existing);
        const mat = new DocumentMaterializer(repo);
        const opts: MaterializeOptions = { existingDocumentId: 'doc-existing', createdBy: 'user-1' };
        await mat.materialize(makeItem(), makeContent('New body'), makeMapping({ fieldMapping: { content: 'body' } }), opts);
        const saved = (repo.create as jest.Mock).mock.calls[0][0] as TypedDocumentItem;
        expect(saved.values.other).toBe('preserved');
        expect(saved.values.body).toBe('New body');
    });

    test('emits document.updated event on update', async () => {
        const existing: TypedDocumentItem = {
            documentId: 'doc-upd',
            typeId: 'doc-type',
            values: {},
            createdBy: 'user-1',
            createdAt: '2026-01-01T00:00:00Z',
            updatedAt: '2026-01-01T00:00:00Z',
        };
        const repo = makeDocRepo(existing);
        const emitter = makeEventEmitter();
        const mat = new DocumentMaterializer(repo, emitter);
        const opts: MaterializeOptions = { existingDocumentId: 'doc-upd', createdBy: 'user-1' };
        await mat.materialize(makeItem(), makeContent(), makeMapping(), opts);
        const event = emitter.events[0] as { type: string };
        expect(event.type).toBe('document.updated');
    });

    test('falls through to create when existing document is not found', async () => {
        const repo = makeDocRepo(null); // get() always returns null
        const mat = new DocumentMaterializer(repo);
        const opts: MaterializeOptions = { existingDocumentId: 'ghost-doc', createdBy: 'user-1' };
        const result = await mat.materialize(makeItem(), makeContent(), makeMapping(), opts);
        expect(result.created).toBe(true);
    });
});
