// Phase 51 Phase G — integration tests for bulk typed documents operations.
//
// Tests POST /api/typed-documents/bulk with operation={create,update,delete}.
// Validates that schema validation, cardinality rules, and caps (100 items)
// are enforced across bulk operations.

import express, { type NextFunction, type Request, type Response } from 'express';

// ---------------------------------------------------------------------------
// In-memory repo fakes — same pattern as documentTypes.test.ts
// ---------------------------------------------------------------------------

interface FakeTypeStore { [typeId: string]: Record<string, unknown> }
interface FakeDocStore  { [documentId: string]: Record<string, unknown> }

const typeStore: FakeTypeStore = {};
const docStore:  FakeDocStore  = {};

jest.mock('../../repositories', () => ({
  documentTypeRepo: {
    get: jest.fn(async (typeId: string) => typeStore[typeId] ?? null),
  },
  typedDocumentRepo: {
    create: jest.fn(async (item: Record<string, unknown>) => {
      docStore[item.documentId as string] = { ...item };
    }),
    get: jest.fn(async (documentId: string) => docStore[documentId] ?? null),
    delete: jest.fn(async (documentId: string) => {
      delete docStore[documentId];
    }),
    bulkCreate: jest.fn(async (items: Array<Record<string, unknown>>) => {
      for (const item of items) {
        docStore[item.documentId as string] = { ...item };
      }
    }),
    bulkUpdate: jest.fn(async (updates: Array<{ documentId: string; values: Record<string, unknown>; updatedAt: string }>) => {
      for (const upd of updates) {
        const existing = docStore[upd.documentId];
        if (!existing) continue;
        docStore[upd.documentId] = {
          ...existing,
          values: upd.values,
          updatedAt: upd.updatedAt,
        };
      }
    }),
    bulkDelete: jest.fn(async (documentIds: string[]) => {
      for (const id of documentIds) {
        delete docStore[id];
      }
    }),
  },
}));

import { typedDocumentsRouter } from '../typedDocuments';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// req/res rig — mirrors documentTypes.test.ts
// ---------------------------------------------------------------------------

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => { resolveFinished = r; });
  const r: MockRes = {
    statusCode: 200, headers: {}, body: undefined, ended: false, finished,
    status(code) { this.statusCode = code; return this; },
    json(body) { this.body = body; this.ended = true; resolveFinished(); return this; },
    setHeader(name, value) { this.headers[String(name)] = value; return this; },
    getHeader(name) { return this.headers[String(name)]; },
    end(..._args) { this.ended = true; resolveFinished(); return this; },
  };
  return r;
}

interface RunOpts { body?: unknown; userId?: string }

async function run(
  app: express.Express,
  method: string,
  url: string,
  opts: RunOpts = {},
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: method.toUpperCase(),
    url,
    originalUrl: url,
    path: url,
    headers: { 'content-type': 'application/json' },
    query: {},
    body: opts.body,
    user: { sub: opts.userId ?? 'tester' },
    ip: '127.0.0.1',
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) { reject(err); return; }
      if (!res.ended) { res.status(404).json({ error: 'route not found' }); }
      resolve();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req, res as unknown as Response, finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/api/typed-documents', typedDocumentsRouter);
  app.use(errorHandler);
  return app;
}

beforeEach(() => {
  for (const k of Object.keys(typeStore)) delete typeStore[k];
  for (const k of Object.keys(docStore))  delete docStore[k];
});

// ---------------------------------------------------------------------------
// Bulk create
// ---------------------------------------------------------------------------

describe('Bulk create (operation=create)', () => {
  test('201 with valid batch of 5 instances', async () => {
    // Seed a type
    typeStore['type-article'] = {
      typeId: 'type-article',
      name: 'Article',
      version: 1,
      fields: [
        { fieldId: 'f-title', name: 'Title', fieldType: 'text', widget: 'text_field', cardinality: 1, required: true, helpText: '' },
        { fieldId: 'f-body', name: 'Body', fieldType: 'long_text', widget: 'textarea', cardinality: 1, required: false, helpText: '' },
      ],
      createdBy: 'admin',
      createdAt: '2026-05-04T10:00:00Z',
      updatedAt: '2026-05-04T10:00:00Z',
    };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'create',
        typeId: 'type-article',
        items: [
          { 'f-title': 'Article 1' },
          { 'f-title': 'Article 2' },
          { 'f-title': 'Article 3', 'f-body': 'Body 3' },
          { 'f-title': 'Article 4' },
          { 'f-title': 'Article 5' },
        ],
      },
    });

    expect(res.statusCode).toBe(201);
    expect((res.body as { created: number }).created).toBe(5);
    expect(Object.keys(docStore)).toHaveLength(5);
  });

  test('400 when batch exceeds 100-item cap', async () => {
    typeStore['type-simple'] = {
      typeId: 'type-simple',
      name: 'Simple',
      version: 1,
      fields: [{ fieldId: 'f-text', name: 'Text', fieldType: 'text', widget: 'text_field', cardinality: 1, required: false, helpText: '' }],
      createdBy: 'admin',
      createdAt: '2026-05-04T10:00:00Z',
      updatedAt: '2026-05-04T10:00:00Z',
    };

    const items = Array.from({ length: 101 }, (_, i) => ({ 'f-text': `item-${i}` }));

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'create', typeId: 'type-simple', items },
    });

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/bulk create is capped at 100/);
  });

  test('400 when typeId is missing', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'create',
        items: [{ 'f-title': 'Article' }],
      },
    });

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/typeId is required/);
  });

  test('400 when one instance fails validation (required field missing)', async () => {
    typeStore['type-article'] = {
      typeId: 'type-article',
      name: 'Article',
      version: 1,
      fields: [
        { fieldId: 'f-title', name: 'Title', fieldType: 'text', widget: 'text_field', cardinality: 1, required: true, helpText: '' },
      ],
      createdBy: 'admin',
      createdAt: '2026-05-04T10:00:00Z',
      updatedAt: '2026-05-04T10:00:00Z',
    };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'create',
        typeId: 'type-article',
        items: [
          { 'f-title': 'Article 1' },
          {}, // missing required field
          { 'f-title': 'Article 3' },
        ],
      },
    });

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/field "Title" is required/);
  });

  test('201 validates cardinality across bulk-created items', async () => {
    typeStore['type-tags'] = {
      typeId: 'type-tags',
      name: 'Tags',
      version: 1,
      fields: [
        { fieldId: 'f-tags', name: 'Tags', fieldType: 'text', widget: 'text_field', cardinality: 'unlimited', required: false, helpText: '' },
      ],
      createdBy: 'admin',
      createdAt: '2026-05-04T10:00:00Z',
      updatedAt: '2026-05-04T10:00:00Z',
    };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'create',
        typeId: 'type-tags',
        items: [
          { 'f-tags': ['tag1', 'tag2'] },
          { 'f-tags': ['tag3'] },
        ],
      },
    });

    expect(res.statusCode).toBe(201);
    expect(Object.keys(docStore)).toHaveLength(2);
  });
});

// ---------------------------------------------------------------------------
// Bulk update
// ---------------------------------------------------------------------------

describe('Bulk update (operation=update)', () => {
  test('200 with valid updates (modify values on existing instances)', async () => {
    // Seed type + instances
    typeStore['type-article'] = {
      typeId: 'type-article',
      name: 'Article',
      version: 1,
      fields: [
        { fieldId: 'f-title', name: 'Title', fieldType: 'text', widget: 'text_field', cardinality: 1, required: true, helpText: '' },
      ],
      createdBy: 'admin',
      createdAt: '2026-05-04T10:00:00Z',
      updatedAt: '2026-05-04T10:00:00Z',
    };
    docStore['doc-1'] = { documentId: 'doc-1', typeId: 'type-article', schemaVersion: 1, values: { 'f-title': 'Old Title 1' }, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };
    docStore['doc-2'] = { documentId: 'doc-2', typeId: 'type-article', schemaVersion: 1, values: { 'f-title': 'Old Title 2' }, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'update',
        updates: [
          { documentId: 'doc-1', values: { 'f-title': 'New Title 1' } },
          { documentId: 'doc-2', values: { 'f-title': 'New Title 2' } },
        ],
      },
    });

    expect(res.statusCode).toBe(200);
    expect((docStore['doc-1'].values as Record<string, unknown>)['f-title']).toBe('New Title 1');
    expect((docStore['doc-2'].values as Record<string, unknown>)['f-title']).toBe('New Title 2');
  });

  test('400 when batch exceeds 100-item cap', async () => {
    const updates = Array.from({ length: 101 }, (_, i) => ({ documentId: `doc-${i}`, values: {} }));

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'update', updates },
    });

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/bulk update is capped at 100/);
  });

  test('404 when documentId does not exist', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'update',
        updates: [
          { documentId: 'doc-nonexistent', values: { 'f-title': 'Should fail' } },
        ],
      },
    });

    expect(res.statusCode).toBe(404);
    expect((res.body as { error: string }).error).toMatch(/typed document doc-nonexistent not found/);
  });
});

// ---------------------------------------------------------------------------
// Bulk delete
// ---------------------------------------------------------------------------

describe('Bulk delete (operation=delete)', () => {
  test('200 with valid documentIds list', async () => {
    docStore['doc-1'] = { documentId: 'doc-1', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };
    docStore['doc-2'] = { documentId: 'doc-2', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };
    docStore['doc-3'] = { documentId: 'doc-3', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'delete',
        documentIds: ['doc-1', 'doc-2'],
      },
    });

    expect(res.statusCode).toBe(200);
    expect(docStore['doc-1']).toBeUndefined();
    expect(docStore['doc-2']).toBeUndefined();
    expect(docStore['doc-3']).toBeDefined();
  });

  test('400 when batch exceeds 100-item cap', async () => {
    const documentIds = Array.from({ length: 101 }, (_, i) => `doc-${i}`);

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'delete', documentIds },
    });

    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/bulk delete is capped at 100/);
  });

  test('200 deletes only specified IDs, leaves others intact', async () => {
    docStore['doc-keep'] = { documentId: 'doc-keep', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };
    docStore['doc-delete-1'] = { documentId: 'doc-delete-1', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };
    docStore['doc-delete-2'] = { documentId: 'doc-delete-2', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'tester' };

    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: {
        operation: 'delete',
        documentIds: ['doc-delete-1', 'doc-delete-2'],
      },
    });

    expect(res.statusCode).toBe(200);
    expect(docStore['doc-keep']).toBeDefined();
    expect(docStore['doc-delete-1']).toBeUndefined();
    expect(docStore['doc-delete-2']).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Missing / unknown operation
// ---------------------------------------------------------------------------

describe('Missing or unknown operation', () => {
  test('400 when operation field is absent', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { typeId: 'type-x', items: [] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/operation is required/);
  });

  test('400 when operation is an unsupported string', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'upsert' },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/unsupported operation: upsert/);
  });
});

// ---------------------------------------------------------------------------
// Bulk create — empty items array
// ---------------------------------------------------------------------------

describe('Bulk create — empty items', () => {
  test('400 when items array is empty', async () => {
    typeStore['type-simple'] = {
      typeId: 'type-simple', name: 'Simple', version: 1,
      fields: [{ fieldId: 'f-text', name: 'Text', fieldType: 'text', widget: 'text_field', cardinality: 1, required: false, helpText: '' }],
      createdBy: 'admin', createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z',
    };
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'create', typeId: 'type-simple', items: [] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/items cannot be empty/);
  });
});

// ---------------------------------------------------------------------------
// Bulk update — empty array + malformed item edge cases
// ---------------------------------------------------------------------------

describe('Bulk update — empty / malformed inputs', () => {
  test('400 when updates array is empty', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'update', updates: [] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/updates cannot be empty/);
  });

  test('400 when an update item is null', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'update', updates: [null] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/update 0: must be an object/);
  });

  test('400 when an update item is missing documentId', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'update', updates: [{ values: { 'f-title': 'x' } }] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/update 0: documentId is required/);
  });
});

// ---------------------------------------------------------------------------
// Bulk delete — empty array + non-string ID edge cases
// ---------------------------------------------------------------------------

describe('Bulk delete — empty / malformed inputs', () => {
  test('400 when documentIds array is empty', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'delete', documentIds: [] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/documentIds cannot be empty/);
  });

  test('400 when documentIds contains a non-string element', async () => {
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', {
      body: { operation: 'delete', documentIds: ['doc-1', 42, 'doc-3'] },
    });
    expect(res.statusCode).toBe(400);
    expect((res.body as { error: string }).error).toMatch(/all documentIds must be strings/);
  });
});

// Loop 29c: rows are changed only by the person who created them.
describe('bulk writes are the rows\' creator\'s (Loop 29c)', () => {
  test('bulk delete of someone else\'s row is 403 and deletes nothing', async () => {
    docStore['doc-theirs'] = { documentId: 'doc-theirs', typeId: 'type-x', values: {}, createdAt: '2026-05-04T10:00:00Z', updatedAt: '2026-05-04T10:00:00Z', createdBy: 'someone-else' };
    const res = await run(buildApp(), 'POST', '/api/typed-documents/bulk', { body: { operation: 'delete', documentIds: ['doc-theirs'] } });
    expect(res.statusCode).toBe(403);
    expect(docStore['doc-theirs']).toBeDefined();
  });
});
