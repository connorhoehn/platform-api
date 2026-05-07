// Phase 51 hardening: CSV import edge cases not covered by typedDocuments.test.ts.
//
// Covers:
//   - Column lookup by fieldId (not just field name)
//   - Date field CSV coercion
//   - Invalid boolean string → row failure with clear error
//   - Multi-value numbers split on semicolons
//   - Invalid multi-value number → row failure
//   - Validation rules applied during CSV rows (min length, number range)
//   - MAX_BULK_IMPORT_ROWS exceeded → 400

import http from 'http';
import express from 'express';

const mockDocTypeGet = jest.fn();
const mockTypedDocGet = jest.fn();
const mockTypedDocBulkCreate = jest.fn();

jest.mock('../../repositories', () => ({
  documentTypeRepo: { get: mockDocTypeGet },
  typedDocumentRepo: {
    get: mockTypedDocGet,
    create: jest.fn(),
    bulkCreate: mockTypedDocBulkCreate,
    listByType: jest.fn(),
  },
}));

import { typedDocumentsRouter } from '../typedDocuments';
import { errorHandler } from '../../middleware/error-handler';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildServer(): http.Server {
  const app = express();
  app.use(express.json());
  app.use((_req, _res, next) => {
    (_req as unknown as { user: { sub: string } }).user = { sub: 'tester' };
    next();
  });
  app.use('/api/typed-documents', typedDocumentsRouter);
  app.use(errorHandler);
  return app.listen(0, '127.0.0.1');
}

function multipartBody(csv: string, fieldName = 'file'): { body: Buffer; boundary: string } {
  const boundary = '----TestBoundary' + Date.now();
  const parts = [
    `--${boundary}\r\n`,
    `Content-Disposition: form-data; name="${fieldName}"; filename="import.csv"\r\n`,
    'Content-Type: text/csv\r\n\r\n',
    csv,
    `\r\n--${boundary}--\r\n`,
  ];
  return { body: Buffer.from(parts.join('')), boundary };
}

async function postCsv(
  server: http.Server,
  path: string,
  csv: string,
  fieldName = 'file',
): Promise<{ status: number; json: Record<string, unknown> }> {
  const { body, boundary } = multipartBody(csv, fieldName);
  const addr = server.address() as { port: number };
  return new Promise((resolve, reject) => {
    const req = http.request(
      {
        hostname: '127.0.0.1',
        port: addr.port,
        path,
        method: 'POST',
        headers: {
          'Content-Type': `multipart/form-data; boundary=${boundary}`,
          'Content-Length': body.length,
        },
      },
      (res) => {
        let data = '';
        res.on('data', (chunk: Buffer) => { data += chunk.toString(); });
        res.on('end', () => {
          try { resolve({ status: res.statusCode!, json: JSON.parse(data) }); }
          catch { resolve({ status: res.statusCode!, json: { raw: data } as Record<string, unknown> }); }
        });
      },
    );
    req.on('error', reject);
    req.end(body);
  });
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const TEXT_FIELD   = { fieldId: 'f-name',   name: 'Name',   fieldType: 'text',    cardinality: 1,           required: true  };
const DATE_FIELD   = { fieldId: 'f-dob',    name: 'DOB',    fieldType: 'date',    cardinality: 1,           required: false };
const BOOL_FIELD   = { fieldId: 'f-active', name: 'Active', fieldType: 'boolean', cardinality: 1,           required: false };
const MULTI_NUM    = { fieldId: 'f-scores', name: 'Scores', fieldType: 'number',  cardinality: 'unlimited', required: false };
const VALIDATED_TEXT = {
  fieldId: 'f-code', name: 'Code', fieldType: 'text', cardinality: 1, required: false,
  validation: { min: 3 },   // min means minLength for string fields
};
const VALIDATED_NUM = {
  fieldId: 'f-qty', name: 'Qty', fieldType: 'number', cardinality: 1, required: false,
  validation: { min: 1, max: 100 },
};

const FULL_TYPE = {
  typeId: 'type-full',
  name: 'Full',
  fields: [TEXT_FIELD, DATE_FIELD, BOOL_FIELD, MULTI_NUM, VALIDATED_TEXT, VALIDATED_NUM],
};

beforeEach(() => {
  mockDocTypeGet.mockReset();
  mockTypedDocGet.mockReset().mockResolvedValue(null);
  mockTypedDocBulkCreate.mockReset().mockResolvedValue(undefined);
});

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('bulk-import — column lookup by fieldId', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('maps column header that matches fieldId (not just field name)', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Use fieldId 'f-name' as CSV header instead of field name 'Name'
    const csv = 'f-name\nAlice\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
    const items = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(items[0].values['f-name']).toBe('Alice');
  });
});

describe('bulk-import — date field coercion', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('accepts YYYY-MM-DD format and stores canonical ISO string', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,DOB\nAlice,2000-06-15\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
    const items = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(items[0].values['f-dob']).toBe('2000-06-15');
  });

  it('rejects invalid date format as a row error', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,DOB\nAlice,not-a-date\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ row: number; reason: string }>;
    expect(errors[0].reason).toMatch(/ISO date/i);
  });
});

describe('bulk-import — boolean coercion edge cases', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('"no" coerces to false', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Active\nAlice,no\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
    const items = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(items[0].values['f-active']).toBe(false);
  });

  it('empty string coerces to false', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Active\nAlice,\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
    const items = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(items[0].values['f-active']).toBe(false);
  });

  it('unrecognised boolean string produces a row error with clear message', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Active\nAlice,maybe\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ row: number; reason: string }>;
    expect(errors[0].reason).toMatch(/not a valid boolean/i);
    expect(errors[0].row).toBe(2); // header is row 1, data is row 2
  });
});

describe('bulk-import — multi-value number fields', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('splits semicolon-separated numbers into a number array', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Scores\nAlice,10;20;30\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
    const items = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(items[0].values['f-scores']).toEqual([10, 20, 30]);
  });

  it('row fails when any segment in multi-value number is not a number', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Scores\nAlice,10;oops;30\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ row: number; reason: string }>;
    expect(errors[0].reason).toMatch(/not a valid number/i);
  });
});

describe('bulk-import — validation rules applied to CSV rows', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('rejects row when text field violates minLength rule', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Code field has minLength:3 — "ab" is too short
    const csv = 'Name,Code\nAlice,ab\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/length/i);
  });

  it('accepts row when text field satisfies minLength rule', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Code\nAlice,abc\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 0 });
  });

  it('rejects row when number field violates max rule', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Qty field has max:100
    const csv = 'Name,Qty\nAlice,150\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/above max|max/i);
  });

  it('rejects row when number field violates min rule', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Qty field has min:1
    const csv = 'Name,Qty\nAlice,0\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/below min|min/i);
  });
});

describe('bulk-import — limit and format guards', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('returns 400 when row count exceeds MAX_BULK_IMPORT_ROWS (10,000)', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Generate 10,001 data rows (+ 1 header = 10,002 lines total)
    const rows = ['Name'];
    for (let i = 0; i < 10_001; i++) rows.push('Alice');
    const csv = rows.join('\n') + '\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(400);
    expect(JSON.stringify(res.json)).toMatch(/10[,\s_]?000|exceeds/i);
  }, 30_000);

  it('returns 400 on malformed CSV with descriptive parse error', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    // Unclosed quoted field causes csv-parse to throw
    const csv = 'Name\n"unclosed\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(400);
    expect(JSON.stringify(res.json)).toMatch(/CSV parse error/i);
  });
});

describe('error message quality — enum and empty-required', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('enum error includes the list of valid options', async () => {
    // VALIDATED_TEXT does not have enum; use a type with an enum field inline
    const enumType = {
      typeId: 'type-enum', name: 'EnumType',
      fields: [
        { fieldId: 'f-name', name: 'Name', fieldType: 'text', cardinality: 1, required: true },
        { fieldId: 'f-size', name: 'Size', fieldType: 'enum', cardinality: 1, required: false,
          options: ['small', 'medium', 'large'] },
      ],
    };
    mockDocTypeGet.mockResolvedValue(enumType);
    const csv = 'Name,Size\nAlice,xl\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-enum', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    // Error must include the submitted value AND the valid options
    expect(errors[0].reason).toContain('"xl"');
    expect(errors[0].reason).toContain('small');
    expect(errors[0].reason).toContain('medium');
    expect(errors[0].reason).toContain('large');
  });

  it('empty required field reports "cannot be empty" (distinct from missing)', async () => {
    // Use a two-column type so the row is non-empty (skip_empty_lines won't drop it)
    // but the required Name column has an explicit empty value.
    const twoColType = {
      typeId: 'type-two', name: 'Two',
      fields: [
        { fieldId: 'f-name', name: 'Name', fieldType: 'text', cardinality: 1, required: true },
        { fieldId: 'f-tag',  name: 'Tag',  fieldType: 'text', cardinality: 1, required: false },
      ],
    };
    mockDocTypeGet.mockResolvedValue(twoColType);
    // Row has a value for Tag but empty Name → the non-empty Tag keeps the row from being skipped
    const csv = 'Name,Tag\n,some-tag\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-two', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/cannot be empty/i);
  });
});

// ---------------------------------------------------------------------------
// Mixed valid + invalid rows — partial success
// ---------------------------------------------------------------------------

describe('bulk-import — mixed valid and invalid rows', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  const SCORE_TYPE = {
    typeId: 'type-score',
    name: 'Score',
    fields: [
      { fieldId: 'f-name',  name: 'Name',  fieldType: 'text',   cardinality: 1, required: true  },
      { fieldId: 'f-score', name: 'Score', fieldType: 'number', cardinality: 1, required: false },
    ],
  };

  it('imports valid rows and collects per-row errors for invalid rows', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    // 3 valid rows, then 1 with a bad number value
    const csv = [
      'Name,Score',
      'Alice,10',
      'Bob,20',
      'Carol,30',
      'Dave,oops',
    ].join('\n') + '\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 3, failed: 1 });

    const errors = res.json.errors as Array<{ row: number; reason: string }>;
    expect(errors).toHaveLength(1);
    expect(errors[0].row).toBe(5); // header=1, Alice=2, Bob=3, Carol=4, Dave=5
    expect(errors[0].reason).toMatch(/not a valid number/i);

    // Valid items must have been persisted
    expect(mockTypedDocBulkCreate).toHaveBeenCalledTimes(1);
    const saved = mockTypedDocBulkCreate.mock.calls[0][0] as Array<{ values: Record<string, unknown> }>;
    expect(saved).toHaveLength(3);
    const names = saved.map((d) => d.values['f-name']);
    expect(names).toEqual(expect.arrayContaining(['Alice', 'Bob', 'Carol']));
  });

  it('reports correct row number when the invalid row is not the first data row', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    // Row 3 (0-indexed: index 1) is invalid; rows 2 and 4 are valid
    const csv = [
      'Name,Score',
      'Alice,10',
      'BadRow,notanumber',
      'Carol,30',
    ].join('\n') + '\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 2, failed: 1 });

    const errors = res.json.errors as Array<{ row: number; reason: string }>;
    expect(errors[0].row).toBe(3); // header=1, Alice=2, BadRow=3
  });

  it('returns imported:0 failed:N with full errors array when all rows are invalid', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    const csv = [
      'Name,Score',
      'Alice,bad1',
      'Bob,bad2',
    ].join('\n') + '\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 2 });
    expect(mockTypedDocBulkCreate).not.toHaveBeenCalled();

    const errors = res.json.errors as Array<{ row: number }>;
    expect(errors.map((e) => e.row)).toEqual([2, 3]);
  });
});

// ---------------------------------------------------------------------------
// NaN and Infinity strings in number fields
// ---------------------------------------------------------------------------

describe('bulk-import — NaN and Infinity in number fields', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  it('rejects row with "NaN" string in a number field', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Qty\nAlice,NaN\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/not a valid number/i);
  });

  it('rejects row with "Infinity" string in a number field', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Qty\nAlice,Infinity\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/not a valid number/i);
  });

  it('rejects row with "-Infinity" string in a number field', async () => {
    mockDocTypeGet.mockResolvedValue(FULL_TYPE);
    const csv = 'Name,Qty\nAlice,-Infinity\n';
    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-full', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 1 });
    const errors = res.json.errors as Array<{ reason: string }>;
    expect(errors[0].reason).toMatch(/not a valid number/i);
  });
});

// ---------------------------------------------------------------------------
// Failed-row CSV download — response shape carries rawRow + columns so the
// frontend can re-emit a CSV of only the failed rows for re-upload.
// ---------------------------------------------------------------------------

describe('bulk-import — rawRow + columns in error response', () => {
  let server: http.Server;
  beforeAll((done) => { server = buildServer().on('listening', done); });
  afterAll((done) => { server.close(done); });

  const SCORE_TYPE = {
    typeId: 'type-score',
    name: 'Score',
    fields: [
      { fieldId: 'f-name',  name: 'Name',  fieldType: 'text',   cardinality: 1, required: true  },
      { fieldId: 'f-score', name: 'Score', fieldType: 'number', cardinality: 1, required: false },
    ],
  };

  it('error entries include rawRow with the original parsed CSV cells', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    const csv = [
      'Name,Score',
      'Alice,10',
      'Bob,not-a-number',
      'Carol,30',
    ].join('\n') + '\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 2, failed: 1 });

    const errors = res.json.errors as Array<{ row: number; reason: string; rawRow: Record<string, string> }>;
    expect(errors).toHaveLength(1);
    // rawRow preserves the original CSV cells for the failing row, so the
    // frontend can reconstruct a downloadable CSV of failed rows.
    expect(errors[0].rawRow).toEqual({ Name: 'Bob', Score: 'not-a-number' });
  });

  it('response includes columns array preserving original CSV header order', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    const csv = [
      'Score,Name',
      '10,Alice',
      'bad,Bob',
    ].join('\n') + '\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 1, failed: 1 });
    // columns reflect the order the operator sent — Score before Name —
    // so a re-emitted CSV of failed rows matches the source layout.
    expect(res.json.columns).toEqual(['Score', 'Name']);
  });

  it('empty CSV (headers only) returns columns: [] for shape consistency', async () => {
    mockDocTypeGet.mockResolvedValue(SCORE_TYPE);
    const csv = 'Name,Score\n';

    const res = await postCsv(server, '/api/typed-documents/bulk-import?typeId=type-score', csv);
    expect(res.status).toBe(200);
    expect(res.json).toMatchObject({ imported: 0, failed: 0, columns: [], errors: [] });
  });
});
