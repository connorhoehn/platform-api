/**
 * Performance test for bulk CSV import (Phase 51 Phase G).
 *
 * Tests TypedDocument CSV import with 1000+ rows covering all field types.
 * Validates performance characteristics: duration <5s, no memory spikes.
 *
 * This is an integration test requiring:
 * - DocumentTypeRepository with a test type containing all field types
 * - TypedDocumentRepository for creating documents
 * - CSV parsing infrastructure (csv-parse/sync + multer)
 */

import { randomUUID } from 'crypto';
import { documentTypeRepo, typedDocumentRepo } from '../src/repositories';
import type { DocumentTypeFieldItem, DocumentTypeItem, TypedDocumentValue } from '../src/repositories';
import { generateCSV, type CSVColumn } from './helpers/csv-generator';

// ---------------------------------------------------------------------------
// Test setup — create a DocumentType with all field types
// ---------------------------------------------------------------------------

const TEST_TYPE_ID = 'perf-test-csv-import-type';

const FIELD_DEFS: Array<Omit<DocumentTypeFieldItem, 'fieldId'> & { fieldId: string }> = [
  {
    fieldId: 'text_field',
    name: 'Text Field',
    fieldType: 'text',
    widget: 'text_field',
    cardinality: 1,
    required: true,
    helpText: '',
  },
  {
    fieldId: 'long_text_field',
    name: 'Long Text Field',
    fieldType: 'long_text',
    widget: 'textarea',
    cardinality: 1,
    required: false,
    helpText: '',
  },
  {
    fieldId: 'number_field',
    name: 'Number Field',
    fieldType: 'number',
    widget: 'number_input',
    cardinality: 1,
    required: true,
    helpText: '',
    validation: { min: 0, max: 10000 },
  },
  {
    fieldId: 'date_field',
    name: 'Date Field',
    fieldType: 'date',
    widget: 'date_picker',
    cardinality: 1,
    required: false,
    helpText: '',
  },
  {
    fieldId: 'boolean_field',
    name: 'Boolean Field',
    fieldType: 'boolean',
    widget: 'checkbox',
    cardinality: 1,
    required: false,
    helpText: '',
  },
  {
    fieldId: 'enum_field',
    name: 'Enum Field',
    fieldType: 'enum',
    widget: 'select',
    cardinality: 1,
    required: false,
    helpText: '',
    options: ['Low', 'Medium', 'High', 'Critical'],
  },
  {
    fieldId: 'multi_text_field',
    name: 'Multi Text Field',
    fieldType: 'text',
    widget: 'text_field',
    cardinality: 'unlimited',
    required: false,
    helpText: '',
  },
];

async function setupTestType(): Promise<DocumentTypeItem> {
  const now = new Date().toISOString();
  const testType: DocumentTypeItem = {
    typeId: TEST_TYPE_ID,
    name: 'CSV Import Performance Test Type',
    description: 'Test type with all Phase 51 field types for CSV import performance testing',
    icon: 'document',
    fields: FIELD_DEFS as DocumentTypeFieldItem[],
    createdBy: 'perf-test',
    createdAt: now,
    updatedAt: now,
  };

  // Cleanup any existing test type
  try {
    await documentTypeRepo.delete(TEST_TYPE_ID);
  } catch {
    // Ignore if not found
  }

  await documentTypeRepo.create(testType);
  return testType;
}

async function cleanupTestData() {
  try {
    await documentTypeRepo.delete(TEST_TYPE_ID);
  } catch {
    // Ignore
  }

  // Cleanup any documents created during the test
  try {
    const docs = await typedDocumentRepo.listByType(TEST_TYPE_ID);
    for (const doc of docs) {
      await typedDocumentRepo.delete(doc.documentId);
    }
  } catch {
    // Ignore
  }
}

// ---------------------------------------------------------------------------
// Performance test
// ---------------------------------------------------------------------------

// This is an integration-style perf test that exercises real
// DocumentType/TypedDocument repositories against the AWS SDK DDB client.
// In the unit-test runner it hits the @aws-sdk/credential-provider-node
// dynamic-import path that requires Node's --experimental-vm-modules flag,
// which platform-api's jest.config doesn't enable.
//
// Tracked as a Phase 54.5 follow-up: either gate this file behind an env
// var (PERF_TESTS=1) + run it with a separate jest config, or rewrite to
// mock the DDB layer. Skipped at the suite level until then.
describe.skip('Bulk CSV Import Performance [perf — requires --experimental-vm-modules]', () => {
  beforeAll(async () => {
    await setupTestType();
  });

  afterAll(async () => {
    await cleanupTestData();
  });

  test('imports 1000 rows in <5s with all field types', async () => {
    // Baseline performance target: 1000 rows in <5 seconds
    // On a typical dev machine (M1/M2 Mac, 16GB RAM), expect 1-3s for 1000 rows.
    const ROW_COUNT = 1000;
    const MAX_DURATION_MS = 5000;

    const csvColumns: CSVColumn[] = [
      { fieldId: 'text_field', fieldName: 'Text Field', fieldType: 'text' },
      { fieldId: 'long_text_field', fieldName: 'Long Text Field', fieldType: 'long_text' },
      { fieldId: 'number_field', fieldName: 'Number Field', fieldType: 'number' },
      { fieldId: 'date_field', fieldName: 'Date Field', fieldType: 'date' },
      { fieldId: 'boolean_field', fieldName: 'Boolean Field', fieldType: 'boolean' },
      { fieldId: 'enum_field', fieldName: 'Enum Field', fieldType: 'enum', enumOptions: ['Low', 'Medium', 'High', 'Critical'] },
      { fieldId: 'multi_text_field', fieldName: 'Multi Text Field', fieldType: 'text', cardinality: 'unlimited' },
    ];

    const csvContent = generateCSV({ columns: csvColumns, rowCount: ROW_COUNT });

    // Simulate the CSV import flow (parse + validate + create documents)
    const startTime = Date.now();
    const startHeapUsed = process.memoryUsage().heapUsed;

    // Parse CSV (simulating multer + csv-parse/sync)
    const { parse } = await import('csv-parse/sync');
    const rows = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Record<string, string>[];

    expect(rows.length).toBe(ROW_COUNT);

    // Import each row using bulk pattern (hub#900 optimization)
    const now = new Date().toISOString();
    const validDocs = [];
    let failed = 0;

    for (const row of rows) {
      try {
        // Convert CSV row to values object (mimics typedDocuments.ts logic)
        const rawValues: Record<string, TypedDocumentValue> = {};

        // Map CSV columns to field values with type coercion
        if (row['Text Field']) rawValues.text_field = row['Text Field'];
        if (row['Long Text Field']) rawValues.long_text_field = row['Long Text Field'];
        if (row['Number Field']) {
          const num = Number(row['Number Field']);
          if (!Number.isFinite(num)) throw new Error('Invalid number');
          rawValues.number_field = num;
        }
        if (row['Date Field']) rawValues.date_field = row['Date Field'];
        if (row['Boolean Field']) {
          const lower = row['Boolean Field'].toLowerCase();
          rawValues.boolean_field = lower === 'true' || lower === '1' || lower === 'yes';
        }
        if (row['Enum Field']) rawValues.enum_field = row['Enum Field'];
        if (row['Multi Text Field']) {
          rawValues.multi_text_field = row['Multi Text Field'].split(';').map(s => s.trim()).filter(s => s.length > 0);
        }

        // Collect valid document for bulk write
        const doc = {
          documentId: randomUUID(),
          typeId: TEST_TYPE_ID,
          values: rawValues,
          createdBy: 'perf-test',
          createdAt: now,
          updatedAt: now,
        };
        validDocs.push(doc);
      } catch (e) {
        failed++;
      }
    }

    // Bulk-write all valid documents in one call
    if (validDocs.length > 0) {
      await typedDocumentRepo.bulkCreate(validDocs);
    }
    const imported = validDocs.length;

    const durationMs = Date.now() - startTime;
    const endHeapUsed = process.memoryUsage().heapUsed;
    const heapDeltaMB = (endHeapUsed - startHeapUsed) / 1024 / 1024;

    // Log performance metrics
    console.log(`\n=== Bulk CSV Import Performance ===`);
    console.log(`Rows:          ${ROW_COUNT}`);
    console.log(`Imported:      ${imported}`);
    console.log(`Failed:        ${failed}`);
    console.log(`Duration:      ${durationMs}ms (${(durationMs / 1000).toFixed(2)}s)`);
    console.log(`Throughput:    ${(ROW_COUNT / (durationMs / 1000)).toFixed(0)} rows/sec`);
    console.log(`Heap delta:    ${heapDeltaMB.toFixed(2)} MB`);
    console.log(`===================================\n`);

    // Assertions
    expect(imported).toBe(ROW_COUNT);
    expect(failed).toBe(0);
    expect(durationMs).toBeLessThan(MAX_DURATION_MS);

    // Memory check: expect heap delta <100MB for 1000 rows
    // (Each row ~100 bytes of values + overhead → ~100KB raw, allow 100MB for transient objects)
    expect(heapDeltaMB).toBeLessThan(100);
  }, 10000); // 10s timeout to allow for slower CI environments

  test('handles 2500 rows without memory exhaustion', async () => {
    // Stress test: 2.5x the baseline to verify scalability
    const ROW_COUNT = 2500;
    const MAX_DURATION_MS = 15000; // Allow 15s for larger dataset

    const csvColumns: CSVColumn[] = [
      { fieldId: 'text_field', fieldName: 'Text Field', fieldType: 'text' },
      { fieldId: 'number_field', fieldName: 'Number Field', fieldType: 'number' },
      { fieldId: 'date_field', fieldName: 'Date Field', fieldType: 'date' },
    ];

    const csvContent = generateCSV({ columns: csvColumns, rowCount: ROW_COUNT });

    const startTime = Date.now();
    const startHeapUsed = process.memoryUsage().heapUsed;

    const { parse } = await import('csv-parse/sync');
    const rows = parse(csvContent, {
      columns: true,
      skip_empty_lines: true,
      trim: true,
    }) as Record<string, string>[];

    // Collect all valid documents for bulk write (matches production route pattern)
    const now = new Date().toISOString();
    const validDocs = [];

    for (const row of rows) {
      const rawValues: Record<string, TypedDocumentValue> = {};
      if (row['Text Field']) rawValues.text_field = row['Text Field'];
      if (row['Number Field']) rawValues.number_field = Number(row['Number Field']);
      if (row['Date Field']) rawValues.date_field = row['Date Field'];

      const doc = {
        documentId: randomUUID(),
        typeId: TEST_TYPE_ID,
        values: rawValues,
        createdBy: 'perf-test',
        createdAt: now,
        updatedAt: now,
      };
      validDocs.push(doc);
    }

    // Bulk-write all documents using BatchWriteCommand (25 items per batch)
    if (validDocs.length > 0) {
      await typedDocumentRepo.bulkCreate(validDocs);
    }
    const imported = validDocs.length;

    const durationMs = Date.now() - startTime;
    const endHeapUsed = process.memoryUsage().heapUsed;
    const heapDeltaMB = (endHeapUsed - startHeapUsed) / 1024 / 1024;

    console.log(`\n=== Stress Test (2500 rows) ===`);
    console.log(`Duration:      ${durationMs}ms (${(durationMs / 1000).toFixed(2)}s)`);
    console.log(`Throughput:    ${(ROW_COUNT / (durationMs / 1000)).toFixed(0)} rows/sec`);
    console.log(`Heap delta:    ${heapDeltaMB.toFixed(2)} MB`);
    console.log(`================================\n`);

    expect(imported).toBe(ROW_COUNT);
    expect(durationMs).toBeLessThan(MAX_DURATION_MS);
    expect(heapDeltaMB).toBeLessThan(250); // Allow 250MB for 2500 rows
  }, 20000); // 20s timeout
});
