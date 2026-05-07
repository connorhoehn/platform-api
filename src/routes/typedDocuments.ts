// Phase 51 Phase A — REST API for TypedDocument instances.
//
// Endpoints:
//   POST  /api/typed-documents              → create (validates against schema)
//   GET   /api/typed-documents/:documentId  → fetch one
//   GET   /api/typed-documents?typeId=<id>  → list instances of a type
//
// Validation: every required field on the type must have a value; values
// shape must match cardinality (string for cardinality=1, string[] for
// cardinality='unlimited'); unknown fields are rejected.

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import multer from 'multer';
import { parse } from 'csv-parse/sync';
import { documentTypeRepo, typedDocumentRepo } from '../repositories';
import type {
  TypedDocumentItem,
  TypedDocumentValue,
  DocumentTypeFieldItem,
} from '../repositories';
import { asyncHandler, ValidationError, NotFoundError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import {
  recordTypedDocumentCreated,
  recordTypedDocumentBulkImported,
  recordTypedDocumentValidationError,
} from '../observability/metrics';

const log = withContext({ route: 'typedDocuments' });

export const typedDocumentsRouter = Router();

// Classify ValidationError messages into bounded error types for metrics labeling.
// Exported so it can be unit-tested directly; the set of labels is the contract
// for the typed_document_validation_error metric.
export function classifyValidationError(message: string): string {
  if (/is required/.test(message)) return 'required_field';
  if (/expects a|expects an|type mismatch|cannot have unlimited cardinality/i.test(message)) return 'type_mismatch';
  if (/is type .+, expected/.test(message)) return 'reference_type_mismatch';
  if (/not found|does not exist/.test(message)) return 'reference_not_found';
  if (/not in the configured options/.test(message)) return 'enum_invalid';
  // schema_invalid before validation_rule: "must be an object keyed" contains "must be"
  if (/unknown field|must be an object keyed/.test(message)) return 'schema_invalid';
  if (/length|below min|above max|does not match|must be/.test(message)) return 'validation_rule';
  return 'other';
}

// ---------------------------------------------------------------------------
// Schema-aware value validation
// ---------------------------------------------------------------------------

// Permissive ISO-8601 date or date-time parser — accepts "YYYY-MM-DD" or any
// string Date.parse() can read. Returns the canonical ISO string for storage.
const DATE_RE = /^\d{4}-\d{2}-\d{2}(T.*)?$/;
function parseDate(raw: string, fieldName: string): string {
  if (!DATE_RE.test(raw) || Number.isNaN(Date.parse(raw))) {
    throw new ValidationError(`field "${fieldName}" expects an ISO date (YYYY-MM-DD or ISO datetime)`);
  }
  return raw;
}

function validatePrimitive(
  field: DocumentTypeFieldItem,
  raw: unknown,
): string | number | boolean {
  switch (field.fieldType) {
    case 'text':
    case 'long_text':
      if (typeof raw !== 'string') {
        throw new ValidationError(`field "${field.name}" expects a string value`);
      }
      return raw;
    case 'number':
      if (typeof raw !== 'number' || !Number.isFinite(raw)) {
        throw new ValidationError(`field "${field.name}" expects a finite number`);
      }
      return raw;
    case 'date':
      if (typeof raw !== 'string') {
        throw new ValidationError(`field "${field.name}" expects a string ISO date`);
      }
      return parseDate(raw, field.name);
    case 'boolean':
      if (typeof raw !== 'boolean') {
        throw new ValidationError(`field "${field.name}" expects a boolean`);
      }
      return raw;
    case 'enum':
      if (typeof raw !== 'string') {
        throw new ValidationError(`field "${field.name}" expects a string value`);
      }
      if (!field.options || !field.options.includes(raw)) {
        const valid = (field.options ?? []).join(', ');
        throw new ValidationError(
          `field "${field.name}": "${raw}" is not in the configured options (valid: ${valid})`,
        );
      }
      return raw;
    case 'reference':
      if (typeof raw !== 'string' || raw.length === 0) {
        throw new ValidationError(`field "${field.name}" expects a referenced documentId`);
      }
      // Cross-document existence check happens in a separate async pass after
      // shape validation completes (see assertReferencesExist below).
      return raw;
  }
}

function validateValueAgainstField(
  field: DocumentTypeFieldItem,
  raw: unknown,
): TypedDocumentValue {
  if (field.cardinality === 1) {
    const v = validatePrimitive(field, raw);
    // Required-empty check applies to strings; numbers/booleans/dates are
    // present-or-absent. (Booleans defaulting to false are "set", which is
    // the right semantics — operator can model "must be checked" via a
    // separate validation rule in Phase D.)
    if (field.required && typeof v === 'string' && v.length === 0) {
      throw new ValidationError(`field "${field.name}" is required and cannot be empty`);
    }
    return v;
  }
  // cardinality === 'unlimited'
  if (field.fieldType === 'boolean') {
    throw new ValidationError(
      `field "${field.name}": boolean fields cannot have unlimited cardinality`,
    );
  }
  if (!Array.isArray(raw)) {
    throw new ValidationError(`field "${field.name}" expects an array of values`);
  }
  const out: (string | number)[] = [];
  for (const item of raw) {
    const v = validatePrimitive(field, item);
    out.push(v as string | number);
  }
  if (field.required && out.length === 0) {
    throw new ValidationError(`field "${field.name}" is required (at least one value)`);
  }
  return out as string[] | number[];
}

// Phase C — second-pass async check that every reference value points at an
// existing typed-document of the configured target type. Sync shape validation
// has already run; this only resolves cross-document references.
async function assertReferencesExist(
  fields: DocumentTypeFieldItem[],
  values: Record<string, TypedDocumentValue>,
): Promise<void> {
  for (const field of fields) {
    if (field.fieldType !== 'reference') continue;
    const v = values[field.fieldId];
    if (v === undefined) continue;
    const ids: string[] = Array.isArray(v) ? (v as string[]) : [v as string];
    for (const id of ids) {
      const referenced = await typedDocumentRepo.get(id);
      if (!referenced) {
        log.warn({ fieldName: field.name, missingId: id }, 'reference-check-failed');
        throw new ValidationError(`field "${field.name}": referenced document ${id} not found`);
      }
      if (field.referenceTypeId && referenced.typeId !== field.referenceTypeId) {
        log.warn({
          fieldName: field.name,
          documentId: id,
          actualType: referenced.typeId,
          expectedType: field.referenceTypeId,
        }, 'reference-type-mismatch');
        throw new ValidationError(
          `field "${field.name}": referenced document ${id} is type ${referenced.typeId}, expected ${field.referenceTypeId}`,
        );
      }
    }
  }
}

// Phase D — fields whose showWhen precondition is not met are treated as
// absent: required check is skipped and any provided value is silently
// dropped from the saved payload. The check uses the pre-coerced raw value
// of the referenced field so it works for booleans, numbers, and strings
// before primitive validation runs.
function isVisible(
  field: DocumentTypeFieldItem,
  rawValues: Record<string, unknown>,
): boolean {
  if (!field.showWhen) return true;
  return rawValues[field.showWhen.fieldId] === field.showWhen.equals;
}

// Phase D — additive validation rules applied after primitive shape
// validation. Throws ValidationError with a field-named, constraint-named
// message so the operator + frontend get an actionable error.
function applyValidation(field: DocumentTypeFieldItem, coerced: TypedDocumentValue): void {
  const v = field.validation;
  if (!v) return;
  const checkOne = (val: string | number | boolean): void => {
    if (typeof val === 'string') {
      if (v.min !== undefined && val.length < v.min) {
        throw new ValidationError(`field "${field.name}": value length ${val.length} is below min ${v.min}`);
      }
      if (v.max !== undefined && val.length > v.max) {
        throw new ValidationError(`field "${field.name}": value length ${val.length} is above max ${v.max}`);
      }
      if (v.regex !== undefined && !new RegExp(v.regex).test(val)) {
        throw new ValidationError(`field "${field.name}": value does not match required pattern`);
      }
    } else if (typeof val === 'number') {
      if (v.min !== undefined && val < v.min) {
        throw new ValidationError(`field "${field.name}": value ${val} is below min ${v.min}`);
      }
      if (v.max !== undefined && val > v.max) {
        throw new ValidationError(`field "${field.name}": value ${val} is above max ${v.max}`);
      }
    } else if (typeof val === 'boolean') {
      if (v.requireTrue && !val) {
        throw new ValidationError(`field "${field.name}" must be checked`);
      }
    }
  };
  if (Array.isArray(coerced)) {
    for (const item of coerced) checkOne(item as string | number);
  } else {
    checkOne(coerced);
  }
}

function validateValuesAgainstSchema(
  fields: DocumentTypeFieldItem[],
  rawValues: unknown,
): Record<string, TypedDocumentValue> {
  if (typeof rawValues !== 'object' || rawValues === null || Array.isArray(rawValues)) {
    throw new ValidationError('values must be an object keyed by fieldId');
  }
  const valuesObj = rawValues as Record<string, unknown>;
  const knownIds = new Set(fields.map((f) => f.fieldId));

  for (const incomingId of Object.keys(valuesObj)) {
    if (!knownIds.has(incomingId)) {
      throw new ValidationError(`unknown field "${incomingId}" — not in type schema`);
    }
  }

  const out: Record<string, TypedDocumentValue> = {};
  for (const field of fields) {
    if (!isVisible(field, valuesObj)) continue;
    const raw = valuesObj[field.fieldId];
    if (raw === undefined) {
      if (field.required) {
        throw new ValidationError(`field "${field.name}" is required`);
      }
      continue;
    }
    const coerced = validateValueAgainstField(field, raw);
    applyValidation(field, coerced);
    out[field.fieldId] = coerced;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

typedDocumentsRouter.post('/', asyncHandler(async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  const typeId = body.typeId;
  if (typeof typeId !== 'string' || !typeId) {
    throw new ValidationError('typeId is required');
  }
  const type = await documentTypeRepo.get(typeId);
  if (!type) throw new NotFoundError(`document type ${typeId} not found`);

  let values: Record<string, TypedDocumentValue>;
  try {
    values = validateValuesAgainstSchema(type.fields, body.values ?? {});
    await assertReferencesExist(type.fields, values);
  } catch (e) {
    if (e instanceof ValidationError) {
      const errorType = classifyValidationError(e.message);
      recordTypedDocumentValidationError(errorType);
    }
    throw e;
  }

  const now = new Date().toISOString();
  const item: TypedDocumentItem = {
    documentId: randomUUID(),
    typeId,
    values,
    createdBy: req.user!.sub,
    createdAt: now,
    updatedAt: now,
  };
  await typedDocumentRepo.create(item);
  recordTypedDocumentCreated();
  log.info({ typeId, userId: req.user!.sub, fieldCount: Object.keys(values).length }, 'create');
  res.status(201).json(item);
}));

typedDocumentsRouter.get('/', asyncHandler(async (req: Request, res: Response) => {
  const typeId = typeof req.query.typeId === 'string' ? req.query.typeId : '';
  if (!typeId) throw new ValidationError('typeId query parameter is required');
  const items = await typedDocumentRepo.listByType(typeId);
  log.info({ typeId, count: items.length }, 'list');
  res.status(200).json({ items });
}));

typedDocumentsRouter.get('/:documentId', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { documentId: string };
  const item = await typedDocumentRepo.get(params.documentId);
  if (!item) throw new NotFoundError(`typed document ${params.documentId} not found`);
  log.debug({ documentId: params.documentId, typeId: item.typeId }, 'fetch');
  res.status(200).json(item);
}));

// PATCH /api/typed-documents/:documentId — update field values
typedDocumentsRouter.patch('/:documentId', asyncHandler(async (req: Request, res: Response) => {
  const { documentId } = req.params;
  const body = (req.body ?? {}) as Record<string, unknown>;

  const existing = await typedDocumentRepo.get(documentId);
  if (!existing) throw new NotFoundError(`typed document ${documentId} not found`);

  const type = await documentTypeRepo.get(existing.typeId);
  if (!type) throw new NotFoundError(`document type ${existing.typeId} not found`);

  // Accept either { values: { field: val } } or { field, value } shorthand
  let rawValues: Record<string, unknown>;
  if (body.values && typeof body.values === 'object') {
    rawValues = body.values as Record<string, unknown>;
  } else if (typeof body.field === 'string') {
    rawValues = { [body.field]: body.value };
  } else {
    throw new ValidationError('Either "values" object or "field"+"value" pair is required');
  }

  const values = validateValuesAgainstSchema(type.fields, { ...existing.values, ...rawValues });
  const updated = await typedDocumentRepo.update(documentId, values);
  log.info({ documentId, typeId: existing.typeId, userId: req.user?.sub }, 'update');
  res.status(200).json(updated);
}));

// Phase 51 Phase G — bulk CSV import
const MAX_BULK_IMPORT_ROWS = 10_000;
const MAX_BODY_BYTES = 5 * 1024 * 1024;
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: MAX_BODY_BYTES } });
typedDocumentsRouter.post(
  '/bulk-import',
  upload.single('file'),
  asyncHandler(async (req: Request, res: Response) => {
    const typeId = typeof req.query.typeId === 'string' ? req.query.typeId : '';
    if (!typeId) throw new ValidationError('typeId query parameter is required');

    const type = await documentTypeRepo.get(typeId);
    if (!type) throw new NotFoundError(`document type ${typeId} not found`);

    if (!req.file) throw new ValidationError('CSV file is required (upload as multipart/form-data with field name "file")');

    const csvContent = req.file.buffer.toString('utf-8');
    let rows: Record<string, string>[];
    try {
      rows = parse(csvContent, {
        columns: true,
        skip_empty_lines: true,
        trim: true,
      }) as Record<string, string>[];
    } catch (e) {
      throw new ValidationError(`CSV parse error: ${(e as Error).message}`);
    }

    if (rows.length === 0) {
      return res.status(200).json({ imported: 0, failed: 0, columns: [], errors: [] });
    }

    if (rows.length > MAX_BULK_IMPORT_ROWS) {
      throw new ValidationError(`CSV contains ${rows.length} rows, which exceeds the limit of ${MAX_BULK_IMPORT_ROWS}`);
    }

    const startTime = Date.now();
    log.info({ typeId, rowCount: rows.length, userId: req.user!.sub }, 'bulk-import.start');

    // Map CSV column headers to fieldIds. Accept either the field name or the fieldId itself.
    const fieldMap = new Map<string, DocumentTypeFieldItem>();
    for (const field of type.fields) {
      fieldMap.set(field.fieldId, field);
      fieldMap.set(field.name, field);
    }

    const now = new Date().toISOString();
    // Preserve the original CSV column order so the frontend can rebuild a
    // valid CSV of just the failed rows for re-upload after the operator
    // fixes them.
    const columns: string[] = Object.keys(rows[0]);
    const results: {
      imported: number;
      failed: number;
      columns: string[];
      errors: Array<{ row: number; reason: string; rawRow: Record<string, string> }>;
    } = {
      imported: 0,
      failed: 0,
      columns,
      errors: [],
    };

    const validItems: TypedDocumentItem[] = [];

    for (let i = 0; i < rows.length; i++) {
      const row = rows[i];
      const rowNum = i + 2; // CSV row numbering starts at 1, header is row 1, data starts at row 2

      try {
        // Convert CSV row to values object keyed by fieldId
        const rawValues: Record<string, unknown> = {};
        for (const [csvKey, csvValue] of Object.entries(row)) {
          const field = fieldMap.get(csvKey);
          if (!field) {
            // Unknown column — skip silently (allows extra columns for notes/metadata)
            continue;
          }

          // Coerce CSV string to the expected type
          let coerced: unknown;
          if (field.cardinality === 'unlimited') {
            // Multi-value: split on semicolon
            const parts = csvValue.split(';').map((s) => s.trim()).filter((s) => s.length > 0);
            if (field.fieldType === 'number') {
              coerced = parts.map((p) => {
                const n = Number(p);
                if (!Number.isFinite(n)) throw new Error(`"${p}" is not a valid number`);
                return n;
              });
            } else {
              coerced = parts;
            }
          } else {
            // Single value
            if (field.fieldType === 'number') {
              const n = Number(csvValue);
              if (!Number.isFinite(n)) throw new Error(`"${csvValue}" is not a valid number`);
              coerced = n;
            } else if (field.fieldType === 'boolean') {
              const lower = csvValue.toLowerCase();
              if (lower === 'true' || lower === '1' || lower === 'yes') {
                coerced = true;
              } else if (lower === 'false' || lower === '0' || lower === 'no' || lower === '') {
                coerced = false;
              } else {
                throw new Error(`"${csvValue}" is not a valid boolean (use true/false, yes/no, 1/0)`);
              }
            } else {
              coerced = csvValue;
            }
          }

          rawValues[field.fieldId] = coerced;
        }

        // Validate against schema (reuses the same logic as POST /api/typed-documents)
        const values = validateValuesAgainstSchema(type.fields, rawValues);
        await assertReferencesExist(type.fields, values);

        const item: TypedDocumentItem = {
          documentId: randomUUID(),
          typeId,
          values,
          createdBy: req.user!.sub,
          createdAt: now,
          updatedAt: now,
        };
        validItems.push(item);
      } catch (e) {
        results.failed++;
        const errorMsg = (e as Error).message;
        results.errors.push({
          row: rowNum,
          reason: errorMsg,
          rawRow: row,
        });
        if (e instanceof ValidationError) {
          const errorType = classifyValidationError(errorMsg);
          recordTypedDocumentValidationError(errorType);
        }
      }
    }

    // Bulk-write all valid items in one call (chunks internally to 25/batch)
    if (validItems.length > 0) {
      await typedDocumentRepo.bulkCreate(validItems);
      results.imported = validItems.length;
    }

    const durationMs = Date.now() - startTime;
    recordTypedDocumentBulkImported();
    log.info({
      typeId,
      imported: results.imported,
      failed: results.failed,
      durationMs,
    }, 'bulk-import.done');
    return res.status(200).json(results);
  }),
);

// ---------------------------------------------------------------------------
// POST /api/typed-documents/bulk — bulk operations
// ---------------------------------------------------------------------------

typedDocumentsRouter.post(
  '/bulk',
  asyncHandler(async (req: Request, res: Response) => {
    const body = (req.body ?? {}) as Record<string, unknown>;
    const { operation } = body;

    if (typeof operation !== 'string') {
      throw new ValidationError('operation is required');
    }

    const startTime = Date.now();

    // Bulk create
    if (operation === 'create') {
      const { typeId, items } = body;

      if (typeof typeId !== 'string' || !typeId) {
        throw new ValidationError('typeId is required for bulk create');
      }

      if (!Array.isArray(items)) {
        throw new ValidationError('items must be an array');
      }

      if (items.length === 0) {
        throw new ValidationError('items cannot be empty');
      }

      if (items.length > 100) {
        throw new ValidationError('bulk create is capped at 100 items');
      }

      const type = await documentTypeRepo.get(typeId);
      if (!type) throw new NotFoundError(`document type ${typeId} not found`);

      const now = new Date().toISOString();
      const documentsToCreate: TypedDocumentItem[] = [];

      // Validate all items before creating any
      for (let i = 0; i < items.length; i++) {
        try {
          const values = validateValuesAgainstSchema(type.fields, items[i]);
          await assertReferencesExist(type.fields, values);

          documentsToCreate.push({
            documentId: randomUUID(),
            typeId,
            values,
            createdBy: req.user!.sub,
            createdAt: now,
            updatedAt: now,
          });
        } catch (e) {
          if (e instanceof ValidationError) {
            const errorType = classifyValidationError(e.message);
            recordTypedDocumentValidationError(errorType);
            throw new ValidationError(`item ${i}: ${e.message}`);
          }
          throw e;
        }
      }

      // Create all documents
      await typedDocumentRepo.bulkCreate(documentsToCreate);

      const durationMs = Date.now() - startTime;
      log.info({ operation, typeId, created: documentsToCreate.length, durationMs }, 'bulk-operation.done');

      return res.status(201).json({ created: documentsToCreate.length });
    }

    // Bulk update
    if (operation === 'update') {
      const { updates } = body;

      if (!Array.isArray(updates)) {
        throw new ValidationError('updates must be an array');
      }

      if (updates.length === 0) {
        throw new ValidationError('updates cannot be empty');
      }

      if (updates.length > 100) {
        throw new ValidationError('bulk update is capped at 100 items');
      }

      const now = new Date().toISOString();
      const updatesToApply: Array<{ documentId: string; values: Record<string, TypedDocumentValue>; updatedAt: string }> = [];

      // Validate all updates before applying any
      for (let i = 0; i < updates.length; i++) {
        const update = updates[i];

        if (typeof update !== 'object' || update === null) {
          throw new ValidationError(`update ${i}: must be an object`);
        }

        const { documentId, values } = update as { documentId?: unknown; values?: unknown };

        if (typeof documentId !== 'string' || !documentId) {
          throw new ValidationError(`update ${i}: documentId is required`);
        }

        const existing = await typedDocumentRepo.get(documentId);
        if (!existing) {
          throw new NotFoundError(`typed document ${documentId} not found`);
        }

        const type = await documentTypeRepo.get(existing.typeId);
        if (!type) throw new NotFoundError(`document type ${existing.typeId} not found`);

        try {
          const validatedValues = validateValuesAgainstSchema(type.fields, values ?? {});
          await assertReferencesExist(type.fields, validatedValues);

          updatesToApply.push({
            documentId,
            values: validatedValues,
            updatedAt: now,
          });
        } catch (e) {
          if (e instanceof ValidationError) {
            const errorType = classifyValidationError(e.message);
            recordTypedDocumentValidationError(errorType);
            throw new ValidationError(`update ${i} (${documentId}): ${e.message}`);
          }
          throw e;
        }
      }

      // Apply all updates
      await typedDocumentRepo.bulkUpdate(updatesToApply);

      const durationMs = Date.now() - startTime;
      log.info({ operation, updated: updatesToApply.length, durationMs }, 'bulk-operation.done');

      return res.status(200).json({ updated: updatesToApply.length });
    }

    // Bulk delete
    if (operation === 'delete') {
      const { documentIds } = body;

      if (!Array.isArray(documentIds)) {
        throw new ValidationError('documentIds must be an array');
      }

      if (documentIds.length === 0) {
        throw new ValidationError('documentIds cannot be empty');
      }

      if (documentIds.length > 100) {
        throw new ValidationError('bulk delete is capped at 100 items');
      }

      if (!documentIds.every((id) => typeof id === 'string')) {
        throw new ValidationError('all documentIds must be strings');
      }

      await typedDocumentRepo.bulkDelete(documentIds as string[]);

      const durationMs = Date.now() - startTime;
      log.info({ operation, deleted: documentIds.length, durationMs }, 'bulk-operation.done');

      return res.status(200).json({ deleted: documentIds.length });
    }

    throw new ValidationError(`unsupported operation: ${operation}`);
  }),
);
