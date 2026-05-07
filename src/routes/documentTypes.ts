// Phase 51 Phase A — REST API for document type schemas.
//
// Endpoints:
//   POST   /api/document-types               → create
//   GET    /api/document-types               → list (capped)
//   GET    /api/document-types/:typeId       → fetch one
//   PUT    /api/document-types/:typeId       → update name/description/icon/fields
//   DELETE /api/document-types/:typeId       → delete (instances unaffected)
//
// Request/response shape mirrors `DocumentTypeItem` from the repository.
// All routes require auth; `req.user.sub` becomes `createdBy` on create.

import { Router, type Request, type Response } from 'express';
import { randomUUID } from 'crypto';
import { documentTypeRepo } from '../repositories';
import type {
  DocumentTypeItem,
  DocumentTypeFieldItem,
  DocumentTypeFieldKind,
  DocumentTypeFieldWidget,
  DocumentTypeFieldCardinality,
  DocumentTypeFieldValidation,
  DocumentTypeFieldShowWhen,
  DocumentTypeFieldDisplayModes,
  DocumentTypeVersionSnapshot,
} from '../repositories';
import { asyncHandler, ValidationError, NotFoundError } from '../middleware/error-handler';
import { withContext } from '../lib/logger';
import {
  recordDocumentTypeCreated,
  recordDocumentTypeUpdated,
  recordDocumentTypeDeleted,
} from '../observability/metrics';

const log = withContext({ route: 'documentTypes' });

export const documentTypesRouter = Router();

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

const VALID_FIELD_TYPES = new Set<DocumentTypeFieldKind>([
  'text', 'long_text', 'number', 'date', 'boolean', 'enum', 'reference',
]);
const VALID_WIDGETS = new Set<DocumentTypeFieldWidget>([
  'text_field', 'textarea', 'number_input', 'date_picker', 'checkbox', 'select', 'reference_picker',
]);

function parseField(raw: unknown): DocumentTypeFieldItem {
  if (typeof raw !== 'object' || raw === null) {
    throw new ValidationError('field must be an object');
  }
  const r = raw as Record<string, unknown>;

  const fieldType = r.fieldType;
  if (typeof fieldType !== 'string' || !VALID_FIELD_TYPES.has(fieldType as DocumentTypeFieldKind)) {
    throw new ValidationError(`fieldType must be one of: ${[...VALID_FIELD_TYPES].join(', ')}`);
  }
  const widget = r.widget;
  if (typeof widget !== 'string' || !VALID_WIDGETS.has(widget as DocumentTypeFieldWidget)) {
    throw new ValidationError(`widget must be one of: ${[...VALID_WIDGETS].join(', ')}`);
  }
  const cardinalityRaw = r.cardinality;
  let cardinality: DocumentTypeFieldCardinality;
  if (cardinalityRaw === 1 || cardinalityRaw === 'unlimited') {
    cardinality = cardinalityRaw;
  } else {
    throw new ValidationError('cardinality must be 1 or "unlimited"');
  }
  if (typeof r.name !== 'string' || r.name.trim().length === 0) {
    throw new ValidationError('field.name is required');
  }

  // Phase C — enum + reference carry extra schema-level config that the
  // value-shape validator depends on. Reject malformed config at type-creation
  // so instance writes don't fail later with a stale schema.
  let options: string[] | undefined;
  let referenceTypeId: string | undefined;
  if (fieldType === 'enum') {
    if (!Array.isArray(r.options) || r.options.length === 0) {
      throw new ValidationError(`field "${r.name}" (enum) requires a non-empty options array`);
    }
    if (!r.options.every((o) => typeof o === 'string' && o.length > 0)) {
      throw new ValidationError(`field "${r.name}" (enum) options must be non-empty strings`);
    }
    options = r.options as string[];
  }
  if (fieldType === 'reference') {
    if (typeof r.referenceTypeId !== 'string' || r.referenceTypeId.length === 0) {
      throw new ValidationError(`field "${r.name}" (reference) requires referenceTypeId`);
    }
    referenceTypeId = r.referenceTypeId;
  }

  // Phase D — additive validation rules + showWhen conditional. Both are
  // optional; absent ⇒ no constraints / always-visible (legacy behavior).
  const validation = parseValidation(r.validation, r.name as string);
  const showWhen = parseShowWhen(r.showWhen, r.name as string);
  const displayModes = parseDisplayModes(r.displayModes, r.name as string);

  return {
    fieldId: typeof r.fieldId === 'string' && r.fieldId ? r.fieldId : randomUUID(),
    name: r.name,
    fieldType: fieldType as DocumentTypeFieldKind,
    widget: widget as DocumentTypeFieldWidget,
    cardinality,
    required: r.required === true,
    helpText: typeof r.helpText === 'string' ? r.helpText : '',
    ...(options !== undefined ? { options } : {}),
    ...(referenceTypeId !== undefined ? { referenceTypeId } : {}),
    ...(validation !== undefined ? { validation } : {}),
    ...(showWhen !== undefined ? { showWhen } : {}),
    ...(displayModes !== undefined ? { displayModes } : {}),
  };
}

function parseDisplayModes(raw: unknown, fieldName: string): DocumentTypeFieldDisplayModes | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new ValidationError(`field "${fieldName}" displayModes must be an object`);
  }
  const r = raw as Record<string, unknown>;
  const out: DocumentTypeFieldDisplayModes = {};
  for (const key of ['full', 'teaser', 'list'] as const) {
    if (r[key] === undefined) continue;
    if (typeof r[key] !== 'boolean') {
      throw new ValidationError(`field "${fieldName}" displayModes.${key} must be a boolean`);
    }
    out[key] = r[key] as boolean;
  }
  for (const key of Object.keys(r)) {
    if (key !== 'full' && key !== 'teaser' && key !== 'list') {
      throw new ValidationError(`field "${fieldName}" displayModes has unknown key "${key}"`);
    }
  }
  return Object.keys(out).length > 0 ? out : undefined;
}

function parseValidation(raw: unknown, fieldName: string): DocumentTypeFieldValidation | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new ValidationError(`field "${fieldName}" validation must be an object`);
  }
  const r = raw as Record<string, unknown>;
  const v: DocumentTypeFieldValidation = {};
  if (r.min !== undefined) {
    if (typeof r.min !== 'number' || !Number.isFinite(r.min)) {
      throw new ValidationError(`field "${fieldName}" validation.min must be a finite number`);
    }
    v.min = r.min;
  }
  if (r.max !== undefined) {
    if (typeof r.max !== 'number' || !Number.isFinite(r.max)) {
      throw new ValidationError(`field "${fieldName}" validation.max must be a finite number`);
    }
    v.max = r.max;
  }
  if (v.min !== undefined && v.max !== undefined && v.min > v.max) {
    throw new ValidationError(`field "${fieldName}" validation: min (${v.min}) > max (${v.max})`);
  }
  if (r.regex !== undefined) {
    if (typeof r.regex !== 'string') {
      throw new ValidationError(`field "${fieldName}" validation.regex must be a string`);
    }
    try { new RegExp(r.regex); } catch (e) {
      throw new ValidationError(`field "${fieldName}" validation.regex did not compile: ${(e as Error).message}`);
    }
    v.regex = r.regex;
  }
  if (r.requireTrue !== undefined) {
    if (typeof r.requireTrue !== 'boolean') {
      throw new ValidationError(`field "${fieldName}" validation.requireTrue must be a boolean`);
    }
    v.requireTrue = r.requireTrue;
  }
  return Object.keys(v).length > 0 ? v : undefined;
}

function parseShowWhen(raw: unknown, fieldName: string): DocumentTypeFieldShowWhen | undefined {
  if (raw === undefined) return undefined;
  if (typeof raw !== 'object' || raw === null || Array.isArray(raw)) {
    throw new ValidationError(`field "${fieldName}" showWhen must be an object`);
  }
  const r = raw as Record<string, unknown>;
  if (typeof r.fieldId !== 'string' || r.fieldId.length === 0) {
    throw new ValidationError(`field "${fieldName}" showWhen.fieldId must be a non-empty string`);
  }
  if (typeof r.equals !== 'string' && typeof r.equals !== 'number' && typeof r.equals !== 'boolean') {
    throw new ValidationError(`field "${fieldName}" showWhen.equals must be a string, number, or boolean`);
  }
  return { fieldId: r.fieldId, equals: r.equals };
}

function parseFields(raw: unknown): DocumentTypeFieldItem[] {
  if (!Array.isArray(raw)) throw new ValidationError('fields must be an array');
  if (raw.length > 100) throw new ValidationError('fields cap is 100 per type');
  const fields = raw.map(parseField);
  // Phase D — showWhen.fieldId must reference a real field on this type.
  // Caller must pass an explicit fieldId on the source field if dependents
  // reference it; otherwise the auto-generated UUID won't match.
  const ids = new Set(fields.map((f) => f.fieldId));
  for (const f of fields) {
    if (f.showWhen && !ids.has(f.showWhen.fieldId)) {
      throw new ValidationError(
        `field "${f.name}" showWhen.fieldId "${f.showWhen.fieldId}" is not a field on this type`,
      );
    }
  }
  return fields;
}

// ---------------------------------------------------------------------------
// Routes
// ---------------------------------------------------------------------------

documentTypesRouter.post('/', asyncHandler(async (req: Request, res: Response) => {
  const body = (req.body ?? {}) as Record<string, unknown>;
  const name = body.name;
  if (typeof name !== 'string' || name.trim().length === 0) {
    throw new ValidationError('name is required');
  }
  const fields = parseFields(body.fields ?? []);

  const now = new Date().toISOString();
  const item: DocumentTypeItem = {
    typeId: randomUUID(),
    name: name.trim(),
    description: typeof body.description === 'string' ? body.description : '',
    icon: typeof body.icon === 'string' ? body.icon : '📄',
    fields,
    createdBy: req.user!.sub,
    createdAt: now,
    updatedAt: now,
    version: 1,
  };
  await documentTypeRepo.create(item);
  recordDocumentTypeCreated();
  log.info({ typeId: item.typeId, name: item.name, fieldCount: fields.length, userId: req.user!.sub }, 'create');
  res.status(201).json(item);
}));

documentTypesRouter.get('/', asyncHandler(async (_req: Request, res: Response) => {
  const items = await documentTypeRepo.list();
  log.debug({ count: items.length }, 'list');
  res.status(200).json({ items });
}));

documentTypesRouter.get('/:typeId', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string };
  const item = await documentTypeRepo.get(params.typeId);
  if (!item) throw new NotFoundError(`document type ${params.typeId} not found`);
  log.debug({ typeId: params.typeId, name: item.name }, 'fetch');
  res.status(200).json(item);
}));

documentTypesRouter.put('/:typeId', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string };
  const body = (req.body ?? {}) as Record<string, unknown>;

  const existing = await documentTypeRepo.get(params.typeId);
  if (!existing) throw new NotFoundError(`document type ${params.typeId} not found`);

  const patch: Partial<Pick<DocumentTypeItem, 'name' | 'description' | 'icon' | 'fields'>> = {};
  if (typeof body.name === 'string') {
    if (body.name.trim().length === 0) throw new ValidationError('name cannot be empty');
    patch.name = body.name.trim();
  }
  if (typeof body.description === 'string') patch.description = body.description;
  if (typeof body.icon === 'string') patch.icon = body.icon;
  if (body.fields !== undefined) patch.fields = parseFields(body.fields);

  const updated = await documentTypeRepo.update(params.typeId, patch, existing);
  recordDocumentTypeUpdated();
  log.info({
    typeId: params.typeId,
    name: updated.name,
    fieldCount: updated.fields.length,
    userId: req.user!.sub,
    version: updated.version,
  }, 'update');
  res.status(200).json(updated);
}));

documentTypesRouter.delete('/:typeId', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string };
  const item = await documentTypeRepo.get(params.typeId);
  await documentTypeRepo.delete(params.typeId);
  recordDocumentTypeDeleted();
  log.warn({ typeId: params.typeId, name: item?.name, userId: req.user!.sub }, 'delete');
  res.status(204).end();
}));

// Schema versioning — list previous versions of a document type
documentTypesRouter.get('/:typeId/versions', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string };
  const item = await documentTypeRepo.get(params.typeId);
  if (!item) throw new NotFoundError(`document type ${params.typeId} not found`);

  const currentSnapshot: DocumentTypeVersionSnapshot = {
    version: item.version ?? 1,
    name: item.name,
    description: item.description,
    icon: item.icon,
    fields: item.fields,
    updatedAt: item.updatedAt,
  };

  const history = item.previousVersions ?? [];
  res.status(200).json({ typeId: params.typeId, current: currentSnapshot, versions: history });
}));

documentTypesRouter.get('/:typeId/versions/:version', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string; version: string };
  const versionNum = parseInt(params.version, 10);
  if (!Number.isFinite(versionNum) || versionNum < 1) {
    throw new ValidationError('version must be a positive integer');
  }

  const item = await documentTypeRepo.get(params.typeId);
  if (!item) throw new NotFoundError(`document type ${params.typeId} not found`);

  if (versionNum === (item.version ?? 1)) {
    res.status(200).json({
      version: item.version ?? 1,
      name: item.name,
      description: item.description,
      icon: item.icon,
      fields: item.fields,
      updatedAt: item.updatedAt,
    });
    return;
  }

  const snapshot = (item.previousVersions ?? []).find(v => v.version === versionNum);
  if (!snapshot) throw new NotFoundError(`version ${versionNum} not found for type ${params.typeId}`);
  res.status(200).json(snapshot);
}));

// Phase 51 Phase G — JSON Schema export
documentTypesRouter.get('/:typeId/schema', asyncHandler(async (req: Request, res: Response) => {
  const params = req.params as { typeId: string };
  const item = await documentTypeRepo.get(params.typeId);
  if (!item) throw new NotFoundError(`document type ${params.typeId} not found`);

  // Generate JSON Schema draft-07 representation of the document type
  const schema: Record<string, unknown> = {
    $schema: 'http://json-schema.org/draft-07/schema#',
    $id: `#/document-types/${item.typeId}`,
    title: item.name,
    description: item.description || undefined,
    type: 'object',
    properties: {} as Record<string, unknown>,
    required: [] as string[],
  };

  const properties: Record<string, unknown> = {};
  const required: string[] = [];

  for (const field of item.fields) {
    const propSchema: Record<string, unknown> = {
      title: field.name,
      description: field.helpText || undefined,
    };

    // Map field type to JSON Schema type
    switch (field.fieldType) {
      case 'text':
      case 'long_text':
      case 'enum':
      case 'reference':
        propSchema.type = 'string';
        break;
      case 'number':
        propSchema.type = 'number';
        break;
      case 'date':
        propSchema.type = 'string';
        propSchema.format = 'date';
        break;
      case 'boolean':
        propSchema.type = 'boolean';
        break;
    }

    // Add enum constraints
    if (field.fieldType === 'enum' && field.options) {
      propSchema.enum = field.options;
    }

    // Add validation constraints
    if (field.validation) {
      const v = field.validation;
      if (v.min !== undefined) {
        if (field.fieldType === 'number') {
          propSchema.minimum = v.min;
        } else if (field.fieldType === 'text' || field.fieldType === 'long_text') {
          propSchema.minLength = v.min;
        }
      }
      if (v.max !== undefined) {
        if (field.fieldType === 'number') {
          propSchema.maximum = v.max;
        } else if (field.fieldType === 'text' || field.fieldType === 'long_text') {
          propSchema.maxLength = v.max;
        }
      }
      if (v.regex !== undefined) {
        propSchema.pattern = v.regex;
      }
    }

    // Handle cardinality
    if (field.cardinality === 'unlimited') {
      properties[field.fieldId] = {
        type: 'array',
        items: propSchema,
        minItems: field.required ? 1 : 0,
      };
    } else {
      properties[field.fieldId] = propSchema;
    }

    // Track required fields (respecting showWhen visibility — schema export
    // doesn't model conditionals, so we export all fields as optional if any
    // have showWhen; consumers must validate with the live type schema)
    if (field.required && !field.showWhen) {
      required.push(field.fieldId);
    }
  }

  schema.properties = properties;
  if (required.length > 0) {
    schema.required = required;
  } else {
    delete schema.required;
  }

  res.status(200).json(schema);
}));
