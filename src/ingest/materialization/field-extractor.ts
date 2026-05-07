// Ingest subsystem — field extraction from raw file content.
//
// Extracts structured field values from FileContent based on a field mapping.
// Supports JSON (dot-path), CSV (column name), and text/markdown extraction.

import { FileContent } from '../types';

export interface ExtractionResult {
  fields: Record<string, unknown>;
  warnings: string[]; // non-fatal issues (missing optional fields, etc.)
}

/**
 * Resolve a dot-path (e.g. "metadata.title") against an object.
 * Returns undefined if any segment is missing.
 */
function resolveDotPath(obj: unknown, path: string): unknown {
  const segments = path.split('.');
  let current: unknown = obj;
  for (const seg of segments) {
    if (current === null || current === undefined || typeof current !== 'object') {
      return undefined;
    }
    current = (current as Record<string, unknown>)[seg];
  }
  return current;
}

/**
 * Extract the first markdown heading (# ...) or the first non-empty line.
 */
function extractTitle(text: string): string | undefined {
  const lines = text.split('\n');
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    // Match "# Heading" or "## Heading" — return text after the hashes
    const headingMatch = trimmed.match(/^#+\s+(.+)$/);
    if (headingMatch) return headingMatch[1].trim();
    // Otherwise return first non-empty line as title
    return trimmed;
  }
  return undefined;
}

function extractJson(
  content: FileContent,
  fieldMapping: Record<string, string>,
): ExtractionResult {
  const warnings: string[] = [];
  const fields: Record<string, unknown> = {};

  let parsed: unknown = content.parsed;
  if (parsed === undefined) {
    // Attempt to parse from raw data
    try {
      parsed = JSON.parse(content.data.toString('utf-8'));
    } catch {
      return { fields: {}, warnings: ['Failed to parse JSON content'] };
    }
  }

  for (const [sourceField, targetFieldId] of Object.entries(fieldMapping)) {
    const value = resolveDotPath(parsed, sourceField);
    if (value === undefined) {
      warnings.push(`Source field "${sourceField}" not found in JSON`);
      continue;
    }
    fields[targetFieldId] = value;
  }

  return { fields, warnings };
}

function extractCsv(
  content: FileContent,
  fieldMapping: Record<string, string>,
): ExtractionResult {
  const warnings: string[] = [];
  const fields: Record<string, unknown> = {};

  const rows = content.parsed as Record<string, unknown>[] | undefined;
  if (!Array.isArray(rows) || rows.length === 0) {
    return { fields: {}, warnings: ['CSV content has no parsed rows'] };
  }

  // For a single document materialization, use the first row.
  // (The orchestrator calls this per-file; multi-row CSVs produce multiple items
  // at a higher layer, not here.)
  const row = rows[0];

  for (const [sourceField, targetFieldId] of Object.entries(fieldMapping)) {
    const value = row[sourceField];
    if (value === undefined) {
      warnings.push(`Column "${sourceField}" not found in CSV row`);
      continue;
    }
    fields[targetFieldId] = value;
  }

  return { fields, warnings };
}

function extractText(
  content: FileContent,
  fieldMapping: Record<string, string>,
): ExtractionResult {
  const warnings: string[] = [];
  const fields: Record<string, unknown> = {};

  const text = content.data.toString('utf-8');

  for (const [sourceField, targetFieldId] of Object.entries(fieldMapping)) {
    if (sourceField === 'content') {
      fields[targetFieldId] = text;
    } else if (sourceField === 'title') {
      const title = extractTitle(text);
      if (title === undefined) {
        warnings.push('No title found (no heading or non-empty line)');
        continue;
      }
      fields[targetFieldId] = title;
    } else {
      warnings.push(`Unsupported source field "${sourceField}" for text/markdown — only "content" and "title" are supported`);
    }
  }

  return { fields, warnings };
}

/**
 * Extracts field values from file content using a field mapping.
 * fieldMapping maps: sourceField -> targetFieldId
 *
 * For JSON: sourceField is a dot-path (e.g. "metadata.title")
 * For CSV: sourceField is a column name (content.parsed is array of row objects)
 * For text/markdown: sourceField is "content" (entire text) or "title" (first heading)
 */
export function extractFields(
  content: FileContent,
  fieldMapping: Record<string, string>,
  mimeType: string,
): ExtractionResult {
  if (mimeType === 'application/json') {
    return extractJson(content, fieldMapping);
  }

  if (mimeType === 'text/csv') {
    return extractCsv(content, fieldMapping);
  }

  if (mimeType === 'text/plain' || mimeType === 'text/markdown') {
    return extractText(content, fieldMapping);
  }

  // Fallback: attempt JSON extraction (covers application/json variants)
  if (mimeType.includes('json')) {
    return extractJson(content, fieldMapping);
  }

  return {
    fields: {},
    warnings: [`Unsupported mimeType "${mimeType}" — no extraction strategy available`],
  };
}
