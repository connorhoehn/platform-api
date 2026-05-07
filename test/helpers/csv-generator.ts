/**
 * CSV generation helper for bulk import performance tests.
 * Generates CSV strings with typed data covering all Phase 51 field types.
 */

export interface CSVColumn {
  fieldId: string;
  fieldName: string;
  fieldType: 'text' | 'long_text' | 'number' | 'date' | 'boolean' | 'enum' | 'reference';
  cardinality?: 1 | 'unlimited';
  /** For enum fields: list of valid options */
  enumOptions?: string[];
  /** For reference fields: list of valid document IDs */
  referenceIds?: string[];
}

export interface CSVGeneratorOptions {
  columns: CSVColumn[];
  rowCount: number;
  /** Optional seed for reproducible data generation */
  seed?: number;
}

/**
 * Generate a CSV string with typed test data.
 */
export function generateCSV(options: CSVGeneratorOptions): string {
  const { columns, rowCount } = options;

  // Header row: use field names
  const header = columns.map(col => col.fieldName).join(',');

  // Data rows
  const rows: string[] = [header];
  for (let i = 0; i < rowCount; i++) {
    const row = columns.map(col => generateCell(col, i)).join(',');
    rows.push(row);
  }

  return rows.join('\n');
}

function generateCell(column: CSVColumn, rowIndex: number): string {
  const { fieldType, cardinality, enumOptions, referenceIds } = column;

  if (cardinality === 'unlimited') {
    // Multi-value fields: generate 2-3 semicolon-separated values
    const count = 2 + (rowIndex % 2); // alternate between 2 and 3 values
    const values: string[] = [];
    for (let i = 0; i < count; i++) {
      values.push(generateSingleValue(fieldType, rowIndex * 10 + i, enumOptions, referenceIds));
    }
    return values.join(';');
  } else {
    return generateSingleValue(fieldType, rowIndex, enumOptions, referenceIds);
  }
}

function generateSingleValue(
  fieldType: string,
  index: number,
  enumOptions?: string[],
  referenceIds?: string[],
): string {
  switch (fieldType) {
    case 'text':
      return `Text value ${index}`;
    case 'long_text':
      return `Long text value ${index} with more content to simulate multi-line or paragraph text that users would typically enter in a textarea widget.`;
    case 'number':
      return String(100 + index);
    case 'date':
      // Generate dates in YYYY-MM-DD format, spread across 2026
      const day = (index % 28) + 1;
      const month = ((Math.floor(index / 28) % 12) + 1).toString().padStart(2, '0');
      const dayStr = day.toString().padStart(2, '0');
      return `2026-${month}-${dayStr}`;
    case 'boolean':
      // Alternate between true/false
      return index % 2 === 0 ? 'true' : 'false';
    case 'enum':
      if (!enumOptions || enumOptions.length === 0) {
        throw new Error('enumOptions required for enum field type');
      }
      return enumOptions[index % enumOptions.length];
    case 'reference':
      if (!referenceIds || referenceIds.length === 0) {
        throw new Error('referenceIds required for reference field type');
      }
      return referenceIds[index % referenceIds.length];
    default:
      return `Value ${index}`;
  }
}
