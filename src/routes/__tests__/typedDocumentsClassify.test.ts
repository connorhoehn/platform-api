// Unit tests for classifyValidationError — the metric-label classifier that
// maps TypedDocument validation failure messages to bounded error type strings.
//
// These tests are a contract: if the label set or the routing logic changes,
// the test must change too. Two bugs were fixed by this test suite:
//   1. "boolean fields cannot have unlimited cardinality" was 'other' → now 'type_mismatch'
//   2. "referenced document X is type Y, expected Z" was 'other' → now 'reference_type_mismatch'

import { classifyValidationError } from '../typedDocuments';

describe('classifyValidationError — required_field', () => {
  test('"is required" triggers required_field', () => {
    expect(classifyValidationError('field "Name" is required')).toBe('required_field');
  });

  test('"is required and cannot be empty" triggers required_field', () => {
    expect(classifyValidationError('field "Name" is required and cannot be empty')).toBe('required_field');
  });

  test('"is required (at least one value)" triggers required_field', () => {
    expect(classifyValidationError('field "Tags" is required (at least one value)')).toBe('required_field');
  });
});

describe('classifyValidationError — type_mismatch', () => {
  test('"expects a string value" triggers type_mismatch', () => {
    expect(classifyValidationError('field "Name" expects a string value')).toBe('type_mismatch');
  });

  test('"expects a finite number" triggers type_mismatch', () => {
    expect(classifyValidationError('field "Age" expects a finite number')).toBe('type_mismatch');
  });

  test('"expects a boolean" triggers type_mismatch', () => {
    expect(classifyValidationError('field "Active" expects a boolean')).toBe('type_mismatch');
  });

  test('"expects an ISO date" triggers type_mismatch', () => {
    expect(classifyValidationError('field "DOB" expects an ISO date (YYYY-MM-DD or ISO datetime)')).toBe('type_mismatch');
  });

  test('"expects an array of values" triggers type_mismatch', () => {
    expect(classifyValidationError('field "Tags" expects an array of values')).toBe('type_mismatch');
  });

  test('"expects a referenced documentId" triggers type_mismatch', () => {
    expect(classifyValidationError('field "Parent" expects a referenced documentId')).toBe('type_mismatch');
  });

  test('"cannot have unlimited cardinality" triggers type_mismatch (was: other)', () => {
    // Regression: previously fell through to 'other'
    expect(classifyValidationError('field "Flags": boolean fields cannot have unlimited cardinality')).toBe('type_mismatch');
  });
});

describe('classifyValidationError — reference_type_mismatch', () => {
  test('"is type X, expected Y" triggers reference_type_mismatch (was: other)', () => {
    // Regression: previously fell through to 'other'
    expect(classifyValidationError(
      'field "Parent": referenced document doc-1 is type wrong-type, expected type-parent',
    )).toBe('reference_type_mismatch');
  });
});

describe('classifyValidationError — reference_not_found', () => {
  test('"not found" triggers reference_not_found', () => {
    expect(classifyValidationError('field "Parent": referenced document doc-1 not found')).toBe('reference_not_found');
  });

  test('"does not exist" triggers reference_not_found', () => {
    expect(classifyValidationError('referenced document does not exist')).toBe('reference_not_found');
  });
});

describe('classifyValidationError — enum_invalid', () => {
  test('"not in the configured options" triggers enum_invalid', () => {
    expect(classifyValidationError(
      'field "Status": "deleted" is not in the configured options (valid: draft, published)',
    )).toBe('enum_invalid');
  });
});

describe('classifyValidationError — validation_rule', () => {
  test('"is below min" for number triggers validation_rule', () => {
    expect(classifyValidationError('field "Qty": value 0 is below min 1')).toBe('validation_rule');
  });

  test('"is above max" for number triggers validation_rule', () => {
    expect(classifyValidationError('field "Qty": value 200 is above max 100')).toBe('validation_rule');
  });

  test('"length ... below min" for string triggers validation_rule', () => {
    expect(classifyValidationError('field "Code": value length 2 is below min 3')).toBe('validation_rule');
  });

  test('"length ... above max" for string triggers validation_rule', () => {
    expect(classifyValidationError('field "Code": value length 10 is above max 5')).toBe('validation_rule');
  });

  test('"does not match required pattern" triggers validation_rule', () => {
    expect(classifyValidationError('field "Code": value does not match required pattern')).toBe('validation_rule');
  });

  test('"must be checked" triggers validation_rule', () => {
    expect(classifyValidationError('field "Terms" must be checked')).toBe('validation_rule');
  });
});

describe('classifyValidationError — schema_invalid', () => {
  test('"unknown field" triggers schema_invalid', () => {
    expect(classifyValidationError('unknown field "f-bogus" — not in type schema')).toBe('schema_invalid');
  });

  test('"must be an object keyed" triggers schema_invalid', () => {
    expect(classifyValidationError('values must be an object keyed by fieldId')).toBe('schema_invalid');
  });
});

describe('classifyValidationError — other (catch-all)', () => {
  test('unrecognised message falls to other', () => {
    expect(classifyValidationError('something completely unexpected happened')).toBe('other');
  });
});
