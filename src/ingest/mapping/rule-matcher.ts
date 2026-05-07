import { MappingRule, MappingResult, FileDescriptor } from '../types';

/**
 * Evaluates mapping rules against a file descriptor in priority order (lower = first).
 * Returns the first matching rule's result, or null if no rules match.
 */
export function matchByRules(file: FileDescriptor, rules: MappingRule[]): MappingResult | null {
  // Sort rules by priority (lower number = higher priority)
  const sorted = [...rules].sort((a, b) => a.priority - b.priority);

  for (const rule of sorted) {
    if (evaluateCondition(rule, file)) {
      return {
        targetTypeId: rule.targetTypeId,
        fieldMapping: rule.fieldMapping,
        method: 'rule',
        confidence: 1.0,
        ruleId: rule.ruleId,
      };
    }
  }

  return null;
}

function evaluateCondition(rule: MappingRule, file: FileDescriptor): boolean {
  const { field, op, value } = rule.condition;
  const actual = getFieldValue(file, field);

  if (actual === null) return false;

  switch (op) {
    case 'eq': return actual === value;
    case 'contains': return actual.includes(value);
    case 'startsWith': return actual.startsWith(value);
    case 'regex': return new RegExp(value).test(actual);
    default: return false;
  }
}

function getFieldValue(file: FileDescriptor, field: MappingRule['condition']['field']): string | null {
  switch (field) {
    case 'extension': {
      const dot = file.name.lastIndexOf('.');
      return dot >= 0 ? file.name.slice(dot + 1).toLowerCase() : null;
    }
    case 'path': return file.path;
    case 'mimeType': return file.mimeType;
    case 'name': return file.name;
    case 'tag': return file.tags?.join(',') ?? null;  // for 'contains' checks against tags
    default: return null;
  }
}
