// test/ingest-rule-matcher.test.ts
//
// Tests for matchByRules: all condition operators, priority ordering, and
// field extraction (extension, path, mimeType, name, tag).

import { matchByRules } from '../src/ingest/mapping/rule-matcher';
import type { FileDescriptor } from '../src/ingest/types';
import type { MappingRule } from '../src/ingest/types';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
    return {
        path: 'docs/report.pdf',
        name: 'report.pdf',
        hash: 'abc',
        size: 100,
        mimeType: 'application/pdf',
        lastModified: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeRule(overrides: Partial<MappingRule> = {}): MappingRule {
    return {
        ruleId: 'rule-1',
        targetTypeId: 'type-a',
        priority: 10,
        condition: { field: 'extension', op: 'eq', value: 'pdf' },
        fieldMapping: { name: 'title' },
        ...overrides,
    };
}

// ─── no match ─────────────────────────────────────────────────────────────

describe('matchByRules — no match', () => {
    test('returns null for empty rules', () => {
        const file = makeFile();
        expect(matchByRules(file, [])).toBeNull();
    });

    test('returns null when no rules match', () => {
        const file = makeFile({ name: 'data.csv' });
        const rules = [makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

// ─── result shape ────────────────────────────────────────────────────────

describe('matchByRules — result shape', () => {
    test('returns targetTypeId, fieldMapping, method=rule, confidence=1, ruleId', () => {
        const rule = makeRule({ ruleId: 'r-99', targetTypeId: 'my-type', fieldMapping: { path: 'docPath' } });
        const result = matchByRules(makeFile(), [rule]);
        expect(result).toMatchObject({
            targetTypeId: 'my-type',
            fieldMapping: { path: 'docPath' },
            method: 'rule',
            confidence: 1.0,
            ruleId: 'r-99',
        });
    });
});

// ─── priority ordering ────────────────────────────────────────────────────

describe('matchByRules — priority ordering', () => {
    test('lower-priority-number rule wins over higher', () => {
        const file = makeFile({ name: 'f.pdf' });
        const rules = [
            makeRule({ priority: 20, targetTypeId: 'low-prio', condition: { field: 'extension', op: 'eq', value: 'pdf' } }),
            makeRule({ priority: 5, targetTypeId: 'high-prio', condition: { field: 'extension', op: 'eq', value: 'pdf' } }),
        ];
        const result = matchByRules(file, rules);
        expect(result?.targetTypeId).toBe('high-prio');
    });

    test('second rule matches when first does not', () => {
        const file = makeFile({ name: 'f.csv' });
        const rules = [
            makeRule({ priority: 1, targetTypeId: 'pdf-type', condition: { field: 'extension', op: 'eq', value: 'pdf' } }),
            makeRule({ priority: 2, targetTypeId: 'csv-type', condition: { field: 'extension', op: 'eq', value: 'csv' } }),
        ];
        const result = matchByRules(file, rules);
        expect(result?.targetTypeId).toBe('csv-type');
    });

    test('does not mutate the original rules array', () => {
        const rules = [
            makeRule({ priority: 3 }),
            makeRule({ priority: 1 }),
        ];
        const originalOrder = rules.map((r) => r.priority);
        matchByRules(makeFile(), rules);
        expect(rules.map((r) => r.priority)).toEqual(originalOrder);
    });
});

// ─── condition operators ──────────────────────────────────────────────────

describe('matchByRules — op: eq', () => {
    test('matches exact extension', () => {
        const file = makeFile({ name: 'budget.xlsx' });
        const rules = [makeRule({ condition: { field: 'extension', op: 'eq', value: 'xlsx' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('does not match different extension', () => {
        const file = makeFile({ name: 'budget.csv' });
        const rules = [makeRule({ condition: { field: 'extension', op: 'eq', value: 'xlsx' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

describe('matchByRules — op: contains', () => {
    test('matches path containing substring', () => {
        const file = makeFile({ path: 'finance/2026/report.pdf' });
        const rules = [makeRule({ condition: { field: 'path', op: 'contains', value: 'finance' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('does not match when substring absent', () => {
        const file = makeFile({ path: 'hr/contracts.pdf' });
        const rules = [makeRule({ condition: { field: 'path', op: 'contains', value: 'finance' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

describe('matchByRules — op: startsWith', () => {
    test('matches path starting with prefix', () => {
        const file = makeFile({ path: 'reports/2026/q1.pdf' });
        const rules = [makeRule({ condition: { field: 'path', op: 'startsWith', value: 'reports/' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('does not match path with different prefix', () => {
        const file = makeFile({ path: 'archive/reports/q1.pdf' });
        const rules = [makeRule({ condition: { field: 'path', op: 'startsWith', value: 'reports/' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

describe('matchByRules — op: regex', () => {
    test('matches name against regex pattern', () => {
        const file = makeFile({ name: 'invoice-2026-01.pdf' });
        const rules = [makeRule({ condition: { field: 'name', op: 'regex', value: '^invoice-\\d{4}' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('does not match name failing regex', () => {
        const file = makeFile({ name: 'receipt.pdf' });
        const rules = [makeRule({ condition: { field: 'name', op: 'regex', value: '^invoice-\\d{4}' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

// ─── condition fields ─────────────────────────────────────────────────────

describe('matchByRules — condition field: extension', () => {
    test('extension is lowercased before match', () => {
        const file = makeFile({ name: 'doc.PDF' });
        const rules = [makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('returns null when file has no extension', () => {
        const file = makeFile({ name: 'README' });
        const rules = [makeRule({ condition: { field: 'extension', op: 'eq', value: 'md' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});

describe('matchByRules — condition field: mimeType', () => {
    test('matches by exact mimeType', () => {
        const file = makeFile({ mimeType: 'application/json' });
        const rules = [makeRule({ condition: { field: 'mimeType', op: 'eq', value: 'application/json' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('contains match on mimeType', () => {
        const file = makeFile({ mimeType: 'application/json' });
        const rules = [makeRule({ condition: { field: 'mimeType', op: 'contains', value: 'json' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });
});

describe('matchByRules — condition field: tag', () => {
    test('contains match on tags (joined as comma string)', () => {
        const file = makeFile({ tags: ['finance', 'q1'] });
        const rules = [makeRule({ condition: { field: 'tag', op: 'contains', value: 'finance' } })];
        expect(matchByRules(file, rules)).not.toBeNull();
    });

    test('returns null when file has no tags', () => {
        const file = makeFile({ tags: undefined });
        const rules = [makeRule({ condition: { field: 'tag', op: 'contains', value: 'finance' } })];
        expect(matchByRules(file, rules)).toBeNull();
    });
});
