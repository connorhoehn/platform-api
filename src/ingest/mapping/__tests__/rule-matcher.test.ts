// social-api/src/ingest/mapping/__tests__/rule-matcher.test.ts
//
// Tests for matchByRules: priority ordering, all condition ops, all field types, no-match.

import { matchByRules } from '../rule-matcher';
import type { FileDescriptor, MappingRule } from '../../types';

function makeFile(overrides: Partial<FileDescriptor> = {}): FileDescriptor {
    return {
        path: '/docs/report.txt',
        name: 'report.txt',
        hash: 'abc',
        size: 100,
        mimeType: 'text/plain',
        lastModified: '2026-01-01T00:00:00Z',
        ...overrides,
    };
}

function makeRule(overrides: Partial<MappingRule> & { ruleId?: string } = {}): MappingRule {
    return {
        ruleId: 'r1',
        priority: 1,
        condition: { field: 'mimeType', op: 'eq', value: 'text/plain' },
        targetTypeId: 'doc-type',
        fieldMapping: {},
        ...overrides,
    };
}

// ─── no-match ─────────────────────────────────────────────────────────────

describe('matchByRules — no match', () => {
    test('returns null for empty rules array', () => {
        expect(matchByRules(makeFile(), [])).toBeNull();
    });

    test('returns null when no rule condition is satisfied', () => {
        const rule = makeRule({ condition: { field: 'mimeType', op: 'eq', value: 'application/json' } });
        expect(matchByRules(makeFile({ mimeType: 'text/plain' }), [rule])).toBeNull();
    });
});

// ─── match result shape ────────────────────────────────────────────────────

describe('matchByRules — result shape', () => {
    test('returns MappingResult with correct fields on match', () => {
        const rule = makeRule({ targetTypeId: 'note', fieldMapping: { content: 'body' } });
        const result = matchByRules(makeFile(), [rule]);
        expect(result).not.toBeNull();
        expect(result!.targetTypeId).toBe('note');
        expect(result!.fieldMapping).toEqual({ content: 'body' });
        expect(result!.method).toBe('rule');
        expect(result!.confidence).toBe(1.0);
        expect(result!.ruleId).toBe('r1');
    });
});

// ─── priority ordering ────────────────────────────────────────────────────

describe('matchByRules — priority ordering', () => {
    test('lower priority number wins when both rules match', () => {
        const low = makeRule({ ruleId: 'low', priority: 1, targetTypeId: 'low-type' });
        const high = makeRule({ ruleId: 'high', priority: 10, targetTypeId: 'high-type' });
        // Pass high-priority (10) first in array — should still pick low (1)
        const result = matchByRules(makeFile(), [high, low]);
        expect(result!.targetTypeId).toBe('low-type');
        expect(result!.ruleId).toBe('low');
    });

    test('returns first matching rule by priority when earlier rule does not match', () => {
        const noMatch = makeRule({ ruleId: 'no', priority: 1, condition: { field: 'mimeType', op: 'eq', value: 'application/json' } });
        const match = makeRule({ ruleId: 'yes', priority: 2, targetTypeId: 'found', condition: { field: 'mimeType', op: 'eq', value: 'text/plain' } });
        const result = matchByRules(makeFile(), [match, noMatch]);
        expect(result!.ruleId).toBe('yes');
    });
});

// ─── op: eq ───────────────────────────────────────────────────────────────

describe('matchByRules — op:eq', () => {
    test('eq matches exact mimeType', () => {
        const rule = makeRule({ condition: { field: 'mimeType', op: 'eq', value: 'text/plain' } });
        expect(matchByRules(makeFile({ mimeType: 'text/plain' }), [rule])).not.toBeNull();
    });

    test('eq does not match partial mimeType', () => {
        const rule = makeRule({ condition: { field: 'mimeType', op: 'eq', value: 'text' } });
        expect(matchByRules(makeFile({ mimeType: 'text/plain' }), [rule])).toBeNull();
    });
});

// ─── op: contains ─────────────────────────────────────────────────────────

describe('matchByRules — op:contains', () => {
    test('contains matches substring of path', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'contains', value: '/docs/' } });
        expect(matchByRules(makeFile({ path: '/docs/report.txt' }), [rule])).not.toBeNull();
    });

    test('contains does not match absent substring', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'contains', value: '/archive/' } });
        expect(matchByRules(makeFile({ path: '/docs/report.txt' }), [rule])).toBeNull();
    });
});

// ─── op: startsWith ───────────────────────────────────────────────────────

describe('matchByRules — op:startsWith', () => {
    test('startsWith matches path prefix', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'startsWith', value: '/docs' } });
        expect(matchByRules(makeFile({ path: '/docs/report.txt' }), [rule])).not.toBeNull();
    });

    test('startsWith does not match mid-path value', () => {
        const rule = makeRule({ condition: { field: 'path', op: 'startsWith', value: 'report' } });
        expect(matchByRules(makeFile({ path: '/docs/report.txt' }), [rule])).toBeNull();
    });
});

// ─── op: regex ────────────────────────────────────────────────────────────

describe('matchByRules — op:regex', () => {
    test('regex matches pattern against name', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'regex', value: '\\.(txt|md)$' } });
        expect(matchByRules(makeFile({ name: 'notes.md' }), [rule])).not.toBeNull();
    });

    test('regex does not match non-matching name', () => {
        const rule = makeRule({ condition: { field: 'name', op: 'regex', value: '\\.pdf$' } });
        expect(matchByRules(makeFile({ name: 'notes.md' }), [rule])).toBeNull();
    });
});

// ─── field: extension ─────────────────────────────────────────────────────

describe('matchByRules — field:extension', () => {
    test('extension extracted from name (lowercased)', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'pdf' } });
        expect(matchByRules(makeFile({ name: 'Report.PDF' }), [rule])).not.toBeNull();
    });

    test('extension returns null for name with no dot → no match', () => {
        const rule = makeRule({ condition: { field: 'extension', op: 'eq', value: 'txt' } });
        expect(matchByRules(makeFile({ name: 'README' }), [rule])).toBeNull();
    });
});

// ─── field: tag ───────────────────────────────────────────────────────────

describe('matchByRules — field:tag', () => {
    test('tag:contains matches when tag is in tags array', () => {
        const rule = makeRule({ condition: { field: 'tag', op: 'contains', value: 'report' } });
        expect(matchByRules(makeFile({ tags: ['report', 'finance'] }), [rule])).not.toBeNull();
    });

    test('tag returns null when tags is undefined', () => {
        const rule = makeRule({ condition: { field: 'tag', op: 'contains', value: 'report' } });
        const file = makeFile();
        delete file.tags;
        expect(matchByRules(file, [rule])).toBeNull();
    });
});

// ─── unknown op ───────────────────────────────────────────────────────────

describe('matchByRules — unknown op', () => {
    test('unknown op returns no match', () => {
        const rule = makeRule({ condition: { field: 'mimeType', op: 'unknown' as never, value: 'text/plain' } });
        expect(matchByRules(makeFile(), [rule])).toBeNull();
    });
});
