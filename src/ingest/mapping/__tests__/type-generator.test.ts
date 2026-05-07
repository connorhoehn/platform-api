// social-api/src/ingest/mapping/__tests__/type-generator.test.ts
//
// Tests for suggestTypeMatch, generateTypeProposal, buildStoredProposal.
// Uses FixtureLLMClient from distributed-core for deterministic LLM simulation.

import { FixtureLLMClient } from 'distributed-core';
import {
    suggestTypeMatch,
    generateTypeProposal,
    buildStoredProposal,
    type ExistingTypeDescriptor,
    type TypeProposal,
} from '../type-generator';

// ─── Helpers ───────────────────────────────────────────────────────────────

const existingTypes: ExistingTypeDescriptor[] = [
    {
        typeId: 'meeting-notes',
        name: 'Meeting Notes',
        description: 'Notes from a meeting',
        fields: [{ name: 'title', sectionType: 'text' }, { name: 'date', sectionType: 'date' }],
    },
    {
        typeId: 'invoice',
        name: 'Invoice',
        description: 'Financial invoice document',
        fields: [{ name: 'amount', sectionType: 'number' }, { name: 'vendor', sectionType: 'text' }],
    },
];

// ─── suggestTypeMatch ─────────────────────────────────────────────────────

describe('suggestTypeMatch', () => {
    test('returns MappingResult when LLM returns valid match above confidence floor', async () => {
        const response = JSON.stringify({
            typeId: 'meeting-notes',
            confidence: 0.9,
            fieldMapping: { title: 'name', date: 'meetingDate' },
        });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('Meeting content', 'notes.txt', 'text/plain', existingTypes, llm);
        expect(result).not.toBeNull();
        expect(result!.targetTypeId).toBe('meeting-notes');
        expect(result!.confidence).toBe(0.9);
        expect(result!.method).toBe('llm-match');
        expect(result!.fieldMapping).toEqual({ title: 'name', date: 'meetingDate' });
    });

    test('returns null when confidence is below threshold (0.3)', async () => {
        const response = JSON.stringify({
            typeId: 'invoice',
            confidence: 0.1,
            fieldMapping: {},
        });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('some content', 'file.txt', 'text/plain', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('returns null when confidence is exactly 0 (no match)', async () => {
        const response = JSON.stringify({ typeId: 'invoice', confidence: 0, fieldMapping: {} });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('blob', 'blob.bin', 'application/octet-stream', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('returns null when LLM response is not valid JSON', async () => {
        const llm = new FixtureLLMClient(['This is not JSON at all.']);
        const result = await suggestTypeMatch('content', 'file.txt', 'text/plain', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('handles JSON wrapped in markdown code fence', async () => {
        const jsonBody = JSON.stringify({ typeId: 'meeting-notes', confidence: 0.8, fieldMapping: {} });
        const llm = new FixtureLLMClient([`\`\`\`json\n${jsonBody}\n\`\`\``]);
        const result = await suggestTypeMatch('content', 'file.txt', 'text/plain', existingTypes, llm);
        expect(result).not.toBeNull();
        expect(result!.targetTypeId).toBe('meeting-notes');
    });

    test('defaults fieldMapping to {} when absent from LLM response', async () => {
        const response = JSON.stringify({ typeId: 'meeting-notes', confidence: 0.7 });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('content', 'file.txt', 'text/plain', existingTypes, llm);
        expect(result!.fieldMapping).toEqual({});
    });

    test('uses provided model parameter', async () => {
        const response = JSON.stringify({ typeId: 'invoice', confidence: 0.5, fieldMapping: {} });
        const llm = new FixtureLLMClient([response]);
        // Just verify it doesn't throw with a non-default model
        const result = await suggestTypeMatch('content', 'file.txt', 'text/plain', existingTypes, llm, 'claude-opus-4-7');
        expect(result).not.toBeNull();
    });
});

// ─── generateTypeProposal ─────────────────────────────────────────────────

describe('generateTypeProposal', () => {
    const validProposal: TypeProposal = {
        name: 'MeetingNotes',
        description: 'Structured notes from a meeting',
        fields: [
            { name: 'title', sectionType: 'text', required: true },
            { name: 'date', sectionType: 'date', required: true },
            { name: 'summary', sectionType: 'text', required: false },
        ],
        reasoning: 'The content follows a meeting notes pattern',
    };

    test('returns TypeProposal from valid LLM response', async () => {
        const llm = new FixtureLLMClient([JSON.stringify(validProposal)]);
        const result = await generateTypeProposal('Meeting: ...\nDate: 2026-01-01', 'notes.txt', 'text/plain', llm);
        expect(result.name).toBe('MeetingNotes');
        expect(result.fields).toHaveLength(3);
        expect(result.fields[0].name).toBe('title');
        expect(result.fields[0].required).toBe(true);
    });

    test('throws on invalid JSON from LLM', async () => {
        const llm = new FixtureLLMClient(['definitely not JSON']);
        await expect(generateTypeProposal('content', 'file.txt', 'text/plain', llm)).rejects.toThrow(
            'failed to parse LLM response',
        );
    });

    test('throws when LLM returns JSON missing name', async () => {
        const llm = new FixtureLLMClient([JSON.stringify({ fields: [{ name: 'x' }] })]);
        await expect(generateTypeProposal('content', 'file.txt', 'text/plain', llm)).rejects.toThrow(
            'missing required fields',
        );
    });

    test('throws when LLM returns JSON with empty fields array', async () => {
        const llm = new FixtureLLMClient([JSON.stringify({ name: 'SomeType', fields: [] })]);
        await expect(generateTypeProposal('content', 'file.txt', 'text/plain', llm)).rejects.toThrow(
            'missing required fields',
        );
    });

    test('handles missing description and reasoning gracefully', async () => {
        const minimal = { name: 'SimpleType', fields: [{ name: 'content', sectionType: 'text', required: true }] };
        const llm = new FixtureLLMClient([JSON.stringify(minimal)]);
        const result = await generateTypeProposal('content', 'file.txt', 'text/plain', llm);
        expect(result.description).toBe('');
        expect(result.reasoning).toBe('');
    });

    test('handles JSON in markdown fence', async () => {
        const fenced = `\`\`\`\n${JSON.stringify(validProposal)}\n\`\`\``;
        const llm = new FixtureLLMClient([fenced]);
        const result = await generateTypeProposal('content', 'file.txt', 'text/plain', llm);
        expect(result.name).toBe('MeetingNotes');
    });
});

// ─── buildStoredProposal ──────────────────────────────────────────────────

describe('buildStoredProposal', () => {
    const proposal: TypeProposal = {
        name: 'InvoiceRecord',
        description: 'Financial invoice',
        fields: [{ name: 'amount', sectionType: 'number', required: true }],
        reasoning: 'Invoice-like content',
    };

    test('returns StoredTypeProposal with status=pending-approval', () => {
        const stored = buildStoredProposal('item-1', proposal);
        expect(stored.status).toBe('pending-approval');
        expect(stored.ingestId).toBe('item-1');
        expect(stored.proposal).toBe(proposal);
    });

    test('proposalId is a non-empty string', () => {
        const stored = buildStoredProposal('item-2', proposal);
        expect(typeof stored.proposalId).toBe('string');
        expect(stored.proposalId.length).toBeGreaterThan(0);
    });

    test('createdAt is a valid ISO date string', () => {
        const stored = buildStoredProposal('item-3', proposal);
        expect(new Date(stored.createdAt).getTime()).not.toBeNaN();
    });

    test('two calls produce different proposalIds', () => {
        const a = buildStoredProposal('item-1', proposal);
        const b = buildStoredProposal('item-1', proposal);
        expect(a.proposalId).not.toBe(b.proposalId);
    });
});
