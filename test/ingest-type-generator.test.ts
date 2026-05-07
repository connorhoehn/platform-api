// test/ingest-type-generator.test.ts
//
// Tests for type-generator: buildStoredProposal, suggestTypeMatch (LLM path),
// and generateTypeProposal (LLM path). Uses FixtureLLMClient from distributed-core.

import { FixtureLLMClient } from 'distributed-core';
import {
    buildStoredProposal,
    suggestTypeMatch,
    generateTypeProposal,
} from '../src/ingest/mapping/type-generator';
import type { ExistingTypeDescriptor, TypeProposal } from '../src/ingest/mapping/type-generator';

// ─── helpers ──────────────────────────────────────────────────────────────

function makeExistingType(overrides: Partial<ExistingTypeDescriptor> = {}): ExistingTypeDescriptor {
    return {
        typeId: 'type-a',
        name: 'Invoice',
        description: 'Financial invoice record',
        fields: [{ name: 'amount', sectionType: 'number' }],
        ...overrides,
    };
}

function makeProposal(overrides: Partial<TypeProposal> = {}): TypeProposal {
    return {
        name: 'MeetingNotes',
        description: 'Notes from team meetings',
        fields: [{ name: 'attendees', sectionType: 'text', required: false }],
        reasoning: 'Contains meeting structure',
        ...overrides,
    };
}

// ─── buildStoredProposal ──────────────────────────────────────────────────

describe('buildStoredProposal', () => {
    test('returns a StoredTypeProposal with status=pending-approval', () => {
        const proposal = makeProposal();
        const stored = buildStoredProposal('ingest-1', proposal);
        expect(stored.status).toBe('pending-approval');
        expect(stored.ingestId).toBe('ingest-1');
        expect(stored.proposal).toBe(proposal);
    });

    test('proposalId is a non-empty string', () => {
        const stored = buildStoredProposal('ingest-2', makeProposal());
        expect(typeof stored.proposalId).toBe('string');
        expect(stored.proposalId.length).toBeGreaterThan(0);
    });

    test('createdAt is a valid ISO string', () => {
        const stored = buildStoredProposal('ingest-3', makeProposal());
        expect(() => new Date(stored.createdAt)).not.toThrow();
        expect(new Date(stored.createdAt).getTime()).toBeGreaterThan(0);
    });

    test('two calls produce different proposalIds', () => {
        const a = buildStoredProposal('x', makeProposal());
        const b = buildStoredProposal('x', makeProposal());
        expect(a.proposalId).not.toBe(b.proposalId);
    });
});

// ─── suggestTypeMatch ─────────────────────────────────────────────────────

describe('suggestTypeMatch', () => {
    const existingTypes = [makeExistingType()];

    test('returns MappingResult when LLM responds with high confidence', async () => {
        const response = JSON.stringify({ typeId: 'type-a', confidence: 0.9, fieldMapping: { name: 'title' } });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('some content', 'doc.txt', 'text/plain', existingTypes, llm);
        expect(result).not.toBeNull();
        expect(result!.targetTypeId).toBe('type-a');
        expect(result!.method).toBe('llm-match');
        expect(result!.confidence).toBe(0.9);
        expect(result!.fieldMapping).toEqual({ name: 'title' });
    });

    test('returns null when confidence is below floor (0.3)', async () => {
        const response = JSON.stringify({ typeId: 'type-a', confidence: 0.2, fieldMapping: {} });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('content', 'f.txt', 'text/plain', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('returns null when confidence is exactly 0', async () => {
        const response = JSON.stringify({ typeId: 'type-a', confidence: 0, fieldMapping: {} });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('content', 'f.txt', 'text/plain', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('returns null when LLM response is not valid JSON', async () => {
        const llm = new FixtureLLMClient(['not json at all']);
        const result = await suggestTypeMatch('content', 'f.txt', 'text/plain', existingTypes, llm);
        expect(result).toBeNull();
    });

    test('strips markdown code fence from LLM response', async () => {
        const json = JSON.stringify({ typeId: 'type-a', confidence: 0.85, fieldMapping: {} });
        const fenced = `\`\`\`json\n${json}\n\`\`\``;
        const llm = new FixtureLLMClient([fenced]);
        const result = await suggestTypeMatch('content', 'f.txt', 'text/plain', existingTypes, llm);
        expect(result).not.toBeNull();
        expect(result!.targetTypeId).toBe('type-a');
    });

    test('returns empty fieldMapping when absent from LLM response', async () => {
        const response = JSON.stringify({ typeId: 'type-a', confidence: 0.7 });
        const llm = new FixtureLLMClient([response]);
        const result = await suggestTypeMatch('content', 'f.txt', 'text/plain', existingTypes, llm);
        expect(result!.fieldMapping).toEqual({});
    });
});

// ─── generateTypeProposal ─────────────────────────────────────────────────

describe('generateTypeProposal', () => {
    const validResponse = JSON.stringify({
        name: 'InvoiceRecord',
        description: 'Tracks financial invoices',
        fields: [
            { name: 'invoiceId', sectionType: 'text', required: true },
            { name: 'amount', sectionType: 'number', required: true },
        ],
        reasoning: 'Contains invoice number and amount fields',
    });

    test('returns a TypeProposal with name and fields', async () => {
        const llm = new FixtureLLMClient([validResponse]);
        const proposal = await generateTypeProposal('invoice content', 'inv.txt', 'text/plain', llm);
        expect(proposal.name).toBe('InvoiceRecord');
        expect(proposal.fields).toHaveLength(2);
        expect(proposal.fields[0]).toMatchObject({ name: 'invoiceId', sectionType: 'text', required: true });
    });

    test('strips markdown fence from LLM response', async () => {
        const fenced = `\`\`\`json\n${validResponse}\n\`\`\``;
        const llm = new FixtureLLMClient([fenced]);
        const proposal = await generateTypeProposal('content', 'f.txt', 'text/plain', llm);
        expect(proposal.name).toBe('InvoiceRecord');
    });

    test('throws when LLM response is not valid JSON', async () => {
        const llm = new FixtureLLMClient(['not json']);
        await expect(generateTypeProposal('content', 'f.txt', 'text/plain', llm)).rejects.toThrow(
            'type-generator: failed to parse LLM response as JSON',
        );
    });

    test('throws when LLM response is missing required fields', async () => {
        const incomplete = JSON.stringify({ description: 'no name or fields' });
        const llm = new FixtureLLMClient([incomplete]);
        await expect(generateTypeProposal('content', 'f.txt', 'text/plain', llm)).rejects.toThrow(
            'type-generator: LLM response missing required fields',
        );
    });

    test('throws when fields array is empty', async () => {
        const noFields = JSON.stringify({ name: 'Empty', fields: [] });
        const llm = new FixtureLLMClient([noFields]);
        await expect(generateTypeProposal('content', 'f.txt', 'text/plain', llm)).rejects.toThrow(
            'type-generator: LLM response missing required fields',
        );
    });

    test('defaults description and reasoning to empty string when absent', async () => {
        const minimal = JSON.stringify({ name: 'MinType', fields: [{ name: 'f', sectionType: 'text', required: false }] });
        const llm = new FixtureLLMClient([minimal]);
        const proposal = await generateTypeProposal('content', 'f.txt', 'text/plain', llm);
        expect(proposal.description).toBe('');
        expect(proposal.reasoning).toBe('');
    });
});
