// platform-api/src/ingest/mapping/type-generator.ts
//
// LLM-powered document type matching and generation for the ingest subsystem.
// Used when rule-based mapping (rule-matcher.ts) fails to match a file to an
// existing document type. Two strategies:
//
//   1. suggestTypeMatch  — ask LLM which existing type best matches
//   2. generateTypeProposal — ask LLM to propose an entirely new type schema
//
// Accepts the pipeline LLMClient interface (streaming). Internally collects
// the full streamed response and parses JSON from it.

import type { LLMClient, LLMChunk } from '../../pipeline/LLMClient';
import type { MappingResult } from '../types';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ExistingTypeDescriptor {
  typeId: string;
  name: string;
  description: string;
  fields: Array<{ name: string; sectionType: string }>;
}

export interface TypeProposal {
  name: string;
  description: string;
  fields: Array<{ name: string; sectionType: string; required: boolean }>;
  reasoning: string;
}

/**
 * Stored proposal for human review workflow.
 */
export interface StoredTypeProposal {
  proposalId: string;
  ingestId: string;
  proposal: TypeProposal;
  status: 'pending-approval' | 'approved' | 'rejected';
  createdAt: string;
  reviewedAt?: string;
  reviewedBy?: string;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CONTENT_SAMPLE_LIMIT = 2000;
const MATCH_CONFIDENCE_FLOOR = 0.3;
const DEFAULT_MODEL = 'claude-sonnet-4-20250514';

/**
 * Collect the full response text from an LLMClient stream.
 */
async function collectResponse(stream: AsyncIterable<LLMChunk>): Promise<string> {
  let response = '';
  for await (const chunk of stream) {
    if (chunk.done) {
      response = chunk.response;
      break;
    }
  }
  return response;
}

/**
 * Extract JSON from an LLM response that may contain markdown fences or
 * surrounding prose.
 */
function extractJSON(raw: string): string {
  // Try to find a JSON code block first
  const fenceMatch = raw.match(/```(?:json)?\s*\n?([\s\S]*?)\n?```/);
  if (fenceMatch) return fenceMatch[1].trim();

  // Otherwise find the first { ... } or [ ... ] block
  const objStart = raw.indexOf('{');
  const objEnd = raw.lastIndexOf('}');
  if (objStart !== -1 && objEnd > objStart) {
    return raw.slice(objStart, objEnd + 1);
  }

  // Return as-is and let JSON.parse throw
  return raw.trim();
}

function truncateContent(content: string): string {
  if (content.length <= CONTENT_SAMPLE_LIMIT) return content;
  return content.slice(0, CONTENT_SAMPLE_LIMIT) + '\n... [truncated]';
}

// ---------------------------------------------------------------------------
// 1. LLM-assisted matching (match to existing types)
// ---------------------------------------------------------------------------

const MATCH_SYSTEM_PROMPT = `You are a document type classification system. Given file content and a list of available document types, determine which type best matches the file. Respond with ONLY valid JSON, no other text.

Response schema:
{
  "typeId": "<id of best matching type>",
  "confidence": <number 0-1>,
  "fieldMapping": { "<sourceField>": "<docTypeField>", ... }
}

If no type is a reasonable match, set confidence to 0.`;

/**
 * Ask the LLM which existing document type best matches the given file.
 * Returns a MappingResult with method='llm-match', or null if confidence is
 * below the floor threshold (0.3).
 */
export async function suggestTypeMatch(
  fileContent: string,
  fileName: string,
  mimeType: string,
  existingTypes: ExistingTypeDescriptor[],
  llmClient: LLMClient,
  model: string = DEFAULT_MODEL,
): Promise<MappingResult | null> {
  const sample = truncateContent(fileContent);

  const typesDescription = existingTypes
    .map(
      (t) =>
        `- ${t.typeId}: "${t.name}" — ${t.description}\n  Fields: ${t.fields.map((f) => `${f.name} (${f.sectionType})`).join(', ')}`,
    )
    .join('\n');

  const userPrompt = `File: "${fileName}" (${mimeType})

Content sample:
---
${sample}
---

Available document types:
${typesDescription}

Which document type best matches this file? Return JSON only.`;

  try {
    const stream = llmClient.stream(model, MATCH_SYSTEM_PROMPT, userPrompt, {
      maxTokens: 512,
      temperature: 0,
    });

    const raw = await collectResponse(stream);
    const json = JSON.parse(extractJSON(raw)) as {
      typeId: string;
      confidence: number;
      fieldMapping: Record<string, string>;
    };

    if (typeof json.confidence !== 'number' || json.confidence < MATCH_CONFIDENCE_FLOOR) {
      return null;
    }

    return {
      targetTypeId: json.typeId,
      fieldMapping: json.fieldMapping ?? {},
      method: 'llm-match',
      confidence: json.confidence,
    };
  } catch {
    // JSON parse failure or stream error — treat as no match
    return null;
  }
}

// ---------------------------------------------------------------------------
// 2. LLM-generated type proposal
// ---------------------------------------------------------------------------

const GENERATE_SYSTEM_PROMPT = `You are a document schema designer. Analyze file content and propose a document type schema. Respond with ONLY valid JSON, no other text.

Response schema:
{
  "name": "<PascalCase type name>",
  "description": "<1-2 sentence description>",
  "fields": [
    { "name": "<camelCase field name>", "sectionType": "<text|number|date|boolean|enum>", "required": <true|false> }
  ],
  "reasoning": "<why this schema fits the content>"
}

Guidelines:
- Name should describe the document category (e.g., "MeetingNotes", "InvoiceRecord")
- Include 3-8 fields that capture the document's key structure
- sectionType must be one of: text, number, date, boolean, enum
- Mark fields as required only if they appear essential to the document type`;

/**
 * Ask the LLM to propose a new document type schema based on the file content.
 * Throws if the LLM response cannot be parsed.
 */
export async function generateTypeProposal(
  fileContent: string,
  fileName: string,
  mimeType: string,
  llmClient: LLMClient,
  model: string = DEFAULT_MODEL,
): Promise<TypeProposal> {
  const sample = truncateContent(fileContent);

  const userPrompt = `File: "${fileName}" (${mimeType})

Content:
---
${sample}
---

Propose a document type schema for files like this. Return JSON only.`;

  const stream = llmClient.stream(model, GENERATE_SYSTEM_PROMPT, userPrompt, {
    maxTokens: 1024,
    temperature: 0,
  });

  const raw = await collectResponse(stream);

  let json: unknown;
  try {
    json = JSON.parse(extractJSON(raw));
  } catch (err) {
    throw new Error(
      `type-generator: failed to parse LLM response as JSON — ${(err as Error).message}`,
    );
  }

  const parsed = json as Record<string, unknown>;

  // Basic validation
  if (!parsed.name || !Array.isArray(parsed.fields) || parsed.fields.length === 0) {
    throw new Error(
      'type-generator: LLM response missing required fields (name, fields)',
    );
  }

  return {
    name: parsed.name as string,
    description: (parsed.description as string) ?? '',
    fields: (parsed.fields as Array<Record<string, unknown>>).map((f) => ({
      name: String(f.name ?? ''),
      sectionType: String(f.sectionType ?? 'text'),
      required: Boolean(f.required),
    })),
    reasoning: (parsed.reasoning as string) ?? '',
  };
}

// ---------------------------------------------------------------------------
// Storage helper (for human review workflow)
// ---------------------------------------------------------------------------

/**
 * Store a type generation proposal for human review.
 * In a full implementation this persists to DynamoDB; here it builds the
 * record shape for the caller to persist.
 */
export function buildStoredProposal(
  ingestId: string,
  proposal: TypeProposal,
): StoredTypeProposal {
  return {
    proposalId: `proposal-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    ingestId,
    proposal,
    status: 'pending-approval',
    createdAt: new Date().toISOString(),
  };
}
