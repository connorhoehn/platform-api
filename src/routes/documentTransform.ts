// POST /api/documents/transform — one document, re-shaped as another known type.
//
// The premise the document system now rests on: a document TYPE is a shape a
// light model can move content between, not a schema a person has to author
// field by field before writing anything. So "make this Meeting Notes into a
// Bug Report" is one call: the page's markdown goes in with the target type's
// field list, and markdown in the target's shape comes back. The client seeds a
// NEW page from it — the original is never touched, which is what makes the
// action safe to offer on every document.
//
// ## What the model is told, and what it is told not to do
//
// Macro fences (```macro:...```) are the document's structured citizens —
// action items, callouts, decisions, fields, links, files — and they are
// carried VERBATIM. A model that "helpfully" rewrote a callout's YAML would
// orphan the inbox row keyed on its id. Headings follow the target's fields in
// order; content the target has no field for lands under a final "Notes"
// heading rather than being dropped — this is a transform, not a summary.
//
// ## Why the Claude CLI and not an SDK
//
// The project rule (no black-box LLM libraries): `createLLMClient()` already
// wraps `claude -p` for pipelines, honours `LLM_PROVIDER`, and the same client
// serves here. The default model is the light one, by design.

import { Router, Request, Response } from 'express';
import type { LLMClient } from 'distributed-core/applications/pipeline';
import { requireAuth } from '../middleware/auth';
import { asyncHandler, ValidationError } from '../middleware/error-handler';
import { createLLMClient } from '../pipeline/factories/LLMClientFactory';
import log from '../lib/logger';

/** Generous for a page; a book is not a document. */
const MAX_MARKDOWN_BYTES = 200 * 1024;
/** A light model: the transform is structural, not creative. */
const DEFAULT_MODEL = process.env.DOCUMENT_TRANSFORM_MODEL || 'haiku';

export interface TransformTargetField {
  name: string;
  /** The renderer id — `text`, `date`, `checklist`, `link-block`, … Advisory. */
  sectionType?: string;
}

export interface TransformRequest {
  markdown: string;
  sourceType?: string;
  target: { name: string; fields: TransformTargetField[] };
}

/** The instruction, as one place to read what the model was asked. */
export function transformPrompts(req: TransformRequest): { system: string; user: string } {
  const fieldList = req.target.fields.length
    ? req.target.fields.map((f) => `- ${f.name}${f.sectionType ? ` (${f.sectionType})` : ''}`).join('\n')
    : '- (the type declares no fields; keep the document\'s own headings)';
  const system = [
    'You restructure documents from one known type into another. Output ONLY the transformed document as markdown — no preamble, no explanation, no code fence around the whole document.',
    'You never ask for more input and never refuse. A thin source is still a document: produce the target headings, place whatever content exists under them, carry every macro block, and leave the rest empty. Questions and apologies are not a document and will be discarded.',
    'Rules:',
    '1. Preserve every ```macro:...``` fenced block VERBATIM, byte for byte, including its YAML. Move it under the most relevant heading; never edit, merge, split or drop one.',
    '2. Use the target type\'s fields as level-2 headings, in the order given. Put the source content that answers each field under it. Leave a field\'s section empty rather than inventing content.',
    '3. Content that fits no target field goes under a final "## Notes" heading. Do not summarise it away.',
    '4. Keep the level-1 title line if the source has one.',
    '5. Keep links, tables and images as they are.',
  ].join('\n');
  const user = [
    `Source type: ${req.sourceType?.trim() || 'unknown'}`,
    `Target type: ${req.target.name}`,
    'Target fields, in order:',
    fieldList,
    '',
    'Source document:',
    '<<<DOCUMENT',
    req.markdown,
    'DOCUMENT>>>',
  ].join('\n');
  return { system, user };
}

/**
 * Does the answer look like a document at all?
 *
 * A model that asks a question ("Could you provide the source?") returns prose
 * with no heading and no macro fence. Seeding a page with that would put the
 * refusal on screen as the document — measured, once — so it is rejected here
 * with a message the person can act on, and the source page stays untouched.
 */
export function looksLikeDocument(markdown: string): boolean {
  return /^#{1,6}\s+\S/m.test(markdown) || /^```macro:/m.test(markdown);
}

/** A model that wraps the whole answer in one fence gets unwrapped; anything else is kept as-is. */
export function extractMarkdown(response: string): string {
  const trimmed = response.trim();
  const m = /^```(?:markdown|md)?\s*\n([\s\S]*?)\n```\s*$/.exec(trimmed);
  return (m ? m[1]! : trimmed).trim();
}

function validate(body: unknown): TransformRequest {
  const b = (body ?? {}) as Record<string, unknown>;
  if (typeof b.markdown !== 'string' || b.markdown.trim() === '') {
    throw new ValidationError('markdown is required');
  }
  if (Buffer.byteLength(b.markdown, 'utf8') > MAX_MARKDOWN_BYTES) {
    throw new ValidationError(`markdown exceeds ${MAX_MARKDOWN_BYTES} bytes`);
  }
  const target = b.target as Record<string, unknown> | undefined;
  if (!target || typeof target.name !== 'string' || target.name.trim() === '') {
    throw new ValidationError('target.name is required');
  }
  const rawFields = Array.isArray(target.fields) ? target.fields : [];
  const fields: TransformTargetField[] = rawFields
    .filter((f): f is Record<string, unknown> => typeof f === 'object' && f !== null && typeof (f as Record<string, unknown>).name === 'string')
    .map((f) => ({ name: String(f.name), ...(typeof f.sectionType === 'string' ? { sectionType: f.sectionType } : {}) }));
  return {
    markdown: b.markdown,
    ...(typeof b.sourceType === 'string' ? { sourceType: b.sourceType } : {}),
    target: { name: target.name.trim(), fields },
  };
}

/** Collects a stream into its final response. */
export async function completeStream(client: LLMClient, model: string, system: string, user: string, signal?: AbortSignal): Promise<string> {
  let response = '';
  let tokens = '';
  for await (const chunk of client.stream(model, system, user, { signal, temperature: 0 })) {
    if (chunk.done) { response = chunk.response; break; }
    tokens += chunk.token;
  }
  return response || tokens;
}

export function createDocumentTransformRouter(client?: LLMClient): Router {
  const router = Router();
  let llm: LLMClient | null = client ?? null;

  router.post('/transform', requireAuth, asyncHandler(async (req: Request, res: Response): Promise<void> => {
    const request = validate(req.body);
    llm = llm ?? createLLMClient();
    const { system, user } = transformPrompts(request);
    const started = Date.now();
    const raw = await completeStream(llm, DEFAULT_MODEL, system, user);
    const markdown = extractMarkdown(raw);
    if (!markdown) throw new ValidationError('The model returned an empty document');
    if (!looksLikeDocument(markdown)) {
      log.warn({ event: 'document.transform.refused', target: request.target.name, head: markdown.slice(0, 160) }, 'model did not return a document');
      throw new ValidationError('The model did not return a document for this page — try again, or add a little prose first.');
    }
    log.info({ event: 'document.transform', provider: llm.provider, model: DEFAULT_MODEL, target: request.target.name, ms: Date.now() - started, bytesIn: request.markdown.length, bytesOut: markdown.length, userId: req.user?.sub }, 'transformed');
    res.status(200).json({ markdown, model: DEFAULT_MODEL, provider: llm.provider });
  }));

  return router;
}

export const documentTransformRouter = createDocumentTransformRouter();
