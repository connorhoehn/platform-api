// platform-api/src/lib/proposalDrafter.ts
//
// Turn a work item into a concrete, applicable proposal.
//
// Three constraints shape everything here, and all three are structural rather
// than stylistic:
//
// 1. THERE IS NO SERVER-SIDE Y.DOC. The gateway relays CRDT bytes opaquely and
//    nothing in this process ever materialises a document. So the drafter cannot
//    read the document it is proposing changes to — the caller must SHOW it one,
//    and the values it is shown become the `from` half of the compare-and-set.
//    That is not a workaround; it is what makes staleness detectable, because
//    the snapshot is timestamped evidence of what was true at drafting time.
//
// 2. THE MODEL NEVER PICKS THE TARGET. The context frame already decided which
//    section the remark was about, by rules, from what was on screen when the
//    speaker pressed the button. The model chooses the ACTION only. `validate`
//    below enforces this by rejecting any proposal naming a section other than
//    the one it was given — a model that tries to redirect the edit fails
//    closed, loudly, rather than quietly filing it somewhere plausible.
//
// 3. NO BLACK-BOX LIBRARIES. This shells out to `claude -p` through the same
//    ClaudeCodeLLMClient the judge and agent steps use. No SDK, no framework, no
//    hidden retry loop.
//
// Deliberately NOT built on the pipeline engine: every node there has a 50ms
// sleep floor, its approval gate parks a live `resolve` callback in an
// in-process Map that no reload survives, and a reject is terminal for the whole
// run. An interactive accept/reject loop cannot be built on any of that. The
// pipeline remains the right home for the BATCH path (transcript.ready -> agent
// -> create work item) and this is the interactive one.

import { randomUUID } from 'node:crypto';
import { ClaudeCodeLLMClient } from '../pipeline/ClaudeCodeLLMClient';
import type { LLMClient } from '../pipeline/LLMClient';
import type { Proposal, FieldChange, ProseEdit } from '../types/workItem';
import log from './logger';

/**
 * What the document held when the caller asked for a proposal.
 *
 * Supplied by the browser, which is the only place a Y.Doc exists. Sections
 * carry both `title` (a Y.Map scalar) and `text` (the flattened prose of the
 * section's Y.XmlFragment) because those are two different substrates and a
 * proposal against each takes a different form.
 */
export interface DocumentSnapshot {
  meta: Record<string, unknown>;
  sections: Array<{ id: string; title?: string; text?: string }>;
  /** When the snapshot was taken, client clock. Carried into the receipt. */
  observedAt?: string;
}

export interface DraftProposalInput {
  utterance: string;
  /** The section the RULES chose. May be absent for a document-level remark. */
  targetSectionId?: string | undefined;
  snapshot: DocumentSnapshot;
  model?: string;
  signal?: AbortSignal;
}

export class ProposalDraftError extends Error {}

const DEFAULT_MODEL = process.env['WORK_ITEM_PROPOSAL_MODEL'] ?? 'sonnet';

const SYSTEM_PROMPT = `You convert a spoken remark about a document into ONE concrete edit.

You will be given the remark verbatim, the document's current values, and the ID
of the section the remark was about. That target was chosen by rules from what
the speaker had on screen. You MUST NOT choose a different one.

Reply with a single JSON object and nothing else. No prose, no markdown fence.

Two shapes are allowed.

A) Changing a scalar the document holds outside its prose — the document title,
   or a section's title:

   {"kind":"field","path":"meta.title","to":"<new value>","rationale":"<one short sentence>"}
   {"kind":"field","path":"sections.<SECTION_ID>.title","to":"<new value>","rationale":"..."}

B) Changing the prose inside the target section. Give one or more find-and-replace
   edits. "anchorQuote" MUST be copied VERBATIM from the section text you were
   shown, character for character, and must be long enough to occur exactly once.
   An empty "replacement" deletes the quoted text.

   {"kind":"prose","edits":[{"anchorQuote":"<verbatim>","replacement":"<new text>"}],"rationale":"..."}

Rules:
- "path" may only ever be "meta.title" or "sections.<the section id you were given>.title".
- Prose edits apply only to the section you were given.
- Do not invent facts. If the remark asks for content you were not given, edit
  only what it plainly names.
- If the remark is not an actionable edit request (a question, an aside, a note
  to self), reply exactly: {"kind":"none","rationale":"<why>"}`;

function buildUserPrompt(input: DraftProposalInput): string {
  const { utterance, targetSectionId, snapshot } = input;
  const target = snapshot.sections.find((s) => s.id === targetSectionId);

  const lines: string[] = [];
  lines.push(`REMARK (verbatim): ${JSON.stringify(utterance)}`);
  lines.push('');
  lines.push(`DOCUMENT TITLE (meta.title): ${JSON.stringify(snapshot.meta['title'] ?? null)}`);
  lines.push('');
  if (target) {
    lines.push(`TARGET SECTION ID: ${target.id}`);
    lines.push(`TARGET SECTION TITLE: ${JSON.stringify(target.title ?? null)}`);
    lines.push('TARGET SECTION TEXT (verbatim — quote from this exactly):');
    lines.push(target.text ?? '(empty)');
  } else {
    lines.push('TARGET SECTION: none — this remark is about the document as a whole.');
    lines.push('Only "meta.title" is editable for this remark.');
  }
  lines.push('');
  lines.push('Reply with the JSON object only.');
  return lines.join('\n');
}

/**
 * Pull the JSON object out of a model reply.
 *
 * Tolerates a stray fence or a leading sentence — the prompt forbids both, but a
 * hard failure on a recoverable formatting slip would put an unusable error in
 * front of a person who just spoke a perfectly clear sentence.
 */
function extractJson(raw: string): Record<string, unknown> {
  const text = raw.trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end <= start) {
    throw new ProposalDraftError(`model reply contained no JSON object: ${raw.slice(0, 200)}`);
  }
  try {
    return JSON.parse(text.slice(start, end + 1)) as Record<string, unknown>;
  } catch (err) {
    throw new ProposalDraftError(
      `model reply was not valid JSON: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

/** Read the value a `FieldChange.path` addresses out of the snapshot. */
export function readSnapshotPath(snapshot: DocumentSnapshot, path: string): unknown {
  if (path.startsWith('meta.')) {
    return snapshot.meta[path.slice('meta.'.length)];
  }
  const sectionMatch = /^sections\.(.+)\.title$/.exec(path);
  if (sectionMatch) {
    const section = snapshot.sections.find((s) => s.id === sectionMatch[1]);
    return section?.title;
  }
  return undefined;
}

/**
 * Turn a parsed reply into a Proposal, or throw.
 *
 * This is the enforcement point for "rules pick the target". Everything the
 * model returned about WHERE is checked against what it was given, and a
 * mismatch is a hard failure rather than a correction — silently rewriting the
 * model's target to the right one would hide the fact that it tried.
 */
export function validate(
  parsed: Record<string, unknown>,
  input: DraftProposalInput,
  proposalId: string,
): Proposal | null {
  const kind = parsed['kind'];
  if (kind === 'none') return null;

  const rationale = typeof parsed['rationale'] === 'string' ? parsed['rationale'] : undefined;

  if (kind === 'field') {
    const path = parsed['path'];
    if (typeof path !== 'string') {
      throw new ProposalDraftError('field proposal is missing a string "path"');
    }
    const allowed = new Set<string>(['meta.title']);
    if (input.targetSectionId) allowed.add(`sections.${input.targetSectionId}.title`);
    if (!allowed.has(path)) {
      throw new ProposalDraftError(
        `proposal targets "${path}", which is not the target the rules chose ` +
          `(allowed: ${[...allowed].join(', ')})`,
      );
    }
    if (!('to' in parsed)) {
      throw new ProposalDraftError('field proposal is missing "to"');
    }
    const from = readSnapshotPath(input.snapshot, path);
    if (from === parsed['to']) {
      throw new ProposalDraftError('proposal would not change anything');
    }
    const change: FieldChange = { path, from, to: parsed['to'] };
    return { kind: 'field', proposalId, change, ...(rationale ? { rationale } : {}) };
  }

  if (kind === 'prose') {
    if (!input.targetSectionId) {
      throw new ProposalDraftError('a prose proposal needs a target section, and none was chosen');
    }
    const rawEdits = parsed['edits'];
    if (!Array.isArray(rawEdits) || rawEdits.length === 0) {
      throw new ProposalDraftError('prose proposal has no edits');
    }
    const sectionText =
      input.snapshot.sections.find((s) => s.id === input.targetSectionId)?.text ?? '';
    const edits: ProseEdit[] = [];
    for (const raw of rawEdits) {
      const e = raw as Record<string, unknown>;
      const anchorQuote = typeof e['anchorQuote'] === 'string' ? e['anchorQuote'] : '';
      const replacement = typeof e['replacement'] === 'string' ? e['replacement'] : '';
      if (!anchorQuote && !replacement) continue;
      // A quote the model paraphrased will not resolve in the live editor, and
      // the failure would surface much later as "accept did nothing". Catch it
      // here, where we still have the text it was shown.
      if (anchorQuote && !sectionText.includes(anchorQuote)) {
        throw new ProposalDraftError(
          `anchor quote does not occur in the section text: ${JSON.stringify(anchorQuote.slice(0, 80))}`,
        );
      }
      if (anchorQuote && sectionText.split(anchorQuote).length > 2) {
        throw new ProposalDraftError(
          `anchor quote occurs more than once, so it does not identify a range: ` +
            JSON.stringify(anchorQuote.slice(0, 80)),
        );
      }
      edits.push({ anchorQuote, replacement });
    }
    if (edits.length === 0) throw new ProposalDraftError('prose proposal had no usable edits');
    return {
      kind: 'prose',
      proposalId,
      sectionId: input.targetSectionId,
      edits,
      ...(rationale ? { rationale } : {}),
    };
  }

  throw new ProposalDraftError(`unknown proposal kind: ${JSON.stringify(kind)}`);
}

/**
 * Draft a proposal for one work item. Returns null when the remark was not an
 * edit request — a perfectly ordinary outcome that must not read as a failure.
 */
export async function draftProposal(
  input: DraftProposalInput,
  client: LLMClient = new ClaudeCodeLLMClient(),
): Promise<Proposal | null> {
  const proposalId = randomUUID();
  const model = input.model ?? DEFAULT_MODEL;

  let response = '';
  const stream = client.stream(model, SYSTEM_PROMPT, buildUserPrompt(input), {
    ...(input.signal ? { signal: input.signal } : {}),
  });
  for await (const chunk of stream) {
    if (chunk.done) response = chunk.response;
  }

  const parsed = extractJson(response);
  const proposal = validate(parsed, input, proposalId);
  log.info(
    {
      event: 'workitem.proposal.drafted',
      proposalId,
      kind: proposal?.kind ?? 'none',
      targetSectionId: input.targetSectionId,
    },
    'proposal drafted',
  );
  return proposal;
}
