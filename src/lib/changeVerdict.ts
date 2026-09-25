// platform-api/src/lib/changeVerdict.ts
//
// Judge one proposed document change: pass / concern / fail.
//
// A sibling of proposalDrafter.ts, and it follows the same three rules that
// file spells out — no server-side Y.Doc (so the caller must SHOW us the
// document), the model decides the judgement and nothing else, and the LLM goes
// through the same client FACTORY the drafter and the judge steps use. In this
// deployment `PIPELINE_LLM_PROVIDER=claude-code`, which shells out to
// `claude -p`. No SDK, no framework, no hidden retry loop.
//
// THE THING THAT MAKES THIS WORTH HAVING
//
// A verdict computed from the change alone is a style opinion. "Rewrite the
// second sentence to say 40%" is unjudgeable in isolation and perfectly
// judgeable next to a document whose table says 27%. So `buildVerdictContext`
// assembles the document around the change — the target section verbatim, the
// document's shape, and the OTHER changes currently pending against it — and
// that block is what the model is asked about. Two pending changes that
// contradict each other are invisible to any reviewer that sees them one at a
// time, and that is exactly the case a human most wants flagged.
//
// THE THING THAT MAKES IT SAFE
//
// `pass` is the state people skim past. So an uncertain model cannot produce
// one: `parseVerdict` only emits `pass` for a literal, unambiguous "pass", and
// anything else it can parse — a missing verdict, an unrecognised word, a
// hedge — becomes `concern`. Defaulting the other way would make the feature
// actively dangerous, because the failure would be silent by construction.

import { createHash } from 'node:crypto';
import { inspect } from 'node:util';
import { createLLMClient } from '../pipeline/createLLMClient';
import { gatedLLMClient, isModelUnavailable } from './modelGate';
import type { LLMClient } from '../pipeline/LLMClient';
import type { Proposal } from '../types/workItem';
import type { ChangeVerdict, ChangeReviewVerdict } from '../types/changeVerdict';
import type { DocumentSnapshot } from './proposalDrafter';
import log from './logger';

const DEFAULT_MODEL = process.env['CHANGE_VERDICT_MODEL'] ?? 'sonnet';

/**
 * How long the reviewing model gets before we give up on it.
 *
 * `claude -p` is a subprocess; an unbounded await on one holds an express
 * handler open until the client gives up first, and then nobody logs why. When
 * this fires the caller gets an explicit FAILED state, never a synthesised
 * verdict.
 */
const DEFAULT_TIMEOUT_MS = Number(process.env['CHANGE_VERDICT_TIMEOUT_MS'] ?? 60_000);

/** Cap the section text we ship, so one enormous section cannot blow the prompt. */
const MAX_SECTION_CHARS = 12_000;
/** Cap on how many other pending changes we describe. */
const MAX_PEER_CHANGES = 20;

/** The model was unreachable, unauthenticated, or exited non-zero. */
export class VerdictProviderError extends Error {}
/** The model did not answer inside the timeout. */
export class VerdictTimeoutError extends Error {}
/** The model answered, but with nothing we can turn into a verdict. */
export class VerdictParseError extends Error {}

/**
 * Render an unknown thrown value into a line worth reading.
 *
 * This repo has a live example of the alternative: a shutdown hook logging
 * `err?.message ?? String(err)` produced `failed:` with an empty message for
 * every non-Error rejection, so the one line saying it failed said nothing
 * about why (see src/tracing.ts). The constructor name plus the inspected
 * value are what actually identify it.
 */
export function describeError(err: unknown): string {
  if (err instanceof Error) {
    const name = err.constructor?.name ?? 'Error';
    return err.message ? `${name}: ${err.message}` : `${name}: ${inspect(err, { depth: 1 })}`;
  }
  return `${err === null ? 'null' : typeof err}: ${inspect(err, { depth: 2 })}`;
}

// ---------------------------------------------------------------------------
// Revision hashing — the reason a stale verdict cannot masquerade as a fresh one
// ---------------------------------------------------------------------------

/**
 * Deterministic JSON for a value of unknown shape.
 *
 * `FieldChange.from`/`to` are `unknown`, so they can be objects, and
 * `JSON.stringify` orders object keys by insertion. Two identical values that
 * arrived by different routes would then hash differently, and every read would
 * miss. Sorting keys makes the hash a function of the VALUE.
 */
function stable(value: unknown): string {
  if (value === null || typeof value !== 'object') return JSON.stringify(value) ?? 'undefined';
  if (Array.isArray(value)) return `[${value.map(stable).join(',')}]`;
  const entries = Object.entries(value as Record<string, unknown>)
    .filter(([, v]) => v !== undefined)
    .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
  return `{${entries.map(([k, v]) => `${JSON.stringify(k)}:${stable(v)}`).join(',')}}`;
}

function digest(prefix: string, material: string): string {
  return `${prefix}-${createHash('sha256').update(material).digest('hex').slice(0, 32)}`;
}

/**
 * Hash of the change's ACTUAL CONTENT — the whole point of the cache key.
 *
 * Editing a change changes this, so the cached verdict for the old text is no
 * longer addressable and the next read is a miss. That is the invalidation: it
 * is not a TTL, not a flag someone has to remember to clear, and not something
 * the caller can forget to pass. It falls out of asking for the verdict by
 * content.
 *
 * `proposalId` is deliberately EXCLUDED. It is a fresh uuid on every re-draft,
 * so including it would make a re-draft that produced byte-identical edits look
 * like a different change and pay for the model call again. Identity here is
 * what the change DOES, not which drafting run emitted it.
 */
export function computeChangeRevision(proposal: Proposal): string {
  const material =
    proposal.kind === 'field'
      ? [
          'field',
          proposal.change.path,
          stable(proposal.change.from),
          stable(proposal.change.to),
        ].join('\n')
      : [
          'prose',
          proposal.sectionId,
          ...proposal.edits.map((e) => `${stable(e.anchorQuote)}=>${stable(e.replacement)}`),
        ].join('\n');
  return digest('v1', material);
}

// ---------------------------------------------------------------------------
// Context — the document around the change
// ---------------------------------------------------------------------------

/** One other change pending against the same document. */
export interface PeerChange {
  itemId: string;
  /** The task text / utterance the peer change came from, if any. */
  summary?: string;
  proposal: Proposal;
}

export interface VerdictContextInput {
  proposal: Proposal;
  snapshot: DocumentSnapshot;
  /** The remark the change was drafted from, verbatim. */
  utterance?: string;
  /** Other changes awaiting a decision on this document. */
  peers?: PeerChange[];
}

export interface VerdictContext {
  /** The literal text handed to the model. */
  block: string;
  /**
   * Hash of that text.
   *
   * NOT part of the cache key — the key is the change revision, because that is
   * what the UI reasons about. But a cached row carries this, and a row whose
   * context digest no longer matches is recomputed rather than served: the
   * document moved underneath a verdict about it, and "does this contradict the
   * document" is only answerable against the document it was answered for.
   */
  digest: string;
}

function describeProposal(p: Proposal): string[] {
  if (p.kind === 'field') {
    return [
      `kind: field (a scalar the document holds outside its prose)`,
      `field path: ${p.change.path}`,
      `current value: ${stable(p.change.from)}`,
      `proposed value: ${stable(p.change.to)}`,
    ];
  }
  const lines = [`kind: prose (find-and-replace inside section ${p.sectionId})`];
  p.edits.forEach((e, i) => {
    lines.push(`  edit ${i + 1}:`);
    lines.push(`    REPLACE (verbatim from the section): ${JSON.stringify(e.anchorQuote)}`);
    lines.push(
      e.replacement === ''
        ? `    WITH: (nothing — this edit DELETES the quoted text)`
        : `    WITH: ${JSON.stringify(e.replacement)}`,
    );
  });
  return lines;
}

/**
 * Assemble what the reviewer is allowed to reason from.
 *
 * Bounded on purpose: the target section in full, every section's title (so
 * "this belongs in another section" and "this contradicts the structure" are
 * answerable), the document title, and the peers. NOT every section's body —
 * that would make an unrelated typo three sections away change the context
 * digest and force a re-review of every pending change in the document.
 */
export function buildVerdictContext(input: VerdictContextInput): VerdictContext {
  const { proposal, snapshot, utterance, peers = [] } = input;
  const targetSectionId = proposal.kind === 'prose' ? proposal.sectionId : undefined;
  const target = targetSectionId
    ? snapshot.sections.find((s) => s.id === targetSectionId)
    : undefined;

  const lines: string[] = [];
  lines.push('THE DOCUMENT');
  lines.push(`Title: ${stable(snapshot.meta['title'] ?? null)}`);
  lines.push('Sections, in order:');
  for (const s of snapshot.sections) {
    lines.push(`  - ${s.id}: ${stable(s.title ?? null)}`);
  }
  lines.push('');

  if (target) {
    const text = target.text ?? '';
    lines.push(`THE SECTION THIS CHANGE TOUCHES (${target.id} — ${stable(target.title ?? null)})`);
    lines.push('Its full current text follows, verbatim:');
    lines.push('---');
    lines.push(
      text.length > MAX_SECTION_CHARS
        ? `${text.slice(0, MAX_SECTION_CHARS)}\n[... section truncated at ${MAX_SECTION_CHARS} characters ...]`
        : text || '(this section is empty)',
    );
    lines.push('---');
  } else if (targetSectionId) {
    // The change names a section the snapshot does not contain. Say so rather
    // than quietly reviewing against nothing: a reviewer that cannot see the
    // text it is editing must not be allowed to sound confident about it.
    lines.push(
      `THE SECTION THIS CHANGE TOUCHES (${targetSectionId}) IS NOT IN THE DOCUMENT ` +
        `YOU WERE SHOWN. You cannot verify this change against its section.`,
    );
  } else {
    lines.push('This change is to a document-level value, not to prose inside a section.');
  }
  lines.push('');

  lines.push('THE PROPOSED CHANGE');
  lines.push(...describeProposal(proposal));
  if (proposal.rationale) lines.push(`the drafter's stated rationale: ${stable(proposal.rationale)}`);
  if (utterance) lines.push(`the remark it was drafted from: ${stable(utterance)}`);
  lines.push('');

  const shown = peers.slice(0, MAX_PEER_CHANGES);
  if (shown.length > 0) {
    lines.push('OTHER CHANGES ALREADY PENDING ON THIS DOCUMENT (not yet applied)');
    lines.push(
      'If the change you are judging contradicts, duplicates, or fights with any ' +
        'of these, say so — nobody else is looking at them together.',
    );
    for (const peer of shown) {
      lines.push(`  * pending change ${peer.itemId}${peer.summary ? ` — ${peer.summary}` : ''}`);
      for (const l of describeProposal(peer.proposal)) lines.push(`    ${l}`);
    }
    if (peers.length > shown.length) {
      lines.push(`  (${peers.length - shown.length} further pending changes not shown)`);
    }
  } else {
    lines.push('OTHER CHANGES ALREADY PENDING ON THIS DOCUMENT: none.');
  }

  const block = lines.join('\n');
  return { block, digest: digest('c1', block) };
}

// ---------------------------------------------------------------------------
// The prompt
// ---------------------------------------------------------------------------

const SYSTEM_PROMPT = `You are reviewing ONE proposed change to a shared document, immediately before a person decides whether to accept it. Your answer is shown to that person as a single chip beside the change.

Judge one question: would applying this change to THIS document be safe and coherent? Concretely, check:
- does it contradict something else the document says?
- does it contradict another change that is already pending on the document?
- does it assert a fact, a number, a name or a date that the document you were shown cannot support?
- does it lose information — a deletion dressed up as an edit, or a replacement that drops content the original carried?
- for a change to a field, is the new value the right SHAPE and kind of thing for that field?

Do NOT judge writing style, tone, or whether you would have phrased it differently. A change that is consistent with the document and loses nothing is fine even if you would have worded it another way.

Reply with a single JSON object and nothing else. No prose, no markdown fence.

{"verdict":"pass","headline":"<one line, at most 110 characters, no markdown>","reasoning":"<2-5 sentences: what you checked, and what you found. Quote the document where the quote is the point.>"}

"verdict" must be exactly one of "pass", "concern", or "fail".

  fail     Applying this would damage the document. It destroys information, states something the
           document elsewhere contradicts, or writes a value that does not belong in that field.
  concern  A human should look at this before accepting it.
  pass     Routine and safe: consistent with everything you were shown, and nothing is lost.

"pass" is the verdict people skim past without reading, so it is the one that has to be earned. Use it only when you actually checked and found nothing wrong. If you are unsure, if you are guessing, or if the document you were shown does not contain enough for you to verify the claim, the answer is "concern" — never "pass" to mean "probably fine", and never "fail" merely because you could not check.

The headline is the whole message for most readers. Make it say what you found ("Changes the retry count to 5, but §Limits still says 3"), not what you did ("Reviewed the change").`;

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

function extractJson(raw: string): Record<string, unknown> {
  const text = raw.trim().replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '');
  const start = text.indexOf('{');
  const end = text.lastIndexOf('}');
  if (start === -1 || end <= start) {
    throw new VerdictParseError(`reply contained no JSON object: ${raw.slice(0, 200)}`);
  }
  try {
    return JSON.parse(text.slice(start, end + 1)) as Record<string, unknown>;
  } catch (err) {
    throw new VerdictParseError(`reply was not valid JSON: ${describeError(err)}`);
  }
}

const HEADLINE_MAX = 160;

function firstLine(s: string, max = HEADLINE_MAX): string {
  const one = s.replace(/\s+/g, ' ').trim();
  return one.length > max ? `${one.slice(0, max - 1)}…` : one;
}

/**
 * Turn a parsed reply into a verdict, WITHOUT ever inventing a `pass`.
 *
 * Only a literal "pass" (case and whitespace aside) yields `pass`. "fail" and
 * "concern" map straight through. Everything else — an absent field, "unsure",
 * "needs review", "PASS?", a nested object, a number — becomes `concern`,
 * because every one of those is a model that would not commit, and a model that
 * would not commit is precisely the case a person needs to look at.
 *
 * Exported so the test suite can pin this behaviour: it is the single rule that
 * keeps the feature from being dangerous.
 */
export function parseVerdict(
  parsed: Record<string, unknown>,
  forRevision: string,
  producedAt = new Date().toISOString(),
): ChangeReviewVerdict {
  const rawVerdict = typeof parsed['verdict'] === 'string' ? parsed['verdict'].trim().toLowerCase() : '';
  const reasoning = typeof parsed['reasoning'] === 'string' ? parsed['reasoning'].trim() : '';
  const rawHeadline = typeof parsed['headline'] === 'string' ? parsed['headline'].trim() : '';

  let verdict: ChangeVerdict;
  let headline: string;

  if (rawVerdict === 'pass' || rawVerdict === 'concern' || rawVerdict === 'fail') {
    verdict = rawVerdict;
    headline = rawHeadline
      ? firstLine(rawHeadline)
      : reasoning
        ? firstLine(reasoning)
        : verdict === 'pass'
          ? 'No problems found with this change.'
          : 'The reviewer flagged this change but gave no headline.';
  } else {
    verdict = 'concern';
    headline = rawHeadline
      ? firstLine(rawHeadline)
      : 'The reviewer would not commit to a verdict — a human should look at this.';
  }

  return {
    verdict,
    headline,
    ...(reasoning ? { reasoning } : {}),
    producedAt,
    forRevision,
  };
}

// ---------------------------------------------------------------------------
// The call
// ---------------------------------------------------------------------------

export interface JudgeChangeInput extends VerdictContextInput {
  /** The change revision this verdict will be stamped with. */
  forRevision: string;
  model?: string;
  timeoutMs?: number;
  /** Pre-built context, when the caller already computed the digest. */
  context?: VerdictContext;
}

export interface JudgeChangeResult {
  verdict: ChangeReviewVerdict;
  context: VerdictContext;
  model: string;
  elapsedMs: number;
}

/**
 * Ask the model for a verdict. Throws — it never returns a fabricated one.
 *
 * Three distinct failures, kept distinct because they need different words in
 * front of a person: the provider was unreachable (a deployment problem), it
 * did not answer in time (a capacity problem), or it answered with nothing
 * usable (a model problem). Collapsing them into one 500 is how "the pod's CLI
 * credentials expired" ends up looking like a bug in someone's sentence.
 */
export async function judgeChange(
  input: JudgeChangeInput,
  // Through the model gate (breaker, `verdict` slot pool, single-flight on identical input).
  client: LLMClient = gatedLLMClient(createLLMClient(), 'verdict', { singleFlight: true }),
): Promise<JudgeChangeResult> {
  const model = input.model ?? DEFAULT_MODEL;
  const timeoutMs = input.timeoutMs ?? DEFAULT_TIMEOUT_MS;
  const context = input.context ?? buildVerdictContext(input);

  const controller = new AbortController();
  let timedOut = false;
  const timer = setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  const startedAt = Date.now();
  let response = '';
  try {
    const stream = client.stream(model, SYSTEM_PROMPT, context.block, {
      signal: controller.signal,
    });
    for await (const chunk of stream) {
      if (chunk.done) response = chunk.response;
    }
  } catch (err) {
    // An open breaker: say "Model unavailable: <reason>" at once, as it is.
    if (isModelUnavailable(err)) throw new VerdictProviderError(err.message);
    if (timedOut) {
      throw new VerdictTimeoutError(
        `the ${client.provider} provider did not answer within ${timeoutMs}ms`,
      );
    }
    throw new VerdictProviderError(
      `the ${client.provider} provider could not be reached: ${describeError(err)}`,
    );
  } finally {
    clearTimeout(timer);
  }

  const elapsedMs = Date.now() - startedAt;
  if (!response.trim()) {
    // A clean exit with an empty body is a provider problem, not a model one —
    // `claude -p` does this when the CLI is unauthenticated.
    throw new VerdictProviderError(
      `the ${client.provider} provider returned an empty reply after ${elapsedMs}ms`,
    );
  }

  const verdict = parseVerdict(extractJson(response), input.forRevision);
  log.info(
    {
      event: 'change.verdict.produced',
      verdict: verdict.verdict,
      forRevision: input.forRevision,
      contextDigest: context.digest,
      model,
      provider: client.provider,
      elapsedMs,
    },
    'change verdict produced',
  );
  return { verdict, context, model, elapsedMs };
}
