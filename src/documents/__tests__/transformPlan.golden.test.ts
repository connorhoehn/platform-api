// Transform quality is measured, not eyeballed.
//
// Haiku's plan for each of nine source×target pairs is recorded ONCE
// (`RECORD_GOLDENS=1 npx jest src/documents/__tests__/transformPlan.golden`)
// into __fixtures__/transform-goldens/<source>__<target>.json, by rendering
// the prompt exactly as the pipeline does and running the same `claude -p`
// path the `agent` step runs. The default run loads the goldens and asserts
// STRUCTURAL invariants over `parseOps(planText)` — the same parse the apply
// step does — and prints one row per pair. A failing invariant fails the test.
//
// A golden whose prompt no longer matches the rendered prompt fails too:
// change DOCUMENT_TRANSFORM_PROMPT and you must re-record, or the goldens are
// measuring a prompt nobody sends any more.
//
// `GOLDEN_ONLY=<pair key>[,<pair key>…]` narrows either mode to those pairs.
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'fs';
import { join } from 'path';
import { parseOps, type DocOp, type OutlineBlock } from '../documentAgentOps';
import { ClaudeCodeAgentClient } from '../../pipeline/agent/ClaudeCodeAgentClient';
import { renderTransformPrompt, SHORT_FIELD_TYPES, TRANSFORM_PAIRS, type TransformPair } from './transformPlan.fixtures';

const GOLDEN_DIR = join(__dirname, '__fixtures__', 'transform-goldens');
const RECORD = process.env.RECORD_GOLDENS === '1';
const ONLY = process.env.GOLDEN_ONLY;
const MODEL = process.env.DOCUMENT_AGENT_MODEL || 'haiku';
const pairs = ONLY ? TRANSFORM_PAIRS.filter((p) => ONLY.split(',').includes(p.key)) : TRANSFORM_PAIRS;

interface Golden { source: string; target: string; model: string; recordedAt: string; prompt: string; planText: string }

const goldenPath = (pair: TransformPair): string => join(GOLDEN_DIR, `${pair.key}.json`);

// ── Recording: the same model path the agent step takes ────────────────────

async function record(pair: TransformPair): Promise<Golden> {
  const { prompt } = renderTransformPrompt(pair);
  const client = new ClaudeCodeAgentClient();
  let planText = '';
  let model = MODEL;
  for await (const ev of client.run(prompt, { model: MODEL, allowedTools: [], maxTurns: 1, timeoutMs: 180_000 })) {
    if (ev.type === 'done') { planText = ev.text; model = ev.model || MODEL; if (ev.stopReason !== 'complete') throw new Error(`${pair.key}: agent stopped with ${ev.stopReason}`); }
  }
  const golden: Golden = { source: pair.source.key, target: pair.target.key, model, recordedAt: new Date().toISOString(), prompt, planText };
  mkdirSync(GOLDEN_DIR, { recursive: true });
  writeFileSync(goldenPath(pair), `${JSON.stringify(golden, null, 2)}\n`);
  return golden;
}

async function recordAll(list: TransformPair[], concurrency = 3): Promise<void> {
  const queue = [...list];
  await Promise.all(Array.from({ length: Math.min(concurrency, queue.length) }, async () => {
    for (let next = queue.shift(); next; next = queue.shift()) await record(next);
  }));
}

// ── Invariants ─────────────────────────────────────────────────────────────

export interface InvariantResult { name: string; ok: boolean; detail?: string }

const norm = (s: string): string => s.trim().toLowerCase().replace(/\s+/g, ' ');
const words = (s: string): string[] => Array.from(new Set(s.toLowerCase().match(/[a-z0-9][a-z0-9'-]{3,}/g) ?? []));

interface FieldMacro { label: string; type?: string; value?: string }

/** The `label:`/`type:`/`value:` scalars of a field macro body (the reader tolerates extra keys; we only read these). */
function readFieldYaml(yaml: string): FieldMacro | null {
  const get = (key: string): string | undefined => {
    const m = yaml.match(new RegExp(`^${key}:[ \\t]*(.*)$`, 'm'));
    return m ? m[1]!.trim().replace(/^'(.*)'$/, '$1').replace(/^"(.*)"$/, '$1') : undefined;
  };
  const label = get('label');
  if (!label) return null;
  return { label, type: get('type'), value: get('value') };
}

/**
 * What the plan appended as text — headings, paragraphs, macro bodies — so a
 * source block whose content was reworded or MOVED (a field value into a
 * target field macro) still counts as carried.
 */
function appendedCorpus(ops: DocOp[]): string {
  return ops.map((o) => (o.op === 'appendBlock' ? o.text : o.op === 'appendMacro' ? o.yaml : o.op === 'setTitle' ? o.text : '')).join('\n').toLowerCase();
}

/** The target FIELD macros, parsed — the one place a source field's value may move to without a copyBlock. */
function targetFieldMacros(ops: DocOp[]): FieldMacro[] {
  return ops.flatMap((o) => (o.op === 'appendMacro' && o.macroName === 'field' ? [readFieldYaml(o.yaml)] : [])).filter((m): m is FieldMacro => !!m);
}

function coveredBy(block: OutlineBlock, corpus: string, targetFields: FieldMacro[]): boolean {
  if (block.kind === 'macro') {
    const fm = block.macroName === 'field' ? readFieldYaml(block.text) : null;
    // A moved field: its value landed in a target field macro (or its label did, for an unanswered one).
    // A paragraph saying "Severity: High" is a retyped macro, not a carried one.
    if (fm) return fm.value !== undefined ? targetFields.some((t) => norm(t.value ?? '') === norm(fm.value!)) : targetFields.some((t) => norm(t.label) === norm(fm.label));
    return false; // a non-field macro is only ever carried by copyBlock
  }
  const t = block.text.toLowerCase();
  if (corpus.includes(t)) return true;
  const ws = words(block.text);
  if (ws.length === 0) return false;
  const hit = ws.filter((w) => corpus.includes(w)).length;
  return hit / ws.length >= 0.4;
}

export function checkPlan(pair: TransformPair, golden: Golden, ops: DocOp[], blocks: OutlineBlock[], renderedPrompt: string): InvariantResult[] {
  const out: InvariantResult[] = [];
  const push = (name: string, ok: boolean, detail?: string) => out.push({ name, ok, ...(ok || !detail ? {} : { detail }) });

  push('prompt-current', golden.prompt === renderedPrompt, 'golden recorded against a different prompt — re-record');
  push('parsed', ops.length > 0, `parseOps found ${ops.length} ops in ${golden.planText.length} chars`);

  const first = ops[0];
  push('type-first', !!first && first.op === 'setDocumentType' && first.typeName === pair.target.name, `first op ${first ? JSON.stringify(first).slice(0, 80) : 'missing'}`);
  const second = ops[1];
  const titleRe = new RegExp(`^${pair.source.title.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')} [—–-] ${pair.target.name.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}$`);
  push('title-second', !!second && second.op === 'setTitle' && titleRe.test(second.text.trim()), `second op ${second ? JSON.stringify(second).slice(0, 100) : 'missing'}`);

  const headings = ops.filter((o): o is Extract<DocOp, { op: 'appendBlock' }> => o.op === 'appendBlock' && o.kind === 'heading');
  const fieldMacros = ops.filter((o): o is Extract<DocOp, { op: 'appendMacro' }> => o.op === 'appendMacro' && o.macroName === 'field').map((o) => readFieldYaml(o.yaml)).filter((m): m is FieldMacro => !!m);
  const missing: string[] = []; const dup: string[] = []; const misshaped: string[] = [];
  for (const field of pair.target.fields) {
    const asHeading = headings.filter((h) => norm(h.text) === norm(field.name)).length;
    const asMacro = fieldMacros.filter((m) => norm(m.label) === norm(field.name));
    const n = asHeading + asMacro.length;
    if (n === 0) missing.push(field.name); else if (n > 1) dup.push(field.name);
    if (SHORT_FIELD_TYPES.has(field.sectionType)) {
      if (asMacro.length !== 1 || asHeading > 0) misshaped.push(`${field.name}: expected a field macro`);
      else if (asMacro[0]!.type !== field.sectionType) misshaped.push(`${field.name}: type ${asMacro[0]!.type ?? '(none)'} ≠ ${field.sectionType}`);
    } else if (asHeading !== 1 || asMacro.length > 0) misshaped.push(`${field.name}: expected a heading`);
  }
  push('fields-once', missing.length === 0 && dup.length === 0, `${missing.length ? `missing ${missing.join(', ')}` : ''}${dup.length ? ` duplicated ${dup.join(', ')}` : ''}`.trim());
  push('fields-shaped', misshaped.length === 0, misshaped.join('; '));

  // Every value a field macro carries is something the source says (case-insensitively, or by a shared word/number) — never a guess.
  const sourceText = blocks.map((b) => b.text).join('\n').toLowerCase();
  const ungrounded = fieldMacros.filter((m) => m.value !== undefined && m.value !== '' && !sourceText.includes(m.value.toLowerCase())
    && !words(m.value).some((w) => sourceText.includes(w)) && !(m.value.match(/\d{2,}/g) ?? []).some((d) => sourceText.includes(d)));
  push('values-grounded', ungrounded.length === 0, ungrounded.map((m) => `${m.label}=${m.value}`).join('; '));

  const copies = ops.filter((o): o is Extract<DocOp, { op: 'copyBlock' }> => o.op === 'copyBlock').map((o) => o.index);
  const seen = new Set<number>(); const dupCopies = copies.filter((i) => (seen.has(i) ? true : (seen.add(i), false)));
  const outOfRange = copies.filter((i) => !blocks[i]);
  push('copy-unique', dupCopies.length === 0 && outOfRange.length === 0, `${dupCopies.length ? `duplicate copyBlock ${dupCopies.join(',')}` : ''} ${outOfRange.length ? `out of range ${outOfRange.join(',')}` : ''}`.trim());

  // The title heading is covered by setTitle; a source heading is a label for what follows, so it is
  // carried when it is copied, reworded, or its first following block is carried.
  const corpus = appendedCorpus(ops);
  const copied = new Set(copies);
  const carried = (b: OutlineBlock): boolean => copied.has(b.index) || coveredBy(b, corpus, fieldMacros);
  const dropped: string[] = [];
  for (const b of blocks) {
    if (!b.text.trim()) continue;
    if (b.kind === 'heading' && b.level === 1 && b.index === 0) continue;
    if (carried(b)) continue;
    if (b.kind === 'heading') { const next = blocks[b.index + 1]; if (next && next.kind !== 'heading' && carried(next)) continue; }
    dropped.push(`#${b.index} ${b.kind}${b.macroName ? `(${b.macroName})` : ''}`);
  }
  push('source-covered', dropped.length === 0, `dropped ${dropped.join(', ')}`);

  // Non-field source macros cross by copyBlock exactly once and are never retyped.
  const macroBlocks = blocks.filter((b) => b.kind === 'macro' && b.macroName !== 'field');
  const notCopied = macroBlocks.filter((b) => !copied.has(b.index)).map((b) => `#${b.index}(${b.macroName})`);
  const retyped = ops.filter((o) => o.op === 'appendMacro' && o.macroName !== 'field' && !/^(items|links): \[\]\s*$/.test(o.yaml)).length;
  push('macros-carried', notCopied.length === 0 && retyped === 0, `${notCopied.length ? `not copied ${notCopied.join(', ')}` : ''} ${retyped ? `${retyped} retyped macro(s)` : ''}`.trim());

  const destructive = ops.filter((o) => o.op === 'deleteBlock' || o.op === 'replaceBlock' || o.op === 'insertBlock' || o.op === 'skip').map((o) => o.op);
  push('no-destructive', destructive.length === 0, destructive.join(','));

  // Leftovers come last: when a "Notes" heading exists it is the final heading, and every target field precedes it.
  const notesAt = ops.findIndex((o) => o.op === 'appendBlock' && o.kind === 'heading' && norm(o.text) === 'notes');
  const afterNotes = notesAt < 0 ? [] : ops.slice(notesAt + 1).filter((o) => (o.op === 'appendBlock' && o.kind === 'heading') || (o.op === 'appendMacro' && o.macroName === 'field'));
  push('notes-last', afterNotes.length === 0, `${afterNotes.length} heading/field op(s) after Notes`);

  return out;
}

// ── The test ───────────────────────────────────────────────────────────────

const INVARIANTS = ['prompt-current', 'parsed', 'type-first', 'title-second', 'fields-once', 'fields-shaped', 'values-grounded', 'copy-unique', 'source-covered', 'macros-carried', 'no-destructive', 'notes-last'];
const table: Array<{ key: string; ops: number; results: InvariantResult[] }> = [];

describe('the transform plan Haiku produces (recorded goldens)', () => {
  beforeAll(async () => {
    if (RECORD) await recordAll(pairs);
  }, 15 * 60_000);

  afterAll(() => {
    if (table.length === 0) return;
    const w = Math.max(...table.map((r) => r.key.length));
    const lines = [
      `${'pair'.padEnd(w)}  ops  ${INVARIANTS.map((n) => n.replace(/^(.{6}).*/, '$1')).join(' ')}`,
      ...table.map((r) => `${r.key.padEnd(w)}  ${String(r.ops).padStart(3)}  ${INVARIANTS.map((n) => (r.results.find((x) => x.name === n)?.ok ? '  ok  ' : ' FAIL ')).join(' ')}`),
      ...table.flatMap((r) => r.results.filter((x) => !x.ok).map((x) => `  ${r.key} ${x.name}: ${x.detail ?? ''}`)),
    ];
    process.stdout.write(`\ntransform plan invariants (${table.length} pairs)\n${lines.join('\n')}\n\n`);
  });

  it.each(pairs.map((p) => [p.key, p] as const))('%s', (key, pair) => {
    const path = goldenPath(pair);
    if (!existsSync(path)) throw new Error(`no golden for ${key} at ${path} — run with RECORD_GOLDENS=1`);
    const golden = JSON.parse(readFileSync(path, 'utf8')) as Golden;
    const { prompt, outline } = renderTransformPrompt(pair);
    const ops = parseOps(golden.planText);
    const results = checkPlan(pair, golden, ops, outline.blocks, prompt);
    table.push({ key, ops: ops.length, results });
    const failed = results.filter((r) => !r.ok).map((r) => `${r.name}${r.detail ? ` (${r.detail})` : ''}`);
    expect(failed).toEqual([]);
  });
});
