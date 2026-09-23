// `/agent <instruction> [--model x] [<N>k]` — from a line of chat to a
// pipeline whose nodes are the steps a person can see, in an editor, in the
// Active Now graph, and on the run's card.
//
// Three things live here, in the order they happen:
//
//   1. parseAgentLoopCommand  — the grammar. Pure, so it is testable and so
//                               the server can be the authority over whatever
//                               the browser thought the command said.
//   2. planAgentLoop          — one toolless model call that turns the
//                               instruction into 1..6 steps with acceptance
//                               sentences. Strictly validated; a planner that
//                               is down or talks nonsense yields a SINGLE
//                               step and says `source: 'fallback'`, which the
//                               card shows. Never hidden.
//   3. buildAgentLoopDefinition — plan → nodes and edges.
//
// WHY THE SERVER RE-PARSES: the browser parses the same grammar to render the
// composer chip before the round-trip. Two parsers means two answers, and the
// one that decides what a person is charged 200k tokens for has to be this
// one. `extra` is consulted only where the text said nothing.

import type { AgentClient, AgentStopReason } from '../pipeline/agent/types';
import { DECK_STEP_KIND } from '../deck/DeckStepHandler';
import { MEETING_STEP_KIND, teamNameFrom } from '../loop/meetingStepHandler';
import { AGENDA_STEP_KIND, type Agenda } from '../loop/agendaStepHandler';
import { isSilentClaudeFailure } from '../pipeline/claudeCliErrors';
import { routeSummary } from '../pipeline/definition-metadata';
import type { DeckSpec } from '../deck/DeckSpec';
import log from '../lib/logger';

export const CHAT_AGENT_LOOP_PIPELINE_ID = 'chat-agent-loop';

/** The card's agent identity, matching HAIKU_CARD_AGENT's shape. */
export const AGENT_LOOP_CARD_AGENT = { userId: 'agent:loop', displayName: 'Agent loop' } as const;

export const DEFAULT_CONTEXT_BUDGET_TOKENS = 200_000;
export const MIN_CONTEXT_BUDGET_TOKENS = 20_000;
export const MAX_CONTEXT_BUDGET_TOKENS = 1_000_000;
export const DEFAULT_LOOP_MODEL = 'sonnet';
export const MAX_INSTRUCTION_CHARS = 4_000;
export const MAX_HINTS = 6;
export const MAX_STEPS = 6;

/** How long one devbox step may run, and how many resumes it gets. */
export const LOOP_STEP_MAX_TURNS = 40;
export const LOOP_STEP_TIMEOUT_MS = 20 * 60_000;
export const LOOP_STEP_TOOLS = ['Read', 'Write', 'Edit', 'Bash', 'Glob', 'Grep'] as const;
/**
 * Every system pipeline must declare a retry policy (systemPipelineLint).
 * Two attempts, not three: a retried devbox step RESUMES the same session, so
 * a second go continues where the first stopped rather than starting over —
 * useful once, pointless three times.
 */
export const LOOP_STEP_RETRY = { maxAttempts: 2, backoffMs: 5_000, backoffMultiplier: 2, retryUnknown: true } as const;

/** The planner is a small model on a short leash: it writes JSON, nothing else. */
export const PLANNER_MODEL = 'haiku';
/**
 * MEASURED, NOT GUESSED. This was 30s, and half the live runs fell back.
 * Timed in the deployed pod against the real prompt, three consecutive runs:
 * 23.7s, 21.6s, 22.0s wall clock — the `claude -p --output-format stream-json`
 * start-up dominates, not the model (the same prompt answers in ~2s once the
 * CLI is warm). A 30s cap against a 22s median is a coin flip, and losing it
 * killed the child mid-answer and produced an EMPTY result, which read as
 * "the planner said nothing" rather than "the planner ran out of time".
 *
 * 90s is the cap that makes the common case work while still bounding the
 * command: a person waits for one chat message, not forever.
 */
export const PLANNER_TIMEOUT_MS = 90_000;
/**
 * Not 1. The CLI emits one `assistant` event per content block, and a
 * thinking block and its text usually share a message id — but not always.
 * When they split, `maxTurns: 1` killed the process at the second id and
 * yielded empty text. The planner is TOOLLESS, so it cannot loop and turns
 * are only message boundaries; a small ceiling costs nothing and removes a
 * failure that depended on how the model happened to chunk its answer.
 */
export const PLANNER_MAX_TURNS = 4;

/**
 * One retry, because a cold pod loses the first one.
 *
 * Observed live: a loop started ~30 seconds after a pod restart threw
 * `claude exited 1: (no stderr)` after 3.3s and fell back to a single step. A
 * three-second silent non-zero exit is not a planner that disagrees with the
 * prompt, it is a CLI that was not ready — and the second attempt succeeds.
 *
 * Only a SILENT failure is retried. One that came back with a reason (a bad
 * model name, a missing credential) will fail the same way twice, and
 * retrying it just makes a person wait twice as long for the same fallback.
 */
export const PLANNER_RETRY_DELAY_MS = 2_000;

/**
 * A non-zero exit that said nothing — the cold-start signature.
 *
 * Shared with the CLI clients, and it deliberately excludes an auth failure:
 * stale mounted credentials fail identically the second time, so retrying
 * only makes a person wait twice as long for the same fallback.
 */
export const isSilentCliFailure = isSilentClaudeFailure;

export const LOOP_STEP_KINDS = ['meeting', 'agenda', 'research', 'document', 'deck', 'code', 'review', 'other'] as const;
export type LoopStepKind = (typeof LOOP_STEP_KINDS)[number];

export interface LoopStep {
  id: string;
  title: string;
  kind: LoopStepKind;
  prompt: string;
  dependsOn: string[];
  /** One sentence naming what "finished" means. It is appended to the prompt. */
  done: string;
}

export interface LoopPlan {
  steps: LoopStep[];
  /**
   * 'pending' while the planner is still running (the POST answers before it
   * finishes); 'fallback' when it did not answer usably. Both are surfaced on
   * the card, never hidden behind an empty step list.
   */
  source: 'planner' | 'fallback' | 'pending';
  /** Why it fell back, in a sentence a person can read. */
  notice?: string;
}

export interface AgentLoopReferences {
  documentId?: string;
  workGraph?: { personId: string; nodeId?: string };
}

/** A document the deck may cite, named by the conversation rather than guessed. */
export interface AgentLoopSource {
  documentId: string;
  title: string;
  /** 'document' or 'transcript' — what the citation opens. */
  kind?: string;
}

/** How much source text a generated prompt may carry. */
export const MAX_SOURCE_TITLES = 12;
export const MAX_TRANSCRIPT_PROMPT_CHARS = 8_000;

/**
 * Dev-only escape hatch. Under `MOCK_AGENTCORE` the drafting agent answers
 * with the mock's prose, which is not a DeckSpec — correctly, the deck step
 * then refuses to render. That leaves no way to exercise the real pptx path
 * locally, so an instruction carrying this marker pins a known-good spec onto
 * the render node instead. It never fires without the marker, and the marker
 * is something a person has to type.
 */
export const DECK_FIXTURE_MARKER = '[deck-fixture]';

export function wantsDeckFixture(instruction: string): boolean {
  return String(instruction ?? '').toLowerCase().includes(DECK_FIXTURE_MARKER);
}

/** A small, valid, self-citing deck, so the local render path has something real to render. */
export function deckFixtureSpec(request: { instruction: string; requestedByName?: string }, sources: AgentLoopSource[]): DeckSpec {
  const cite = sources[0] ?? { documentId: 'deck-fixture', title: 'Deck fixture' };
  const source = [{ documentId: cite.documentId, title: cite.title }];
  const topic = request.instruction.replace(DECK_FIXTURE_MARKER, '').trim().slice(0, 80) || 'the agent loop';
  return {
    title: `Fixture deck — ${topic}`.slice(0, 120),
    subtitle: 'Rendered from a pinned spec, not from a model',
    theme: 'plain',
    slides: [
      { id: 'title', layout: 'title', title: `Fixture deck — ${topic}`.slice(0, 120), sources: source },
      {
        id: 'what-this-is', layout: 'bullets', title: 'What this is', eyebrow: 'Fixture',
        bullets: [
          'A pinned DeckSpec, used only when the instruction carries the fixture marker.',
          'It exercises the real renderer: pptx, per-slide SVG, a stored revision.',
          'No model wrote it, and nothing here is a claim about the sources.',
        ],
        notes: 'This deck exists so the local render path can be exercised end to end.',
        sources: source,
      },
      { id: 'end', layout: 'section', title: 'End of fixture', sources: source },
    ],
  };
}

export interface AgentLoopRequest {
  instruction: string;
  hints: string[];
  contextBudgetTokens: number;
  model: string;
  chatChannel: string;
  requestedBy: string;
  requestedByName?: string;
  references?: AgentLoopReferences;
  /** Documents the conversation named, which a deck step must cite. */
  sources?: AgentLoopSource[];
  /** Transcript text from the conversation's recordings, already capped. */
  transcript?: string;
}

/** The part of the command the grammar alone decides. */
export type ParsedAgentLoopCommand = Pick<AgentLoopRequest, 'instruction' | 'hints' | 'contextBudgetTokens' | 'model'>;

/** What the client already parsed. Consulted ONLY where the text said nothing. */
export interface AgentLoopCommandExtra {
  contextBudgetTokens?: unknown;
  model?: unknown;
  hints?: unknown;
}

function clampBudget(value: number): number {
  if (!Number.isFinite(value)) return DEFAULT_CONTEXT_BUDGET_TOKENS;
  return Math.min(MAX_CONTEXT_BUDGET_TOKENS, Math.max(MIN_CONTEXT_BUDGET_TOKENS, Math.round(value)));
}

/**
 * `/meeting /agenda create a meeting with X … --model opus 200k`
 *   → hints ['meeting','agenda'], model 'opus', budget 200_000,
 *     instruction 'create a meeting with X …'
 *
 * Hints are LEADING `/word` tokens only: a slash in the middle of a sentence
 * ("either/or", a URL) is part of the instruction. Budget and model are
 * TRAILING tokens, consumed from the end in any order, because people type
 * them in either. Everything left is the instruction.
 */
export function parseAgentLoopCommand(argumentText: string, extra: AgentLoopCommandExtra = {}): ParsedAgentLoopCommand {
  const tokens = String(argumentText ?? '').trim().split(/\s+/).filter(Boolean);

  const hints: string[] = [];
  while (tokens.length > 0 && /^\/[a-z][\w-]*$/i.test(tokens[0]) && hints.length < MAX_HINTS) {
    hints.push(tokens.shift()!.slice(1).toLowerCase());
  }

  let budget: number | undefined;
  let model: string | undefined;
  // Consume trailing modifiers until the last token is part of the sentence.
  for (;;) {
    const last = tokens[tokens.length - 1];
    if (last === undefined) break;
    const kTokens = /^(\d+)k$/i.exec(last);
    if (kTokens && budget === undefined) {
      budget = Number(kTokens[1]) * 1000;
      tokens.pop();
      continue;
    }
    const inlineModel = /^--model=(.+)$/i.exec(last);
    if (inlineModel && model === undefined) {
      model = inlineModel[1];
      tokens.pop();
      continue;
    }
    const before = tokens[tokens.length - 2];
    if (before !== undefined && /^--model$/i.test(before) && model === undefined) {
      model = last;
      tokens.pop();
      tokens.pop();
      continue;
    }
    break;
  }

  // `extra` fills only the gaps: whatever the text said stands.
  if (budget === undefined && typeof extra.contextBudgetTokens === 'number') budget = extra.contextBudgetTokens;
  if (model === undefined && typeof extra.model === 'string' && extra.model.trim()) model = extra.model.trim();
  if (hints.length === 0 && Array.isArray(extra.hints)) {
    for (const hint of extra.hints) {
      if (typeof hint === 'string' && /^[a-z][\w-]*$/i.test(hint.trim()) && hints.length < MAX_HINTS) {
        hints.push(hint.trim().toLowerCase());
      }
    }
  }

  return {
    instruction: tokens.join(' ').slice(0, MAX_INSTRUCTION_CHARS),
    hints,
    contextBudgetTokens: clampBudget(budget ?? DEFAULT_CONTEXT_BUDGET_TOKENS),
    model: (model ?? DEFAULT_LOOP_MODEL).slice(0, 40),
  };
}

/** The whole request, from the parsed command plus who and where. */
export function buildAgentLoopRequest(
  parsed: ParsedAgentLoopCommand,
  who: { chatChannel: string; requestedBy: string; requestedByName?: string; references?: AgentLoopReferences },
): AgentLoopRequest {
  return {
    ...parsed,
    chatChannel: who.chatChannel,
    requestedBy: who.requestedBy,
    ...(who.requestedByName ? { requestedByName: who.requestedByName } : {}),
    ...(who.references ? { references: who.references } : {}),
  };
}

// ─── The planner ──────────────────────────────────────────────────────

/** What the planner is told. JSON only, the hints, and an acceptance sentence per step. */
export function plannerPrompt(request: AgentLoopRequest): string {
  const hints = request.hints.length
    ? `The person prefixed the command with these step hints, in order: ${request.hints.map((h) => `"${h}"`).join(', ')}. They are ADVISORY — follow them when they describe real work in this instruction, ignore any that do not fit, and never invent a step just to use one up.`
    : 'The person gave no step hints.';

  return [
    'You break one instruction into the smallest sequence of agent steps that delivers it.',
    'Answer with ONLY a JSON object. No prose, no explanation, no code fence.',
    '',
    'Shape:',
    '{"steps":[{"id":"s1","title":"...","kind":"...","prompt":"...","dependsOn":[],"done":"..."}]}',
    '',
    'Rules:',
    `  - Between 1 and ${MAX_STEPS} steps. Fewer is better: do not split work that one agent session does in one pass.`,
    `  - "id" is "s1", "s2", … in order, up to "s${MAX_STEPS}".`,
    '  - "title" is 2-6 words, what a person would call this step on a card.',
    `  - "kind" is exactly one of: ${LOOP_STEP_KINDS.join(', ')}.`,
    '  - "prompt" is the whole task for that step, written TO the agent doing it, in the second person. It may refer to what earlier steps left in the shared workspace.',
    '  - "dependsOn" lists ids of EARLIER steps only ("s2" may depend on "s1"; "s1" depends on nothing). Leave it empty when the step needs nothing from another.',
    '  - "done" is ONE sentence naming the acceptance: the concrete artefact or state that means this step is finished, in terms someone could check. Not "the task is complete" — name the file, the document, the deck, the answer.',
    '',
    hints,
    '',
    `The steps run in a shared cloud workspace with a total context budget of ${Math.round(request.contextBudgetTokens / 1000)}k tokens, so keep each step's scope small enough to finish inside it.`,
    '',
    `Instruction from ${request.requestedByName || request.requestedBy}:`,
    request.instruction,
  ].join('\n');
}

/** Strip a ```json fence and take the outermost object, so a chatty model still parses. */
export function extractJsonObject(text: string): unknown {
  const raw = String(text ?? '').trim();
  const unfenced = raw.replace(/^```(?:json)?\s*/i, '').replace(/\s*```$/, '').trim();
  const start = unfenced.indexOf('{');
  const end = unfenced.lastIndexOf('}');
  if (start === -1 || end <= start) return null;
  try {
    return JSON.parse(unfenced.slice(start, end + 1)) as unknown;
  } catch {
    return null;
  }
}

const trimmed = (v: unknown, max: number): string => (typeof v === 'string' ? v.trim().slice(0, max) : '');

/**
 * Validate the planner's answer, or say why not. Strict on purpose: a plan
 * that half-parses becomes nodes a person is asked to trust, so anything
 * doubtful is rejected whole and the fallback runs instead.
 */
export function validateLoopPlan(value: unknown): { ok: true; steps: LoopStep[] } | { ok: false; reason: string } {
  if (!value || typeof value !== 'object') return { ok: false, reason: 'the planner did not answer with a JSON object' };
  const raw = (value as { steps?: unknown }).steps;
  if (!Array.isArray(raw)) return { ok: false, reason: 'the plan had no steps array' };
  if (raw.length === 0) return { ok: false, reason: 'the plan had no steps' };
  if (raw.length > MAX_STEPS) return { ok: false, reason: `the plan had ${raw.length} steps, more than the ${MAX_STEPS} allowed` };

  const steps: LoopStep[] = [];
  const seen = new Set<string>();
  for (let i = 0; i < raw.length; i += 1) {
    const entry = raw[i];
    if (!entry || typeof entry !== 'object') return { ok: false, reason: `step ${i + 1} was not an object` };
    const s = entry as Record<string, unknown>;
    const id = trimmed(s.id, 8);
    if (id !== `s${i + 1}`) return { ok: false, reason: `step ${i + 1} should have id "s${i + 1}", got "${id}"` };
    if (seen.has(id)) return { ok: false, reason: `step id "${id}" appears twice` };

    const title = trimmed(s.title, 80);
    if (!title) return { ok: false, reason: `step ${id} has no title` };
    const prompt = trimmed(s.prompt, MAX_INSTRUCTION_CHARS);
    if (!prompt) return { ok: false, reason: `step ${id} has no prompt` };
    const done = trimmed(s.done, 400);
    if (!done) return { ok: false, reason: `step ${id} has no acceptance sentence` };

    const kindRaw = trimmed(s.kind, 20).toLowerCase() as LoopStepKind;
    const kind: LoopStepKind = (LOOP_STEP_KINDS as readonly string[]).includes(kindRaw) ? kindRaw : 'other';

    const dependsOnRaw = Array.isArray(s.dependsOn) ? s.dependsOn : [];
    const dependsOn: string[] = [];
    for (const dep of dependsOnRaw) {
      const d = trimmed(dep, 8);
      if (!seen.has(d)) return { ok: false, reason: `step ${id} depends on "${d}", which is not an earlier step` };
      if (!dependsOn.includes(d)) dependsOn.push(d);
    }

    seen.add(id);
    steps.push({ id, title, kind, prompt, dependsOn, done });
  }
  return { ok: true, steps };
}

/** The honest single-step plan: the instruction, run once, as itself. */
export function fallbackPlan(request: AgentLoopRequest, notice: string): LoopPlan {
  const instruction = request.instruction || 'Do what was asked.';
  return {
    source: 'fallback',
    notice,
    steps: [{
      id: 's1',
      kind: 'other',
      title: instruction.slice(0, 80),
      prompt: instruction,
      dependsOn: [],
      done: 'The instruction has been carried out and the result is summarised in the reply.',
    }],
  };
}

/** What one toolless planner call produced, and HOW it stopped. */
interface PlannerOutcome {
  text: string;
  stopReason: AgentStopReason | 'no_result';
}

/**
 * Collect a toolless agent call's final text AND its stop reason.
 *
 * The stop reason is not optional bookkeeping. A bounded agent client that
 * hits its timeout or turn ceiling kills the child and still yields a `done`
 * event — with empty text, because the answer never finished. Reading only
 * `ev.text` turns every one of those into "the planner answered with
 * nothing", which is both wrong and unactionable: it was the planner running
 * out of time, and the fix is a bigger cap, not a better model.
 */
async function runPlannerOnce(agentClient: AgentClient, prompt: string): Promise<PlannerOutcome> {
  let outcome: PlannerOutcome = { text: '', stopReason: 'no_result' };
  for await (const ev of agentClient.run(prompt, {
    model: PLANNER_MODEL,
    allowedTools: [],
    maxTurns: PLANNER_MAX_TURNS,
    timeoutMs: PLANNER_TIMEOUT_MS,
  })) {
    if (ev.type === 'done') outcome = { text: ev.text ?? '', stopReason: ev.stopReason };
  }
  return outcome;
}

/** Once more, after a pause, when the first attempt died without saying anything. */
async function runPlanner(
  agentClient: AgentClient,
  prompt: string,
  sleep: (ms: number) => Promise<void> = (ms) => new Promise((r) => setTimeout(r, ms)),
): Promise<PlannerOutcome> {
  try {
    return await runPlannerOnce(agentClient, prompt);
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (!isSilentCliFailure(message)) throw err;
    log.warn({ err: message }, 'agent loop planner failed silently; retrying once');
    await sleep(PLANNER_RETRY_DELAY_MS);
    return runPlannerOnce(agentClient, prompt);
  }
}

/** The sentence a person reads on the card when the planner did not deliver. */
export function plannerStopNotice(stopReason: PlannerOutcome['stopReason']): string {
  switch (stopReason) {
    case 'timeout':
      return `Planner timed out after ${Math.round(PLANNER_TIMEOUT_MS / 1000)}s · single step`;
    case 'max_turns':
      return 'Planner stopped at its turn ceiling before answering · single step';
    case 'cancelled':
      return 'Planner was cancelled · single step';
    case 'error':
      return 'Planner reported an error · single step';
    case 'no_result':
      return 'Planner exited without answering · single step';
    default:
      return 'Planner answered with nothing · single step';
  }
}

/**
 * Instruction → plan. Never throws: every failure is a `source: 'fallback'`
 * plan carrying the reason, because a person who typed a command gets a run
 * either way and gets told what they got.
 */
export async function planAgentLoop(
  request: AgentLoopRequest,
  agentClient: AgentClient | null | undefined,
  sleep?: (ms: number) => Promise<void>,
): Promise<LoopPlan> {
  if (!agentClient) return fallbackPlan(request, 'Planner unavailable · single step');
  let outcome: PlannerOutcome;
  const startedAt = Date.now();
  try {
    outcome = await runPlanner(agentClient, plannerPrompt(request), sleep);
  } catch (err) {
    const reason = err instanceof Error ? err.message : String(err);
    log.warn({ durationMs: Date.now() - startedAt, err: reason }, 'agent loop planner threw');
    return fallbackPlan(request, `Planner unavailable (${reason}) · single step`);
  }
  const durationMs = Date.now() - startedAt;

  // A bounded stop is a REASON, not an empty answer. Say which one.
  if (!outcome.text.trim()) {
    const notice = plannerStopNotice(outcome.stopReason);
    log.warn({ durationMs, stopReason: outcome.stopReason }, 'agent loop planner produced no text');
    return fallbackPlan(request, notice);
  }

  const validated = validateLoopPlan(extractJsonObject(outcome.text));
  if (!validated.ok) {
    log.warn({ durationMs, stopReason: outcome.stopReason, reason: validated.reason }, 'agent loop planner plan rejected');
    return fallbackPlan(request, `Planner plan rejected: ${validated.reason} · single step`);
  }
  log.info({ durationMs, steps: validated.steps.length }, 'agent loop planned');
  return { steps: validated.steps, source: 'planner' };
}

// ─── Plan → definition ────────────────────────────────────────────────

/** `agent:s1`. The id a node keeps in the editor, the graph and the run row. */
export function agentLoopNodeId(stepId: string): string {
  return `agent:${stepId}`;
}

/** `deck:s2` — the render node a `kind: 'deck'` step expands into. */
export function deckLoopNodeId(stepId: string): string {
  return `${DECK_STEP_KIND}:${stepId}`;
}

/** `meeting:create:s1` / `agenda:write:s2` — the capability node a step expands into. */
export function meetingLoopNodeId(stepId: string): string {
  return `${MEETING_STEP_KIND}:${stepId}`;
}

export function agendaLoopNodeId(stepId: string): string {
  return `${AGENDA_STEP_KIND}:${stepId}`;
}

export function agendaDraftTitle(step: LoopStep): string {
  return `Draft the agenda for ${step.title}`.slice(0, 120);
}

export function agendaWriteTitle(step: LoopStep): string {
  return `Write ${step.title} as a page`.slice(0, 120);
}

/**
 * What the agenda's drafting agent is told. The same shape the `agenda:write`
 * step validates, so a draft that does not fit is refused there rather than
 * written into a page under the name of an agenda.
 */
export function agendaDraftPrompt(step: LoopStep, request: AgentLoopRequest): string {
  return [
    'You draft a meeting agenda. Answer with ONLY a JSON object, no prose, no code fence.',
    '',
    '{"title":"…","items":[{"text":"…","owner":"…","minutes":10}],"owners":["…"]}',
    '',
    'Rules:',
    '- Between 3 and 12 items, each a single sentence under 200 characters.',
    '- An item names an owner only when the instruction or an earlier step did. Do not assign people.',
    '- `minutes` only when a duration was asked for.',
    '',
    `Meeting: ${step.title}`,
    `Requested by: ${request.requestedByName ?? request.requestedBy}`,
    `The instruction: ${request.instruction}`,
    'What the steps before you produced: {{ context.steps }}',
  ].join('\n');
}

/** What the fixture loop pins on a meeting node: the team it names, and a fixed proposal. */
export function meetingFixture(request: AgentLoopRequest): { team?: string; proposeInMinutes: number } {
  const team = teamNameFrom(request.instruction);
  return { ...(team ? { team } : {}), proposeInMinutes: 24 * 60 };
}

/** A small, valid agenda, so the local loop completes this kind with no model. */
export function agendaFixture(step: LoopStep, request: AgentLoopRequest): Agenda {
  const topic = request.instruction.replace(DECK_FIXTURE_MARKER, '').trim().slice(0, 80) || step.title;
  return {
    title: `Fixture agenda — ${topic}`.slice(0, 120),
    items: [
      { text: 'What this loop was asked to do, read back in one sentence.' },
      { text: 'What each step produced, and where it landed.' },
      { text: 'What a person still has to decide.' },
    ],
    owners: [],
  };
}

/** What the draft node is called; the render node is `Render <title> (.pptx)`. */
export function deckDraftTitle(step: LoopStep): string {
  return `Draft slides for ${step.title}`.slice(0, 120);
}

export function deckRenderTitle(step: LoopStep): string {
  return `Render ${step.title} (.pptx)`.slice(0, 120);
}

/**
 * What the drafting agent is told. GROUNDED OR NOTHING: it may cite only the
 * documents the conversation actually named, and the rule that every slide
 * carries a source is what stops the deck being decorative.
 *
 * The sources are listed by id at BUILD time (they are known then), while the
 * transcript and the earlier steps' work are interpolated from the run
 * context, so a step that runs an hour later still sees what preceded it.
 */
export function deckDraftPrompt(step: LoopStep, request: AgentLoopRequest): string {
  const sources = (request.sources ?? []).slice(0, MAX_SOURCE_TITLES);
  const sourceList = sources.length
    ? sources.map((s) => `- ${s.documentId} — ${s.title}${s.kind === 'transcript' ? ' (transcript)' : ''}`).join('\n')
    : '- (none: the conversation named no documents)';

  return [
    'You draft a presentation. Answer with ONLY a JSON object, no prose, no code fence.',
    '',
    'Shape:',
    '{"title":"…","subtitle":"…","theme":"product|engineering|plain","slides":[ … ]}',
    'Each slide:',
    '  {"id":"kebab-id","layout":"title|section|bullets|two-column|chart|quote|image","title":"…","eyebrow":"…",',
    '   "bullets":["…"], "columns":[["…"],["…"]],',
    '   "chart":{"kind":"bar|line","labels":["…"],"series":[{"name":"…","values":[1,2]}]},',
    '   "quote":{"text":"…","by":"…"}, "notes":"what to say out loud",',
    '   "sources":[{"documentId":"…","title":"…","anchor":"optional heading"}]}',
    '',
    'Rules:',
    '- Aim for 8-14 slides. Open with a `title` slide and divide the deck with `section` slides.',
    '- EVERY slide must cite at least one source, by the exact documentId listed below. A slide you cannot source is a slide you must not write.',
    '- Only state what the sources actually say. No invented numbers: a chart\'s values must appear in the sources.',
    '',
    'Sources you may cite:',
    sourceList,
    '',
    'Transcript of the conversation\'s recordings, if any:',
    '{{ context.deckTranscript }}',
    '',
    'What the earlier steps of this run produced (they also left their work in your workspace — read it):',
    '{{ context.deckPriorWork }}',
    '',
    `Requested by: ${request.requestedByName || request.requestedBy}`,
    `Build the deck for this step: ${step.prompt}`,
    `Acceptance: ${step.done}`,
  ].join('\n');
}

/**
 * The NODES one plan step becomes — the single source of truth for the
 * expansion, so a reader of the run does not have to re-derive it.
 *
 * WHY THIS EXISTS: `GET /api/agent-loops/:runId` used to map a plan step to
 * `agent:<id>` and stop there. A `meeting` step has no agent node at all, so
 * a failed `meeting:create:s2` was reported as `agent:s2`, `pending`, with no
 * error and no run-level error either — the one step that had actually broken
 * was the one the pane said had not started. Everything that enumerates a
 * loop's steps now goes through here, and a test asserts this agrees with
 * what `buildAgentLoopDefinition` really emitted.
 */
export interface LoopStepNode {
  nodeId: string;
  title: string;
  /** 'agent' for a drafting/working node, else the capability's own kind. */
  nodeKind: string;
  /** What this node does within the step: the only node, a draft, or the act. */
  role: 'only' | 'draft' | 'render' | 'write' | 'create';
}

export function loopStepNodes(step: LoopStep): LoopStepNode[] {
  switch (step.kind) {
    case 'deck':
      return [
        { nodeId: agentLoopNodeId(step.id), title: deckDraftTitle(step), nodeKind: 'agent', role: 'draft' },
        { nodeId: deckLoopNodeId(step.id), title: deckRenderTitle(step), nodeKind: DECK_STEP_KIND, role: 'render' },
      ];
    case 'agenda':
      return [
        { nodeId: agentLoopNodeId(step.id), title: agendaDraftTitle(step), nodeKind: 'agent', role: 'draft' },
        { nodeId: agendaLoopNodeId(step.id), title: agendaWriteTitle(step), nodeKind: AGENDA_STEP_KIND, role: 'write' },
      ];
    case 'meeting':
      // No drafting agent: there is nothing for a model to write.
      return [{ nodeId: meetingLoopNodeId(step.id), title: step.title, nodeKind: MEETING_STEP_KIND, role: 'create' }];
    default:
      return [{ nodeId: agentLoopNodeId(step.id), title: step.title, nodeKind: 'agent', role: 'only' }];
  }
}

/** The prompt a node runs: the step, then the sentence that ends the loop. */
export function agentLoopNodePrompt(step: LoopStep): string {
  return `${step.prompt}\n\nAcceptance: ${step.done}`;
}

/**
 * Plan → pipeline definition.
 *
 * CHAINED, NOT FORKED. The brief allows a fork/join for independent steps
 * "only if the existing definition builders already use fork/join" — none of
 * them does (document edit, transform, summarize, diagram are all straight
 * chains), and a generated graph is the worst place to debut a node shape no
 * hand-written pipeline in this service has exercised. The steps therefore
 * run in topological order, which the planner's own contract already
 * guarantees: `dependsOn` may only name EARLIER ids, so s1..sN is a valid
 * topological order by construction. `dependsOn` is preserved verbatim in
 * each node's `loop` data, so a later fork/join pass has what it needs.
 *
 * Every node shares the run's devbox session, so "chained" costs nothing in
 * setup: step 2 resumes the box step 1 left behind.
 *
 * A `kind: 'deck'` step is the one that is not one node: it expands into a
 * draft (agent) and a render (deck) pair, chained draft → render → next.
 */
export function buildAgentLoopDefinition(plan: LoopPlan, request: AgentLoopRequest): Record<string, unknown> {
  const pos = (i: number) => ({ x: i * 240, y: 0 });
  const steps = plan.steps;
  const fixture = wantsDeckFixture(request.instruction);

  const nodes: Array<Record<string, unknown>> = [
    {
      id: 'trigger',
      type: 'trigger',
      position: pos(0),
      data: { type: 'trigger', triggerType: 'platform', label: '/agent command' },
    },
  ];

  const agentNode = (step: LoopStep, index: number, over: Record<string, unknown> = {}): Record<string, unknown> => ({
    id: agentLoopNodeId(step.id),
    type: 'agent',
    position: pos(index),
    data: {
      type: 'agent',
      prompt: agentLoopNodePrompt(step),
      model: request.model,
      runtime: 'devbox',
      contextBudgetTokens: request.contextBudgetTokens,
      maxTurns: LOOP_STEP_MAX_TURNS,
      timeoutMs: LOOP_STEP_TIMEOUT_MS,
      allowedTools: [...LOOP_STEP_TOOLS],
      label: step.title,
      loop: { kind: step.kind, stepId: step.id, ...(step.dependsOn.length ? { dependsOn: step.dependsOn } : {}) },
      retryPolicy: LOOP_STEP_RETRY,
      ...over,
    },
  });

  for (const step of steps) {
    if (step.kind === 'deck') {
      // A DECK STEP IS TWO NODES, because drafting and rendering fail
      // differently and a person needs to see which one did. The draft is an
      // ordinary devbox agent step that must answer with a DeckSpec; the
      // render is the real generator, and it refuses anything that is not a
      // spec rather than inventing a deck.
      nodes.push(agentNode(step, nodes.length, {
        prompt: deckDraftPrompt(step, request),
        label: deckDraftTitle(step),
        loop: { kind: step.kind, stepId: step.id, role: 'draft', ...(step.dependsOn.length ? { dependsOn: step.dependsOn } : {}) },
      }));
      nodes.push({
        id: deckLoopNodeId(step.id),
        type: DECK_STEP_KIND,
        position: pos(nodes.length),
        data: {
          type: DECK_STEP_KIND,
          label: deckRenderTitle(step),
          // The draft step's final text IS the spec.
          specFrom: `$.steps.${agentLoopNodeId(step.id)}.text`,
          title: step.title,
          // No documentId: the handler creates the presentation for the
          // requester, the way /diagram creates its page.
          loop: { kind: step.kind, stepId: step.id, role: 'render' },
          retryPolicy: LOOP_STEP_RETRY,
          ...(fixture ? { spec: deckFixtureSpec(request, request.sources ?? []) } : {}),
        },
      });
      continue;
    }
    if (step.kind === 'meeting') {
      // ONE node, and a real one: it creates the room and puts the named
      // team's members on it. No drafting agent — there is nothing for a model
      // to write, and a step that only talks about a meeting is the thing this
      // replaces.
      nodes.push({
        id: meetingLoopNodeId(step.id),
        type: MEETING_STEP_KIND,
        position: pos(nodes.length),
        data: {
          type: MEETING_STEP_KIND,
          label: step.title,
          title: step.title,
          loop: { kind: step.kind, stepId: step.id, ...(step.dependsOn.length ? { dependsOn: step.dependsOn } : {}) },
          retryPolicy: LOOP_STEP_RETRY,
          // The fixture path pins what the sentence would otherwise have to be
          // read for: the team, and a fixed proposal. The room it creates is
          // still real — that is the capability being exercised — but nothing
          // about it depends on a model.
          ...(fixture ? meetingFixture(request) : {}),
        },
      });
      continue;
    }

    if (step.kind === 'agenda') {
      // Draft, then write: a model writes the agenda, and the write is a real
      // page in the Documents explorer rather than text in a run's output.
      nodes.push(agentNode(step, nodes.length, {
        prompt: agendaDraftPrompt(step, request),
        label: agendaDraftTitle(step),
        loop: { kind: step.kind, stepId: step.id, role: 'draft', ...(step.dependsOn.length ? { dependsOn: step.dependsOn } : {}) },
      }));
      nodes.push({
        id: agendaLoopNodeId(step.id),
        type: AGENDA_STEP_KIND,
        position: pos(nodes.length),
        data: {
          type: AGENDA_STEP_KIND,
          label: agendaWriteTitle(step),
          agendaFrom: `$.steps.${agentLoopNodeId(step.id)}.text`,
          title: step.title,
          loop: { kind: step.kind, stepId: step.id, role: 'write' },
          retryPolicy: LOOP_STEP_RETRY,
          ...(fixture ? { agenda: agendaFixture(step, request) } : {}),
        },
      });
      continue;
    }

    nodes.push(agentNode(step, nodes.length));
  }

  const edges = nodes.slice(0, -1).map((node, i) => ({
    id: `e${i + 1}`,
    source: node.id as string,
    sourceHandle: 'out',
    target: nodes[i + 1].id as string,
    targetHandle: 'in',
  }));

  return {
    id: CHAT_AGENT_LOOP_PIPELINE_ID,
    name: 'Agent loop',
    workType: 'agents', kind: 'agent', route: routeSummary({ nodes, edges }), origin: { kind: 'system' },
    description: 'An /agent command from a chat: a planner turns the instruction into steps, and each step runs as an agent in the run\'s shared cloud workspace until it meets its acceptance. A deck step drafts a grounded DeckSpec and renders a real .pptx.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    nodes,
    edges,
  };
}

/** What the card shows about the lab before a single event has arrived. */
export function agentLoopCardLab(plan: LoopPlan, request: AgentLoopRequest): Record<string, unknown> {
  return {
    model: request.model,
    contextBudgetTokens: request.contextBudgetTokens,
    planSource: plan.source,
    ...(plan.notice ? { notice: plan.notice } : {}),
    steps: plan.steps.map((s) => ({ id: s.id, title: s.title, kind: s.kind })),
  };
}
