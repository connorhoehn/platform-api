// The nine transform pairs the golden test measures: three source pages of
// different shapes × three built-in target types. Shared by the test (default
// mode: load the recorded plan, check invariants) and its record mode (render
// the same prompt the pipeline renders, ask Haiku once, write the golden).
//
// Field lists are the frontend's built-in cookbooks (ui-components
// doc-types/cookbooks/index.ts: Meeting Notes, Sprint Retrospective, RFC /
// Technical Proposal) as the transform-run route receives them — `name` +
// `sectionType` — and rendered by the same `- Name (sectionType)` rule.
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import { CANVAS_BODY_KEY, CANVAS_META_KEY, outlineOf, type DocumentOutline } from '../documentAgentOps';
import { DOCUMENT_TRANSFORM_PROMPT } from '../documentAgentPipeline';
import { interpolateContext } from '../../pipeline/agent/AgentStepHandler';
import { transformFieldList } from '../../chat/builtinChatCommands';

export interface TargetField { name: string; sectionType: string }
export interface TransformTargetFixture { key: string; id: string; name: string; icon: string; fields: TargetField[] }
export interface TransformSourceFixture { key: string; title: string; build: () => YT.Doc }

/** Section types that become ONE `field` macro on a canvas (documentTypeCanvasModel's TYPED_FIELD_SECTION_TYPES). */
export const SHORT_FIELD_TYPES = new Set(['text', 'date', 'number', 'boolean']);

// ── Sources ────────────────────────────────────────────────────────────────

type Block =
  | { kind: 'heading'; level: number; text: string }
  | { kind: 'paragraph'; text: string }
  | { kind: 'macro'; macroName: string; yaml: string };

function canvas(title: string, blocks: Block[]): YT.Doc {
  const doc = new Y.Doc();
  doc.getMap<unknown>(CANVAS_META_KEY).set('title', title);
  const body = doc.getXmlFragment(CANVAS_BODY_KEY);
  const els = blocks.map((b) => {
    const el = new Y.XmlElement(b.kind);
    if (b.kind === 'heading') el.setAttribute('level', String(b.level));
    if (b.kind === 'macro') el.setAttribute('macroName', b.macroName);
    const t = new Y.XmlText();
    t.insert(0, b.kind === 'macro' ? b.yaml : b.text);
    el.insert(0, [t]);
    return el;
  });
  body.insert(0, els);
  return doc;
}

const h = (level: number, text: string): Block => ({ kind: 'heading', level, text });
const p = (text: string): Block => ({ kind: 'paragraph', text });
const field = (label: string, type: string, value?: string): Block => ({ kind: 'macro', macroName: 'field', yaml: `label: ${label}\ntype: ${type}${value !== undefined ? `\nvalue: ${value}` : ''}` });

export const TRANSFORM_SOURCES: TransformSourceFixture[] = [
  {
    // A Bug Report canvas: typed field macros up top, then the long-form fields as headings + prose.
    key: 'bug-report',
    title: 'Checkout double-charges on retry',
    build: () => canvas('Checkout double-charges on retry', [
      h(1, 'Checkout double-charges on retry'),
      field('Severity', 'text', 'High'),
      field('Status', 'text', 'Open'),
      field('Reported By', 'text', 'Grace Hopper'),
      field('Reported Date', 'date', "'2026-09-02'"),
      h(2, 'Steps to Reproduce'),
      p('Add any item to the cart, pay with a card that times out at the gateway, then press Retry on the error page. The order confirmation shows one order but the card statement shows two authorisations.'),
      h(2, 'Actual Behavior'),
      p('The retry posts a second payment intent because the first intent id is dropped from the form state when the error page renders.'),
      p('Suspected cause: the checkout reducer resets on route change; Linus has a draft fix that keeps the intent id in session storage.'),
    ]),
  },
  {
    // A Meeting Notes-shaped page: two field macros, discussion prose, an action-items macro, a decision macro.
    key: 'meeting-notes',
    title: 'Platform sync, September 3',
    build: () => canvas('Platform sync, September 3', [
      h(1, 'Platform sync, September 3'),
      field('Date', 'date', "'2026-09-03'"),
      field('Attendees', 'text', 'Grace Hopper, Ada Lovelace, Linus Torvalds'),
      h(2, 'Discussion Notes'),
      p('The gateway reconnect storm from last week traced back to a stale close evicting the socket that replaced it; the fix shipped Tuesday and the warning count went from 65 to 0.'),
      p('Ada raised that the document explorer still scrolls 24px when a page does not claim the shell as full-bleed; the fix is a one-line prop but every route has to pass it.'),
      h(2, 'Action Items'),
      { kind: 'macro', macroName: 'action-items', yaml: "items:\n  - assignee: ada\n    due: '2026-09-10'\n    status: pending\n    text: pass fullBleed on every /documents route\n  - assignee: linus\n    status: pending\n    text: write the reconnect regression test" },
      h(2, 'Decisions Made'),
      { kind: 'macro', macroName: 'decision', yaml: 'decision: Keep the canvas as the default document shape\nrationale: Every new document lands on the section branch today; defaulting to canvas closes the gap instead of migrating twice.\nstatus: accepted' },
    ]),
  },
  {
    // A plain page: three headings with a paragraph each, no macros, no type.
    key: 'plain-page',
    title: 'Moving the session store to Redis Cluster',
    build: () => canvas('Moving the session store to Redis Cluster', [
      h(1, 'Moving the session store to Redis Cluster'),
      h(2, 'Why'),
      p('A single Redis node holds every gateway session, so one restart logs everyone out and the failover window is measured in minutes rather than seconds.'),
      h(2, 'Plan'),
      p('Stand up a three-shard cluster beside the current node, dual-write sessions for one release, then flip reads and retire the single node once the miss rate is below one in ten thousand.'),
      h(2, 'Risks'),
      p('Hash-tag mistakes split a user\'s keys across shards, and the client library pins an older protocol; both need a canary on cell-b before the flip.'),
    ]),
  },
];

// ── Targets (built-in cookbooks, as the route receives them) ───────────────

const f = (name: string, sectionType: string): TargetField => ({ name, sectionType });

export const TRANSFORM_TARGETS: TransformTargetFixture[] = [
  {
    key: 'meeting-notes', id: 'meeting-notes', name: 'Meeting Notes', icon: '📝',
    fields: [
      f('Meeting Title', 'text'), f('Date', 'date'), f('Meeting Type', 'text'), f('Attendees', 'text'), f('Facilitator', 'text'),
      f('Agenda', 'rich-text'), f('Discussion Notes', 'rich-text'), f('Action Items', 'rich-text'), f('Decisions Made', 'rich-text'),
      f('Follow-Up Date', 'date'), f('Recording Link', 'text'),
    ],
  },
  {
    key: 'sprint-retro', id: 'sprint-retro', name: 'Sprint Retrospective', icon: '🔄',
    fields: [
      f('Sprint Name', 'text'), f('Sprint Dates', 'text'), f('Facilitator', 'text'), f('Team', 'text'),
      f('Velocity', 'number'), f('Planned Points', 'number'), f('Completed Points', 'number'),
      f('What Went Well', 'rich-text'), f("What Didn't Go Well", 'rich-text'), f('Start Doing', 'rich-text'), f('Stop Doing', 'rich-text'), f('Continue Doing', 'rich-text'),
      f('Action Items', 'rich-text'), f('Action Item Owners', 'text'), f('Follow-Up Date', 'date'), f('Overall Sentiment', 'text'),
    ],
  },
  {
    key: 'rfc', id: 'rfc', name: 'RFC / Technical Proposal', icon: '📨',
    fields: [
      f('RFC Title', 'text'), f('Author', 'text'), f('Status', 'text'), f('Created Date', 'date'), f('Decision Date', 'date'), f('Reviewers', 'text'),
      f('Problem Statement', 'rich-text'), f('Proposed Solution', 'rich-text'), f('Alternatives Considered', 'rich-text'), f('Technical Design', 'rich-text'),
      f('Migration Plan', 'rich-text'), f('Risks', 'rich-text'), f('Open Questions', 'rich-text'), f('Decision', 'rich-text'), f('Superseded By', 'text'),
    ],
  },
];

// ── Pairs + the prompt exactly as the pipeline renders it ──────────────────

export interface TransformPair { key: string; source: TransformSourceFixture; target: TransformTargetFixture }

export const TRANSFORM_PAIRS: TransformPair[] = TRANSFORM_SOURCES.flatMap((source) =>
  TRANSFORM_TARGETS.map((target) => ({ key: `${source.key}__${target.key}`, source, target })));

/**
 * The run context the plan step sees: the read step's output plus the
 * transform-run route's trigger payload, interpolated by the agent step's own
 * `interpolateContext`. Same prompt bytes the pipeline sends.
 */
export function renderTransformPrompt(pair: TransformPair): { prompt: string; outline: DocumentOutline } {
  const outline = outlineOf(pair.source.build());
  const context: Record<string, unknown> = {
    documentOutline: outline.text, documentTitle: outline.title ?? '', documentBlocks: outline.blocks.length,
    targetName: pair.target.name, targetId: pair.target.id, targetIcon: pair.target.icon,
    targetFieldList: transformFieldList(pair.target.fields),
    requestedByName: 'Grace', instruction: `Transform into ${pair.target.name}`,
  };
  return { prompt: interpolateContext(DOCUMENT_TRANSFORM_PROMPT, context), outline };
}
