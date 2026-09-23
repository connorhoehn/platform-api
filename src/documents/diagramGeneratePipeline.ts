// "/diagram <what>" in a chat becomes a PIPELINE RUN that draws a whiteboard:
// a light model sketches the boxes and arrows → the sketch is normalised into
// real Excalidraw element records → they land in the diagram-shaped document
// the client created, through the CRDT, as a collaborator → a card in the
// thread opens the diagram.
//
//   diagram-generate:  trigger → plan → apply → publish
//
// The model never sees Excalidraw's record shape. It answers with a SKETCH —
// ids, kinds, a grid position, a label, which boxes an arrow joins — and the
// normaliser here owns every other property: versions and nonces, fractional
// indices, bound label text elements, arrow bindings at box edges. That keeps
// the contract small enough for a light model to get right, and keeps what
// reaches the canvas exactly what `ExcalidrawYjsBinding` would have written.
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import type * as YT from 'yjs' with { 'resolution-mode': 'import' };
// yjs ships ESM types but a CJS build; under `module: node16` the value has to
// come in through require while the types come in as type-only.
// eslint-disable-next-line @typescript-eslint/no-require-imports
const Y = require('yjs') as typeof YT;
import type { StepHandler, StepOutcome, PipelineNode } from 'distributed-core/applications/pipeline';
import { AgentCollaborator, HAIKU_AGENT } from './AgentCollaborator';
import { CANVAS_META_KEY } from './documentAgentOps';
import { postRunCard, runCardMetadata, runCardOutcomeOf, runCardOutputOf, str, type RunCardInput, type RunCardPostOutcome } from './chatRunCard';
import log from '../lib/logger';

export const DIAGRAM_GENERATE_PIPELINE_ID = 'diagram-generate';
export const DIAGRAM_APPLY_KIND = 'diagram:apply';
export const DIAGRAM_PUBLISH_KIND = 'diagram:publish';

/** The Y.Doc root the diagram pane binds (`DEFAULT_DIAGRAM_ROOT` in realtime-modules) and the elements container under it. */
export const DIAGRAM_ROOT_KEY = 'excalidraw';
export const DIAGRAM_ELEMENTS_KEY = 'elements';
const CANVAS_SCHEMA_VERSION = 2;

const PLAN_RETRY = { maxAttempts: 3, backoffMs: 2000, backoffMultiplier: 2, retryUnknown: true } as const;
const MAX_SKETCH_ELEMENTS = 14;
const GRID = 40;

export const DIAGRAM_PLAN_PROMPT = [
  'You sketch an architecture diagram. Answer with ONLY a JSON array of elements, no prose, no code fence.',
  'Element shapes:',
  '  {"id":"api","type":"rectangle","x":40,"y":40,"width":200,"height":80,"text":"API gateway"}     — a box with its label inside (types: rectangle, ellipse, diamond)',
  '  {"id":"a1","type":"arrow","startId":"api","endId":"db","text":"reads"}                          — an arrow between two boxes, by id; "text" is an optional short label',
  '  {"id":"note","type":"text","x":40,"y":400,"width":300,"height":25,"text":"..."}               — free-standing text, only when a caption is needed',
  'Rules: at most 14 elements in total. Boxes are 200 wide and 80 tall. x and y are multiples of 40; lay boxes out on a grid so arrows read left-to-right or top-to-bottom, leaving at least 120 between boxes. Every id is a unique short string. Every arrow names existing boxes. Labels are 1–4 words. Draw the components and flows the request describes — no more.',
  '',
  'Title: {{ context.title }}',
  'Requested by: {{ context.requestedByName }}',
  'Draw: {{ context.prompt }}',
].join('\n');

export function buildDiagramGenerateDefinition(model = process.env.DOCUMENT_AGENT_MODEL || 'haiku'): Record<string, unknown> {
  const pos = (x: number) => ({ x, y: 0 });
  const edge = (source: string, target: string) => ({ id: `${source}->${target}`, source, sourceHandle: 'out', target, targetHandle: 'in' });
  return {
    id: DIAGRAM_GENERATE_PIPELINE_ID,
    name: 'Diagram from a description',
    workType: 'documents', kind: 'diagram', route: ['/diagram', 'Sketch', 'Draw', 'Card'], origin: { kind: 'system' },
    description: 'A /diagram command from a chat: a light model sketches the boxes and arrows, the sketch becomes Excalidraw elements in the new diagram document, a card in the thread opens it.',
    version: 1, status: 'published', publishedVersion: 1,
    nodes: [
      { id: 'trigger', type: 'trigger', position: pos(0), data: { type: 'trigger', triggerType: 'platform', label: '/diagram command' } },
      { id: 'plan', type: 'agent', position: pos(200), data: { type: 'agent', label: 'Sketch the diagram (Haiku)', prompt: DIAGRAM_PLAN_PROMPT, model, allowedTools: [], maxTurns: 1, retryPolicy: PLAN_RETRY } },
      { id: 'apply', type: DIAGRAM_APPLY_KIND, position: pos(400), data: { type: DIAGRAM_APPLY_KIND, label: 'Draw it as a collaborator', sketchFrom: 'steps.plan.text', retryPolicy: { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } } },
      { id: 'publish', type: DIAGRAM_PUBLISH_KIND, position: pos(600), data: { type: DIAGRAM_PUBLISH_KIND, label: 'Post the card to the conversation', retryPolicy: { maxAttempts: 3, backoffMs: 1500, backoffMultiplier: 2, retryUnknown: true } } },
    ],
    edges: [edge('trigger', 'plan'), edge('plan', 'apply'), edge('apply', 'publish')],
  };
}

// ---------------------------------------------------------------------------
// The sketch the model answers with
// ---------------------------------------------------------------------------

export type SketchKind = 'rectangle' | 'ellipse' | 'diamond' | 'arrow' | 'text';
export interface SketchElement {
  id: string;
  type: SketchKind;
  x?: number;
  y?: number;
  width?: number;
  height?: number;
  text?: string;
  points?: Array<[number, number]>;
  startId?: string;
  endId?: string;
}

const KINDS: ReadonlySet<string> = new Set(['rectangle', 'ellipse', 'diamond', 'arrow', 'text']);
const num = (v: unknown): number | undefined => (typeof v === 'number' && Number.isFinite(v) ? v : undefined);

/** Pull the first JSON array out of whatever the model said and keep the well-formed elements, first come first served, up to the cap. */
export function parseSketch(text: string): SketchElement[] {
  const start = text.indexOf('[');
  const end = text.lastIndexOf(']');
  if (start < 0 || end <= start) return [];
  let raw: unknown;
  try { raw = JSON.parse(text.slice(start, end + 1)); } catch { return []; }
  if (!Array.isArray(raw)) return [];
  const out: SketchElement[] = [];
  const ids = new Set<string>();
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue;
    const o = item as Record<string, unknown>;
    const type = typeof o.type === 'string' ? o.type : '';
    if (!KINDS.has(type)) continue;
    let id = typeof o.id === 'string' && o.id.trim() ? o.id.trim().slice(0, 40) : `${type}-${out.length + 1}`;
    while (ids.has(id)) id = `${id}_`;
    ids.add(id);
    const points = Array.isArray(o.points)
      ? o.points.filter((p): p is [number, number] => Array.isArray(p) && p.length === 2 && num(p[0]) !== undefined && num(p[1]) !== undefined).map((p) => [p[0], p[1]] as [number, number])
      : undefined;
    out.push({
      id, type: type as SketchKind,
      x: num(o.x), y: num(o.y), width: num(o.width), height: num(o.height),
      text: typeof o.text === 'string' && o.text.trim() ? o.text.trim().slice(0, 200) : undefined,
      points: points && points.length >= 2 ? points : undefined,
      startId: typeof o.startId === 'string' ? o.startId : undefined,
      endId: typeof o.endId === 'string' ? o.endId : undefined,
    });
    if (out.length >= MAX_SKETCH_ELEMENTS) break;
  }
  return out;
}

// ---------------------------------------------------------------------------
// Normalising the sketch into Excalidraw 0.18 element records
// ---------------------------------------------------------------------------

export type DiagramElementRecord = Record<string, unknown> & { id: string; type: string };

const randomInt = () => Math.floor(Math.random() * 2 ** 31);
const BASE62 = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz';

/**
 * The n-th key of the fractional-indexing scheme Excalidraw sorts by:
 * `a0`…`az` (one base-62 digit after `a`), then `b00`… (two digits after `b`).
 * Lexicographic order IS numeric order, which is the whole point of the scheme.
 */
export function fractionalIndex(n: number): string {
  if (n < 62) return `a${BASE62[n]}`;
  const m = n - 62;
  return `b${BASE62[Math.floor(m / 62) % 62]}${BASE62[m % 62]}`;
}

const snap = (v: number | undefined, fallback: number) => (v === undefined ? fallback : Math.round(v / GRID) * GRID);

function base(id: string, type: string, x: number, y: number, width: number, height: number, index: number, roundness: { type: number } | null): DiagramElementRecord {
  return {
    id, type, x, y, width, height,
    version: 1, versionNonce: randomInt(), index: fractionalIndex(index), isDeleted: false,
    angle: 0, strokeColor: '#1e1e1e', backgroundColor: 'transparent', fillStyle: 'solid', strokeWidth: 2, strokeStyle: 'solid',
    roughness: 1, opacity: 100, groupIds: [], frameId: null, roundness, seed: randomInt(), boundElements: [] as Array<{ type: string; id: string }>,
    updated: Date.now(), link: null, locked: false,
  };
}

const FONT_SIZE = 20;
const LINE_HEIGHT = 1.25;
/** Roughly what Excalidraw's default (Virgil) measures at 20px; the editor re-measures on first render. */
const CHAR_WIDTH = 11;

function textRecord(id: string, text: string, x: number, y: number, width: number, height: number, index: number, containerId: string | null): DiagramElementRecord {
  return {
    ...base(id, 'text', x, y, width, height, index, null),
    text, originalText: text, fontSize: FONT_SIZE, fontFamily: 1, textAlign: 'center', verticalAlign: 'middle',
    lineHeight: LINE_HEIGHT, autoResize: true, containerId,
  };
}

/** A label's box: one line, fitted inside the container; wraps onto lines when it cannot fit. */
function fitLabel(text: string, maxWidth: number): { width: number; height: number; lines: number } {
  const chars = text.length;
  const naturalWidth = chars * CHAR_WIDTH;
  const lines = Math.max(1, Math.ceil(naturalWidth / Math.max(maxWidth, CHAR_WIDTH)));
  return { width: Math.min(naturalWidth, maxWidth), height: Math.round(FONT_SIZE * LINE_HEIGHT * lines), lines };
}

export interface NormalisedDiagram { elements: DiagramElementRecord[]; boxes: number; arrows: number; labels: number }

/**
 * Turn the model's sketch into the records Excalidraw reads: every box gets a
 * bound label text element, every arrow that names two boxes is anchored on
 * their edges and bound at both ends, and indices ascend in the order given
 * (boxes, then their labels, then arrows, then free text) so z-order is sane.
 */
export function normaliseSketch(sketch: readonly SketchElement[]): NormalisedDiagram {
  const boxes = sketch.filter((s) => s.type === 'rectangle' || s.type === 'ellipse' || s.type === 'diamond');
  const arrows = sketch.filter((s) => s.type === 'arrow');
  const texts = sketch.filter((s) => s.type === 'text');
  const records: DiagramElementRecord[] = [];
  const byId = new Map<string, DiagramElementRecord>();
  let index = 0;

  // Boxes first, on the grid, default 200×80; a box with no position takes the next grid slot.
  boxes.forEach((b, i) => {
    const width = b.width && b.width > 0 ? snap(b.width, 200) : 200;
    const height = b.height && b.height > 0 ? snap(b.height, 80) : 80;
    const x = snap(b.x, GRID + (i % 3) * 320);
    const y = snap(b.y, GRID + Math.floor(i / 3) * 200);
    const rec = base(b.id, b.type, x, y, width, height, index++, b.type === 'rectangle' ? { type: 3 } : null);
    records.push(rec);
    byId.set(b.id, rec);
  });
  // Their labels: a separate text element bound into the box.
  let labels = 0;
  for (const b of boxes) {
    if (!b.text) continue;
    const box = byId.get(b.id)!;
    const bw = box.width as number; const bh = box.height as number;
    const fit = fitLabel(b.text, bw - 20);
    const labelId = `${b.id}-label`;
    const rec = textRecord(labelId, b.text, (box.x as number) + (bw - fit.width) / 2, (box.y as number) + (bh - fit.height) / 2, fit.width, fit.height, index++, b.id);
    (box.boundElements as Array<{ type: string; id: string }>).push({ type: 'text', id: labelId });
    records.push(rec);
    labels++;
  }
  // Arrows: anchored on the source's right edge and the target's left edge when they name boxes; the model's own geometry otherwise.
  let bound = 0;
  for (const a of arrows) {
    const from = a.startId ? byId.get(a.startId) : undefined;
    const to = a.endId ? byId.get(a.endId) : undefined;
    let x: number; let y: number; let points: Array<[number, number]>;
    if (from && to) {
      const fx = from.x as number; const fy = from.y as number; const fw = from.width as number; const fh = from.height as number;
      const tx = to.x as number; const ty = to.y as number; const tw = to.width as number; const th = to.height as number;
      const fromCx = fx + fw / 2; const fromCy = fy + fh / 2; const toCx = tx + tw / 2; const toCy = ty + th / 2;
      // Leave from the side that faces the target: right/left when the flow is mostly horizontal, bottom/top otherwise.
      const horizontal = Math.abs(toCx - fromCx) >= Math.abs(toCy - fromCy);
      const start: [number, number] = horizontal ? [toCx >= fromCx ? fx + fw : fx, fromCy] : [fromCx, toCy >= fromCy ? fy + fh : fy];
      const end: [number, number] = horizontal ? [toCx >= fromCx ? tx : tx + tw, toCy] : [toCx, toCy >= fromCy ? ty : ty + th];
      x = start[0]; y = start[1];
      points = [[0, 0], [end[0] - x, end[1] - y]];
    } else {
      x = a.x ?? 0; y = a.y ?? 0;
      points = a.points && a.points.length >= 2 ? a.points : [[0, 0], [a.width ?? 160, a.height ?? 0]];
    }
    const last = points[points.length - 1]!;
    const rec: DiagramElementRecord = {
      ...base(a.id, 'arrow', x, y, Math.abs(last[0]), Math.abs(last[1]), index++, null),
      points, lastCommittedPoint: null, startArrowhead: null, endArrowhead: 'arrow', elbowed: false,
      startBinding: from ? { elementId: from.id, focus: 0, gap: 1 } : null,
      endBinding: to ? { elementId: to.id, focus: 0, gap: 1 } : null,
    };
    if (from) (from.boundElements as Array<{ type: string; id: string }>).push({ type: 'arrow', id: a.id });
    if (to) (to.boundElements as Array<{ type: string; id: string }>).push({ type: 'arrow', id: a.id });
    if (from && to) bound++;
    records.push(rec);
    // An arrow's label is a text element bound to the arrow, centred on its midpoint.
    if (a.text) {
      const fit = fitLabel(a.text, 200);
      const labelId = `${a.id}-label`;
      records.push(textRecord(labelId, a.text, x + last[0] / 2 - fit.width / 2, y + last[1] / 2 - fit.height / 2, fit.width, fit.height, index++, a.id));
      (rec.boundElements as Array<{ type: string; id: string }>).push({ type: 'text', id: labelId });
      labels++;
    }
  }
  // Free-standing text last.
  for (const t of texts) {
    if (!t.text) continue;
    const fit = fitLabel(t.text, t.width && t.width > 0 ? t.width : 400);
    records.push({ ...textRecord(t.id, t.text, snap(t.x, GRID), snap(t.y, GRID), fit.width, fit.height, index++, null), textAlign: 'left', verticalAlign: 'top' });
  }
  return { elements: records, boxes: boxes.length, arrows: arrows.length, labels };
}

// ---------------------------------------------------------------------------
// Writing into the document
// ---------------------------------------------------------------------------

export interface DiagramMetaInput { documentId: string; title: string; createdBy?: string }

/**
 * ONE transaction: the meta a diagram-shaped canvas needs (only what is
 * missing — the client may have seeded it already), then every element as a
 * `Y.Map` under `excalidraw → elements`. The elements container is created
 * only when absent, for the reason `ExcalidrawYjsBinding._ensureElements`
 * gives: a speculative second container is a detached one.
 */
export function writeDiagram(doc: YT.Doc, elements: readonly DiagramElementRecord[], meta: DiagramMetaInput, origin: unknown = 'agent'): { written: number } {
  let written = 0;
  doc.transact(() => {
    const yMeta = doc.getMap<unknown>(CANVAS_META_KEY);
    const setIfMissing = (k: string, v: unknown) => { if (!yMeta.has(k)) yMeta.set(k, v); };
    setIfMissing('id', meta.documentId);
    setIfMissing('sourceType', 'notes');
    setIfMissing('sourceId', '');
    setIfMissing('createdBy', meta.createdBy ?? HAIKU_AGENT.userId);
    setIfMissing('createdAt', new Date().toISOString());
    setIfMissing('aiModel', '');
    setIfMissing('status', 'draft');
    if (meta.title && (!yMeta.has('title') || !yMeta.get('title'))) yMeta.set('title', meta.title);
    yMeta.set('docShape', 'diagram');
    yMeta.set('templateApplied', true);
    yMeta.set('schemaVersion', CANVAS_SCHEMA_VERSION);

    const root = doc.getMap<unknown>(DIAGRAM_ROOT_KEY);
    if (!root.has(DIAGRAM_ELEMENTS_KEY)) root.set(DIAGRAM_ELEMENTS_KEY, new Y.Map<unknown>());
    const container = root.get(DIAGRAM_ELEMENTS_KEY) as YT.Map<unknown>;
    for (const el of elements) {
      const m = new Y.Map<unknown>();
      container.set(el.id, m);
      for (const [k, v] of Object.entries(el)) if (v !== undefined) m.set(k, v);
      written++;
    }
  }, origin);
  return { written };
}

/** Where the sketch lives in the run context; default the planning agent's text. */
function readPath(obj: unknown, path: string): unknown {
  return path.split('.').reduce<unknown>((cur, key) => (cur && typeof cur === 'object' ? (cur as Record<string, unknown>)[key] : undefined), obj);
}

export function sketchFromContext(ctx: Record<string, unknown>, data: { sketchFrom?: string }): SketchElement[] {
  const value = readPath(ctx, data.sketchFrom ?? 'steps.plan.text');
  if (Array.isArray(value)) return parseSketch(JSON.stringify(value));
  if (typeof value === 'string') return parseSketch(value);
  return [];
}

/** Step 2: join the diagram document as the agent and draw. */
export function createDiagramApplyHandler(): DescribedStepHandler {
  return {
    kind: DIAGRAM_APPLY_KIND,
    description:
      'Draws the model\'s proposed diagram into the document as a collaborator: joins the page, writes the boxes and arrows, and leaves. Fails when the model proposed no elements rather than writing an empty canvas.',
    reads: 'documentId and the sketch, from the planning step or the node’s sketchFrom path.',
    emits:
      'How many elements were written, split into boxes, arrows and labels.',
    async execute(node: PipelineNode, ctx: Record<string, unknown>): Promise<StepOutcome> {
      const documentId = str(ctx.documentId);
      if (!documentId) return { status: 'failed', error: 'documentId missing from the run context' };
      const sketch = sketchFromContext(ctx, ((node as { data?: { sketchFrom?: string } }).data ?? {}));
      if (sketch.length === 0) return { status: 'failed', error: 'the model proposed no elements' };
      const drawn = normaliseSketch(sketch);
      const agent = new AgentCollaborator({ documentId, identity: HAIKU_AGENT });
      try {
        await agent.join();
        agent.announce();
        const { written } = writeDiagram(agent.doc, drawn.elements, { documentId, title: str(ctx.title) ?? 'Diagram' });
        log.info({ documentId, written, boxes: drawn.boxes, arrows: drawn.arrows }, 'diagram drawn as a collaborator');
        void agent.leave(Number(process.env.DOCUMENT_AGENT_PRESENCE_HOLD_MS) || 4000).catch(() => undefined);
        return { status: 'completed', output: { elements: written, boxes: drawn.boxes, arrows: drawn.arrows, labels: drawn.labels } };
      } catch (err) {
        await agent.leave(0).catch(() => undefined);
        return { status: 'failed', error: err instanceof Error ? err.message : String(err) };
      }
    },
  };
}

/** Last step: the card in the thread — `/diagram <title>` — carrying the document's id. */
export function createDiagramPublishHandler(deps: { postCard?: (input: RunCardInput) => Promise<boolean | RunCardPostOutcome> } = {}): DescribedStepHandler {
  const post = deps.postCard ?? postRunCard;
  return {
    kind: DIAGRAM_PUBLISH_KIND,
    description:
      'Posts the finished diagram back to the conversation as a run card naming the title and how many elements were drawn.',
    reads: 'chatChannel, documentId, title and the apply step’s element count.',
    emits:
      'The card metadata, returned whether or not the step posted it — the client posts it itself when clientPostsCard is set.',
    async execute(_node: PipelineNode, ctx: Record<string, unknown>, helpers): Promise<StepOutcome> {
      const steps = (ctx.steps ?? {}) as Record<string, Record<string, unknown> | undefined>;
      const title = str(ctx.title) ?? 'Diagram';
      const elements = typeof steps.apply?.elements === 'number' ? steps.apply.elements : 0;
      // `context: ctx` lets the post step down when the client posted the card (clientPostsCard); the metadata is returned either way.
      const cardInput = {
        channel: str(ctx.chatChannel), runId: helpers.runId, pipelineId: DIAGRAM_GENERATE_PIPELINE_ID,
        command: '/diagram', instruction: title,
        extra: { documentId: ctx.documentId, title, elements },
        context: ctx,
      };
      // A refused post (a DM that would not admit the sender) is on the step's output as `cardError`, not swallowed.
      const cardOutcome = runCardOutcomeOf(await post(cardInput));
      const resultDocumentId = typeof ctx.documentId === 'string' && ctx.documentId ? ctx.documentId : undefined;
      const result = resultDocumentId ? { kind: 'document' as const, documentId: resultDocumentId } : undefined;
      return { status: 'completed', output: { ...runCardOutputOf(cardOutcome), card: runCardMetadata(cardInput), title, elements, ...(result ? { result } : {}) } };
    },
  };
}

export function buildDiagramGenerateHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const h of [createDiagramApplyHandler(), createDiagramPublishHandler()]) m.set(h.kind, h);
  return m;
}
