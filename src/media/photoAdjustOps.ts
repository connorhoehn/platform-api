// The ops the photo assistant may propose, and the check every one of them
// passes before it leaves the platform (realtime-examples
// docs/design/media-photo-editor/PLAN.md §2, lane E3).
//
// A photo's edit is a list of ops (`PhotoEditOp` in ui-components
// `photo-editor/model.ts`, mirrored here). The assistant's model is told this
// set and these bounds, and its answer is checked against them HERE: an op of
// another kind, a value out of range, a field that is not a finite number, or
// a mask that is not one of the photo's refuses the WHOLE answer. Nothing is
// clamped or dropped quietly — a proposal that is "mostly" what the model said
// is not what the model said.

export const PHOTO_ADJUST_PIPELINE_ID = 'photo-adjust';

/** kind → the field that carries its value, its bounds and its neutral value. */
export const ASSISTANT_OP_BOUNDS = {
  exposure: { field: 'stops', min: -3, max: 3, neutral: 0 },
  contrast: { field: 'value', min: 0.5, max: 1.5, neutral: 1 },
  highlights: { field: 'amount', min: -100, max: 100, neutral: 0 },
  shadows: { field: 'amount', min: -100, max: 100, neutral: 0 },
  whites: { field: 'amount', min: -100, max: 100, neutral: 0 },
  blacks: { field: 'amount', min: -100, max: 100, neutral: 0 },
  saturation: { field: 'value', min: 0.5, max: 1.5, neutral: 1 },
  vibrance: { field: 'amount', min: -100, max: 100, neutral: 0 },
  temperature: { field: 'kelvin', min: 2500, max: 10000, neutral: 5500 },
  tint: { field: 'amount', min: -100, max: 100, neutral: 0 },
} as const;

export type AssistantOpKind = keyof typeof ASSISTANT_OP_BOUNDS;
export const ASSISTANT_OP_KINDS = Object.keys(ASSISTANT_OP_BOUNDS) as AssistantOpKind[];

/** One op, as the editor stores it: `{ kind, <field>: n, maskId? }`. */
export type PhotoEditOp = { kind: string; maskId?: string } & Record<string, unknown>;

export interface PhotoMaskRef {
  maskId: string;
  label: string;
  kind?: string;
  subjects?: number;
}

/** At most this many ops in one proposal — a panel's worth. */
export const MAX_PROPOSED_OPS = 12;
/** At most this many ops carried in from the photo's current edit. */
export const MAX_CURRENT_OPS = 64;
export const MAX_MASKS = 20;
export const MAX_INSTRUCTION = 2000;

export class PhotoOpRefusal extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'PhotoOpRefusal';
  }
}

const ID = /^[A-Za-z0-9_:.-]{1,80}$/;

/**
 * The model's ops, checked. Throws `PhotoOpRefusal` naming the first problem;
 * returns them normalised to `{ kind, <field>, maskId? }` (nothing else kept).
 */
export function validateAssistantOps(raw: unknown, masks: readonly PhotoMaskRef[]): PhotoEditOp[] {
  if (!Array.isArray(raw)) throw new PhotoOpRefusal('the answer has no ops list');
  if (raw.length > MAX_PROPOSED_OPS) throw new PhotoOpRefusal(`${raw.length} ops proposed; at most ${MAX_PROPOSED_OPS}`);
  const maskIds = new Set(masks.map((m) => m.maskId));
  const seen = new Set<string>();
  return raw.map((entry, i) => {
    if (!entry || typeof entry !== 'object') throw new PhotoOpRefusal(`op ${i + 1} is not an object`);
    const e = entry as Record<string, unknown>;
    const kind = e.kind;
    if (typeof kind !== 'string' || !(kind in ASSISTANT_OP_BOUNDS)) {
      throw new PhotoOpRefusal(`op ${i + 1} has kind ${JSON.stringify(kind)}, which the assistant may not propose (allowed: ${ASSISTANT_OP_KINDS.join(', ')})`);
    }
    const bounds = ASSISTANT_OP_BOUNDS[kind as AssistantOpKind];
    const value = e[bounds.field];
    if (typeof value !== 'number' || !Number.isFinite(value)) throw new PhotoOpRefusal(`op ${i + 1} (${kind}) needs a number in "${bounds.field}"`);
    if (value < bounds.min || value > bounds.max) throw new PhotoOpRefusal(`op ${i + 1} (${kind}) ${bounds.field} ${value} is outside ${bounds.min}..${bounds.max}`);
    const extra = Object.keys(e).filter((k) => k !== 'kind' && k !== bounds.field && k !== 'maskId');
    if (extra.length) throw new PhotoOpRefusal(`op ${i + 1} (${kind}) carries unknown fields: ${extra.join(', ')}`);
    let maskId: string | undefined;
    if (e.maskId !== undefined && e.maskId !== null && e.maskId !== '') {
      if (typeof e.maskId !== 'string' || !maskIds.has(e.maskId)) throw new PhotoOpRefusal(`op ${i + 1} (${kind}) names mask ${JSON.stringify(e.maskId)}, which is not one of this photo's masks`);
      maskId = e.maskId;
    }
    const identity = `${kind}:${maskId ?? ''}`;
    if (seen.has(identity)) throw new PhotoOpRefusal(`op ${i + 1} repeats ${kind}${maskId ? ` on ${maskId}` : ''}`);
    seen.add(identity);
    const rounded = Math.round(value * 100) / 100;
    return { kind, [bounds.field]: rounded, ...(maskId ? { maskId } : {}) } as PhotoEditOp;
  });
}

/** The trigger payload's masks: bounded, ids checked; anything else is a 400-shaped refusal. */
export function readMasks(raw: unknown): PhotoMaskRef[] {
  if (raw === undefined || raw === null) return [];
  if (!Array.isArray(raw)) throw new PhotoOpRefusal('masks must be a list');
  if (raw.length > MAX_MASKS) throw new PhotoOpRefusal(`at most ${MAX_MASKS} masks`);
  return raw.map((m, i) => {
    const e = (m ?? {}) as Record<string, unknown>;
    if (typeof e.maskId !== 'string' || !ID.test(e.maskId)) throw new PhotoOpRefusal(`mask ${i + 1} has no valid maskId`);
    const label = typeof e.label === 'string' ? e.label.slice(0, 80) : e.maskId;
    return {
      maskId: e.maskId,
      label,
      ...(typeof e.kind === 'string' ? { kind: e.kind.slice(0, 20) } : {}),
      ...(typeof e.subjects === 'number' && Number.isFinite(e.subjects) ? { subjects: e.subjects } : {}),
    };
  });
}

/** The photo's current ops, as context for the model: bounded, shallow-copied, never trusted as output. */
export function readCurrentOps(raw: unknown): PhotoEditOp[] {
  if (raw === undefined || raw === null) return [];
  if (!Array.isArray(raw)) throw new PhotoOpRefusal('currentOps must be a list');
  if (raw.length > MAX_CURRENT_OPS) throw new PhotoOpRefusal(`at most ${MAX_CURRENT_OPS} current ops`);
  return raw.filter((o) => o && typeof o === 'object' && typeof (o as { kind?: unknown }).kind === 'string').map((o) => {
    const out: PhotoEditOp = { kind: String((o as { kind: string }).kind).slice(0, 30) };
    for (const [k, v] of Object.entries(o as Record<string, unknown>)) {
      if (k === 'kind') continue;
      if (k === 'maskId' && typeof v === 'string') out.maskId = v.slice(0, 80);
      else if (typeof v === 'number' && Number.isFinite(v)) out[k] = v;
    }
    return out;
  });
}

/** `ops` with each of `changes` replacing the op of the same kind + mask, or appended. */
export function mergeOps(ops: readonly PhotoEditOp[], changes: readonly PhotoEditOp[]): PhotoEditOp[] {
  const out = [...ops];
  for (const op of changes) {
    const at = out.findIndex((o) => o.kind === op.kind && (o.maskId ?? '') === (op.maskId ?? ''));
    if (at >= 0) out[at] = op; else out.push(op);
  }
  return out;
}

export const PHOTO_ADJUST_SYSTEM_PROMPT = [
  'You adjust a photograph by proposing edit operations for a photo editor. You do not see the image; you get the',
  'photographer\'s instruction, the masks the photo has (named regions such as detected people), which masks the',
  'instruction references, and the photo\'s current operations.',
  '',
  'Answer with ONE JSON object and nothing else:',
  '{"ops":[{"kind":"<kind>","<field>":<number>,"maskId":"<optional mask id>"}],"summary":"<one short sentence>"}',
  '',
  'Allowed kinds, their one numeric field, its range and neutral value:',
  ...ASSISTANT_OP_KINDS.map((k) => {
    const b = ASSISTANT_OP_BOUNDS[k];
    return `- ${k}: "${b.field}" ${b.min}..${b.max} (neutral ${b.neutral})`;
  }),
  '',
  'Rules:',
  '- Only the kinds above. Never another kind, never another field, never a value outside its range.',
  '- Each op sets the ABSOLUTE value for that kind (not a delta); start from the current value when one exists.',
  '- When the instruction references masks, scope the ops it asks for on those subjects with "maskId"; ops for the',
  '  whole photo omit "maskId". Never use a maskId that is not listed.',
  '- Keep what the instruction says to keep: do not change those kinds.',
  `- At most ${MAX_PROPOSED_OPS} ops, one per kind per mask. Prefer a few moderate moves.`,
].join('\n');

export function buildPhotoAdjustPrompt(input: {
  name?: string;
  instruction: string;
  masks: readonly PhotoMaskRef[];
  referencedMaskIds: readonly string[];
  currentOps: readonly PhotoEditOp[];
}): string {
  const referenced = input.masks.filter((m) => input.referencedMaskIds.includes(m.maskId));
  return [
    `Photo: ${input.name ?? 'untitled'}`,
    `Instruction: ${input.instruction}`,
    `Masks in this photo: ${input.masks.length ? input.masks.map((m) => `${m.maskId} ("${m.label}"${m.kind ? `, ${m.kind}` : ''}${m.subjects ? `, ${m.subjects} subjects` : ''})`).join('; ') : 'none'}`,
    `Referenced masks: ${referenced.length ? referenced.map((m) => `${m.maskId} ("${m.label}")`).join('; ') : 'none — the whole photo'}`,
    `Current ops: ${input.currentOps.length ? JSON.stringify(input.currentOps) : 'none (the photo as shot)'}`,
  ].join('\n');
}
