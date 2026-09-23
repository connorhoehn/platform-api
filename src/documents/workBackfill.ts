// platform-api/src/documents/workBackfill.ts
//
// Work fields on a document (status, points, owner, priority, rank, outcome,
// acceptance criteria, dependency/decision links) — the pure half. No I/O:
// the repository stores rows, the route authorizes and signals, and this file
// decides what a PATCH means, whether it conflicts, and what a read shows.
//
// Two rules carry the design (docs/design/documents-detail/PLAN.md §2.1):
//
//   * PER-FIELD CONFLICTS. Every applied write bumps `revision`, and stamps
//     the new revision on each field it changed (`fieldRevisions`, and `rev`
//     on a criterion or link). A PATCH names the revision it was made from
//     (`expectedRevision`). It conflicts only when a field it would CHANGE was
//     changed after that revision — so two people editing different fields
//     never conflict, and a replayed PATCH (which changes nothing) never does.
//
//   * BACKFILL ON READ, NOTHING WRITTEN. A field nobody has written yet
//     (no `fieldRevisions` entry) shows the lifecycle row's value: the owner,
//     and a status mapped from the lifecycle status. Once someone writes the
//     field, the work row is the truth and lifecycle no longer feeds it.

export const WORK_STATUSES = ['next', 'in_progress', 'in_review', 'done'] as const;
export type WorkStatus = (typeof WORK_STATUSES)[number];
export const WORK_PRIORITIES = ['low', 'medium', 'high', 'urgent'] as const;
export type WorkPriority = (typeof WORK_PRIORITIES)[number];
export const LINK_RELATIONS = ['depends-on', 'decided-by'] as const;
export type WorkLinkRelation = (typeof LINK_RELATIONS)[number];

/** The scalar fields a PATCH sets with `set`. `null` clears one. */
export const WORK_SCALAR_FIELDS = ['status', 'points', 'ownerId', 'priority', 'rank', 'outcome', 'scopeId'] as const;
export type WorkScalarField = (typeof WORK_SCALAR_FIELDS)[number];

export interface WorkCriterion {
  text: string;
  done: boolean;
  /** Fractional index within the list. */
  rank: string;
  doneBy?: string;
  doneAt?: string;
  /** The revision that last changed this criterion. */
  rev: number;
}

export interface WorkLink {
  documentId: string;
  relation: WorkLinkRelation;
  rev: number;
}

/** The stored row (table `document-work`, PK `documentId`). */
export interface DocumentWork {
  documentId: string;
  organizationId?: string;
  status?: WorkStatus;
  points?: number;
  ownerId?: string;
  priority?: WorkPriority;
  rank?: string;
  outcome?: string;
  /** The Work column this row lists under (the gateway parent, or `type:<type>`). GSI `scope-index`. */
  scopeId?: string;
  criteria: Record<string, WorkCriterion>;
  links: Record<string, WorkLink>;
  fieldRevisions: Partial<Record<WorkScalarField, number>>;
  /** CAS version: +1 per applied write; 0 = no row. */
  revision: number;
  createdAt?: string;
  updatedAt?: string;
  updatedBy?: string;
}

/** What GET answers: the row plus the backfilled fields, and where each came from. */
export interface DocumentWorkView extends DocumentWork {
  /** false = no row yet (every value shown is backfilled or absent). */
  tracked: boolean;
  /** Fields whose value came from the lifecycle row, not the work row. */
  backfilled: Array<'status' | 'ownerId'>;
}

/** The lifecycle facts the backfill reads. */
export interface LifecycleFacts {
  status?: string;
  ownerId?: string;
}

/** Lifecycle status → work status, for a work row that has never set its own. */
export function workStatusFromLifecycle(status: string | undefined): WorkStatus | undefined {
  switch (status) {
    case 'in_review':
    case 'changes_requested':
      return 'in_review';
    case 'approved':
    case 'final':
      return 'done';
    default:
      return undefined;
  }
}

export function emptyWork(documentId: string): DocumentWork {
  return { documentId, criteria: {}, links: {}, fieldRevisions: {}, revision: 0 };
}

/** The read: never writes, never mutates `row`. */
export function backfillWork(
  documentId: string,
  row: DocumentWork | null | undefined,
  lifecycle: LifecycleFacts | null | undefined,
): DocumentWorkView {
  const base: DocumentWork = row
    ? { ...row, criteria: { ...(row.criteria ?? {}) }, links: { ...(row.links ?? {}) }, fieldRevisions: { ...(row.fieldRevisions ?? {}) } }
    : emptyWork(documentId);
  const backfilled: DocumentWorkView['backfilled'] = [];
  if (base.fieldRevisions.status === undefined && base.status === undefined) {
    const mapped = workStatusFromLifecycle(lifecycle?.status);
    if (mapped) { base.status = mapped; backfilled.push('status'); }
  }
  if (base.fieldRevisions.ownerId === undefined && base.ownerId === undefined && lifecycle?.ownerId) {
    base.ownerId = lifecycle.ownerId;
    backfilled.push('ownerId');
  }
  return { ...base, tracked: !!row, backfilled };
}

// ---------------------------------------------------------------------------
// PATCH
// ---------------------------------------------------------------------------

export type CriteriaOp =
  | { op: 'add'; id: string; text: string; rank?: string }
  | { op: 'check'; id: string; done: boolean }
  | { op: 'edit'; id: string; text: string }
  | { op: 'move'; id: string; rank: string }
  | { op: 'remove'; id: string };

export type LinkOp =
  | { op: 'add'; id: string; documentId: string; relation: WorkLinkRelation }
  | { op: 'edit'; id: string; relation: WorkLinkRelation }
  | { op: 'remove'; id: string };

export interface WorkPatch {
  expectedRevision: number;
  set: Partial<Record<WorkScalarField, unknown>>;
  criteria: CriteriaOp[];
  links: LinkOp[];
}

export class WorkPatchError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WorkPatchError';
  }
}

export const WORK_LIMITS = {
  outcomeChars: 2000,
  criterionChars: 500,
  criteria: 50,
  links: 50,
  opsPerPatch: 50,
  idChars: 64,
  rankChars: 64,
  scopeChars: 200,
  userIdChars: 128,
} as const;

const CLIENT_ID = /^[A-Za-z0-9_-]{1,64}$/;
const RANK = /^[0-9A-Za-z]{1,64}$/;
const DOCUMENT_ID = /^[A-Za-z0-9_.:-]{1,200}$/;
/** An explorer type folder (`type:page`). The gateway's doc-work service uses the same pattern. */
export const TYPE_SCOPE = /^type:[A-Za-z0-9_-]{1,64}$/;
/** A parent-document scope: a document id with no ':' — so no scope can spell another org's type channel. */
const PARENT_SCOPE = /^[A-Za-z0-9_.-]{1,200}$/;

/**
 * The gateway channel a scope's signals go to. A parent-document scope is the
 * document's own id (globally unique; the gateway checks `read` on it). A
 * `type:<type>` scope exists in every organisation, so its channel carries
 * the org — `doc-work-scope:<orgId>:type:<type>` — and a row with no org
 * publishes no type-scope signal at all: never one channel across orgs.
 */
export function workScopeChannel(scopeId: string, organizationId: string | undefined): string | null {
  if (TYPE_SCOPE.test(scopeId)) return organizationId && PARENT_SCOPE.test(organizationId) ? `doc-work-scope:${organizationId}:${scopeId}` : null;
  return PARENT_SCOPE.test(scopeId) ? `doc-work-scope:${scopeId}` : null;
}

function req(cond: boolean, message: string): void {
  if (!cond) throw new WorkPatchError(message);
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return !!v && typeof v === 'object' && !Array.isArray(v);
}

/** Validates a PATCH body off the wire. Throws WorkPatchError (→ 400). */
export function parseWorkPatch(raw: unknown): WorkPatch {
  req(isRecord(raw), 'body must be an object');
  const body = raw as Record<string, unknown>;
  const expectedRevision = body.expectedRevision;
  req(typeof expectedRevision === 'number' && Number.isInteger(expectedRevision) && expectedRevision >= 0,
    'expectedRevision (the revision this edit was made from; 0 for an untracked document) is required');

  const set: WorkPatch['set'] = {};
  if (body.set !== undefined) {
    req(isRecord(body.set), 'set must be an object');
    for (const [key, value] of Object.entries(body.set as Record<string, unknown>)) {
      req((WORK_SCALAR_FIELDS as readonly string[]).includes(key), `set.${key} is not a work field`);
      if (value !== null) validateScalar(key as WorkScalarField, value);
      set[key as WorkScalarField] = value;
    }
  }
  const criteria = parseOps(body.criteria, 'criteria').map(parseCriteriaOp);
  const links = parseOps(body.links, 'links').map(parseLinkOp);
  req(Object.keys(set).length + criteria.length + links.length > 0, 'nothing to change: send set, criteria or links');
  req(criteria.length + links.length <= WORK_LIMITS.opsPerPatch, `at most ${WORK_LIMITS.opsPerPatch} operations per PATCH`);
  return { expectedRevision: expectedRevision as number, set, criteria, links };
}

function validateScalar(field: WorkScalarField, value: unknown): void {
  switch (field) {
    case 'status':
      req((WORK_STATUSES as readonly unknown[]).includes(value), `status must be one of ${WORK_STATUSES.join(', ')} (null = not planned)`);
      return;
    case 'priority':
      req((WORK_PRIORITIES as readonly unknown[]).includes(value), `priority must be one of ${WORK_PRIORITIES.join(', ')}`);
      return;
    case 'points':
      req(typeof value === 'number' && Number.isInteger(value) && value >= 0 && value <= 100, 'points must be an integer 0..100');
      return;
    case 'ownerId':
      req(typeof value === 'string' && value.length > 0 && value.length <= WORK_LIMITS.userIdChars, 'ownerId must be a user id');
      return;
    case 'rank':
      req(typeof value === 'string' && RANK.test(value), 'rank must be a fractional index (letters and digits, at most 64)');
      return;
    case 'outcome':
      req(typeof value === 'string' && value.length <= WORK_LIMITS.outcomeChars, `outcome must be text of at most ${WORK_LIMITS.outcomeChars} characters`);
      return;
    case 'scopeId':
      req(typeof value === 'string' && (TYPE_SCOPE.test(value) || PARENT_SCOPE.test(value)), 'scopeId must be a parent document id or type:<type>');
      return;
  }
}

function parseOps(value: unknown, name: string): Record<string, unknown>[] {
  if (value === undefined) return [];
  req(Array.isArray(value), `${name} must be an array of operations`);
  return (value as unknown[]).map((op, i) => {
    req(isRecord(op), `${name}[${i}] must be an object`);
    req(typeof (op as Record<string, unknown>).id === 'string' && CLIENT_ID.test((op as Record<string, string>).id),
      `${name}[${i}].id must be a client-made id (letters, digits, - and _, at most 64)`);
    return op as Record<string, unknown>;
  });
}

function parseCriteriaOp(op: Record<string, unknown>): CriteriaOp {
  const id = op.id as string;
  const text = (t: unknown): string => {
    req(typeof t === 'string' && t.trim().length > 0 && t.trim().length <= WORK_LIMITS.criterionChars,
      `criterion text is required (at most ${WORK_LIMITS.criterionChars} characters)`);
    return (t as string).trim();
  };
  switch (op.op) {
    case 'add':
      if (op.rank !== undefined) req(typeof op.rank === 'string' && RANK.test(op.rank), 'criteria add rank must be a fractional index');
      return { op: 'add', id, text: text(op.text), ...(typeof op.rank === 'string' ? { rank: op.rank } : {}) };
    case 'check':
      req(typeof op.done === 'boolean', 'criteria check needs done: boolean');
      return { op: 'check', id, done: op.done as boolean };
    case 'edit':
      return { op: 'edit', id, text: text(op.text) };
    case 'move':
      req(typeof op.rank === 'string' && RANK.test(op.rank), 'criteria move needs rank');
      return { op: 'move', id, rank: op.rank as string };
    case 'remove':
      return { op: 'remove', id };
    default:
      throw new WorkPatchError('criteria op must be add, check, edit, move or remove');
  }
}

function parseLinkOp(op: Record<string, unknown>): LinkOp {
  const id = op.id as string;
  switch (op.op) {
    case 'add':
      req(typeof op.documentId === 'string' && DOCUMENT_ID.test(op.documentId), 'links add needs documentId');
      req((LINK_RELATIONS as readonly unknown[]).includes(op.relation), `links add relation must be ${LINK_RELATIONS.join(' or ')}`);
      return { op: 'add', id, documentId: op.documentId as string, relation: op.relation as WorkLinkRelation };
    case 'edit':
      req((LINK_RELATIONS as readonly unknown[]).includes(op.relation), `links edit relation must be ${LINK_RELATIONS.join(' or ')}`);
      return { op: 'edit', id, relation: op.relation as WorkLinkRelation };
    case 'remove':
      return { op: 'remove', id };
    default:
      throw new WorkPatchError('links op must be add, edit or remove');
  }
}

export interface WorkPatchResult {
  /** The row to write (revision already bumped). Equal to the input row when nothing changed. */
  next: DocumentWork;
  /** Names of what changed: scalar field names, plus 'criteria' / 'links'. Empty = a replay; write nothing. */
  changed: string[];
  /** Fields this PATCH would change that were changed after `expectedRevision`: `status`, `criteria:<id>`, `links:<id>`. */
  conflicts: string[];
}

/**
 * Apply `patch` to `current` (null = no row yet). Pure. When `conflicts` is
 * non-empty the caller answers 409 and writes nothing; when `changed` is
 * empty the PATCH is a replay and the caller writes nothing either.
 */
export function applyWorkPatch(
  documentId: string,
  current: DocumentWork | null | undefined,
  patch: WorkPatch,
  ctx: { userId: string; now: string; organizationId?: string },
): WorkPatchResult {
  const base = current ?? emptyWork(documentId);
  const nextRevision = base.revision + 1;
  const next: DocumentWork = {
    ...base,
    criteria: Object.fromEntries(Object.entries(base.criteria ?? {}).map(([k, v]) => [k, { ...v }])),
    links: Object.fromEntries(Object.entries(base.links ?? {}).map(([k, v]) => [k, { ...v }])),
    fieldRevisions: { ...(base.fieldRevisions ?? {}) },
  };
  const changed = new Set<string>();
  const conflicts: string[] = [];
  const newer = (rev: number | undefined): boolean => (rev ?? 0) > patch.expectedRevision;

  for (const [key, value] of Object.entries(patch.set)) {
    const field = key as WorkScalarField;
    const cur = (base as unknown as Record<string, unknown>)[field];
    const same = value === null ? cur === undefined : cur === value;
    if (same) continue;
    if (newer(base.fieldRevisions?.[field])) { conflicts.push(field); continue; }
    if (value === null) delete (next as unknown as Record<string, unknown>)[field];
    else (next as unknown as Record<string, unknown>)[field] = value;
    next.fieldRevisions[field] = nextRevision;
    changed.add(field);
  }

  for (const op of patch.criteria) {
    const cur = next.criteria[op.id];
    if (op.op === 'add') {
      if (cur) continue; // replayed add: the id already exists
      if (Object.keys(next.criteria).length >= WORK_LIMITS.criteria) throw new WorkPatchError(`at most ${WORK_LIMITS.criteria} criteria`);
      next.criteria[op.id] = { text: op.text, done: false, rank: op.rank ?? defaultRank(next.criteria), rev: nextRevision };
      changed.add('criteria');
      continue;
    }
    if (!cur) continue; // edit/check/move/remove of a criterion that is gone: nothing to do
    const wouldChange = op.op === 'remove'
      || (op.op === 'check' && cur.done !== op.done)
      || (op.op === 'edit' && cur.text !== op.text)
      || (op.op === 'move' && cur.rank !== op.rank);
    if (!wouldChange) continue;
    if (newer(base.criteria?.[op.id]?.rev)) { conflicts.push(`criteria:${op.id}`); continue; }
    if (op.op === 'remove') delete next.criteria[op.id];
    else if (op.op === 'check') {
      cur.done = op.done;
      if (op.done) { cur.doneBy = ctx.userId; cur.doneAt = ctx.now; } else { delete cur.doneBy; delete cur.doneAt; }
      cur.rev = nextRevision;
    } else if (op.op === 'edit') { cur.text = op.text; cur.rev = nextRevision; }
    else { cur.rank = op.rank; cur.rev = nextRevision; }
    changed.add('criteria');
  }

  for (const op of patch.links) {
    const cur = next.links[op.id];
    if (op.op === 'add') {
      if (cur) continue;
      if (op.documentId === documentId) throw new WorkPatchError('a document cannot link to itself');
      if (Object.keys(next.links).length >= WORK_LIMITS.links) throw new WorkPatchError(`at most ${WORK_LIMITS.links} links`);
      next.links[op.id] = { documentId: op.documentId, relation: op.relation, rev: nextRevision };
      changed.add('links');
      continue;
    }
    if (!cur) continue;
    if (op.op === 'edit' && cur.relation === op.relation) continue;
    if (newer(base.links?.[op.id]?.rev)) { conflicts.push(`links:${op.id}`); continue; }
    if (op.op === 'edit') { cur.relation = op.relation; cur.rev = nextRevision; }
    else delete next.links[op.id];
    changed.add('links');
  }

  if (conflicts.length > 0 || changed.size === 0) {
    return { next: base, changed: conflicts.length > 0 ? [] : [], conflicts };
  }
  next.revision = nextRevision;
  next.updatedAt = ctx.now;
  next.updatedBy = ctx.userId;
  if (!current) {
    next.createdAt = ctx.now;
    if (ctx.organizationId) next.organizationId = ctx.organizationId;
  }
  return { next, changed: [...changed], conflicts };
}

/** A rank after every existing one, for an `add` that did not bring its own. */
function defaultRank(criteria: Record<string, WorkCriterion>): string {
  const ranks = Object.values(criteria).map((c) => c.rank).sort();
  const last = ranks[ranks.length - 1];
  return last ? `${last}V` : 'a0';
}

// ---------------------------------------------------------------------------
// Header rollup for a scope
// ---------------------------------------------------------------------------

export interface WorkRollup {
  pointsDone: number;
  pointsTotal: number;
  /** `decided-by` links across the scope. */
  decisions: number;
  /** Run drafts in the scope that are draft, dispatching or dispatched. */
  agents: number;
  /** Summed recorded cost of the scope's dispatched runs; absent when no run recorded one. */
  spentUsd?: number;
}

export function rollupWork(rows: DocumentWorkView[], agents: number, spentUsd?: number): WorkRollup {
  let pointsDone = 0;
  let pointsTotal = 0;
  let decisions = 0;
  for (const row of rows) {
    const points = typeof row.points === 'number' ? row.points : 0;
    pointsTotal += points;
    if (row.status === 'done') pointsDone += points;
    decisions += Object.values(row.links ?? {}).filter((l) => l.relation === 'decided-by').length;
  }
  return { pointsDone, pointsTotal, decisions, agents, ...(typeof spentUsd === 'number' ? { spentUsd } : {}) };
}
