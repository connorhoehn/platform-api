// `agenda:write` — the step behind a planned `kind: 'agenda'`.
//
// A drafting agent answers with an agenda as JSON; this turns it into a real
// page in the Documents explorer, through the same collaboration channel /doc
// and /summarize write through, and declares the page as the run's artifact so
// the graph shows the run and the page it produced as one fact. The deck step
// can then cite it like any other document.
//
// The document row is written first and the body second, because those fail
// separately: a gateway that will not take the write leaves a real, empty page
// and says `written: 'row-only'` rather than pretending the agenda exists.

import { randomUUID } from 'node:crypto';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode, StepHandlerHelpers, StepOutcome } from 'distributed-core/applications/pipeline';
import type { DescribedStepHandler } from '../pipeline/customStepHandlers';
import { docClient } from '../lib/aws-clients';
import log from '../lib/logger';
import { readPath } from '../pipeline/image/ScatterStepHandler';
import { AgentCollaborator, HAIKU_AGENT } from '../documents/AgentCollaborator';
import { applyOps, ensureCanvasMeta, type DocOp } from '../documents/documentAgentOps';
import { DOCUMENTS_TABLE } from '../deck/presentationDocuments';

export const AGENDA_STEP_KIND = 'agenda:write';
export const AGENDA_DOCUMENT_TYPE = 'page';

export const MAX_AGENDA_ITEMS = 20;
export const MAX_ITEM_CHARS = 200;

export interface AgendaItem {
  text: string;
  owner?: string;
  minutes?: number;
}

export interface Agenda {
  title: string;
  items: AgendaItem[];
  owners: string[];
}

export interface AgendaNodeData {
  type: typeof AGENDA_STEP_KIND;
  /** Where the drafted agenda is: a context path, or a `{{ … }}` template. */
  agendaFrom?: string;
  /** The agenda inline — how the fixture path runs with no model in the loop. */
  agenda?: Agenda | string;
  /** The page's title. Defaults to the agenda's own. */
  title?: string;
  label?: string;
}

const AGENDA_FALLBACK_PATHS = ['$.steps.draft.text', '$.agenda', '$.agendaDraft'];
const REQUESTER_PATHS = ['$.requestedBy', '$.trigger.requestedBy'];

function str(value: unknown, limit = 200): string | undefined {
  if (typeof value !== 'string') return undefined;
  const trimmed = value.replace(/\s+/g, ' ').trim();
  return trimmed ? trimmed.slice(0, limit) : undefined;
}

function firstOf(paths: readonly string[], context: Record<string, unknown>): string | undefined {
  for (const path of paths) {
    const value = str(readPath(context, path));
    if (value) return value;
  }
  return undefined;
}

/** The first JSON object in whatever the drafting agent said. */
export function parseAgendaJson(text: string): unknown {
  const fenced = /```(?:json)?\s*([\s\S]*?)```/i.exec(text);
  for (const candidate of [fenced?.[1], text].filter((value): value is string => typeof value === 'string')) {
    const start = candidate.indexOf('{');
    const end = candidate.lastIndexOf('}');
    if (start < 0 || end <= start) continue;
    try { return JSON.parse(candidate.slice(start, end + 1)); } catch { continue; }
  }
  return undefined;
}

/**
 * Validates an agenda. An agenda with no items is not an agenda — the step
 * fails rather than writing an empty page over the name of one.
 */
export function validateAgenda(input: unknown, fallbackTitle: string): { ok: true; agenda: Agenda } | { ok: false; error: string } {
  if (!input || typeof input !== 'object' || Array.isArray(input)) return { ok: false, error: 'expected an object' };
  const raw = input as { title?: unknown; items?: unknown; owners?: unknown };
  const title = str(raw.title, 120) ?? fallbackTitle;
  if (!Array.isArray(raw.items) || raw.items.length === 0) return { ok: false, error: 'an agenda needs at least one item' };
  const items: AgendaItem[] = [];
  for (const entry of raw.items.slice(0, MAX_AGENDA_ITEMS)) {
    const text = typeof entry === 'string'
      ? str(entry, MAX_ITEM_CHARS)
      : str((entry as { text?: unknown; item?: unknown })?.text ?? (entry as { item?: unknown })?.item, MAX_ITEM_CHARS);
    if (!text) continue;
    const owner = typeof entry === 'object' ? str((entry as { owner?: unknown }).owner, 80) : undefined;
    const minutesRaw = typeof entry === 'object' ? (entry as { minutes?: unknown }).minutes : undefined;
    const minutes = typeof minutesRaw === 'number' && Number.isFinite(minutesRaw) && minutesRaw > 0 ? Math.round(minutesRaw) : undefined;
    items.push({ text, ...(owner ? { owner } : {}), ...(minutes ? { minutes } : {}) });
  }
  if (!items.length) return { ok: false, error: 'no usable item survived validation' };
  const declared = Array.isArray(raw.owners)
    ? raw.owners.map((owner) => str(owner, 80)).filter((owner): owner is string => !!owner)
    : [];
  const owners = [...new Set([...declared, ...items.map((item) => item.owner).filter((owner): owner is string => !!owner)])].slice(0, 20);
  return { ok: true, agenda: { title, items, owners } };
}

/** The agenda as document operations: a heading, then one bullet per item. */
export function agendaOps(agenda: Agenda, meetingLine?: string): DocOp[] {
  const ops: DocOp[] = [{ op: 'setTitle', text: agenda.title }];
  if (meetingLine) ops.push({ op: 'appendBlock', kind: 'paragraph', text: meetingLine });
  ops.push({ op: 'appendBlock', kind: 'heading', level: 2, text: 'Agenda' });
  for (const item of agenda.items) {
    const suffix = [item.owner ? `owner: ${item.owner}` : undefined, item.minutes ? `${item.minutes} min` : undefined]
      .filter(Boolean).join(' · ');
    ops.push({ op: 'appendBlock', kind: 'bulletList', text: suffix ? `${item.text} (${suffix})` : item.text });
  }
  if (agenda.owners.length) {
    ops.push({ op: 'appendBlock', kind: 'heading', level: 2, text: 'Owners' });
    for (const owner of agenda.owners) ops.push({ op: 'appendBlock', kind: 'bulletList', text: owner });
  }
  return ops;
}

/** The plain-text agenda kept on the row, so a page nobody could write still says what it was. */
export function agendaText(agenda: Agenda): string {
  return [
    agenda.title,
    ...agenda.items.map((item) => `- ${item.text}${item.owner ? ` (owner: ${item.owner})` : ''}`),
  ].join('\n');
}

export interface AgendaStepHandlerOpts {
  /** Overridden in tests; defaults to the shared DDB document client. */
  documents?: { send: (command: unknown) => Promise<unknown> };
  /** Overridden in tests; defaults to writing through the collaboration channel. */
  writeBody?: (documentId: string, title: string, ops: DocOp[], requestedBy: string) => Promise<number>;
  newDocumentId?: () => string;
  now?: () => Date;
}

/** The real write: join the document as the agent, apply the ops, leave. */
async function writeThroughGateway(documentId: string, title: string, ops: DocOp[], requestedBy: string): Promise<number> {
  const agent = new AgentCollaborator({ documentId, identity: HAIKU_AGENT });
  try {
    await agent.join();
    agent.announce();
    ensureCanvasMeta(agent.doc, { id: documentId, title, createdBy: requestedBy });
    const result = applyOps(agent.doc, ops, 'agent');
    void agent.leave(Number(process.env.DOCUMENT_AGENT_PRESENCE_HOLD_MS) || 2000).catch(() => undefined);
    return result.applied;
  } catch (err) {
    await agent.leave(0).catch(() => undefined);
    throw err;
  }
}

export function createAgendaStepHandler(opts: AgendaStepHandlerOpts = {}): DescribedStepHandler {
  const documents = opts.documents ?? (docClient as unknown as { send: (command: unknown) => Promise<unknown> });
  const writeBody = opts.writeBody ?? writeThroughGateway;
  const newDocumentId = opts.newDocumentId ?? (() => randomUUID());
  const now = opts.now ?? (() => new Date());

  return {
    kind: AGENDA_STEP_KIND,
    description:
      'Turns a drafted agenda into a real page in the Documents explorer, and declares that page as the run\'s artifact so the deck can cite it.',
    reads: 'The drafted agenda JSON at the node\'s agendaFrom (or inline), and the meeting the run created, when there is one.',
    emits: 'documentId, title, the item count and the owners — and whether the body reached the document or only the row.',
    async execute(node: PipelineNode, context: Record<string, unknown>, helpers: StepHandlerHelpers): Promise<StepOutcome> {
      const data = (node.data ?? {}) as unknown as AgendaNodeData;
      const fallbackTitle = str(data.title, 120) ?? str(data.label, 120) ?? str(context.instruction, 120) ?? 'Agenda';

      const raw = data.agenda
        ?? (data.agendaFrom ? readPath(context, data.agendaFrom) : undefined)
        ?? AGENDA_FALLBACK_PATHS.map((path) => readPath(context, path)).find((value) => !!value);
      const parsed = typeof raw === 'string' ? parseAgendaJson(raw) : raw;
      if (parsed === undefined) {
        return { status: 'failed', error: 'agenda: no drafted agenda found — point the node\'s agendaFrom at the step that drafts it' };
      }
      const validation = validateAgenda(parsed, fallbackTitle);
      if (!validation.ok) return { status: 'failed', error: `agenda: ${validation.error}` };
      const agenda = validation.agenda;

      const documentId = newDocumentId();
      const requestedBy = firstOf(REQUESTER_PATHS, context) ?? 'agent:loop';
      const at = now().toISOString();
      const meetingId = str(readPath(context, '$.meetingId'));
      const startsAt = str(readPath(context, '$.startsAt'));
      const meetingLine = meetingId
        ? `For the meeting ${meetingId}${startsAt ? ` · proposed ${startsAt}` : ''}`
        : undefined;

      try {
        await documents.send(new PutCommand({
          TableName: DOCUMENTS_TABLE,
          Item: {
            documentId,
            title: agenda.title,
            type: AGENDA_DOCUMENT_TYPE,
            status: 'draft',
            createdBy: requestedBy,
            createdAt: at,
            updatedAt: at,
            // Provenance on the row itself: this page is a run's output.
            createdByRunId: helpers.runId,
            ...(meetingId ? { meetingId } : {}),
            ...(startsAt ? { meetingStartsAt: startsAt } : {}),
            agendaText: agendaText(agenda),
            outline: agenda.items.map((item, index) => ({ id: `item-${index + 1}`, title: item.text })),
          },
        }));
      } catch (err) {
        return { status: 'failed', error: `agenda: the page could not be created: ${err instanceof Error ? err.message : String(err)}` };
      }

      let applied = 0;
      let written: 'document' | 'row-only' = 'document';
      let notice: string | undefined;
      try {
        applied = await writeBody(documentId, agenda.title, agendaOps(agenda, meetingLine), requestedBy);
      } catch (err) {
        written = 'row-only';
        notice = `The page exists but its body could not be written: ${err instanceof Error ? err.message : String(err)}`;
        log.warn({ documentId, err: String(err) }, 'agenda: the body could not be written through the collaboration channel');
      }

      return {
        status: 'completed',
        output: {
          documentId,
          title: agenda.title,
          itemCount: agenda.items.length,
          owners: agenda.owners,
          written,
          applied,
          ...(notice ? { notices: [notice] } : {}),
          // The page the deck may cite, named the way read-sources reads it.
          agendaDocument: { documentId, title: agenda.title },
          artifacts: [{ artifactId: documentId, kind: 'page', title: agenda.title }],
          result: { kind: 'document', documentId },
        },
      };
    },
  };
}
