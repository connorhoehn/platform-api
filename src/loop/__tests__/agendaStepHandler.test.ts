// The agenda step has to leave a real page behind, and say so honestly when
// only half of it landed.
jest.mock('../../lib/aws-clients', () => ({ docClient: { send: jest.fn(async () => ({})) }, ddbClient: { send: jest.fn() } }));
jest.mock('../../lib/ddb-table-name', () => ({ tableName: (name: string) => `test-${name}` }));

import { PutCommand } from '@aws-sdk/lib-dynamodb';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import {
  agendaOps, agendaText, createAgendaStepHandler, parseAgendaJson, validateAgenda,
} from '../agendaStepHandler';
import type { DocOp } from '../../documents/documentAgentOps';

const helpers = { runId: 'run-9', stepId: 'agenda', emit: async () => undefined } as unknown as StepHandlerHelpers;
const node = (data: Record<string, unknown> = {}): PipelineNode => ({
  id: 'agenda', type: 'agenda:write', position: { x: 0, y: 0 },
  data: { type: 'agenda:write', ...data },
} as unknown as PipelineNode);

const DRAFT = {
  title: 'Platform sync',
  items: [
    { text: 'Release risks', owner: 'Frank', minutes: 10 },
    { text: 'Coverage checks' },
  ],
  owners: ['Connor'],
};

function deps() {
  const puts: Array<Record<string, unknown>> = [];
  const written: Array<{ documentId: string; ops: DocOp[] }> = [];
  return {
    puts,
    written,
    opts: {
      documents: {
        send: async (command: unknown) => {
          if (command instanceof PutCommand) puts.push((command as unknown as { input: { Item: Record<string, unknown> } }).input.Item);
          return {};
        },
      },
      writeBody: async (documentId: string, _title: string, ops: DocOp[]) => {
        written.push({ documentId, ops });
        return ops.length;
      },
      newDocumentId: () => 'doc-agenda-1',
      now: () => new Date('2026-09-22T09:00:00.000Z'),
    },
  };
}

const context = { requestedBy: 'dev-connor', meetingId: 'room-1', startsAt: '2026-09-23T09:00:00.000Z' };

describe('the agenda step', () => {
  it('creates a page and writes the agenda into it', async () => {
    const { opts, puts, written } = deps();
    const outcome = await createAgendaStepHandler(opts).execute(
      node({ agendaFrom: '$.steps.draft.text' }),
      { ...context, steps: { draft: { text: `\`\`\`json\n${JSON.stringify(DRAFT)}\n\`\`\`` } } },
      helpers,
    );

    expect(outcome.status).toBe('completed');
    const output = outcome.output as Record<string, unknown>;
    expect(output).toMatchObject({
      documentId: 'doc-agenda-1', title: 'Platform sync', itemCount: 2, written: 'document',
    });
    // The same items just written to the page, so a run card can excerpt the
    // agenda without a second read of the document body.
    expect(output.items).toEqual([
      { text: 'Release risks', owner: 'Frank' },
      { text: 'Coverage checks' },
    ]);
    expect(output.owners).toEqual(['Connor', 'Frank']);
    expect(output.result).toEqual({ kind: 'document', documentId: 'doc-agenda-1' });
    expect(output.artifacts).toEqual([{ artifactId: 'doc-agenda-1', kind: 'page', title: 'Platform sync' }]);
    // The page the deck may cite.
    expect(output.agendaDocument).toEqual({ documentId: 'doc-agenda-1', title: 'Platform sync' });

    expect(puts[0]).toMatchObject({
      documentId: 'doc-agenda-1', type: 'page', createdBy: 'dev-connor', createdByRunId: 'run-9', meetingId: 'room-1',
    });
    expect(puts[0]!.outline).toEqual([
      { id: 'item-1', title: 'Release risks' },
      { id: 'item-2', title: 'Coverage checks' },
    ]);
    // The meeting it belongs to is the first line of the page.
    expect(written[0]!.ops[1]).toEqual({ op: 'appendBlock', kind: 'paragraph', text: 'For the meeting room-1 · proposed 2026-09-23T09:00:00.000Z' });
  });

  it('takes an inline agenda, which is how the fixture loop runs with no model', async () => {
    const { opts, written } = deps();
    const outcome = await createAgendaStepHandler(opts).execute(node({ agenda: DRAFT }), context, helpers);
    expect(outcome.status).toBe('completed');
    expect(written).toHaveLength(1);
  });

  it('keeps the page and says the body did not land', async () => {
    const { opts, puts } = deps();
    const failing = { ...opts, writeBody: async () => { throw new Error('no snapshot arrived'); } };
    const outcome = await createAgendaStepHandler(failing).execute(node({ agenda: DRAFT }), context, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.written).toBe('row-only');
    expect((output.notices as string[])[0]).toContain('no snapshot arrived');
    // The agenda is still readable on the row it did create.
    expect(String(puts[0]!.agendaText)).toContain('Release risks');
  });

  it('never writes into the document the run was already carrying', async () => {
    // The mirror of the deck's guard: an agenda page is always its own new
    // document, so it cannot land inside a presentation the run is building.
    const { opts, puts } = deps();
    const outcome = await createAgendaStepHandler(opts).execute(
      node({ agenda: DRAFT }),
      { ...context, documentId: 'doc-presentation-1' },
      helpers,
    );
    expect((outcome.output as Record<string, unknown>).documentId).toBe('doc-agenda-1');
    expect(puts[0]!.documentId).toBe('doc-agenda-1');
  });

  it('fails rather than writing a page under the name of an agenda with nothing in it', async () => {
    const { opts, puts } = deps();
    const outcome = await createAgendaStepHandler(opts).execute(node({ agenda: { title: 'Empty', items: [] } as never }), context, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('at least one item');
    expect(puts).toHaveLength(0);
  });

  it('fails when no draft reached it', async () => {
    const { opts } = deps();
    const outcome = await createAgendaStepHandler(opts).execute(node({ agendaFrom: '$.steps.draft.text' }), { steps: {} }, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('no drafted agenda found');
  });
});

describe('the agenda itself', () => {
  it('reads a draft out of fenced JSON', () => {
    expect(parseAgendaJson('here:\n```json\n{"title":"T"}\n```')).toEqual({ title: 'T' });
    expect(parseAgendaJson('nothing here')).toBeUndefined();
  });

  it('accepts plain strings as items and keeps the owners it was told', () => {
    const result = validateAgenda({ items: ['One', { text: 'Two', owner: 'Frank' }] }, 'Fallback');
    expect(result.ok).toBe(true);
    if (!result.ok) throw new Error('unreachable');
    expect(result.agenda.title).toBe('Fallback');
    expect(result.agenda.items).toEqual([{ text: 'One' }, { text: 'Two', owner: 'Frank' }]);
    expect(result.agenda.owners).toEqual(['Frank']);
  });

  it('becomes a heading, one bullet per item, and the owners', () => {
    const ops = agendaOps({ title: 'T', items: [{ text: 'One', owner: 'Frank', minutes: 5 }], owners: ['Frank'] });
    expect(ops[0]).toEqual({ op: 'setTitle', text: 'T' });
    expect(ops).toContainEqual({ op: 'appendBlock', kind: 'bulletList', text: 'One (owner: Frank · 5 min)' });
    expect(ops).toContainEqual({ op: 'appendBlock', kind: 'heading', level: 2, text: 'Owners' });
  });

  it('keeps a plain-text copy for a page whose body never landed', () => {
    expect(agendaText({ title: 'T', items: [{ text: 'One', owner: 'Frank' }], owners: [] }))
      .toBe('T\n- One (owner: Frank)');
  });
});
