import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDeckReadSourcesHandler, parseSourceRefs, sourcesBlock } from '../DeckSourcesStepHandler';

const node = { id: 'read-sources', type: 'deck:read-sources', position: { x: 0, y: 0 }, data: { type: 'deck:read-sources' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1', stepId: 'read-sources', emit: async () => undefined } as unknown as StepHandlerHelpers;

describe('the deck:read-sources step', () => {
  it('reads each referenced document into the prompt block', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [],
      readDocument: async (documentId) => ({ text: `the text of ${documentId}` }),
    });
    const outcome = await handler.execute(node, {
      requestedBy: 'dev-connor', sources: [{ documentId: 'doc-a', title: 'Doc A' }, { id: 'doc-b', name: 'Doc B' }],
      conversationText: '[09:15] Frank: we should cut scope',
    }, helpers);

    expect(outcome.status).toBe('completed');
    const output = outcome.output as Record<string, unknown>;
    expect(output.sourcesRead).toBe(2);
    expect(output.sourceCount).toBe(3);
    expect(output.sourcesBlock).toContain('documentId: doc-a');
    expect(output.sourcesBlock).toContain('the text of doc-b');
    expect(output.sourcesBlock).toContain('we should cut scope');
    expect(output.sourceList).toContain('- Doc A (documentId: doc-a)');
  });

  it('says a document could not be read instead of dropping it', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [],
      readDocument: async () => ({ error: 'no snapshot arrived' }),
    });
    const outcome = await handler.execute(node, { requestedBy: 'dev-connor', sources: [{ documentId: 'doc-a', title: 'Doc A' }] }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.sourcesUnavailable).toBe(1);
    expect(output.sourcesBlock).toContain('could not be read: no snapshot arrived');
  });

  it('fails when there is nothing to ground the deck in', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [], readDocument: async () => ({ text: '' }) });
    const outcome = await handler.execute(node, {}, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('no sources');
  });
});

describe('the deck:read-sources step, with a recording', () => {
  const material = {
    transcript: {
      transcriptId: 'active-now-transcript',
      title: 'Planning sync',
      segments: [
        { speaker: 'Connor', text: 'Release risks before noon.' },
        { speaker: 'Frank', text: 'I will take the rollback owner.' },
      ],
    },
  };

  it('prefers the named recording over the chat text, and cites it by its own id', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [],
      readDocument: async () => ({ text: '' }),
      readMaterial: async () => material,
    });
    const outcome = await handler.execute(node, {
      recordingId: 'rec-1',
      conversationText: '[09:15] Frank: unrelated chatter',
    }, helpers);

    const output = outcome.output as Record<string, unknown>;
    expect(output.transcriptFrom).toBe('recording');
    expect(output.transcriptSource).toEqual({ documentId: 'active-now-transcript', title: 'Planning sync' });
    expect(output.sourcesBlock).toContain('Connor: Release risks before noon.');
    expect(output.sourcesBlock).not.toContain('unrelated chatter');
    expect(output.sourceList).toContain('- Planning sync (documentId: active-now-transcript)');
  });

  it('falls back to the conversation when the recording has no transcript', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [],
      readDocument: async () => ({ text: '' }),
      readMaterial: async () => ({}),
    });
    const outcome = await handler.execute(node, { recordingId: 'rec-1', conversationText: '[09:15] Frank: chatter' }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.transcriptFrom).toBe('conversation');
    expect(output.sourcesBlock).toContain('chatter');
  });
});

describe('a deck cites the page this run wrote', () => {
  it('reads the agenda an earlier step produced as a source', async () => {
    const handler = createDeckReadSourcesHandler({ unreadableFor: async () => [],
      readDocument: async (documentId) => ({ text: `the text of ${documentId}` }),
      readMaterial: async () => ({}),
    });
    const outcome = await handler.execute(node, {
      requestedBy: 'dev-connor', sources: [{ documentId: 'doc-a', title: 'Doc A' }],
      agendaDocument: { documentId: 'doc-agenda-1', title: 'Platform sync' },
    }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.sourceList).toContain('- Platform sync (documentId: doc-agenda-1)');
    expect(output.sourcesBlock).toContain('the text of doc-agenda-1');
  });
});

describe('parseSourceRefs', () => {
  it('takes ids and titles under either name, deduped and capped', () => {
    expect(parseSourceRefs([
      { documentId: 'a', title: 'A' },
      { id: 'a', title: 'A again' },
      { id: 'b' },
      null,
    ])).toEqual([{ documentId: 'a', title: 'A' }, { documentId: 'b', title: 'b' }]);
    expect(parseSourceRefs(undefined)).toEqual([]);
  });
});

describe('sourcesBlock', () => {
  it('marks an empty document as empty, not as missing', () => {
    expect(sourcesBlock([{ documentId: 'a', title: 'A', text: '  ' }])).toContain('(this document is empty)');
  });

  it('says so when there is nothing at all', () => {
    expect(sourcesBlock([])).toBe('(no sources were provided)');
  });
});

describe('the deck:read-sources step checks the requester (runSourceAccess)', () => {
  it('a named source the requester may not read fails the step and reads nothing', async () => {
    const readDocument = jest.fn(async () => ({ text: 'secret' }));
    const handler = createDeckReadSourcesHandler({ readDocument, unreadableFor: async (_user, ids) => ids.filter((id) => id === 'doc-secret') });
    const outcome = await handler.execute(node, { requestedBy: 'dev-connor', sources: [{ documentId: 'doc-a', title: 'A' }, { documentId: 'doc-secret', title: 'S' }] }, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('doc-secret');
    expect((outcome as { retry?: boolean }).retry).toBe(false);
    expect(readDocument).not.toHaveBeenCalled();
  });

  it('named sources with no requester fail; the check sees the requester', async () => {
    const unreadableFor = jest.fn(async () => [] as string[]);
    const handler = createDeckReadSourcesHandler({ readDocument: async () => ({ text: 'x' }), unreadableFor });
    expect((await handler.execute(node, { sources: [{ documentId: 'doc-a', title: 'A' }] }, helpers)).status).toBe('failed');
    await handler.execute(node, { requestedBy: 'dev-frank', organizationId: 'dev-org', sources: [{ documentId: 'doc-a', title: 'A' }] }, helpers);
    expect(unreadableFor).toHaveBeenCalledWith({ sub: 'dev-frank', orgId: 'dev-org' }, ['doc-a']);
  });
});
