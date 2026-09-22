import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDeckReadSourcesHandler, parseSourceRefs, sourcesBlock } from '../DeckSourcesStepHandler';

const node = { id: 'read-sources', type: 'deck:read-sources', position: { x: 0, y: 0 }, data: { type: 'deck:read-sources' } } as unknown as PipelineNode;
const helpers = { runId: 'run-1', stepId: 'read-sources', emit: async () => undefined } as unknown as StepHandlerHelpers;

describe('the deck:read-sources step', () => {
  it('reads each referenced document into the prompt block', async () => {
    const handler = createDeckReadSourcesHandler({
      readDocument: async (documentId) => ({ text: `the text of ${documentId}` }),
    });
    const outcome = await handler.execute(node, {
      sources: [{ documentId: 'doc-a', title: 'Doc A' }, { id: 'doc-b', name: 'Doc B' }],
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
    const handler = createDeckReadSourcesHandler({
      readDocument: async () => ({ error: 'no snapshot arrived' }),
    });
    const outcome = await handler.execute(node, { sources: [{ documentId: 'doc-a', title: 'Doc A' }] }, helpers);
    const output = outcome.output as Record<string, unknown>;
    expect(output.sourcesUnavailable).toBe(1);
    expect(output.sourcesBlock).toContain('could not be read: no snapshot arrived');
  });

  it('fails when there is nothing to ground the deck in', async () => {
    const handler = createDeckReadSourcesHandler({ readDocument: async () => ({ text: '' }) });
    const outcome = await handler.execute(node, {}, helpers);
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toContain('no sources');
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
