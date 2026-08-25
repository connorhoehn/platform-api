// Wave A (2026-08-25): event → pipeline trigger dispatch.
//
// The load-bearing behaviors:
//   - only PUBLISHED definitions with a matching triggerBinding.event fire
//   - activity spellings alias ('doc.finalize' fires 'document.finalize')
//   - documentTypeId-pinned bindings require the event to carry the same id
//   - one failing pipeline doesn't stop siblings; dispatch never throws
//   - runs are attributed to the synthetic `event:<name>` actor

import { describe, test, expect, jest } from '@jest/globals';
import {
  bindingEventsFor,
  dispatchPlatformEvent,
} from '../pipeline-event-dispatcher';

function def(over: Record<string, unknown>): Record<string, unknown> {
  return {
    id: `pipe-${Math.random().toString(36).slice(2, 8)}`,
    status: 'published',
    triggerBinding: { event: 'document.finalize' },
    nodes: [],
    edges: [],
    ...over,
  };
}

function makeBridge(): {
  trigger: jest.Mock<Promise<{ runId: string }>, [unknown]>;
} {
  let n = 0;
  return {
    trigger: jest.fn(async () => ({ runId: `run-${++n}` })),
  };
}

describe('bindingEventsFor', () => {
  test('maps activity doc.* spellings onto document.*', () => {
    expect(bindingEventsFor('doc.finalize')).toEqual(['doc.finalize', 'document.finalize']);
    expect(bindingEventsFor('transcription:summary.ready')).toEqual([
      'transcription:summary.ready',
    ]);
  });
});

describe('dispatchPlatformEvent', () => {
  test('fires published pipelines bound to the (aliased) event', async () => {
    const match = def({ id: 'pipe-match' });
    const otherEvent = def({ id: 'pipe-other', triggerBinding: { event: 'webhook' } });
    const draft = def({ id: 'pipe-draft', status: 'draft' });
    const noBinding = def({ id: 'pipe-none', triggerBinding: undefined });
    const bridge = makeBridge();

    const result = await dispatchPlatformEvent(
      'doc.finalize',
      { documentId: 'doc-1' },
      {
        listDefinitions: () => [match, otherEvent, draft, noBinding],
        getBridge: () => bridge as never,
      },
    );

    expect(result.fired).toEqual([{ pipelineId: 'pipe-match', runId: 'run-1' }]);
    expect(result.failed).toEqual([]);
    expect(bridge.trigger).toHaveBeenCalledTimes(1);
    const args = bridge.trigger.mock.calls[0][0] as Record<string, unknown>;
    expect(args.pipelineId).toBe('pipe-match');
    expect(args.definition).toBe(match);
    expect(args.triggeredBy).toEqual({ userId: 'event:doc.finalize' });
    expect(args.triggerPayload).toMatchObject({
      triggerType: 'doc.finalize',
      documentId: 'doc-1',
    });
  });

  test('documentTypeId-pinned binding requires the event to carry it', async () => {
    const pinned = def({
      id: 'pipe-pinned',
      triggerBinding: { event: 'document.finalize', documentTypeId: 'meeting-transcript' },
    });
    const bridge = makeBridge();
    const deps = {
      listDefinitions: () => [pinned],
      getBridge: () => bridge as never,
    };

    // No documentTypeId on the event → conservative: no fire.
    const miss = await dispatchPlatformEvent('doc.finalize', { documentId: 'd' }, deps);
    expect(miss.fired).toEqual([]);

    // Wrong type → no fire.
    const wrong = await dispatchPlatformEvent(
      'doc.finalize',
      { documentTypeId: 'design-doc' },
      deps,
    );
    expect(wrong.fired).toEqual([]);

    // Matching type → fires.
    const hit = await dispatchPlatformEvent(
      'doc.finalize',
      { documentTypeId: 'meeting-transcript' },
      deps,
    );
    expect(hit.fired).toHaveLength(1);
  });

  test('one failing pipeline does not stop siblings and never throws', async () => {
    const a = def({ id: 'pipe-a' });
    const b = def({ id: 'pipe-b' });
    const trigger = jest.fn<Promise<{ runId: string }>, [unknown]>(
      async (args) => {
        if ((args as { pipelineId: string }).pipelineId === 'pipe-a') {
          throw new Error('boom');
        }
        return { runId: 'run-b' };
      },
    );

    const result = await dispatchPlatformEvent(
      'document.finalize',
      {},
      {
        listDefinitions: () => [a, b],
        getBridge: () => ({ trigger }) as never,
      },
    );

    expect(result.failed).toEqual([{ pipelineId: 'pipe-a', error: 'boom' }]);
    expect(result.fired).toEqual([{ pipelineId: 'pipe-b', runId: 'run-b' }]);
  });

  test('bridge unavailable → all matches reported failed, nothing thrown', async () => {
    const result = await dispatchPlatformEvent(
      'document.finalize',
      {},
      {
        listDefinitions: () => [def({ id: 'pipe-x' })],
        getBridge: () => null,
      },
    );
    expect(result.fired).toEqual([]);
    expect(result.failed).toEqual([
      { pipelineId: 'pipe-x', error: 'pipeline bridge unavailable' },
    ]);
  });

  test('no matches → bridge never consulted', async () => {
    const getBridge = jest.fn(() => makeBridge() as never);
    const result = await dispatchPlatformEvent(
      'some.unbound.event',
      {},
      { listDefinitions: () => [def({})], getBridge },
    );
    expect(result.fired).toEqual([]);
    expect(getBridge).not.toHaveBeenCalled();
  });
});
