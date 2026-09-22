import type { PipelineRun, PipelineRunSnapshot } from '@connorhoehn/pipeline-runtime';
import {
  mapPipeline,
  parsePipelineStepResourceId,
  pipelineStepResourceId,
  type AuthenticatedPipelineContext,
  type PipelineRunSourceRecord,
  type PipelineStepSourceRecord,
} from '../mapPipeline';

const context: AuthenticatedPipelineContext = {
  actor: {
    actorId: 'person-42',
    organizationId: 'org-1',
    sourceSubject: 'cognito-subject-42',
  },
  producer: {
    serviceId: 'platform-api',
    organizationId: 'org-1',
    credentialId: 'service-key-7',
  },
  receivedAt: '2026-09-19T12:00:05.000Z',
};

function run(status: PipelineRun['status'], overrides: Partial<PipelineRunSnapshot> = {}): PipelineRunSnapshot {
  return {
    runId: 'run-7',
    pipelineId: 'pipeline-3',
    status,
    startedAt: '2026-09-19T12:00:00.000Z',
    ...overrides,
  };
}

function source(
  status: PipelineRun['status'],
  overrides: Partial<PipelineRunSourceRecord> = {},
): PipelineRunSourceRecord {
  return {
    kind: 'pipeline-run',
    run: run(status),
    attributedSubject: 'cognito-subject-42',
    occurredAt: '2026-09-19T12:00:01.000Z',
    ...overrides,
  };
}

function eventOf(result: ReturnType<typeof mapPipeline>) {
  expect(result.supported).toBe(true);
  if (!result.supported) throw new Error(result.reason);
  return result.event;
}

describe('mapPipeline', () => {
  it.each([
    ['pending', 'started'],
    ['running', 'started'],
    ['awaiting_approval', 'waiting'],
    ['paused_at_breakpoint', 'waiting'],
    ['completed', 'completed'],
    ['failed', 'failed'],
    ['rejected', 'failed'],
    ['cancelled', 'cancelled'],
  ] as const)('maps canonical %s runs to %s', (status, lifecycle) => {
    expect(eventOf(mapPipeline(source(status), context)).payload.lifecycle).toBe(lifecycle);
  });

  it('normalizes canonical legacy status aliases from persisted run snapshots', () => {
    const completed = source('completed', {
      run: { runId: 'run-7', pipelineId: 'pipeline-3', status: 'succeeded' },
    });
    const cancelled = source('cancelled', {
      run: { runId: 'run-7', pipelineId: 'pipeline-3', status: 'canceled' },
    });

    expect(eventOf(mapPipeline(completed, context)).payload.lifecycle).toBe('completed');
    expect(eventOf(mapPipeline(cancelled, context)).payload.lifecycle).toBe('cancelled');
  });

  it('uses replayAttempts to preserve retry identity', () => {
    const event = eventOf(mapPipeline(source('running', {
      run: { runId: 'run-7', pipelineId: 'pipeline-3', status: 'running', replayAttempts: 2 },
    }), context));

    expect(event.payload.attempt).toBe(3);
    expect(event.eventId).toContain('run-7:3:started');
  });

  it('maps replayed completion deterministically for idempotent redelivery', () => {
    const replayed = source('completed', {
      run: { runId: 'run-7', pipelineId: 'pipeline-3', status: 'completed', replayAttempts: 1 },
      occurredAt: '2026-09-19T12:05:00.000Z',
      sourceSequence: 'wal-144',
    });

    const first = eventOf(mapPipeline(replayed, context));
    const redelivery = eventOf(mapPipeline(replayed, context));
    expect(redelivery).toEqual(first);
    expect(first).toMatchObject({ sourceSequence: 'wal-144', idempotencyKey: first.eventId });
    expect(first.payload).toMatchObject({ lifecycle: 'completed', attempt: 2 });
  });

  it('includes only explicit matching artifact IDs and strips artifact data', () => {
    const event = eventOf(mapPipeline(source('completed', {
      artifactRefs: [
        {
          artifactId: 'image-2', pipelineId: 'pipeline-3', runId: 'run-7',
          url: 'https://signed.example/private-token', payload: { prompt: 'PRIVATE prompt' },
        },
        { artifactId: 'image-1', pipelineId: 'pipeline-3', runId: 'run-7' },
        { artifactId: 'image-2', pipelineId: 'pipeline-3', runId: 'run-7' },
      ],
    }), context));

    expect(event.payload.artifactIds).toEqual(['image-1', 'image-2']);
    expect(JSON.stringify(event)).not.toContain('signed.example');
    expect(JSON.stringify(event)).not.toContain('PRIVATE');
    expect(event.payload).not.toHaveProperty('url');
    expect(event.payload).not.toHaveProperty('payload');
  });

  it('does not copy run output, errors, metadata, or credentials', () => {
    const event = eventOf(mapPipeline(source('failed', {
      run: {
        runId: 'run-7',
        pipelineId: 'pipeline-3',
        status: 'failed',
        output: 'PRIVATE output',
        error: { nodeId: 'private-node', message: 'PRIVATE failure', stack: 'secret stack' },
        metadata: { credentials: 'top-secret', prompt: 'PRIVATE prompt' },
      },
    }), context));

    const serialized = JSON.stringify(event);
    expect(serialized).not.toContain('PRIVATE');
    expect(serialized).not.toContain('credentials');
    expect(serialized).not.toContain('secret');
  });

  it('rejects definitions and unenriched terminal events instead of inventing a run', () => {
    expect(mapPipeline({ kind: 'pipeline-definition', definitionId: 'pipeline-3', status: 'published' }, context))
      .toEqual({ supported: false, unsupported: true, partial: true, reason: 'definition-is-not-run' });

    expect(mapPipeline(source('completed', {
      run: { runId: 'run-7', status: 'completed' },
    }), context)).toMatchObject({ supported: false, partial: true, reason: 'missing-pipeline-id' });
  });

  it('fails closed for mismatched artifact attribution and invalid replay counts', () => {
    expect(mapPipeline(source('completed', {
      artifactRefs: [{ artifactId: 'image-1', pipelineId: 'other-pipeline', runId: 'run-7' }],
    }), context)).toMatchObject({ supported: false, reason: 'invalid-artifact-reference' });

    expect(mapPipeline(source('running', {
      run: { runId: 'run-7', pipelineId: 'pipeline-3', status: 'running', replayAttempts: -1 },
    }), context)).toMatchObject({ supported: false, reason: 'invalid-replay-attempts' });
  });

  it('requires a verified actor/producer organization match', () => {
    expect(mapPipeline(source('running'), {
      ...context,
      producer: { ...context.producer, organizationId: 'other-org' },
    })).toMatchObject({ supported: false, reason: 'missing-identity-context' });
  });

  it('requires authoritative human run attribution that matches the actor', () => {
    expect(mapPipeline(source('running', { attributedSubject: 'other-subject' }), context))
      .toMatchObject({ supported: false, reason: 'actor-does-not-match-run-trigger' });
    expect(mapPipeline(source('running', { attributedSubject: 'event:document.finalize' }), context))
      .toMatchObject({ supported: false, reason: 'synthetic-trigger-is-not-human' });
    expect(mapPipeline(source('running', { attributedSubject: '' }), context))
      .toMatchObject({ supported: false, reason: 'missing-run-attribution' });
  });
});

describe('mapPipeline step records', () => {
  function step(
    status: string,
    overrides: Partial<PipelineStepSourceRecord> = {},
  ): PipelineStepSourceRecord {
    return {
      kind: 'pipeline-step',
      run: run('running'),
      step: { stepId: 'agent:s1', status },
      attributedSubject: 'cognito-subject-42',
      occurredAt: '2026-09-19T12:00:01.000Z',
      ...overrides,
    };
  }

  it('addresses a step within its run, so the run keeps its own node', () => {
    const event = eventOf(mapPipeline(step('running'), context));
    expect(event.payload.runId).toBe('run-7#agent:s1');
    expect(event.resource.resourceId).toBe('run:run-7#agent:s1');
    expect(event.payload.pipelineId).toBe('pipeline-3');
    expect(parsePipelineStepResourceId(event.payload.runId)).toEqual({ runId: 'run-7', stepId: 'agent:s1' });
  });

  it('calls a planned step waiting, never started', () => {
    expect(eventOf(mapPipeline(step('pending'), context)).payload.lifecycle).toBe('waiting');
  });

  it.each([
    ['running', 'started'],
    ['completed', 'completed'],
    ['failed', 'failed'],
    ['skipped', 'cancelled'],
    ['cancelled', 'cancelled'],
  ])('maps step status %s to lifecycle %s', (status, lifecycle) => {
    expect(eventOf(mapPipeline(step(status), context)).payload.lifecycle).toBe(lifecycle);
  });

  it('refuses a step status it cannot describe', () => {
    const result = mapPipeline(step('teleporting'), context);
    expect(result).toMatchObject({ supported: false, reason: 'unsupported-step-status' });
  });

  it('refuses a step with no id, and one whose id would forge another step', () => {
    expect(mapPipeline(step('running', { step: { stepId: '', status: 'running' } }), context))
      .toMatchObject({ reason: 'missing-step-id' });
    expect(mapPipeline(step('running', { step: { stepId: 'a#b', status: 'running' } }), context))
      .toMatchObject({ reason: 'missing-step-id' });
  });

  it('carries the source-declared title as the node label', () => {
    const event = eventOf(mapPipeline(step('running', { safeLabel: 'Draft the agenda' }), context));
    expect(event.payload.safeLabel).toBe('Draft the agenda');
  });

  it('declares the run and the steps it depends on, deduplicated', () => {
    const event = eventOf(mapPipeline(step('running', {
      inputRefs: [
        { source: 'pipeline', resourceId: 'run-7' },
        { source: 'pipeline', resourceId: 'run-7#agent:s0' },
        { source: 'pipeline', resourceId: 'run-7#agent:s0' },
      ],
    }), context));
    expect(event.payload.inputs).toEqual([
      { source: 'pipeline', resourceId: 'run-7' },
      { source: 'pipeline', resourceId: 'run-7#agent:s0' },
    ]);
  });

  it('carries the originating conversation on the run itself', () => {
    const event = eventOf(mapPipeline(source('running', {
      inputRefs: [{ source: 'conversation', resourceId: 'chat:c:general' }],
    }), context));
    expect(event.payload.runId).toBe('run-7');
    expect(event.payload.inputs).toEqual([{ source: 'conversation', resourceId: 'chat:c:general' }]);
  });

  it('omits inputs entirely when the source declared none', () => {
    expect(eventOf(mapPipeline(source('running'), context)).payload.inputs).toBeUndefined();
  });

  it('refuses an input reference from a source that is not a work source', () => {
    const result = mapPipeline(source('running', {
      inputRefs: [{ source: 'sqs' as never, resourceId: 'queue-1' }],
    }), context);
    expect(result).toMatchObject({ reason: 'invalid-input-reference' });
  });

  it('refuses an input reference with no resource', () => {
    const result = mapPipeline(source('running', {
      inputRefs: [{ source: 'conversation', resourceId: '  ' }],
    }), context);
    expect(result).toMatchObject({ reason: 'invalid-input-reference' });
  });

  it('gives the same step transition the same event id on redelivery', () => {
    const first = eventOf(mapPipeline(step('completed'), context));
    const second = eventOf(mapPipeline(step('completed'), context));
    expect(first.eventId).toBe(second.eventId);
    expect(first.eventId).not.toBe(eventOf(mapPipeline(source('completed'), context)).eventId);
  });

  it('still refuses a step whose attribution is not the authenticated actor', () => {
    expect(mapPipeline(step('running', { attributedSubject: 'someone-else' }), context))
      .toMatchObject({ reason: 'actor-does-not-match-run-trigger' });
  });

  it('still refuses a step attributed to a synthetic trigger', () => {
    expect(mapPipeline(step('running', { attributedSubject: 'system' }), context))
      .toMatchObject({ reason: 'synthetic-trigger-is-not-human' });
  });
});

describe('pipelineStepResourceId', () => {
  it('round-trips, and reads a plain run id as a run with no step', () => {
    expect(pipelineStepResourceId('run-7', 'agent:s1')).toBe('run-7#agent:s1');
    expect(parsePipelineStepResourceId('run-7')).toEqual({ runId: 'run-7' });
    expect(parsePipelineStepResourceId('run:run-7')).toEqual({ runId: 'run-7' });
    expect(parsePipelineStepResourceId('run:run-7#agent:s2')).toEqual({ runId: 'run-7', stepId: 'agent:s2' });
  });
});

describe('a step that says where it runs and what it writes', () => {
  const stepRecord = (over: Partial<PipelineStepSourceRecord> = {}): PipelineStepSourceRecord => ({
    kind: 'pipeline-step',
    run: run('running'),
    step: { stepId: 'agent:s1', status: 'running' },
    attributedSubject: 'cognito-subject-42',
    occurredAt: '2026-09-19T12:00:01.000Z',
    ...over,
  });

  it('carries the workspace, which is what makes a running step a lease', () => {
    const event = eventOf(mapPipeline(stepRecord({
      workspace: { resourceId: 'session-28a319a9', safeLabel: 'Cloud workspace · box-7' },
    }), context));
    expect(event.payload.workspace).toEqual({ resourceId: 'session-28a319a9', safeLabel: 'Cloud workspace · box-7' });
  });

  it('accepts a workspace with no label, and refuses one with no resource', () => {
    expect(eventOf(mapPipeline(stepRecord({ workspace: { resourceId: 's-1' } }), context)).payload.workspace)
      .toEqual({ resourceId: 's-1' });
    expect(mapPipeline(stepRecord({ workspace: { resourceId: '  ' } }), context))
      .toMatchObject({ reason: 'invalid-workspace' });
  });

  it('omits the workspace entirely for a step that declared none', () => {
    expect(eventOf(mapPipeline(stepRecord(), context)).payload.workspace).toBeUndefined();
  });

  it('carries what the step is producing, separately from what it consumed', () => {
    const event = eventOf(mapPipeline(stepRecord({
      inputRefs: [{ source: 'pipeline', resourceId: 'run-7' }],
      producesRefs: [{ source: 'document', resourceId: 'doc-1' }],
    }), context));
    expect(event.payload.inputs).toEqual([{ source: 'pipeline', resourceId: 'run-7' }]);
    expect(event.payload.produces).toEqual([{ source: 'document', resourceId: 'doc-1' }]);
  });

  it('refuses a produced reference that names no resource', () => {
    expect(mapPipeline(stepRecord({ producesRefs: [{ source: 'document', resourceId: '' }] }), context))
      .toMatchObject({ reason: 'invalid-input-reference' });
  });
});
