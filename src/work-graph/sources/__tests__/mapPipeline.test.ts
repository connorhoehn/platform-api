import type { PipelineRun, PipelineRunSnapshot } from '@connorhoehn/pipeline-runtime';
import {
  mapPipeline,
  type AuthenticatedPipelineContext,
  type PipelineRunSourceRecord,
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
