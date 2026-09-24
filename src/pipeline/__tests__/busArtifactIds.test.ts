// NFR #161: the bus events every lane relays carry the documents a run wrote.

import { stampPipelineArtifactIds } from '../busArtifactIds';

function fakeBus() {
  const published: Array<{ type: string; payload: any }> = [];
  const bus = {
    publish: async (type: string, payload: any) => {
      published.push({ type, payload });
      return { id: `e${published.length}`, type, payload };
    },
  };
  return { bus, published };
}

describe('stampPipelineArtifactIds', () => {
  it('puts a step\'s declared artifacts on its completed event and the run\'s union on run completed', async () => {
    const { bus, published } = fakeBus();
    stampPipelineArtifactIds(bus);
    stampPipelineArtifactIds(bus); // idempotent: one wrap

    await bus.publish('pipeline:step:started', { runId: 'r1', stepId: 'a', at: 't' });
    await bus.publish('pipeline:step:completed', {
      runId: 'r1', stepId: 'a', durationMs: 5, at: 't',
      output: { artifacts: [{ artifactId: 'doc-1', kind: 'document' }, { documentId: 'doc-2' }, { artifactId: 'doc-1' }] },
    });
    await bus.publish('pipeline:step:completed', { runId: 'r1', stepId: 'b', durationMs: 5, at: 't', output: { text: 'no artifacts' } });
    await bus.publish('pipeline:step:completed', { runId: 'r1', stepId: 'c', durationMs: 5, at: 't', output: { artifacts: [{ artifactId: 'doc-3' }, { artifactId: 'doc-2' }] } });
    await bus.publish('pipeline:run:completed', { runId: 'r1', durationMs: 20, at: 't' });

    expect(published[0].payload).not.toHaveProperty('artifactIds');
    expect(published[1].payload.artifactIds).toEqual(['doc-1', 'doc-2']);
    expect(published[1].payload.output.artifacts).toHaveLength(3); // output untouched
    expect(published[2].payload).not.toHaveProperty('artifactIds');
    expect(published[3].payload.artifactIds).toEqual(['doc-3', 'doc-2']);
    expect(published[4].payload.artifactIds).toEqual(['doc-1', 'doc-2', 'doc-3']);
  });

  it('leaves a run with no declarations bare and forgets a failed run', async () => {
    const { bus, published } = fakeBus();
    stampPipelineArtifactIds(bus);
    await bus.publish('pipeline:run:completed', { runId: 'r0', durationMs: 1, at: 't' });
    await bus.publish('pipeline:step:completed', { runId: 'r2', stepId: 'a', durationMs: 1, at: 't', output: { artifacts: [{ artifactId: 'doc-9' }] } });
    await bus.publish('pipeline:run:failed', { runId: 'r2', error: {}, at: 't' });
    await bus.publish('pipeline:run:completed', { runId: 'r2', durationMs: 1, at: 't' });

    expect(published[0].payload).not.toHaveProperty('artifactIds');
    expect(published[2].payload).not.toHaveProperty('artifactIds');
    expect(published[3].payload).not.toHaveProperty('artifactIds');
  });

  it('keeps ids a publisher already set', async () => {
    const { bus, published } = fakeBus();
    stampPipelineArtifactIds(bus);
    await bus.publish('pipeline:step:completed', { runId: 'r3', stepId: 'a', durationMs: 1, at: 't', artifactIds: ['x'], output: { artifacts: [{ artifactId: 'y' }] } });
    expect(published[0].payload.artifactIds).toEqual(['x']);
  });
});
