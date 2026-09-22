// A completed deck step's published event has to carry the revision it made.
//
// The test runs the real handler for its real output, turns that into artifact
// references the way runPersistence does, and maps the step observation with the
// real mapper — so it fails if any of the three stops agreeing with the others.

import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';
import { createDeckStepHandler } from '../../deck/DeckStepHandler';
import type { AppendRevisionInput, AppendRevisionResult, PresentationDocumentStore } from '../../deck/presentationDocuments';
import { revisionSources } from '../../deck/presentationDocuments';
import { sampleDeck } from '../../deck/__tests__/fixtures';
import { hidePptxNodeDetection } from '../../deck/__tests__/pptxTestEnv';
import { declaredArtifactRefs } from '../declaredArtifacts';
import { mapPipeline } from '../../work-graph/sources/mapPipeline';

const RUN = { pipelineId: 'deck-generate', runId: 'run-77' };

const store: PresentationDocumentStore = {
  get: async () => null,
  appendRevision: async (input: AppendRevisionInput): Promise<AppendRevisionResult> => ({
    documentId: input.documentId,
    revision: {
      id: 'v3',
      label: 'v3',
      createdAt: '2026-09-21T10:00:00.000Z',
      createdBy: input.createdBy,
      slideCount: input.slides.length,
      asset: { key: 'deck.pptx', contentType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation', sizeBytes: input.pptx.byteLength },
      slides: input.slides.map((slide, index) => ({
        id: slide.id, index: index + 1, title: slide.title,
        previewHandle: `${input.documentId}.v3.${slide.id}`,
        asset: { key: `${slide.id}.svg`, contentType: 'image/svg+xml', sizeBytes: slide.svg.length },
      })),
      ...(input.spec ? { spec: input.spec, sources: revisionSources(input.spec) } : {}),
    },
  }),
};

const helpers = { runId: RUN.runId, stepId: 'render', emit: async () => undefined } as unknown as StepHandlerHelpers;
const node = { id: 'render', type: 'deck', position: { x: 0, y: 0 }, data: { type: 'deck', spec: sampleDeck() } } as unknown as PipelineNode;

describe('a completed deck step declares the revision it produced', () => {
  let restore: () => void;
  let output: Record<string, unknown>;

  beforeAll(async () => {
    restore = hidePptxNodeDetection();
    const outcome = await createDeckStepHandler({ store }).execute(node, { documentId: 'doc-deck', requestedBy: 'user-frank' }, helpers);
    expect(outcome.status).toBe('completed');
    output = outcome.output as Record<string, unknown>;
  }, 30_000);

  afterAll(() => restore());

  it('names the document, the revision and the slide count on its output', () => {
    expect(output.artifacts).toEqual([{
      artifactId: 'doc-deck',
      kind: 'presentation',
      revisionId: 'v3',
      slideCount: 9,
      title: 'Q4 platform review',
      sources: [
        { documentId: 'doc-sprint-review', title: 'Sprint review', kind: 'document' },
        { documentId: 'doc-roadmap', title: 'Q4 roadmap', kind: 'document', anchor: 'h2-q4' },
      ],
    }]);
  });

  it('becomes an artifact reference stamped with the run', () => {
    expect(declaredArtifactRefs(output, RUN)).toEqual([{ artifactId: 'doc-deck', pipelineId: 'deck-generate', runId: 'run-77' }]);
  });

  it('reaches the published step event as artifactIds', () => {
    const mapped = mapPipeline({
      kind: 'pipeline-step',
      run: { runId: RUN.runId, pipelineId: RUN.pipelineId, status: 'running', startedAt: '2026-09-21T09:59:00.000Z' } as never,
      step: { stepId: 'render', status: 'completed' },
      attributedSubject: 'user-frank',
      occurredAt: '2026-09-21T10:00:00.000Z',
      artifactRefs: declaredArtifactRefs(output, RUN),
    }, {
      actor: { actorId: 'actor-frank', organizationId: 'org-1', sourceSubject: 'user-frank' },
      producer: { serviceId: 'platform-api', organizationId: 'org-1', credentialId: 'test' },
      receivedAt: '2026-09-21T10:00:01.000Z',
    });

    expect(mapped.supported).toBe(true);
    if (!mapped.supported) throw new Error('unreachable');
    expect((mapped.event.payload as { artifactIds?: string[] }).artifactIds).toEqual(['doc-deck']);
  });

  it('declares nothing for a step that said nothing', () => {
    expect(declaredArtifactRefs({ ok: true }, RUN)).toEqual([]);
    expect(declaredArtifactRefs(undefined, RUN)).toEqual([]);
    expect(declaredArtifactRefs({ artifacts: [{ note: 'no id' }, null] }, RUN)).toEqual([]);
  });

  it('takes a documentId as the artifact id, and never the same one twice', () => {
    expect(declaredArtifactRefs({ artifacts: [{ documentId: 'doc-a' }, { artifactId: 'doc-a' }, { documentId: 'doc-b' }] }, RUN))
      .toEqual([
        { artifactId: 'doc-a', pipelineId: 'deck-generate', runId: 'run-77' },
        { artifactId: 'doc-b', pipelineId: 'deck-generate', runId: 'run-77' },
      ]);
  });
});
