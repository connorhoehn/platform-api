// Platform-owned pipelines — the ones the product itself runs (a /doc edit,
// a recording being finalised, a call being transcribed). They are not a
// person's records in the definitions table: they ship with the code, cannot
// be edited from the UI, and are served read-only beside a user's own so the
// pipeline pages can name their steps and list their runs.
import { buildDocumentAgentEditDefinition, buildDocumentTransformDefinition, buildDocumentReviewApplyDefinition, DOCUMENT_AGENT_EDIT_PIPELINE_ID, DOCUMENT_TRANSFORM_PIPELINE_ID, DOCUMENT_AGENT_REVIEW_PIPELINE_ID } from '../documents/documentAgentPipeline';
import { buildRecordingFinalizeDefinition, buildCallTranscriptionDefinition, RECORDING_FINALIZE_PIPELINE_ID, CALL_TRANSCRIPTION_PIPELINE_ID } from '../recordings/recordingPipelines';
import { buildLiveCaptionsDefinition, LIVE_CAPTIONS_PIPELINE_ID } from '../recordings/liveCaptions';
import { buildConversationSummarizeDefinition, CONVERSATION_SUMMARIZE_PIPELINE_ID } from '../documents/conversationSummarizePipeline';
import { buildDiagramGenerateDefinition, DIAGRAM_GENERATE_PIPELINE_ID } from '../documents/diagramGeneratePipeline';
import { assertSystemPipelinesLint } from './systemPipelineLint';

export interface SystemPipelineEntry {
  id: string;
  build: () => Record<string, unknown>;
}

const REGISTRY: SystemPipelineEntry[] = [
  { id: DOCUMENT_AGENT_EDIT_PIPELINE_ID, build: () => buildDocumentAgentEditDefinition() },
  { id: DOCUMENT_TRANSFORM_PIPELINE_ID, build: () => buildDocumentTransformDefinition() },
  { id: DOCUMENT_AGENT_REVIEW_PIPELINE_ID, build: () => buildDocumentReviewApplyDefinition() },
  { id: RECORDING_FINALIZE_PIPELINE_ID, build: () => buildRecordingFinalizeDefinition() },
  { id: CALL_TRANSCRIPTION_PIPELINE_ID, build: () => buildCallTranscriptionDefinition() },
  { id: LIVE_CAPTIONS_PIPELINE_ID, build: () => buildLiveCaptionsDefinition() },
  { id: CONVERSATION_SUMMARIZE_PIPELINE_ID, build: () => buildConversationSummarizeDefinition() },
  { id: DIAGRAM_GENERATE_PIPELINE_ID, build: () => buildDiagramGenerateDefinition() },
];

// Retries and approvers are platform properties: a definition that forgets
// them fails boot here, with every offender named, rather than shipping a step
// that gives up on its first blip.
assertSystemPipelinesLint(REGISTRY.map((e) => e.build()));

export function registerSystemPipeline(entry: SystemPipelineEntry): void {
  if (REGISTRY.some((e) => e.id === entry.id)) return;
  assertSystemPipelinesLint([entry.build()]);
  REGISTRY.push(entry);
}

export function getSystemPipeline(id: string): Record<string, unknown> | null {
  const e = REGISTRY.find((x) => x.id === id);
  return e ? { ...e.build(), system: true } : null;
}

export function listSystemPipelines(): Record<string, unknown>[] {
  return REGISTRY.map((e) => ({ ...e.build(), system: true }));
}
