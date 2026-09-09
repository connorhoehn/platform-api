// Platform-owned pipelines — the ones the product itself runs (a /doc edit,
// a recording being finalised, a call being transcribed). They are not a
// person's records in the definitions table: they ship with the code, cannot
// be edited from the UI, and are served read-only beside a user's own so the
// pipeline pages can name their steps and list their runs.
import { buildDocumentAgentEditDefinition, DOCUMENT_AGENT_EDIT_PIPELINE_ID } from '../documents/documentAgentPipeline';
import { buildRecordingFinalizeDefinition, buildCallTranscriptionDefinition, RECORDING_FINALIZE_PIPELINE_ID, CALL_TRANSCRIPTION_PIPELINE_ID } from '../recordings/recordingPipelines';

export interface SystemPipelineEntry {
  id: string;
  build: () => Record<string, unknown>;
}

const REGISTRY: SystemPipelineEntry[] = [
  { id: DOCUMENT_AGENT_EDIT_PIPELINE_ID, build: () => buildDocumentAgentEditDefinition() },
  { id: RECORDING_FINALIZE_PIPELINE_ID, build: () => buildRecordingFinalizeDefinition() },
  { id: CALL_TRANSCRIPTION_PIPELINE_ID, build: () => buildCallTranscriptionDefinition() },
];

export function registerSystemPipeline(entry: SystemPipelineEntry): void {
  if (!REGISTRY.some((e) => e.id === entry.id)) REGISTRY.push(entry);
}

export function getSystemPipeline(id: string): Record<string, unknown> | null {
  const e = REGISTRY.find((x) => x.id === id);
  return e ? { ...e.build(), system: true } : null;
}

export function listSystemPipelines(): Record<string, unknown>[] {
  return REGISTRY.map((e) => ({ ...e.build(), system: true }));
}
