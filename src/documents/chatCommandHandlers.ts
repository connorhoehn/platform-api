// The custom step kinds behind the chat's slash-command pipelines
// (/summarize, /diagram), gathered so the pipeline bootstrap registers them
// in one line. Each pipeline owns its handlers; this only merges the maps.
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { buildConversationSummarizeHandlers } from './conversationSummarizePipeline';
import { buildDiagramGenerateHandlers } from './diagramGeneratePipeline';

export function buildChatCommandHandlers(): Map<string, StepHandler> {
  const m = new Map<string, StepHandler>();
  for (const src of [buildConversationSummarizeHandlers(), buildDiagramGenerateHandlers()]) for (const [k, h] of src) m.set(k, h);
  return m;
}
