// platform-api/src/pipeline/agent/ — barrel.
//
// The `agent` step type: runs a real `claude -p` agent loop (multiple
// turns, tool use) and persists the tool-call trace as step output.
//
// Wire by merging `buildAgentStepHandlers()` into the
// `customStepHandlers` Map passed to PipelineModule config.

export { createAgentStepHandler, interpolateContext, capToolPayload, labFromContext } from './AgentStepHandler';
export type { AgentStepHandlerOpts } from './AgentStepHandler';

export {
  ClaudeCodeAgentClient,
  buildAgentArgs,
  baseToolName,
  isToolless,
  FORBIDDEN_FLAGS,
} from './ClaudeCodeAgentClient';

export {
  DevboxAgentClient,
  DevboxUnavailableError,
  DEVBOX_UNAVAILABLE,
  DEVBOX_BASE_PATH,
  DEVBOX_CONTINUE_PROMPT,
  devboxBaseUrl,
  devboxSecret,
  contextTokensOf,
  saysDone,
  readSseFrames,
  rememberRunLab,
  recallRunLab,
  takeRunLab,
  clearRunLabs,
} from './DevboxAgentClient';
export type { DevboxAgentClientOpts, DevboxFrame } from './DevboxAgentClient';

export { buildAgentStepHandlers } from './registerAgentHandlers';
export type { BuildAgentStepHandlersOpts } from './registerAgentHandlers';

export { DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_MS } from './types';
export type {
  AgentClient,
  AgentEvent,
  AgentLabSession,
  AgentNodeData,
  AgentRunOptions,
  AgentStepOutput,
  AgentStopReason,
  AgentToolCall,
} from './types';
