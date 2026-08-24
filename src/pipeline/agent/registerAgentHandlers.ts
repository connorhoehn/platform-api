// platform-api/src/pipeline/agent/registerAgentHandlers.ts
//
// Boot-time wiring for the `agent` custom step handler. Called from
// bootstrap.ts to contribute into the single Map<string, StepHandler>
// forwarded to PipelineModule via buildPipelineModuleConfig.
//
// Registered unconditionally, mirroring the judge handler: a missing or
// broken `claude` binary should surface as a clean dispatch-time
// failure with a real error message, not as "unreachable pipeline node
// type".
//
// AGENT_STEP_CWD scopes the working directory the agent's filesystem
// tools see. It only matters when a node opts into a filesystem tool at
// all — with the default-deny allowlist there are no tools to scope.

import type { Logger } from 'pino';
import type { StepHandler } from 'distributed-core/applications/pipeline';
import { createAgentStepHandler } from './AgentStepHandler';
import { ClaudeCodeAgentClient } from './ClaudeCodeAgentClient';
import type { AgentClient } from './types';

export interface BuildAgentStepHandlersOpts {
  logger?: Logger;
  /** Overrides the CLI-backed client; used by tests. */
  agentClient?: AgentClient;
}

export function buildAgentStepHandlers(
  opts: BuildAgentStepHandlersOpts = {},
): Map<string, StepHandler> {
  const handlers = new Map<string, StepHandler>();

  const agentClient = opts.agentClient ?? new ClaudeCodeAgentClient();
  const cwd = process.env['AGENT_STEP_CWD'];
  const defaultModel = process.env['AGENT_STEP_DEFAULT_MODEL'];

  handlers.set(
    'agent',
    createAgentStepHandler({
      agentClient,
      ...(cwd ? { cwd } : {}),
      ...(defaultModel ? { defaultModel } : {}),
    }),
  );

  opts.logger?.info({ kinds: [...handlers.keys()] }, 'agent step handlers registered');
  return handlers;
}
