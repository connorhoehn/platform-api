// The `claude` child gets a minimal environment (security review §8,
// package 1). It used to inherit the pod's: realtime secret, admin token,
// AWS credentials — all readable by any Bash tool the agent was given.

import { describe, it, expect, jest } from '@jest/globals';
import { buildAgentEnv, AGENT_ENV_ALLOWLIST } from '../ClaudeCodeAgentClient';
import { createAgentStepHandler } from '../AgentStepHandler';
import type { AgentClient } from '../types';
import type { PipelineNode, StepHandlerHelpers } from 'distributed-core/applications/pipeline';

const POD_ENV: NodeJS.ProcessEnv = {
  PATH: '/usr/local/bin:/usr/bin',
  HOME: '/root',
  LANG: 'C.UTF-8',
  ANTHROPIC_MODEL: 'sonnet',
  CLAUDE_CODE_USE_BEDROCK: '0',
  REALTIME_JWT_SECRET: 'shh',
  ADMIN_TOKEN: 'admin-token',
  LVS_ADMIN_TOKEN: 'lvs',
  CLAUDE_CODE_OAUTH_TOKEN: 'oauth',
  AWS_ACCESS_KEY_ID: 'AKIA...',
  AWS_SECRET_ACCESS_KEY: 'secret',
  AWS_SESSION_TOKEN: 'session',
  AWS_REGION: 'us-east-1',
  AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: '/v2/credentials/x',
  DATABASE_URL: 'postgres://u:p@h/db',
  ANTHROPIC_API_KEY: 'sk-ant',
};

describe('buildAgentEnv', () => {
  it('passes only allowlisted keys', () => {
    const env = buildAgentEnv(POD_ENV);
    for (const key of Object.keys(env)) {
      expect(AGENT_ENV_ALLOWLIST).toContain(key);
    }
    expect(env).toEqual({
      PATH: '/usr/local/bin:/usr/bin',
      HOME: '/root',
      LANG: 'C.UTF-8',
      ANTHROPIC_MODEL: 'sonnet',
      CLAUDE_CODE_USE_BEDROCK: '0',
    });
  });

  it('never passes anything named *SECRET*, *TOKEN* or AWS_* credentials', () => {
    const env = buildAgentEnv({ ...POD_ENV, CLAUDE_CODE_USE_BEDROCK: '1' });
    for (const key of Object.keys(env)) {
      expect(key).not.toMatch(/SECRET|TOKEN/);
      if (key.startsWith('AWS_')) expect(['AWS_REGION', 'AWS_DEFAULT_REGION']).toContain(key);
    }
    // Bedrock mode needs the region and nothing else from AWS_*.
    expect(env.AWS_REGION).toBe('us-east-1');
    expect(env.AWS_ACCESS_KEY_ID).toBeUndefined();
    expect(env.AWS_CONTAINER_CREDENTIALS_RELATIVE_URI).toBeUndefined();
  });

  it('drops the region outside Bedrock mode', () => {
    expect(buildAgentEnv(POD_ENV).AWS_REGION).toBeUndefined();
  });
});

describe('agent step — tools run only on the devbox runtime', () => {
  const helpers = {
    emit: async () => undefined,
    runId: 'run-1',
    ownerNodeId: 'node-1',
    stepId: 'a1',
    abortSignal: new AbortController().signal,
    sleep: async () => undefined,
    isCancelled: () => false,
  } as unknown as StepHandlerHelpers;

  it('fails a local step that requests tools without starting the CLI', async () => {
    const run = jest.fn();
    const local = { provider: 'claude-code', run } as unknown as AgentClient;
    const handler = createAgentStepHandler({ agentClient: local });
    const outcome = await handler.execute(
      { id: 'a1', type: 'action', data: { type: 'agent', prompt: 'env', allowedTools: ['Bash'] } } as unknown as PipelineNode,
      {},
      helpers,
    );
    expect(outcome.status).toBe('failed');
    expect(outcome.error).toMatch(/devbox/);
    expect(run).not.toHaveBeenCalled();
  });
});
