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
  ANTHROPIC_BASE_URL: 'http://127.0.0.1:4000',
  CLAUDE_CODE_USE_BEDROCK: '0',
  HTTPS_PROXY: 'http://proxy:3128',
  NO_PROXY: 'localhost',
  REALTIME_JWT_SECRET: 'shh',
  ADMIN_TOKEN: 'admin-token',
  LVS_ADMIN_TOKEN: 'lvs',
  CLAUDE_CODE_OAUTH_TOKEN: 'oauth',
  AWS_ACCESS_KEY_ID: 'AKIA...',
  AWS_SECRET_ACCESS_KEY: 'secret',
  AWS_SESSION_TOKEN: 'session',
  AWS_REGION: 'us-east-1',
  AWS_DEFAULT_REGION: 'us-east-1',
  AWS_PROFILE: 'pod',
  AWS_SDK_LOAD_CONFIG: '1',
  AWS_CONTAINER_CREDENTIALS_RELATIVE_URI: '/v2/credentials/x',
  AWS_CONTAINER_CREDENTIALS_FULL_URI: 'http://169.254.170.23/v1/credentials',
  AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE: '/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token',
  AWS_WEB_IDENTITY_TOKEN_FILE: '/var/run/secrets/eks.amazonaws.com/serviceaccount/token',
  AWS_ROLE_ARN: 'arn:aws:iam::123456789012:role/platform-api',
  AWS_ROLE_SESSION_NAME: 'platform-api',
  AWS_EC2_METADATA_DISABLED: 'false',
  DATABASE_URL: 'postgres://u:p@h/db',
  ANTHROPIC_API_KEY: 'sk-ant',
  SOME_PASSWORD: 'pw',
};

/** The credential provider's pointers: where to get the task role, never the keys. */
const PROVIDER_VARS = [
  'AWS_REGION', 'AWS_DEFAULT_REGION', 'AWS_PROFILE', 'AWS_SDK_LOAD_CONFIG',
  'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI', 'AWS_CONTAINER_CREDENTIALS_FULL_URI',
  'AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE', 'AWS_WEB_IDENTITY_TOKEN_FILE',
  'AWS_ROLE_ARN', 'AWS_ROLE_SESSION_NAME', 'AWS_EC2_METADATA_DISABLED',
];
const NEVER = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN', 'ANTHROPIC_API_KEY',
  'REALTIME_JWT_SECRET', 'ADMIN_TOKEN', 'LVS_ADMIN_TOKEN', 'CLAUDE_CODE_OAUTH_TOKEN', 'SOME_PASSWORD', 'DATABASE_URL'];

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
      ANTHROPIC_BASE_URL: 'http://127.0.0.1:4000',
      CLAUDE_CODE_USE_BEDROCK: '0',
      HTTPS_PROXY: 'http://proxy:3128',
      NO_PROXY: 'localhost',
    });
  });

  it('Bedrock mode gets the task-role provider variables and never a key, token or secret', () => {
    const env = buildAgentEnv({ ...POD_ENV, CLAUDE_CODE_USE_BEDROCK: '1' });
    for (const key of PROVIDER_VARS) expect(env[key]).toBe(POD_ENV[key]);
    for (const key of NEVER) expect(env[key]).toBeUndefined();
    for (const key of Object.keys(env)) {
      if (/SECRET|TOKEN|PASSWORD/.test(key)) expect(key).toMatch(/_TOKEN_FILE$/);
    }
  });

  it('outside Bedrock mode no AWS_* variable passes', () => {
    expect(Object.keys(buildAgentEnv(POD_ENV)).filter((k) => k.startsWith('AWS_'))).toEqual([]);
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
