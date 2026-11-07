// platform-api/src/pipeline/cliChildEnv.ts
//
// The environment every model CLI child gets (`claude -p` for agent steps,
// LLM steps and vision; `codex exec` for summaries).
//
// They used to inherit the pod's whole environment: the realtime JWT secret,
// the admin token, AWS keys. Anything the model could make the CLI run could
// read them (security review, 6 November 2026). Now a child gets what the CLI
// needs to start (PATH, HOME for the mounted ~/.claude or ~/.codex session,
// locale, proxy), the model settings, and — in Bedrock mode only — the AWS
// credential PROVIDER's pointers (region, profile, the task-role or
// web-identity endpoints and token-file paths), so the CLI can fetch the
// pod's role credentials itself. The keys themselves (AWS_ACCESS_KEY_ID,
// AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN), ANTHROPIC_API_KEY and anything
// named like a secret never pass.

/** Always passed when set. */
const BASE_ENV = [
  'PATH',
  'HOME',
  'LANG',
  'LC_ALL',
  'TZ',
  'TMPDIR',
  'CLAUDE_CONFIG_DIR',
  'CODEX_HOME',
  'CLAUDE_CODE_USE_BEDROCK',
  'ANTHROPIC_MODEL',
  'ANTHROPIC_SMALL_FAST_MODEL',
  'ANTHROPIC_DEFAULT_OPUS_MODEL',
  'ANTHROPIC_DEFAULT_SONNET_MODEL',
  'ANTHROPIC_DEFAULT_HAIKU_MODEL',
  // A URL (a local proxy or gateway), not a credential.
  'ANTHROPIC_BASE_URL',
  'DISABLE_TELEMETRY',
  'CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC',
  'HTTP_PROXY',
  'HTTPS_PROXY',
  'NO_PROXY',
] as const;

/**
 * Bedrock mode only: where the AWS SDK finds the pod's role, never the keys.
 * The two `*_TOKEN_FILE` names are paths to a projected token file and the
 * two container-credentials names are endpoints; they are exempt from the
 * credential-name check below by name, and nothing else is.
 */
const BEDROCK_PROVIDER_ENV = [
  'AWS_REGION',
  'AWS_DEFAULT_REGION',
  'AWS_PROFILE',
  'AWS_SDK_LOAD_CONFIG',
  'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
  'AWS_CONTAINER_CREDENTIALS_FULL_URI',
  'AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE',
  'AWS_WEB_IDENTITY_TOKEN_FILE',
  'AWS_ROLE_ARN',
  'AWS_ROLE_SESSION_NAME',
  'AWS_EC2_METADATA_DISABLED',
] as const;

export const CLI_CHILD_ENV_ALLOWLIST = [...BASE_ENV, ...BEDROCK_PROVIDER_ENV] as const;

/** Credential pointers whose names look like secrets but whose values are paths/URLs. */
const POINTER_NAMES = new Set<string>([
  'AWS_CONTAINER_CREDENTIALS_RELATIVE_URI',
  'AWS_CONTAINER_CREDENTIALS_FULL_URI',
  'AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE',
  'AWS_WEB_IDENTITY_TOKEN_FILE',
]);

/** Belt and braces: never pass a name that looks like a credential, even if allowlisted. */
const NEVER_PASS = /SECRET|TOKEN|PASSWORD|CREDENTIAL|ACCESS_KEY|API_KEY/i;

function bedrockMode(source: NodeJS.ProcessEnv): boolean {
  const v = source['CLAUDE_CODE_USE_BEDROCK'];
  return v === '1' || v?.toLowerCase() === 'true';
}

/** Pure: the child's environment from `source`. */
export function buildCliChildEnv(source: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const keys: readonly string[] = bedrockMode(source) ? CLI_CHILD_ENV_ALLOWLIST : BASE_ENV;
  const env: NodeJS.ProcessEnv = {};
  for (const key of keys) {
    if (NEVER_PASS.test(key) && !POINTER_NAMES.has(key)) continue;
    const value = source[key];
    if (typeof value === 'string') env[key] = value;
  }
  return env;
}
