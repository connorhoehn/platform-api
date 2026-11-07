// platform-api/src/pipeline/agent/userDefinitionPolicy.ts
//
// What a person may put in a pipeline definition they write themselves.
//
// An agent step's `allowedTools` reaches `claude --tools`. Accepted from a
// user-submitted definition (the run trigger's inline `definition`, or
// PUT /pipelines/defs/:id) it let any signed-in user run a shell in this
// container (security review, 6 November 2026, "fix first" 1). So a
// definition from a person carries no tools at all: tool-using agent steps
// come only from the platform's own planner and system pipelines, and run
// only on the devbox runtime (AgentStepHandler enforces that half).

export interface ToolRequestingStep {
  nodeId: string;
  tools: string[];
}

function toolList(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value.map((t) => String(t).trim()).filter((t) => t.length > 0);
}

/**
 * Every node that asks for tools. Checks `allowedTools` (the field the agent
 * handler reads) and `tools` (the name the CLI uses), on any node kind, so a
 * renamed or future field cannot slip a tool list past the check.
 */
export function toolRequestingSteps(definition: unknown): ToolRequestingStep[] {
  if (!definition || typeof definition !== 'object') return [];
  const nodes = (definition as { nodes?: unknown }).nodes;
  if (!Array.isArray(nodes)) return [];
  const found: ToolRequestingStep[] = [];
  for (const node of nodes) {
    if (!node || typeof node !== 'object') continue;
    const data = (node as { data?: unknown }).data;
    if (!data || typeof data !== 'object') continue;
    const tools = [
      ...toolList((data as { allowedTools?: unknown }).allowedTools),
      ...toolList((data as { tools?: unknown }).tools),
    ];
    if (tools.length > 0) {
      found.push({ nodeId: String((node as { id?: unknown }).id ?? ''), tools });
    }
  }
  return found;
}

export function describeToolRequestingSteps(steps: ToolRequestingStep[]): string {
  const list = steps.map((s) => `${s.nodeId || '(unnamed)'} (${s.tools.join(', ')})`).join('; ');
  return `agent steps in a pipeline you write may not request tools: ${list}`;
}
