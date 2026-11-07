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

function nodeData(node: unknown): Record<string, unknown> | null {
  if (!node || typeof node !== 'object') return null;
  const data = (node as { data?: unknown }).data;
  return data && typeof data === 'object' ? (data as Record<string, unknown>) : null;
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

/**
 * The tool-requesting steps a re-save may NOT keep: everything
 * `toolRequestingSteps` finds, minus nodes the STORED definition already has
 * under the same id with the same `allowedTools` and `tools` lists, on the
 * devbox runtime in both. That lets a person re-save a generated pipeline
 * (POST /generate writes devbox steps with tools) after editing a prompt or a
 * layout, while a new tool step, a widened or reordered list, or a move off
 * devbox is still refused. `stored` null (no row yet) grandfathers nothing.
 */
export function unapprovedToolSteps(definition: unknown, stored: unknown): ToolRequestingStep[] {
  const found = toolRequestingSteps(definition);
  if (found.length === 0 || !stored || typeof stored !== 'object') return found;
  const storedNodes = (stored as { nodes?: unknown }).nodes;
  if (!Array.isArray(storedNodes)) return found;
  const byId = new Map<string, Record<string, unknown>>();
  for (const node of storedNodes) {
    const data = nodeData(node);
    const id = (node as { id?: unknown })?.id;
    if (data && typeof id === 'string') byId.set(id, data);
  }
  const incoming = new Map<string, Record<string, unknown>>();
  for (const node of (definition as { nodes?: unknown[] }).nodes ?? []) {
    const data = nodeData(node);
    const id = (node as { id?: unknown })?.id;
    if (data && typeof id === 'string') incoming.set(id, data);
  }
  const same = (a: unknown, b: unknown): boolean => JSON.stringify(toolList(a)) === JSON.stringify(toolList(b));
  return found.filter((step) => {
    const before = byId.get(step.nodeId);
    const now = incoming.get(step.nodeId);
    if (!before || !now) return true;
    const unchanged = same(before.allowedTools, now.allowedTools) && same(before.tools, now.tools);
    return !(unchanged && before.runtime === 'devbox' && now.runtime === 'devbox');
  });
}
