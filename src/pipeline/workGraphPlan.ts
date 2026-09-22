/**
 * How a run is shown in the work graph: where it came from, and which of its
 * definition's nodes are the steps a person was shown.
 *
 * It lives beside the pipeline rather than inside run persistence so that any
 * entrypoint which builds a definition can declare it without depending on the
 * persistence service.
 */
/**
 * What the graph needs to show a run as the steps a person was shown, rather
 * than one opaque node. Supplied by the entrypoint that built the definition
 * — it is the only place that knows which node is which step — and kept in
 * memory only: a run whose pod was replaced has already lost its steps' live
 * state, and inventing them from a stored row would be a different claim.
 */
export interface RunWorkGraphPlan {
  /** The conversation the run was started from, when it came from one. */
  conversationId?: string;
  /**
   * What the run itself is called. "Pipeline run" names nothing a person
   * recognises, and the run has to be nameable because it is what the graph
   * falls back to anchoring on when the conversation is not there.
   */
  title?: string;
  steps: Array<{
    /** The definition node id the executor reports, e.g. `agent:s1`. */
    stepId: string;
    /** The step's own title, shown verbatim as the node's name. */
    title: string;
    /** Step ids this one waits on, in the definition's own order. */
    dependsOn: string[];
  }>;
}

/**
 * The steps of a definition, in dependsOn order, read from the definition that
 * is actually being triggered. Reading the definition rather than a command's
 * own plan keeps this true for every pipeline: a node is a step when it has a
 * label, and its dependencies are the edges that reach it.
 */
export function workGraphPlanFromDefinition(
  definition: unknown,
  conversationId?: string,
): RunWorkGraphPlan | undefined {
  const source = definition as { nodes?: unknown; edges?: unknown; name?: unknown } | null;
  const title = typeof source?.name === 'string' && source.name.trim() ? source.name.trim().slice(0, 80) : undefined;
  const nodes = Array.isArray(source?.nodes) ? source.nodes as Array<Record<string, unknown>> : [];
  const edges = Array.isArray(source?.edges) ? source.edges as Array<Record<string, unknown>> : [];
  const labelled = nodes.filter((node) => typeof node?.id === 'string'
    && node.type !== 'trigger'
    && typeof (node.data as Record<string, unknown> | undefined)?.label === 'string'
    && String((node.data as Record<string, unknown>).label).trim().length > 0);
  const named = { ...(conversationId ? { conversationId } : {}), ...(title ? { title } : {}) };
  if (labelled.length === 0) return conversationId || title ? { ...named, steps: [] } : undefined;
  const stepIds = new Set(labelled.map((node) => node.id as string));
  const steps = labelled.map((node) => ({
    stepId: node.id as string,
    title: String((node.data as Record<string, unknown>).label).trim().slice(0, 160),
    dependsOn: [...new Set(edges
      .filter((edge) => edge?.target === node.id && typeof edge.source === 'string' && stepIds.has(edge.source))
      .map((edge) => edge.source as string))],
  }));
  return { ...named, steps };
}
