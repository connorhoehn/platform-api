// A complete external chat-command plugin: `/graph <query>` → one custom step → a `graph` panel.
// Enable with CHAT_COMMAND_PLUGINS=./plugins/example-org-graph (no platform-api source edited).
const PIPELINE_ID = 'org-graph';
const STEP_KIND = 'org:graph';
const panels = new Map(); // panel id → panelData; a real plugin keeps this in its own table

const graphStep = {
  kind: STEP_KIND,
  async execute(_node, ctx, helpers) {
    const query = String(ctx.query || '').trim();
    const id = `${PIPELINE_ID}:${helpers.runId}`; // `<pipelineId>:<runId>` keeps the run-context fallback working
    const panelData = {
      title: `Org graph: ${query}`,
      nodes: [{ id: 'q', label: query, group: 'center' }, { id: 'ada', label: 'Ada' }, { id: 'bo', label: 'Bo' }],
      edges: [{ from: 'q', to: 'ada', label: 'works with' }, { from: 'q', to: 'bo', label: 'works with' }],
    };
    panels.set(id, panelData);
    // `result` is the run-result convention: the card's "Open" opens this panel; `panelData` is what it shows.
    return { status: 'completed', output: { panelData, result: { kind: 'panel', panel: { kind: 'graph', id, title: panelData.title } } } };
  },
};

module.exports = {
  name: 'org-graph',
  setup(ctx) {
    ctx.registerCommand({
      trigger: '/graph', pipelineId: PIPELINE_ID, description: 'Draw the org graph around a query',
      requires: { argumentText: true },
      buildDefinition: () => ({
        id: PIPELINE_ID, name: 'Org graph', description: '/graph <query>', version: 1, status: 'published', publishedVersion: 1,
        nodes: [
          { id: 'trigger', type: 'trigger', position: { x: 0, y: 0 }, data: { type: 'trigger', triggerType: 'manual', label: '/graph command' } },
          { id: 'graph', type: STEP_KIND, position: { x: 200, y: 0 }, data: { type: STEP_KIND, label: 'Build the graph', retryPolicy: { maxAttempts: 2, backoffMs: 500 } } },
        ],
        edges: [{ id: 'trigger->graph', source: 'trigger', sourceHandle: 'out', target: 'graph', targetHandle: 'in' }],
      }),
      triggerPayload: (input) => ({ query: input.argumentText.trim().slice(0, 200) }),
    });
    ctx.registerStepHandlers([graphStep]);
    // Serve GET /api/chat/panels/graph/:id from our store; null → the route falls back to the run's panelData.
    ctx.registerPanelDataProvider('graph', async (id) => panels.get(id) ?? null);
  },
};
