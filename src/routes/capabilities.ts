// platform-api/src/routes/capabilities.ts
//
// /api/capabilities — backend-side introspection surface so consumers can
// query which AgenticPipelines are currently active without going through the
// gateway.  Mirrors the gateway's capabilities shape.
//
// Routes:
//   GET /api/capabilities/pipelines         — list all registered pipelines
//   GET /api/capabilities/pipelines/:name   — detail for a single pipeline
//
// Auth: protected by requireAuth (mounted in app.ts under the auth middleware).
// The data comes from DefinitionsRepository.listAll() which scans the full
// pipeline-definitions table (all users / operator-managed).

import { Router } from 'express';
import type { DefinitionsRepository, PipelineDefinition } from '../pipeline/definitions-repository';
import { listSystemPipelines } from '../pipeline/systemPipelines';

function asBool(v: unknown): boolean {
  return typeof v === 'boolean' ? v : false;
}
function asString(v: unknown): string | undefined {
  return typeof v === 'string' ? v : undefined;
}
function asArray(v: unknown): unknown[] {
  return Array.isArray(v) ? v : [];
}

function summaryOf(def: PipelineDefinition) {
  return {
    name: asString(def.name) ?? def.id,
    enabled: !asBool(def.disabled),
    version: asString(def.version),
    triggers: asArray(def.triggers).length,
  };
}

function detailOf(def: PipelineDefinition) {
  return {
    name: asString(def.name) ?? def.id,
    enabled: !asBool(def.disabled),
    version: asString(def.version),
    steps: asArray(def.steps).length,
    triggers: def.triggers,
  };
}

export function createCapabilitiesRouter(deps: { definitions: DefinitionsRepository; system?: () => Record<string, unknown>[] }) {
  const router = Router();

  // Loop 29c (NFR #174): the caller's own definitions plus the system
  // pipelines — never `listAll()`, which is every user's.
  async function visible(req: { user?: { sub?: string } }): Promise<PipelineDefinition[]> {
    const userId = req.user?.sub ?? '';
    const own = userId ? await deps.definitions.list(userId) : [];
    const system = (deps.system ?? listSystemPipelines)() as unknown as PipelineDefinition[];
    const ids = new Set(own.map((d) => d.id));
    return [...own, ...system.filter((d) => !ids.has(d.id))];
  }

  // GET /api/capabilities/pipelines
  router.get('/pipelines', async (req, res) => {
    try {
      const all = await visible(req);
      res.json({ pipelines: all.map(summaryOf) });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      res.status(500).json({ error: 'internal_error', detail: message });
    }
  });

  // GET /api/capabilities/pipelines/:name
  router.get('/pipelines/:name', async (req, res) => {
    try {
      const all = await visible(req);
      const name = req.params.name;
      // Match by name field or by id (the DDB SK)
      const def = all.find(
        (d) => (asString(d.name) ?? d.id) === name || d.id === name,
      );
      if (!def) {
        res.status(404).json({ error: 'pipeline_not_found' });
        return;
      }
      res.json(detailOf(def));
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : String(err);
      res.status(500).json({ error: 'internal_error', detail: message });
    }
  });

  return router;
}
