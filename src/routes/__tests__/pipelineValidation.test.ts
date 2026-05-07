// Tests for POST /api/pipelines/validate (the structural-rule subset of the
// frontend validator: NO_TRIGGER, MULTIPLE_TRIGGERS, CYCLE_DETECTED).
//
// Issue codes here MUST match the frontend's validatePipeline output exactly,
// since MCP consumers expect identical shapes regardless of which side runs.

import express, { type NextFunction, type Request, type Response } from 'express';
import {
  pipelineValidationRouter,
  validatePipelineStructural,
  type ValidationPipelineDefinition,
} from '../pipelineValidation';
import { errorHandler } from '../../middleware/error-handler';

interface MockRes {
  statusCode: number;
  headers: Record<string, string | number | readonly string[]>;
  body: unknown;
  ended: boolean;
  finished: Promise<void>;
  status(code: number): MockRes;
  json(body: unknown): MockRes;
  setHeader(name: string, value: number | string | readonly string[]): MockRes;
  getHeader(name: string): unknown;
  end(...args: unknown[]): MockRes;
}

function mockRes(): MockRes {
  let resolveFinished!: () => void;
  const finished = new Promise<void>((r) => {
    resolveFinished = r;
  });
  const r: MockRes = {
    statusCode: 200,
    headers: {},
    body: undefined,
    ended: false,
    finished,
    status(code) {
      this.statusCode = code;
      return this;
    },
    json(body) {
      this.body = body;
      this.ended = true;
      resolveFinished();
      return this;
    },
    setHeader(name, value) {
      this.headers[String(name)] = value;
      return this;
    },
    getHeader(name) {
      return this.headers[String(name)];
    },
    end(..._args) {
      this.ended = true;
      resolveFinished();
      return this;
    },
  };
  return r;
}

async function postValidate(
  app: express.Express,
  body: unknown,
): Promise<MockRes> {
  const res = mockRes();
  const req = {
    method: 'POST',
    url: '/pipelines/validate',
    originalUrl: '/pipelines/validate',
    path: '/pipelines/validate',
    headers: { 'content-type': 'application/json' },
    query: {},
    body,
  } as unknown as Request;
  await new Promise<void>((resolve, reject) => {
    const finalHandler = (err?: unknown): void => {
      if (err) {
        reject(err);
        return;
      }
      if (!res.ended) {
        res.status(404).json({ error: 'route not found' });
      }
      resolve();
    };
    (app as unknown as (req: Request, res: Response, next: NextFunction) => void)(
      req,
      res as unknown as Response,
      finalHandler,
    );
    res.finished.then(() => resolve()).catch(reject);
  });
  return res;
}

function buildApp(): express.Express {
  const app = express();
  app.use(express.json());
  app.use('/pipelines/validate', pipelineValidationRouter);
  app.use(errorHandler);
  return app;
}

function emptyDef(overrides: Partial<ValidationPipelineDefinition> = {}): ValidationPipelineDefinition {
  return { id: 'p1', nodes: [], edges: [], ...overrides };
}

// ---------------------------------------------------------------------------
// Pure validator function — covers each rule.
// ---------------------------------------------------------------------------

describe('validatePipelineStructural', () => {
  test('NO_TRIGGER when there are zero trigger nodes', () => {
    const result = validatePipelineStructural(emptyDef());
    expect(result.isValid).toBe(false);
    expect(result.canPublish).toBe(false);
    expect(result.errors).toHaveLength(1);
    expect(result.errors[0].code).toBe('NO_TRIGGER');
    expect(result.errors[0].severity).toBe('error');
  });

  test('passes when there is exactly one trigger and no edges', () => {
    const def = emptyDef({
      nodes: [{ id: 't1', type: 'trigger' }],
    });
    const result = validatePipelineStructural(def);
    expect(result.isValid).toBe(true);
    expect(result.canPublish).toBe(true);
    expect(result.errors).toEqual([]);
  });

  test('MULTIPLE_TRIGGERS when more than one trigger node exists', () => {
    const def = emptyDef({
      nodes: [
        { id: 't1', type: 'trigger' },
        { id: 't2', type: 'trigger' },
      ],
    });
    const result = validatePipelineStructural(def);
    expect(result.isValid).toBe(false);
    const codes = result.errors.map((e) => e.code);
    expect(codes).toContain('MULTIPLE_TRIGGERS');
    // One issue per offending trigger, mirroring the frontend.
    const multi = result.errors.filter((e) => e.code === 'MULTIPLE_TRIGGERS');
    expect(multi).toHaveLength(2);
    expect(multi.map((e) => e.nodeId).sort()).toEqual(['t1', 't2']);
  });

  test('CYCLE_DETECTED on a 3-node cycle, edgeId points at the closing edge', () => {
    const def = emptyDef({
      nodes: [
        { id: 't1', type: 'trigger' },
        { id: 'a', type: 'transform' },
        { id: 'b', type: 'transform' },
      ],
      edges: [
        { id: 'e1', source: 't1', target: 'a' },
        { id: 'e2', source: 'a', target: 'b' },
        { id: 'e3', source: 'b', target: 'a' }, // back-edge → cycle
      ],
    });
    const result = validatePipelineStructural(def);
    expect(result.isValid).toBe(false);
    const cycleErr = result.errors.find((e) => e.code === 'CYCLE_DETECTED');
    expect(cycleErr).toBeDefined();
    expect(cycleErr!.severity).toBe('error');
    expect(cycleErr!.edgeId).toBe('e3');
  });

  test('no cycle on a DAG', () => {
    const def = emptyDef({
      nodes: [
        { id: 't1', type: 'trigger' },
        { id: 'a', type: 'transform' },
        { id: 'b', type: 'transform' },
      ],
      edges: [
        { id: 'e1', source: 't1', target: 'a' },
        { id: 'e2', source: 'a', target: 'b' },
      ],
    });
    const result = validatePipelineStructural(def);
    const cycleErr = result.errors.find((e) => e.code === 'CYCLE_DETECTED');
    expect(cycleErr).toBeUndefined();
    expect(result.isValid).toBe(true);
  });

  test('warnings array is always present (deferred lints — empty for v1)', () => {
    const def = emptyDef({ nodes: [{ id: 't1', type: 'trigger' }] });
    const result = validatePipelineStructural(def);
    expect(Array.isArray(result.warnings)).toBe(true);
    expect(result.warnings).toEqual([]);
  });
});

// ---------------------------------------------------------------------------
// HTTP wiring — POST /pipelines/validate.
// ---------------------------------------------------------------------------

describe('POST /pipelines/validate', () => {
  test('400 when body is missing', async () => {
    const app = buildApp();
    const res = await postValidate(app, undefined);
    expect(res.statusCode).toBe(400);
  });

  test('400 when definition.nodes is not an array', async () => {
    const app = buildApp();
    const res = await postValidate(app, { definition: { id: 'x', nodes: null, edges: [] } });
    expect(res.statusCode).toBe(400);
  });

  test('400 when definition.edges is not an array', async () => {
    const app = buildApp();
    const res = await postValidate(app, {
      definition: { id: 'x', nodes: [], edges: 'nope' },
    });
    expect(res.statusCode).toBe(400);
  });

  test('200 with NO_TRIGGER for an empty definition', async () => {
    const app = buildApp();
    const res = await postValidate(app, {
      definition: emptyDef(),
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof validatePipelineStructural>;
    expect(body.isValid).toBe(false);
    expect(body.errors[0].code).toBe('NO_TRIGGER');
  });

  test('200 with isValid=true on a single-trigger pipeline', async () => {
    const app = buildApp();
    const res = await postValidate(app, {
      definition: emptyDef({
        nodes: [{ id: 't1', type: 'trigger' }],
      }),
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof validatePipelineStructural>;
    expect(body.isValid).toBe(true);
    expect(body.canPublish).toBe(true);
    expect(body.errors).toEqual([]);
  });

  test('200 with CYCLE_DETECTED edgeId on a cyclic graph', async () => {
    const app = buildApp();
    const res = await postValidate(app, {
      definition: emptyDef({
        nodes: [
          { id: 't1', type: 'trigger' },
          { id: 'a', type: 'transform' },
        ],
        edges: [
          { id: 'e1', source: 't1', target: 'a' },
          { id: 'e2', source: 'a', target: 't1' },
        ],
      }),
    });
    expect(res.statusCode).toBe(200);
    const body = res.body as ReturnType<typeof validatePipelineStructural>;
    const cycleErr = body.errors.find((e) => e.code === 'CYCLE_DETECTED');
    expect(cycleErr).toBeDefined();
    expect(cycleErr!.edgeId).toBe('e2');
  });
});
