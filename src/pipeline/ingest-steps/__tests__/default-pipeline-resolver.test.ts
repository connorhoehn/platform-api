// social-api/src/pipeline/ingest-steps/__tests__/default-pipeline-resolver.test.ts
//
// Verifies the precedence order specified in AGENTIC_ETL_VISION.md §C:
//   1. DocType.defaultPipelineId  → 'doctype'
//   2. Source.defaultPipelineId   → 'source'
//   3. neither                    → 'none' (no pipeline runs, just materialize)

import { createDefaultPipelineResolver } from '../default-pipeline-resolver';
import type { IDocTypeLookup, ISourceLookup } from '../types';
import type { IngestSource } from '../../../ingest/types';

function makeSource(overrides: Partial<IngestSource> = {}): IngestSource {
  return {
    sourceId: 'src-1',
    name: 'Source 1',
    type: 'local',
    config: {},
    syncSchedule: null,
    mappingRules: [],
    defaultPipelineId: null,
    status: 'active',
    lastSyncAt: null,
    lastSyncStats: null,
    createdAt: '2026-01-01T00:00:00Z',
    updatedAt: '2026-01-01T00:00:00Z',
    ...overrides,
  };
}

describe('resolveDefaultPipelineId — precedence', () => {
  test('layer 1: DocType wins when it declares a default', async () => {
    const docTypeLookup: IDocTypeLookup = {
      getDefaultPipelineId: jest.fn(async () => 'pipe-from-type'),
    };
    const sourceLookup: ISourceLookup = {
      getById: jest.fn(async () => makeSource({ defaultPipelineId: 'pipe-from-source' })),
    };
    const resolve = createDefaultPipelineResolver({ docTypeLookup, sourceLookup });

    const r = await resolve({ documentTypeId: 't1', sourceId: 'src-1' });
    expect(r).toEqual({ pipelineId: 'pipe-from-type', via: 'doctype' });
    // When the type wins we don't need to consult the source.
    expect(sourceLookup.getById).not.toHaveBeenCalled();
  });

  test('layer 2: source fallback when DocType has no default', async () => {
    const docTypeLookup: IDocTypeLookup = {
      getDefaultPipelineId: jest.fn(async () => null),
    };
    const sourceLookup: ISourceLookup = {
      getById: jest.fn(async () => makeSource({ defaultPipelineId: 'pipe-from-source' })),
    };
    const resolve = createDefaultPipelineResolver({ docTypeLookup, sourceLookup });

    const r = await resolve({ documentTypeId: 't1', sourceId: 'src-1' });
    expect(r).toEqual({ pipelineId: 'pipe-from-source', via: 'source' });
  });

  test('layer 3: none — neither layer has a default', async () => {
    const docTypeLookup: IDocTypeLookup = {
      getDefaultPipelineId: jest.fn(async () => null),
    };
    const sourceLookup: ISourceLookup = {
      getById: jest.fn(async () => makeSource({ defaultPipelineId: null })),
    };
    const resolve = createDefaultPipelineResolver({ docTypeLookup, sourceLookup });

    const r = await resolve({ documentTypeId: 't1', sourceId: 'src-1' });
    expect(r).toEqual({ pipelineId: null, via: 'none' });
  });

  test('layer 3: none — source not found', async () => {
    const docTypeLookup: IDocTypeLookup = {
      getDefaultPipelineId: jest.fn(async () => null),
    };
    const sourceLookup: ISourceLookup = {
      getById: jest.fn(async () => null),
    };
    const resolve = createDefaultPipelineResolver({ docTypeLookup, sourceLookup });

    const r = await resolve({ documentTypeId: 't1', sourceId: 'missing' });
    expect(r).toEqual({ pipelineId: null, via: 'none' });
  });

  test('empty-string defaults are NOT treated as a valid pipeline id', async () => {
    // Defensive: a row with `defaultPipelineId: ''` (instead of null) should
    // fall through. The interface returns string | null; the resolver
    // treats falsy values as "no default".
    const docTypeLookup: IDocTypeLookup = {
      getDefaultPipelineId: jest.fn(async () => '' as unknown as null),
    };
    const sourceLookup: ISourceLookup = {
      getById: jest.fn(async () => makeSource({ defaultPipelineId: 'pipe-from-source' })),
    };
    const resolve = createDefaultPipelineResolver({ docTypeLookup, sourceLookup });

    const r = await resolve({ documentTypeId: 't1', sourceId: 'src-1' });
    expect(r.pipelineId).toBe('pipe-from-source');
    expect(r.via).toBe('source');
  });
});
