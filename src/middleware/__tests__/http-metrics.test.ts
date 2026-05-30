// Unit tests for the http-metrics middleware (hub#1005).
//
// Asserts (a) the route-group derivation produces the bounded enum we
// expect, and (b) the middleware observes a duration after res.on('finish')
// fires. The metric observation is verified through the real
// MetricsRegistry — no module-level mocks — so the test exercises the
// same code path operators read from PromQL.

import type { Request, Response, NextFunction } from 'express';
import { EventEmitter } from 'events';
import { deriveRouteGroup, httpMetrics } from '../http-metrics';
import { getRegistry } from '../../observability/metrics';

beforeEach(() => {
  getRegistry().reset();
});

describe('deriveRouteGroup', () => {
  it.each<[string, string]>([
    ['/health', 'health'],
    ['/api/health', 'health'],
    ['/internal/metrics', 'internal'],
    ['/hooks/lvs/recording', 'hooks'],
    ['/hooks/pipeline/abc', 'hooks'],
    ['/api/admin/sessions', 'api_admin'],
    ['/api/pipelines', 'api_pipelines'],
    ['/api/pipelines/p1/runs', 'api_pipelines'],
    ['/api/video/sessions', 'api_video'],
    ['/api/recordings/r1/share', 'api_recordings'],
    ['/api/rooms/abc-room/members', 'api_rooms'],
    ['/api/document-types/dt-1', 'api_document_types'],
    ['/api/typed-documents/bulk-import', 'api_typed_documents'],
    ['/api/ingest/sources', 'api_ingest'],
    ['/api/capabilities', 'api_capabilities'],
    ['/api/something-new', 'api_other'],
    ['/openapi/pipelines.yaml', 'other'],
    ['/api/pipelines?cursor=abc', 'api_pipelines'], // query string stripped
  ])('%s → %s', (url, expected) => {
    expect(deriveRouteGroup(url)).toBe(expected);
  });
});

describe('httpMetrics middleware', () => {
  it('observes a duration into http_handler_duration_seconds on res.finish', () => {
    const req = { originalUrl: '/api/pipelines/p1' } as Request;
    const res = new EventEmitter() as unknown as Response;
    const next = jest.fn() as unknown as NextFunction;

    httpMetrics(req, res, next);
    (res as unknown as EventEmitter).emit('finish');

    const snap = getRegistry().getSnapshot();
    const m = snap.metrics.find(
      (s) =>
        s.name === 'http_handler_duration_seconds' &&
        s.labels.route_group === 'api_pipelines',
    );
    expect(m).toBeDefined();
    const hist = m!.value as { count: number };
    expect(hist.count).toBe(1);
    expect(next).toHaveBeenCalled();
  });
});
