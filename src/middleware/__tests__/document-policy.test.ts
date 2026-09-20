import type { Request, Response, NextFunction } from 'express';
import { createDocumentPolicy, documentOperation, requireDocumentPolicy } from '../document-policy';
import type { DocumentGrantClaims } from '@connorhoehn/realtime-modules/server';

const DOC = 'b66ba94c-3a52-4fe9-a37a-6ff10d91b069';
const claims = {
  iss: 'assessment-bff', aud: 'realtime-documents', sub: 'u-alice', tenantId: 'assessment',
  documentId: DOC, operations: ['read', 'comment'], sessionId: 'session-1', sessionEpoch: 2,
  iat: 100, exp: 200, jti: 'grant-1',
} as DocumentGrantClaims;

function response() {
  const res = { status: jest.fn(), json: jest.fn() };
  res.status.mockReturnValue(res); res.json.mockReturnValue(res);
  return res;
}
function request(changes: Partial<Request> = {}): Request {
  return { method: 'GET', path: '/comments', params: { documentId: DOC },
    headers: { authorization: 'Bearer grant' }, ...changes } as Request;
}

describe('document grants at the REST boundary', () => {
  it('binds the verified subject, document and tenant', async () => {
    const req = request(); const res = response(); const next = jest.fn();
    await createDocumentPolicy({ tenantId: 'assessment', verify: async () => claims })(req, res as unknown as Response, next);
    expect(next).toHaveBeenCalledTimes(1);
    expect(req.user).toEqual({ sub: 'u-alice', orgId: 'assessment' });
    expect(req.documentGrant).toBe(claims);
  });
  it.each([
    ['another document', { documentId: '014a2273-0d03-4470-b9f0-17b9a1bbb274' }],
    ['another tenant', { tenantId: 'orgiq' }],
    ['missing operation', { operations: ['comment'] }],
  ])('refuses %s before repository work', async (_label, overrides) => {
    const res = response(); const next = jest.fn();
    await createDocumentPolicy({ tenantId: 'assessment', verify: async () => ({ ...claims, ...overrides }) as DocumentGrantClaims })(request(), res as unknown as Response, next);
    expect(res.status).toHaveBeenCalledWith(403);
    expect(next).not.toHaveBeenCalled();
  });
  it('does not turn a read grant into edit access', async () => {
    const res = response(); const next = jest.fn();
    await createDocumentPolicy({ tenantId: 'assessment', verify: async () => claims })(request({ method: 'POST', path: '/import' }), res as unknown as Response, next);
    expect(res.status).toHaveBeenCalledWith(403);
    expect(next).not.toHaveBeenCalled();
  });
  it('fails closed when signature, expiry or live session epoch verification fails', async () => {
    const res = response(); const next = jest.fn();
    await createDocumentPolicy({ tenantId: 'assessment', verify: async () => { throw new Error('revoked'); } })(request(), res as unknown as Response, next);
    expect(res.status).toHaveBeenCalledWith(401);
    expect(next).not.toHaveBeenCalled();
  });
  it('requires a credential', async () => {
    const res = response(); const next = jest.fn(); const verify = jest.fn();
    await createDocumentPolicy({ tenantId: 'assessment', verify })(request({ headers: {} }), res as unknown as Response, next);
    expect(res.status).toHaveBeenCalledWith(401);
    expect(verify).not.toHaveBeenCalled();
  });
  it('refuses an unconfigured production deployment', async () => {
    const before = process.env.NODE_ENV;
    process.env.NODE_ENV = 'production';
    try {
      const res = response(); const next = jest.fn();
      await requireDocumentPolicy(request(), res as unknown as Response, next as NextFunction);
      expect(res.status).toHaveBeenCalledWith(503);
      expect(next).not.toHaveBeenCalled();
    } finally { process.env.NODE_ENV = before; }
  });
});

it.each([
  ['GET', '/comments', undefined, 'read'],
  ['POST', '/comments', {}, 'comment'],
  ['PATCH', '/comments/c1', { text: 'edit' }, 'comment'],
  ['POST', '/lifecycle/transitions', { action: 'approve' }, 'comment'],
  ['POST', '/lifecycle/transitions', { action: 'reopen' }, 'edit'],
  ['POST', '/lifecycle/ownership', {}, 'manage'],
  ['POST', '/review-requests', {}, 'manage'],
  ['PATCH', '/review-requests/r1', { state: 'approved' }, 'comment'],
  ['PATCH', '/review-requests/r1', { state: 'approved', dueDate: null }, 'manage'],
  ['POST', '/sections/s1/items/i1/answer', {}, 'comment'],
  ['POST', '/sections/s1/items', {}, 'edit'],
])('maps %s %s to a document operation', (method, path, body, expected) => {
  expect(documentOperation({ method, path, body })).toBe(expected);
});
