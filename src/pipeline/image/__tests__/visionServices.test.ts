// platform-api/src/pipeline/image/__tests__/visionServices.test.ts
//
// Tests that the lazy singletons in visionServices.ts:
//   - return the same instance on repeated calls (lazy construction)
//   - return a fresh instance after __resetVisionServicesForTests()
//
// Mock conventions mirror ImageBlobStore.test.ts — aws-clients +
// ddb-table-name are both mocked before any import of the SUT so the
// DDB constructors never try to contact real AWS.

jest.mock('../../../lib/ddb-table-name', () => ({
  tableName: (name: string) => `test-${name}`,
}));
jest.mock('../../../lib/aws-clients', () => ({
  docClient: { send: jest.fn() },
}));

import {
  getImageBlobStore,
  getVisionResultRepo,
  getVisionClient,
  __resetVisionServicesForTests,
} from '../visionServices';

beforeEach(() => {
  // Start every test from a clean slate.
  __resetVisionServicesForTests();
});

// ---------------------------------------------------------------------------
// Singleton semantics
// ---------------------------------------------------------------------------

describe('getImageBlobStore', () => {
  it('returns the same instance on repeated calls', () => {
    const a = getImageBlobStore();
    const b = getImageBlobStore();
    expect(a).toBe(b);
  });

  it('after __reset, returns a NEW instance (different reference)', () => {
    const before = getImageBlobStore();
    __resetVisionServicesForTests();
    const after = getImageBlobStore();
    expect(after).not.toBe(before);
  });

  it('returns a non-null object', () => {
    expect(getImageBlobStore()).toBeTruthy();
  });
});

describe('getVisionResultRepo', () => {
  it('returns the same instance on repeated calls', () => {
    const a = getVisionResultRepo();
    const b = getVisionResultRepo();
    expect(a).toBe(b);
  });

  it('after __reset, returns a NEW instance', () => {
    const before = getVisionResultRepo();
    __resetVisionServicesForTests();
    const after = getVisionResultRepo();
    expect(after).not.toBe(before);
  });

  it('returns a non-null object', () => {
    expect(getVisionResultRepo()).toBeTruthy();
  });
});

describe('getVisionClient', () => {
  it('returns the same instance on repeated calls', () => {
    const a = getVisionClient();
    const b = getVisionClient();
    expect(a).toBe(b);
  });

  it('after __reset, returns a NEW instance', () => {
    const before = getVisionClient();
    __resetVisionServicesForTests();
    const after = getVisionClient();
    expect(after).not.toBe(before);
  });

  it('returns a non-null object', () => {
    expect(getVisionClient()).toBeTruthy();
  });
});

// ---------------------------------------------------------------------------
// Cross-service independence after reset
// ---------------------------------------------------------------------------

describe('__resetVisionServicesForTests', () => {
  it('resets all three singletons simultaneously', () => {
    const blobBefore   = getImageBlobStore();
    const repoBefore   = getVisionResultRepo();
    const clientBefore = getVisionClient();

    __resetVisionServicesForTests();

    expect(getImageBlobStore()).not.toBe(blobBefore);
    expect(getVisionResultRepo()).not.toBe(repoBefore);
    expect(getVisionClient()).not.toBe(clientBefore);
  });

  it('calling reset multiple times does not throw', () => {
    expect(() => {
      __resetVisionServicesForTests();
      __resetVisionServicesForTests();
    }).not.toThrow();
  });
});
