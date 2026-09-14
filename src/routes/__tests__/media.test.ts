jest.mock('../../repositories', () => ({ mediaCatalogRepo: {} }));
jest.mock('../../media/mediaCatalogPipeline', () => ({ startMediaCatalogRun: jest.fn() }));

import { validateBatch, MAX_BATCH_FILES } from '../media';

const ID = 'a'.repeat(64);
const good = () => ({ batchId: 'b1', root: '/v', files: [{ fileId: ID, path: '/v/a.jpg', modifiedAt: '2026-09-14T12:00:00.000Z', sizeBytes: 12, mimeType: 'image/jpeg', kind: 'image', dhash: 'ffff0000ffff0000' }] });

describe('validateBatch', () => {
  it('accepts a well-formed batch and fills the optional fields', () => {
    const b = validateBatch(good());
    expect(b.files[0]).toMatchObject({ fileId: ID, relPath: '/v/a.jpg', kind: 'image', capturedAt: null, cameraId: null, width: null, thumbnail: null });
  });
  it('refuses what the walker must never send', () => {
    expect(() => validateBatch({ ...good(), files: [] })).toThrow(/non-empty/);
    expect(() => validateBatch({ ...good(), files: Array.from({ length: MAX_BATCH_FILES + 1 }, () => good().files[0]) })).toThrow(/at most/);
    expect(() => validateBatch({ ...good(), files: [{ ...good().files[0], fileId: 'nope' }] })).toThrow(/sha256/);
    expect(() => validateBatch({ ...good(), files: [{ ...good().files[0], dhash: 'zz' }] })).toThrow(/dhash/);
    expect(() => validateBatch({ ...good(), files: [{ ...good().files[0], thumbnail: { base64: 'A'.repeat(600 * 1024) } }] })).toThrow(/exceeds/);
  });
});
