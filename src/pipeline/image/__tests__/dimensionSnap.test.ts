import { snapGenerationDims, aspectRatio } from '../dimensionSnap';

describe('snapGenerationDims', () => {
  it('passes through already-valid dims', () => {
    expect(snapGenerationDims(512, 512)).toEqual({ width: 512, height: 512 });
    expect(snapGenerationDims(1024, 768)).toEqual({ width: 1024, height: 768 });
  });

  it('rounds to the nearest multiple of 16', () => {
    expect(snapGenerationDims(500, 500)).toEqual({ width: 496, height: 496 });
    expect(snapGenerationDims(510, 510)).toEqual({ width: 512, height: 512 });
  });

  it('downscales to fit maxEdge while preserving aspect', () => {
    // 4000x3000 (4:3) → longest 4000 scaled to 1024 → 1024x768
    expect(snapGenerationDims(4000, 3000)).toEqual({ width: 1024, height: 768 });
  });

  it('honors a custom maxEdge', () => {
    expect(snapGenerationDims(2000, 1000, { maxEdge: 512 })).toEqual({ width: 512, height: 256 });
  });

  it('clamps to minEdge', () => {
    expect(snapGenerationDims(50, 50)).toEqual({ width: 256, height: 256 });
  });

  it('falls back to maxEdge for non-finite/zero input', () => {
    expect(snapGenerationDims(0, 0)).toEqual({ width: 1024, height: 1024 });
    expect(snapGenerationDims(NaN, 600)).toEqual({ width: 1024, height: 608 });
  });

  it('keeps both edges within [minEdge, maxEdge] after rounding', () => {
    const { width, height } = snapGenerationDims(1023, 1021);
    expect(width).toBeLessThanOrEqual(1024);
    expect(height).toBeLessThanOrEqual(1024);
    expect(width % 16).toBe(0);
    expect(height % 16).toBe(0);
  });
});

describe('aspectRatio', () => {
  it('computes width/height to 4dp', () => {
    expect(aspectRatio(1024, 768)).toBeCloseTo(1.3333, 4);
    expect(aspectRatio(512, 512)).toBe(1);
  });
  it('returns 1 for zero height', () => {
    expect(aspectRatio(512, 0)).toBe(1);
  });
});
