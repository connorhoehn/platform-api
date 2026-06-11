// platform-api/src/pipeline/image/dimensionSnap.ts
//
// Diffusion image models (FLUX included) require generation dimensions
// that are multiples of 16 — the VAE downsamples by 8 and the latent is
// patchified by 2. They also have a practical max edge (VRAM-bound) and
// a sensible min. So "use the input image's dimensions" can't be taken
// literally: we preserve the input's aspect ratio but snap to the
// nearest valid generation size.

export interface SnapOptions {
  /** Largest allowed edge (px). Default 1024 (klein known-good). */
  maxEdge?: number;
  /** Smallest allowed edge (px). Default 256. */
  minEdge?: number;
  /** Required divisor. Default 16. */
  multiple?: number;
}

export interface SnappedDims {
  width: number;
  height: number;
}

function roundToMultiple(n: number, m: number): number {
  return Math.max(m, Math.round(n / m) * m);
}

/**
 * Snap an arbitrary (width, height) to a valid generation size while
 * preserving aspect ratio as closely as the divisor allows.
 *
 * - Scales so the longer edge fits within `maxEdge` (never upscales past it).
 * - Rounds each edge to the nearest `multiple`.
 * - Clamps each edge to [minEdge, maxEdge].
 */
export function snapGenerationDims(
  width: number,
  height: number,
  opts: SnapOptions = {},
): SnappedDims {
  const maxEdge = opts.maxEdge ?? 1024;
  const minEdge = opts.minEdge ?? 256;
  const multiple = opts.multiple ?? 16;

  const w0 = Number.isFinite(width) && width > 0 ? width : maxEdge;
  const h0 = Number.isFinite(height) && height > 0 ? height : maxEdge;

  // Downscale to fit maxEdge, preserving aspect ratio.
  const longest = Math.max(w0, h0);
  const scale = longest > maxEdge ? maxEdge / longest : 1;

  let w = roundToMultiple(w0 * scale, multiple);
  let h = roundToMultiple(h0 * scale, multiple);

  // Clamp (rounding can push one edge a multiple over maxEdge).
  w = Math.min(maxEdge, Math.max(minEdge, w));
  h = Math.min(maxEdge, Math.max(minEdge, h));

  return { width: w, height: h };
}

/** Aspect ratio (width / height), rounded to 4dp; 1 when height is 0. */
export function aspectRatio(width: number, height: number): number {
  if (!height) return 1;
  return Math.round((width / height) * 10000) / 10000;
}
