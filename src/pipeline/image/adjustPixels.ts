// platform-api/src/pipeline/image/adjustPixels.ts
//
// The per-pixel half of `image:adjust-layer`: the Light / Color kinds a photo
// panel needs (exposure, white balance, highlights / shadows / whites /
// blacks, vibrance) plus tone curves and vignette, on raw sRGB bytes.
//
// The formulas mirror ui-components `photo-editor/model.ts` `adjustRgb`
// line for line (the live preview's WebGL shader mirrors the same function),
// so what the editor previews is what a saved render writes. Order within a
// run of per-pixel layers: exposure → white balance → contrast → tone →
// saturation / vibrance → hue → brightness, then curve, then vignette —
// the preview's fixed order. Change one side, change the other.

export type PixelLayer =
  | { kind: 'exposure'; stops: number }
  | { kind: 'highlights' | 'shadows' | 'whites' | 'blacks' | 'vibrance' | 'tint' | 'vignette'; amount: number }
  | { kind: 'temperature'; kelvin: number }
  | { kind: 'contrast' | 'saturation' | 'brightness'; value: number }
  | { kind: 'hue'; degrees: number }
  | { kind: 'curve'; channel: 'rgb' | 'r' | 'g' | 'b'; points: Array<[number, number]> };

export const PIXEL_KINDS = new Set([
  'exposure', 'highlights', 'shadows', 'whites', 'blacks', 'vibrance', 'temperature', 'tint', 'curve', 'vignette',
]);

export const NEUTRAL_KELVIN = 5500;

export interface AdjustParams {
  exposureStops: number; warmth: number; tint: number; contrast: number;
  highlights: number; shadows: number; whites: number; blacks: number;
  saturation: number; vibrance: number; hueDegrees: number; brightness: number;
}

const NEUTRAL: AdjustParams = {
  exposureStops: 0, warmth: 0, tint: 0, contrast: 1, highlights: 0, shadows: 0, whites: 0, blacks: 0,
  saturation: 1, vibrance: 0, hueDegrees: 0, brightness: 1,
};

const clamp = (v: number, lo: number, hi: number) => Math.min(hi, Math.max(lo, v));
const smoothstep = (e0: number, e1: number, x: number) => {
  const t = clamp((x - e0) / (e1 - e0), 0, 1);
  return t * t * (3 - 2 * t);
};

export function paramsFromLayers(layers: readonly PixelLayer[]): AdjustParams {
  const p = { ...NEUTRAL };
  for (const op of layers) {
    switch (op.kind) {
      case 'brightness': p.brightness = op.value; break;
      case 'contrast': p.contrast = op.value; break;
      case 'saturation': p.saturation = op.value; break;
      case 'hue': p.hueDegrees = op.degrees; break;
      case 'exposure': p.exposureStops = clamp(op.stops, -5, 5); break;
      case 'highlights': p.highlights = clamp(op.amount, -100, 100) / 100; break;
      case 'shadows': p.shadows = clamp(op.amount, -100, 100) / 100; break;
      case 'whites': p.whites = clamp(op.amount, -100, 100) / 100; break;
      case 'blacks': p.blacks = clamp(op.amount, -100, 100) / 100; break;
      case 'vibrance': p.vibrance = clamp(op.amount, -100, 100) / 100; break;
      case 'temperature': {
        const k = clamp(op.kelvin, 2000, 12000);
        p.warmth = k >= NEUTRAL_KELVIN ? (k - NEUTRAL_KELVIN) / 6500 : (k - NEUTRAL_KELVIN) / 3500;
        break;
      }
      case 'tint': p.tint = clamp(op.amount, -150, 150) / 150; break;
      default: break;
    }
  }
  return p;
}

function hueRotateMatrix(degrees: number): number[] {
  const a = (degrees * Math.PI) / 180;
  const c = Math.cos(a), s = Math.sin(a);
  return [
    0.213 + c * 0.787 - s * 0.213, 0.715 - c * 0.715 - s * 0.715, 0.072 - c * 0.072 + s * 0.928,
    0.213 - c * 0.213 + s * 0.143, 0.715 + c * 0.285 + s * 0.140, 0.072 - c * 0.072 - s * 0.283,
    0.213 - c * 0.213 - s * 0.787, 0.715 - c * 0.715 + s * 0.715, 0.072 + c * 0.928 + s * 0.072,
  ];
}

/** One pixel, sRGB 0..1. Mirrors ui-components `adjustRgb`. */
export function adjustRgb(rgb: readonly [number, number, number], p: AdjustParams): [number, number, number] {
  let [r, g, b] = rgb;
  const e = Math.pow(2, p.exposureStops);
  r *= e; g *= e; b *= e;
  r *= 1 + 0.2 * p.warmth; b *= 1 - 0.2 * p.warmth;
  g *= 1 - 0.15 * p.tint;
  r = (r - 0.5) * p.contrast + 0.5; g = (g - 0.5) * p.contrast + 0.5; b = (b - 0.5) * p.contrast + 0.5;
  let l = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  const d = p.highlights * 0.25 * smoothstep(0.5, 1, l)
    + p.shadows * 0.25 * (1 - smoothstep(0, 0.5, l))
    + p.whites * 0.15 * smoothstep(0.6, 1, l)
    + p.blacks * 0.15 * (1 - smoothstep(0, 0.4, l));
  r += d; g += d; b += d;
  l = 0.2126 * r + 0.7152 * g + 0.0722 * b;
  const sat = Math.max(r, g, b) - Math.min(r, g, b);
  const s = p.saturation * (1 + p.vibrance * (1 - clamp(sat, 0, 1)));
  r = l + (r - l) * s; g = l + (g - l) * s; b = l + (b - l) * s;
  if (p.hueDegrees !== 0) {
    const m = hueRotateMatrix(p.hueDegrees);
    [r, g, b] = [m[0] * r + m[1] * g + m[2] * b, m[3] * r + m[4] * g + m[5] * b, m[6] * r + m[7] * g + m[8] * b];
  }
  r *= p.brightness; g *= p.brightness; b *= p.brightness;
  return [clamp(r, 0, 1), clamp(g, 0, 1), clamp(b, 0, 1)];
}

/** Piecewise-linear 256-entry LUT through 0..255 control points. */
export function curveLut(points: ReadonlyArray<readonly [number, number]>): Uint8Array {
  const p = [...points].map(([x, y]) => [clamp(x, 0, 255), clamp(y, 0, 255)] as const).sort((a, b) => a[0] - b[0]);
  if (p.length === 0 || p[0][0] > 0) p.unshift([0, p[0]?.[1] ?? 0]);
  if (p[p.length - 1][0] < 255) p.push([255, p[p.length - 1][1]]);
  const lut = new Uint8Array(256);
  let j = 0;
  for (let i = 0; i < 256; i++) {
    while (j < p.length - 2 && p[j + 1][0] < i) j++;
    const [x0, y0] = p[j];
    const [x1, y1] = p[j + 1];
    const t = x1 === x0 ? 0 : (i - x0) / (x1 - x0);
    lut[i] = Math.round(clamp(y0 + (y1 - y0) * t, 0, 255));
  }
  return lut;
}

/**
 * Apply per-pixel layers to raw bytes (`channels` 3 or 4) in place. With a
 * `mask` (one byte per pixel, 0..255), the result is blended with the input
 * by the mask's value — the layer applies inside the mask only.
 */
export function applyPixelLayers(
  data: Buffer | Uint8Array,
  width: number,
  height: number,
  channels: number,
  layers: readonly PixelLayer[],
  mask?: Uint8Array | null,
): void {
  const p = paramsFromLayers(layers);
  const curves = layers.filter((l): l is Extract<PixelLayer, { kind: 'curve' }> => l.kind === 'curve')
    .map((l) => ({ channel: l.channel, lut: curveLut(l.points) }));
  const vignette = layers.filter((l) => l.kind === 'vignette').reduce((a, l) => ('amount' in l ? l.amount : a), 0);
  const cx = width / 2, cy = height / 2;
  const rmax = Math.sqrt(cx * cx + cy * cy) || 1;
  const rgb: [number, number, number] = [0, 0, 0];
  for (let px = 0, i = 0; px < width * height; px++, i += channels) {
    const m = mask ? mask[px] / 255 : 1;
    if (m === 0) continue;
    rgb[0] = data[i] / 255; rgb[1] = data[i + 1] / 255; rgb[2] = data[i + 2] / 255;
    let [r, g, b] = adjustRgb(rgb, p);
    for (const c of curves) {
      if (c.channel === 'rgb' || c.channel === 'r') r = c.lut[Math.round(r * 255)] / 255;
      if (c.channel === 'rgb' || c.channel === 'g') g = c.lut[Math.round(g * 255)] / 255;
      if (c.channel === 'rgb' || c.channel === 'b') b = c.lut[Math.round(b * 255)] / 255;
    }
    if (vignette) {
      const x = (px % width) - cx, y = Math.floor(px / width) - cy;
      const d = Math.sqrt(x * x + y * y) / rmax;
      const f = 1 + (clamp(vignette, -100, 100) / 100) * 0.7 * d * d;
      r = clamp(r * f, 0, 1); g = clamp(g * f, 0, 1); b = clamp(b * f, 0, 1);
    }
    data[i] = Math.round((r * m + rgb[0] * (1 - m)) * 255);
    data[i + 1] = Math.round((g * m + rgb[1] * (1 - m)) * 255);
    data[i + 2] = Math.round((b * m + rgb[2] * (1 - m)) * 255);
  }
}
