// platform-api/src/media/burstGroup.ts
//
// Burst detection is not a vision call. A burst is what a camera produces
// when the shutter is held: the same body, frames seconds apart, near-identical
// content. That is three facts already in the file — capture time, camera,
// and a perceptual hash — so grouping is a pure pass over a sorted list, and
// it runs BEFORE any classification so the classifier scores one frame per
// moment instead of every frame of it.
//
// Pure: no I/O, no clock. The walker computes the hash where the bytes are;
// this only compares.

export interface BurstCandidate {
  fileId: string;
  /** ISO capture time (EXIF DateTimeOriginal); null when the file has none. */
  capturedAt: string | null;
  /** ISO filesystem mtime — the fallback clock for files with no EXIF. */
  modifiedAt: string;
  /** `Make Model` from EXIF; null when unknown. */
  cameraId: string | null;
  /** 64-bit difference hash as 16 hex chars; null for files that have none (video). */
  dhash: string | null;
}

export interface Burst {
  burstId: string;
  fileIds: string[];
  /** The frame the classifier scores for the whole burst. */
  representativeFileId: string;
}

export interface BurstOptions {
  /** Two frames further apart than this are never one burst. Default 3 s. */
  windowMs?: number;
  /** Hamming distance between adjacent dhashes beyond which frames differ. Default 10 of 64. */
  maxHamming?: number;
}

export function shotAt(c: Pick<BurstCandidate, 'capturedAt' | 'modifiedAt'>): number {
  const t = Date.parse(c.capturedAt ?? c.modifiedAt);
  return Number.isFinite(t) ? t : 0;
}

/** Bits that differ between two 64-bit hex hashes. 64 when either is missing. */
export function hamming(a: string | null, b: string | null): number {
  if (!a || !b || a.length !== 16 || b.length !== 16) return 64;
  let n = 0;
  for (let i = 0; i < 16; i++) {
    let x = parseInt(a[i]!, 16) ^ parseInt(b[i]!, 16);
    while (x) { n += x & 1; x >>= 1; }
  }
  return n;
}

/** A stable id from the first frame: reruns over the same files regroup identically. */
export function burstIdFor(firstFileId: string): string {
  return `burst-${firstFileId.slice(0, 16)}`;
}

/**
 * Group frames into bursts. Every file lands in exactly one burst; a lone
 * frame is a burst of one. Adjacent in time, same camera (or both unknown),
 * and near-identical content — all three, or it is a new moment.
 */
export function groupBursts(files: readonly BurstCandidate[], opts: BurstOptions = {}): Burst[] {
  const windowMs = opts.windowMs ?? 3000;
  const maxHamming = opts.maxHamming ?? 10;
  const sorted = [...files].sort((a, b) => shotAt(a) - shotAt(b) || a.fileId.localeCompare(b.fileId));
  const bursts: Burst[] = [];
  let current: BurstCandidate[] = [];
  const flush = () => {
    if (current.length === 0) return;
    const first = current[0]!;
    bursts.push({
      burstId: burstIdFor(first.fileId),
      fileIds: current.map((c) => c.fileId),
      // The middle frame: in a held-shutter burst the first frame carries the
      // press and the last the release, and the one between is the steadiest.
      representativeFileId: current[Math.floor((current.length - 1) / 2)]!.fileId,
    });
    current = [];
  };
  for (const f of sorted) {
    const prev = current[current.length - 1];
    const continues =
      prev !== undefined &&
      shotAt(f) - shotAt(prev) <= windowMs &&
      prev.cameraId === f.cameraId &&
      hamming(prev.dhash, f.dhash) <= maxHamming;
    if (!continues) flush();
    current.push(f);
  }
  flush();
  return bursts;
}
