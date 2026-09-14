import { groupBursts, hamming, burstIdFor, type BurstCandidate } from '../burstGroup';

const ID = (n: number) => n.toString(16).padStart(64, '0');
const at = (sec: number) => new Date(Date.UTC(2026, 8, 14, 12, 0, sec)).toISOString();
const frame = (n: number, sec: number, over: Partial<BurstCandidate> = {}): BurstCandidate => ({
  fileId: ID(n), capturedAt: at(sec), modifiedAt: at(sec), cameraId: 'Sony ILCE-7M4', dhash: 'ffff0000ffff0000', ...over,
});

describe('hamming', () => {
  it('counts differing bits; a missing hash is maximally different', () => {
    expect(hamming('ffff0000ffff0000', 'ffff0000ffff0000')).toBe(0);
    expect(hamming('ffff0000ffff0000', 'ffff0000ffff0001')).toBe(1);
    expect(hamming('0000000000000000', 'ffffffffffffffff')).toBe(64);
    expect(hamming(null, 'ffff0000ffff0000')).toBe(64);
  });
});

describe('groupBursts', () => {
  it('a held shutter is one burst with the middle frame as its representative', () => {
    const bursts = groupBursts([frame(1, 0), frame(2, 1), frame(3, 2), frame(4, 3)]);
    expect(bursts).toHaveLength(1);
    expect(bursts[0].fileIds).toEqual([ID(1), ID(2), ID(3), ID(4)]);
    expect(bursts[0].representativeFileId).toBe(ID(2));
    expect(bursts[0].burstId).toBe(burstIdFor(ID(1)));
  });

  it('a gap longer than the window starts a new burst', () => {
    const bursts = groupBursts([frame(1, 0), frame(2, 1), frame(3, 30)]);
    expect(bursts.map((b) => b.fileIds.length)).toEqual([2, 1]);
  });

  it('a different camera or different content is a different moment, however close in time', () => {
    const other = groupBursts([frame(1, 0), frame(2, 1, { cameraId: 'Apple iPhone 15' })]);
    expect(other).toHaveLength(2);
    const moved = groupBursts([frame(1, 0), frame(2, 1, { dhash: '0000ffff0000ffff' })]);
    expect(moved).toHaveLength(2);
  });

  it('sorts by capture time, falling back to mtime for files with none', () => {
    const noExif = frame(9, 0, { capturedAt: null, modifiedAt: at(1) });
    const bursts = groupBursts([frame(2, 2), noExif, frame(1, 0)]);
    expect(bursts).toHaveLength(1);
    expect(bursts[0].fileIds).toEqual([ID(1), ID(9), ID(2)]);
  });

  it('every file lands in exactly one burst; a lone frame is a burst of one', () => {
    const input = [frame(1, 0), frame(2, 100), frame(3, 200)];
    const bursts = groupBursts(input);
    expect(bursts.flatMap((b) => b.fileIds).sort()).toEqual(input.map((f) => f.fileId).sort());
    expect(bursts.every((b) => b.fileIds.length === 1)).toBe(true);
  });

  it('regroups identically on a rerun over the same files', () => {
    const input = [frame(1, 0), frame(2, 1)];
    expect(groupBursts(input)).toEqual(groupBursts([...input].reverse()));
  });
});
