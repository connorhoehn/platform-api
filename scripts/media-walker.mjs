#!/usr/bin/env node
// platform-api/scripts/media-walker.mjs
//
// The host half of the media catalog. The drive is on the host and
// platform-api is in a pod, so everything that needs the BYTES happens here:
// walk, hash, read EXIF, difference-hash, and thumbnail. What leaves the host
// is a batch of probes (~30 KB per image), posted to /api/media/catalog/batches,
// which starts one `media-catalog` run per batch.
//
// Resumable: a state file remembers every path it has posted, keyed by size
// and mtime, so a rerun over 4 TB re-reads only what changed. Content-keyed
// on the server too, so re-posting is safe.
//
//   node scripts/media-walker.mjs --root /Volumes/nvme/Photos [--api http://localhost:13001]
//        [--batch 40] [--limit 200] [--state ~/.media-walker/<root-hash>.json]
//        [--token <jwt>] [--dry-run] [--reset]

import { createHash } from 'node:crypto';
import { createReadStream, promises as fs } from 'node:fs';
import { homedir } from 'node:os';
import { join, relative, extname, dirname } from 'node:path';
import sharp from 'sharp';

const IMAGE_EXT = new Set(['.jpg', '.jpeg', '.png', '.webp', '.tif', '.tiff', '.heic', '.heif', '.avif', '.gif', '.dng', '.arw', '.cr2', '.cr3', '.nef', '.raf']);
const VIDEO_EXT = new Set(['.mp4', '.mov', '.m4v', '.avi', '.mkv', '.mts', '.m2ts']);
const MIME = { '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.png': 'image/png', '.webp': 'image/webp', '.tif': 'image/tiff', '.tiff': 'image/tiff', '.heic': 'image/heic', '.heif': 'image/heif', '.avif': 'image/avif', '.gif': 'image/gif', '.dng': 'image/x-adobe-dng', '.arw': 'image/x-sony-arw', '.cr2': 'image/x-canon-cr2', '.cr3': 'image/x-canon-cr3', '.nef': 'image/x-nikon-nef', '.raf': 'image/x-fuji-raf', '.mp4': 'video/mp4', '.mov': 'video/quicktime', '.m4v': 'video/x-m4v', '.avi': 'video/x-msvideo', '.mkv': 'video/x-matroska', '.mts': 'video/mp2t', '.m2ts': 'video/mp2t' };
const SKIP_DIRS = new Set(['.git', 'node_modules', '.Trashes', '.Spotlight-V100', '.fseventsd', '@eaDir', '.photoslibrary']);

function parseArgs(argv) {
  const out = { api: process.env.PLATFORM_API_URL ?? 'http://localhost:13001', batch: 40, limit: Infinity, token: process.env.ID_TOKEN ?? null, dryRun: false, reset: false, root: process.env.MEDIA_ROOT ?? null, state: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    const next = () => argv[++i];
    if (a === '--root') out.root = next();
    else if (a === '--api') out.api = next();
    else if (a === '--batch') out.batch = Math.max(1, Math.min(100, Number(next())));
    else if (a === '--limit') out.limit = Number(next());
    else if (a === '--state') out.state = next();
    else if (a === '--token') out.token = next();
    else if (a === '--dry-run') out.dryRun = true;
    else if (a === '--reset') out.reset = true;
    else if (a === '-h' || a === '--help') { out.help = true; }
  }
  return out;
}

async function* walk(dir) {
  let entries;
  try { entries = await fs.readdir(dir, { withFileTypes: true }); } catch (err) { console.warn(`skip ${dir}: ${err.message}`); return; }
  entries.sort((a, b) => a.name.localeCompare(b.name));
  for (const e of entries) {
    if (e.name.startsWith('._')) continue;
    const p = join(dir, e.name);
    if (e.isDirectory()) { if (!SKIP_DIRS.has(e.name) && !e.name.endsWith('.photoslibrary')) yield* walk(p); }
    else if (e.isFile()) yield p;
  }
}

function sha256File(path) {
  return new Promise((resolve, reject) => {
    const h = createHash('sha256');
    createReadStream(path).on('data', (c) => h.update(c)).on('end', () => resolve(h.digest('hex'))).on('error', reject);
  });
}

// --- EXIF: the two things burst detection needs, read from the raw IFDs ----
// sharp hands back the EXIF blob; parsing DateTimeOriginal (0x9003), Make
// (0x010f) and Model (0x0110) is forty lines of TIFF and saves a dependency.
function parseExif(buf) {
  if (!buf || buf.length < 16) return {};
  let base = 0;
  if (buf.toString('ascii', 0, 4) === 'Exif') base = 6;
  const le = buf.toString('ascii', base, base + 2) === 'II';
  const u16 = (o) => (le ? buf.readUInt16LE(o) : buf.readUInt16BE(o));
  const u32 = (o) => (le ? buf.readUInt32LE(o) : buf.readUInt32BE(o));
  const ascii = (off, count) => { const s = base + off; return buf.toString('ascii', s, s + count).replace(/\0+$/, '').trim(); };
  const readIfd = (offset, want) => {
    const out = {};
    const start = base + offset;
    if (start + 2 > buf.length) return out;
    const n = u16(start);
    for (let i = 0; i < n; i++) {
      const e = start + 2 + i * 12;
      if (e + 12 > buf.length) break;
      const tag = u16(e), type = u16(e + 2), count = u32(e + 4);
      if (!(tag in want)) continue;
      const size = (type === 2 ? 1 : type === 4 ? 4 : type === 3 ? 2 : 1) * count;
      const valOff = size <= 4 ? e + 8 - base : u32(e + 8);
      out[want[tag]] = type === 2 ? ascii(valOff, count) : type === 4 ? u32(base + valOff) : u16(base + valOff);
    }
    return out;
  };
  try {
    const ifd0 = readIfd(u32(base + 4), { 0x010f: 'make', 0x0110: 'model', 0x8769: 'exifIfd' });
    const exif = ifd0.exifIfd ? readIfd(ifd0.exifIfd, { 0x9003: 'dateTimeOriginal', 0x9291: 'subsec', 0x9011: 'offset' }) : {};
    return { ...ifd0, ...exif };
  } catch { return {}; }
}

function exifDateToIso(s, subsec, offset) {
  const m = typeof s === 'string' && /^(\d{4}):(\d{2}):(\d{2}) (\d{2}):(\d{2}):(\d{2})$/.exec(s);
  if (!m) return null;
  const ms = subsec ? String(subsec).slice(0, 3).padEnd(3, '0') : '000';
  const tz = typeof offset === 'string' && /^[+-]\d{2}:\d{2}$/.test(offset) ? offset : 'Z';
  const iso = `${m[1]}-${m[2]}-${m[3]}T${m[4]}:${m[5]}:${m[6]}.${ms}${tz}`;
  return Number.isNaN(Date.parse(iso)) ? null : new Date(iso).toISOString();
}

/** 64-bit difference hash: 9×8 grayscale, each bit = "left pixel brighter than right". */
async function dhash(image) {
  const { data } = await image.clone().resize(9, 8, { fit: 'fill' }).grayscale().raw().toBuffer({ resolveWithObject: true });
  let bits = '';
  for (let y = 0; y < 8; y++) for (let x = 0; x < 8; x++) bits += data[y * 9 + x] > data[y * 9 + x + 1] ? '1' : '0';
  return BigInt('0b' + bits).toString(16).padStart(16, '0');
}

async function probeFile(root, path, stat) {
  const ext = extname(path).toLowerCase();
  const kind = IMAGE_EXT.has(ext) ? 'image' : VIDEO_EXT.has(ext) ? 'video' : 'other';
  const base = {
    path, relPath: relative(root, path), fileId: await sha256File(path), sizeBytes: stat.size, mimeType: MIME[ext] ?? 'application/octet-stream', kind,
    width: null, height: null, capturedAt: null, modifiedAt: stat.mtime.toISOString(), cameraId: null, dhash: null, thumbnail: null,
  };
  if (kind !== 'image') return base;
  try {
    const image = sharp(path, { failOn: 'none' }).rotate();
    const meta = await image.metadata();
    const exif = parseExif(meta.exif);
    const make = exif.make ? String(exif.make).trim() : '';
    const model = exif.model ? String(exif.model).trim() : '';
    const cameraId = [make, model.startsWith(make) && make ? model.slice(make.length).trim() : model].filter(Boolean).join(' ') || null;
    const thumb = await image.clone().resize(512, 512, { fit: 'inside', withoutEnlargement: true }).jpeg({ quality: 78, mozjpeg: true }).toBuffer({ resolveWithObject: true });
    return {
      ...base,
      width: meta.width ?? null, height: meta.height ?? null,
      capturedAt: exifDateToIso(exif.dateTimeOriginal, exif.subsec, exif.offset),
      cameraId,
      dhash: await dhash(image),
      thumbnail: { base64: thumb.data.toString('base64'), mimeType: 'image/jpeg', width: thumb.info.width, height: thumb.info.height },
    };
  } catch (err) {
    console.warn(`probe fell back to file facts for ${path}: ${err.message}`);
    return base;
  }
}

async function post(api, token, batch) {
  const res = await fetch(`${api}/api/media/catalog/batches`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...(token ? { authorization: `Bearer ${token}` } : {}) },
    body: JSON.stringify(batch),
  });
  if (!res.ok) throw new Error(`POST batch ${batch.batchId}: ${res.status} ${(await res.text()).slice(0, 300)}`);
  return res.json();
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (args.help || !args.root) {
    console.log('usage: media-walker --root <dir> [--api URL] [--batch N] [--limit N] [--state FILE] [--token JWT] [--dry-run] [--reset]');
    process.exit(args.help ? 0 : 2);
  }
  const root = await fs.realpath(args.root);
  const stateFile = args.state ?? join(homedir(), '.media-walker', `${createHash('sha1').update(root).digest('hex').slice(0, 12)}.json`);
  await fs.mkdir(dirname(stateFile), { recursive: true });
  let state = { root, posted: {} };
  if (!args.reset) { try { state = JSON.parse(await fs.readFile(stateFile, 'utf8')); } catch { /* fresh */ } }
  const save = () => fs.writeFile(stateFile, JSON.stringify(state));

  let seen = 0, skipped = 0, probed = 0, posted = 0, runs = [];
  let batch = [];
  const flush = async () => {
    if (batch.length === 0) return;
    const batchId = `${Date.now().toString(36)}-${posted}`;
    if (args.dryRun) {
      console.log(`[dry-run] would post ${batch.length} files (${Math.round(JSON.stringify(batch).length / 1024)} KB)`);
    } else {
      let last;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try { last = await post(args.api, args.token, { batchId, root, files: batch }); break; }
        catch (err) { last = err; if (attempt === 3) throw err; await new Promise((r) => setTimeout(r, 1500 * attempt)); }
      }
      runs.push(last.runId);
      console.log(`posted ${batch.length} files → run ${last.runId}`);
    }
    for (const f of batch) state.posted[f.path] = { size: f.sizeBytes, mtimeMs: Date.parse(f.modifiedAt), fileId: f.fileId };
    posted += batch.length;
    batch = [];
    await save();
  };

  for await (const path of walk(root)) {
    if (probed >= args.limit) break;
    const ext = extname(path).toLowerCase();
    if (!IMAGE_EXT.has(ext) && !VIDEO_EXT.has(ext)) continue;
    seen++;
    const stat = await fs.stat(path);
    const prev = state.posted[path];
    if (prev && prev.size === stat.size && prev.mtimeMs === stat.mtime.getTime()) { skipped++; continue; }
    batch.push(await probeFile(root, path, stat));
    probed++;
    if (batch.length >= args.batch) await flush();
  }
  await flush();
  console.log(JSON.stringify({ root, seen, skipped, probed, posted, runs: runs.length, stateFile }));
}

main().catch((err) => { console.error(err.message); process.exit(1); });
