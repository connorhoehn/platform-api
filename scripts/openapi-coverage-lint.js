#!/usr/bin/env node
// OpenAPI coverage lint — drift-detection for documents/social/video YAMLs.
//
// For each configured prefix, enumerates the Express routes mounted under
// its router files and verifies that every (method + full path) tuple has
// an entry in the corresponding openapi/<prefix>.yaml's `paths:` block.
//
// Schema-level coverage is OUT OF SCOPE (per hub#1009 option B). This is a
// path-presence safety net only — backfill of full schemas is filed as
// separate follow-up tasks per prefix.
//
// Usage:
//   node scripts/openapi-coverage-lint.js          → lint, exits 1 on drift
//   node scripts/openapi-coverage-lint.js --list   → print expected routes
//   node scripts/openapi-coverage-lint.js --json   → emit JSON (CI integration)

'use strict';

const fs = require('node:fs');
const path = require('node:path');
const yaml = require('js-yaml');

const PROJECT_ROOT = path.resolve(__dirname, '..');
const ROUTE_DIR = path.join(PROJECT_ROOT, 'src/routes');
const OPENAPI_DIR = path.join(PROJECT_ROOT, 'openapi');

// Hand-curated mount table mirroring src/app.ts + src/routes/index.ts.
// When adding a new route file under one of the three covered prefixes,
// add an entry here. The lint silently ignores router names not in the
// table (so files that export internal helper routers don't break things).
const PREFIXES = {
  documents: {
    yaml: 'documents.yaml',
    files: [
      { file: 'documentComments.ts', routers: { documentCommentsRouter: '/documents' } },
      { file: 'sectionReviews.ts', routers: {
        sectionReviewsRouter: '/documents/{documentId}/sections/{sectionId}/reviews',
        documentReviewsRouter: '/documents/{documentId}/reviews',
        myReviewsRouter: '/reviews',
      } },
      { file: 'sectionItems.ts', routers: {
        sectionItemsRouter: '/documents/{documentId}/sections/{sectionId}/items',
        myItemsRouter: '/items',
      } },
      { file: 'documentImportExport.ts', routers: { documentImportExportRouter: '/documents/{documentId}' } },
      { file: 'documentTypes.ts', routers: { documentTypesRouter: '/document-types' } },
      { file: 'typedDocuments.ts', routers: { typedDocumentsRouter: '/typed-documents' } },
      { file: 'approvals.ts', routers: { approvalsRouter: '/approvals' } },
    ],
  },
  social: {
    yaml: 'social.yaml',
    files: [
      { file: 'profiles.ts', routers: { profilesRouter: '/profiles' } },
      { file: 'social.ts', routers: { socialRouter: '/social' } },
      { file: 'groups.ts', routers: { groupsRouter: '/groups' } },
      { file: 'group-members.ts', routers: { groupMembersRouter: '/groups/{groupId}' } },
      { file: 'group-rooms.ts', routers: { groupRoomsRouter: '/groups/{groupId}/rooms' } },
      { file: 'rooms.ts', routers: { roomsRouter: '/rooms' } },
      { file: 'room-members.ts', routers: {
        roomMembersRouter: '/rooms/{roomId}',
        myRoomsRouter: '/rooms',
      } },
      { file: 'posts.ts', routers: {
        postsRouter: '/rooms/{roomId}/posts',
        userPostsRouter: '/posts',
      } },
      { file: 'comments.ts', routers: { commentsRouter: '/rooms/{roomId}/posts/{postId}/comments' } },
      { file: 'likes.ts', routers: {
        postLikesRouter: '/rooms/{roomId}/posts/{postId}/likes',
        commentLikesRouter: '/rooms/{roomId}/posts/{postId}/comments/{commentId}/likes',
      } },
      { file: 'reactions.ts', routers: { reactionsRouter: '/rooms/{roomId}/posts/{postId}' } },
      { file: 'activity.ts', routers: { activityRouter: '/activity' } },
    ],
  },
  video: {
    yaml: 'video.yaml',
    files: [
      { file: 'videoSessions.ts', routers: { videoSessionsRouter: '/video' } },
      // recordingsRouter is mounted at '/' inside apiRouter; routes already
      // begin with '/recordings'.
      { file: 'recordings.ts', routers: { recordingsRouter: '' } },
      { file: 'recordingStatus.ts', routers: { router: '' } },
      // hangoutRoomsRouter is mounted at '/api/rooms' in app.ts (intercepts
      // BEFORE the apiRouter so its handlers run first).
      { file: 'hangoutRooms.ts', routers: { hangoutRoomsRouter: '/rooms' } },
    ],
  },
};

const ROUTE_RE = /\b(\w+Router)\.(get|post|put|patch|delete)\s*\(\s*['"`]([^'"`]+)['"`]/gm;

function expressToOpenApi(p) {
  return p.replace(/:([A-Za-z0-9_]+)/g, '{$1}');
}

function joinMount(mount, route) {
  if (route === '/' || route === '') return mount === '' ? '/' : mount;
  if (mount === '') return route;
  if (mount.endsWith('/') && route.startsWith('/')) return mount + route.slice(1);
  if (!mount.endsWith('/') && !route.startsWith('/')) return mount + '/' + route;
  return mount + route;
}

function enumerate(prefix) {
  const out = [];
  const routeDir = prefix.routeDir || ROUTE_DIR;
  for (const f of prefix.files) {
    const filePath = path.join(routeDir, f.file);
    if (!fs.existsSync(filePath)) {
      console.warn(`[openapi-lint] expected route file missing: ${f.file}`);
      continue;
    }
    const src = fs.readFileSync(filePath, 'utf8');
    for (const m of src.matchAll(ROUTE_RE)) {
      const [, routerName, method, rawPath] = m;
      const mount = f.routers[routerName];
      if (mount === undefined) continue;
      out.push({
        method: method.toUpperCase(),
        path: expressToOpenApi(joinMount(mount, rawPath)),
        file: f.file,
      });
    }
  }
  // Deterministic ordering for diff legibility.
  out.sort((a, b) => a.path.localeCompare(b.path) || a.method.localeCompare(b.method));
  return out;
}

// Cheap YAML `paths:` block reader. The skeleton YAMLs are hand-authored
// with a stable shape; we don't need a full YAML dep for key enumeration.
function readYamlPaths(yamlFile) {
  const out = new Map();
  const src = fs.readFileSync(yamlFile, 'utf8');
  const lines = src.split('\n');
  let inPaths = false;
  let currentPath = null;
  let currentPathCol = -1;
  for (const line of lines) {
    const t = line.replace(/\r$/, '');
    if (!inPaths) {
      if (/^paths:\s*$/.test(t)) inPaths = true;
      continue;
    }
    // Exit paths block when a new top-level key appears (column 0, non-space).
    if (/^\S/.test(t) && !/^paths:/.test(t)) break;
    // Path key: 2-space indent, starts with '/'.
    const pm = /^(  )(['"]?)(\/[^:'"\s]*)\2:\s*$/.exec(t);
    if (pm) {
      currentPath = pm[3];
      currentPathCol = pm[1].length;
      if (!out.has(currentPath)) out.set(currentPath, new Set());
      continue;
    }
    if (currentPath) {
      const mm = /^(\s*)(get|post|put|patch|delete):\s*$/i.exec(t);
      if (mm && mm[1].length === currentPathCol + 2) {
        out.get(currentPath).add(mm[2].toUpperCase());
      }
    }
  }
  return out;
}

// Parse each yaml with js-yaml before the path-coverage pass. Catches
// the class of typo where an unquoted `: ` inside a plain scalar
// description (e.g. `Only meaningful when `resolution: merged`.`) is
// interpreted as a mapping key — the regex-based reader silently
// tolerated this and shipped broken YAML to every js-yaml consumer
// (openapi-typescript, redoc, prettier yaml plugin, the frontend type
// generator). See hub#1351.
function parseAll() {
  const failures = [];
  for (const [name, prefix] of Object.entries(PREFIXES)) {
    const yamlPath = path.join(OPENAPI_DIR, prefix.yaml);
    if (!fs.existsSync(yamlPath)) continue;
    try {
      yaml.load(fs.readFileSync(yamlPath, 'utf8'), { filename: yamlPath });
    } catch (e) {
      const mark = e && e.mark;
      failures.push({
        name,
        file: prefix.yaml,
        message: e && e.reason ? e.reason : String(e),
        line: mark ? mark.line + 1 : null,
        column: mark ? mark.column + 1 : null,
      });
    }
  }
  return failures;
}

function lint(opts) {
  const parseFailures = parseAll();
  if (parseFailures.length > 0) {
    if (opts.json) {
      process.stdout.write(JSON.stringify({ parseFailures }, null, 2) + '\n');
    } else {
      for (const f of parseFailures) {
        const loc = f.line !== null ? `:${f.line}:${f.column}` : '';
        console.error(`[openapi-lint] ${f.name}: YAML parse failed — ${f.file}${loc} — ${f.message}`);
      }
    }
    return 1;
  }

  const report = {};
  let drift = 0;
  for (const [name, prefix] of Object.entries(PREFIXES)) {
    const yamlPath = path.join(OPENAPI_DIR, prefix.yaml);
    if (!fs.existsSync(yamlPath)) {
      report[name] = { ok: false, error: `missing ${prefix.yaml}`, missing: [] };
      drift++;
      continue;
    }
    const expected = enumerate(prefix);
    const documented = readYamlPaths(yamlPath);
    const missing = [];
    for (const e of expected) {
      const ops = documented.get(e.path);
      if (!ops || !ops.has(e.method)) missing.push(e);
    }
    report[name] = { ok: missing.length === 0, total: expected.length, missing };
    drift += missing.length;
  }

  if (opts.json) {
    process.stdout.write(JSON.stringify(report, null, 2) + '\n');
    return drift === 0 ? 0 : 1;
  }

  for (const [name, r] of Object.entries(report)) {
    if (r.error) {
      console.error(`[openapi-lint] ${name}: ${r.error}`);
      continue;
    }
    if (r.missing.length === 0) {
      console.log(`[openapi-lint] ${name}: OK (${r.total} routes documented)`);
    } else {
      console.error(`[openapi-lint] ${name}: ${r.missing.length} undocumented route(s):`);
      for (const m of r.missing) {
        console.error(`  ${m.method.padEnd(6)} ${m.path}     (${m.file})`);
      }
    }
  }
  return drift === 0 ? 0 : 1;
}

function listExpected() {
  for (const [name, prefix] of Object.entries(PREFIXES)) {
    process.stdout.write(`# ${name} (${prefix.yaml})\n`);
    for (const e of enumerate(prefix)) {
      process.stdout.write(`  ${e.method.padEnd(6)} ${e.path}     (${e.file})\n`);
    }
    process.stdout.write('\n');
  }
}

// Generate a path-coverage-only YAML for one prefix. Operations are
// schemaless ('200': OK placeholder) — backfill schemas in follow-up
// tasks per hub#1009 option B.
function emitYaml(prefixName) {
  const prefix = PREFIXES[prefixName];
  if (!prefix) {
    console.error(`unknown prefix: ${prefixName} (known: ${Object.keys(PREFIXES).join(', ')})`);
    process.exit(2);
  }
  const titleCase = prefixName[0].toUpperCase() + prefixName.slice(1);
  const lines = [];
  lines.push(`# openapi/${prefix.yaml}`);
  lines.push(`#`);
  lines.push(`# OpenAPI 3.1 path-coverage skeleton for the ${prefixName} surface of`);
  lines.push(`# platform-api. Generated by scripts/openapi-coverage-lint.js.`);
  lines.push(`#`);
  lines.push(`# Per hub#1009 option B: every Express route under the ${prefixName}/* mount`);
  lines.push(`# is listed here so the lint can flag drift when new routes are added.`);
  lines.push(`# Request/response schemas are intentionally OUT OF SCOPE — fill them`);
  lines.push(`# in via the per-prefix schema-backfill follow-up task.`);
  lines.push(`#`);
  lines.push(`# Regenerate via:`);
  lines.push(`#   node scripts/openapi-coverage-lint.js --emit ${prefixName} > openapi/${prefix.yaml}`);
  lines.push(``);
  lines.push(`openapi: 3.1.0`);
  lines.push(``);
  lines.push(`info:`);
  lines.push(`  title: platform-api ${titleCase} HTTP API (skeleton)`);
  lines.push(`  description: |`);
  lines.push(`    Path-coverage skeleton for the ${prefixName} surface. Schemas pending —`);
  lines.push(`    see hub#1009 follow-up tasks per prefix.`);
  lines.push(`  version: 0.1.0-skeleton`);
  lines.push(`  contact:`);
  lines.push(`    name: platform-api`);
  lines.push(``);
  lines.push(`servers:`);
  lines.push(`  - url: /api`);
  lines.push(`    description: platform-api mount`);
  lines.push(``);
  lines.push(`tags:`);
  lines.push(`  - name: ${prefixName}`);
  lines.push(`    description: ${titleCase} surface — schemaless skeleton`);
  lines.push(``);
  lines.push(`paths:`);
  // Group operations by path so each path key has one block.
  const expected = enumerate(prefix);
  const byPath = new Map();
  for (const e of expected) {
    if (!byPath.has(e.path)) byPath.set(e.path, []);
    byPath.get(e.path).push({ method: e.method, file: e.file });
  }
  const paths = [...byPath.keys()].sort();
  for (const p of paths) {
    const ops = byPath.get(p);
    lines.push(`  '${p}':`);
    for (const op of ops) {
      lines.push(`    ${op.method.toLowerCase()}:`);
      lines.push(`      summary: TODO — schema pending (${op.file})`);
      lines.push(`      tags: [${prefixName}]`);
      lines.push(`      responses:`);
      lines.push(`        '200':`);
      lines.push(`          description: OK`);
    }
  }
  process.stdout.write(lines.join('\n') + '\n');
}

const argv = process.argv.slice(2);
if (argv.includes('--list')) {
  listExpected();
  process.exit(0);
}
const emitIdx = argv.indexOf('--emit');
if (emitIdx !== -1) {
  emitYaml(argv[emitIdx + 1]);
  process.exit(0);
}
process.exit(lint({ json: argv.includes('--json') }));

module.exports = { enumerate, readYamlPaths, parseAll, PREFIXES };
