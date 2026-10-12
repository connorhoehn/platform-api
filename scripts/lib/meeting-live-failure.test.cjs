const { test } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs/promises');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawn } = require('node:child_process');
const { listen, closeServer } = require('./acceptance-lifecycle.cjs');

for (const scenario of ['wrong-owner', 'cancel', 'existing-evidence']) {
  test(`acceptance ${scenario} exits cleanly without touching databases or old proof`, { timeout: 15_000 }, async () => {
    const root = await fs.mkdtemp(path.join(os.tmpdir(), 'meeting-harness-failure-'));
    let child;
    const server = http.createServer((req, res) => {
      if (scenario === 'cancel') { child.kill('SIGTERM'); return; }
      res.setHeader('content-type', 'application/json'); res.end(JSON.stringify({ nodeId: 'someone-else' }));
    });
    await listen(server);
    const output = path.join(root, 'result');
    try {
      await fs.writeFile(path.join(root, 'resources.json'), JSON.stringify({ isolated: true, services: {
        ddb: { name: 'meeting-finalize-native-test-ddb', host: '127.0.0.1', port: 1 },
        redis: { name: 'meeting-finalize-native-test-redis', host: '127.0.0.1', port: 1 },
      } }));
      await fs.writeFile(path.join(root, 'private.json'), JSON.stringify({ env: {
        CODEX_EXEC_TOKEN: 'owned-fixture', CODEX_EXEC_URL: 'http://127.0.0.1:1', SPEECH_URL: 'http://127.0.0.1:1/',
        VIDEO_API_URL: `http://127.0.0.1:${server.address().port}`, NODE_ID: 'meeting-live-test',
      } }));
      if (scenario === 'existing-evidence') { await fs.mkdir(output); await fs.writeFile(path.join(output, 'proof.json'), 'original proof'); }
      child = spawn(process.execPath, [path.resolve(__dirname, '../meeting-live-acceptance.cjs')], {
        env: { ...process.env, MEETING_ACCEPTANCE_OPT_IN: 'owned-native-services',
          MEETING_ACCEPTANCE_RESOURCES_JSON: path.join(root, 'resources.json'),
          MEETING_ACCEPTANCE_PRIVATE_CONFIG: path.join(root, 'private.json'), MEETING_ACCEPTANCE_OUTPUT: output },
        stdio: ['ignore', 'pipe', 'pipe'],
      });
      let stderr = ''; child.stderr.on('data', chunk => { stderr += chunk; });
      const exit = await new Promise((resolve, reject) => { child.on('error', reject); child.on('exit', (code, signal) => resolve({ code, signal })); });
      assert.equal(exit.code, 1, stderr); assert.equal(exit.signal, null);
      if (scenario === 'existing-evidence') {
        assert.equal(await fs.readFile(path.join(output, 'proof.json'), 'utf8'), 'original proof');
        assert.deepEqual(await fs.readdir(output), ['proof.json']);
      } else {
        const failure = JSON.parse(await fs.readFile(path.join(output, 'failure.json'), 'utf8'));
        assert.equal(failure.status, 'failed'); assert.deepEqual(failure.ownedTables, []);
        const cleanup = JSON.parse(await fs.readFile(path.join(output, 'cleanup.json'), 'utf8'));
        assert(cleanup.steps.every(step => step.closed));
        assert(!(await fs.readdir(output)).includes('proof.json'));
        assert.equal((await fs.stat(path.join(output, 'failure.json'))).mode & 0o777, 0o600);
      }
    } finally {
      if (child && child.exitCode === null) child.kill('SIGTERM');
      await closeServer(server); await fs.rm(root, { recursive: true, force: true });
    }
  });
}
