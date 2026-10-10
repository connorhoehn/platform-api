const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { createSummaryServer } = require('./codex-summary-server.cjs');
const token = 'owned-test-bridge-credential-0000000000';
const input = { model: '', systemPrompt: 'Summary', userPrompt: 'Owned test speech' };
async function withServer(client, run, timeoutMs) {
  const server = createSummaryServer({ token, client, timeoutMs });
  await new Promise(resolve => server.listen(0, '127.0.0.1', resolve));
  const base = `http://127.0.0.1:${server.address().port}`;
  try { await run(base); } finally { server.closeAllConnections(); await new Promise(resolve => server.close(resolve)); }
}
const request = (base, body = input, credential = token) => fetch(`${base}/complete`, {
  method: 'POST', headers: { authorization: `Bearer ${credential}`, 'content-type': 'application/json' }, body: JSON.stringify(body),
});
test('bridge authenticates and validates input before calling the model', async () => {
  let calls = 0;
  await withServer({ async *stream() { calls++; yield { done: true, response: 'Grounded summary', tokensIn: 8, tokensOut: 3 }; } }, async base => {
    assert.equal((await request(base, input, 'incorrect')).status, 401);
    assert.equal((await request(base, {})).status, 400);
    assert.equal(calls, 0);
    const response = await request(base); assert.equal(response.status, 200);
    assert.equal((await response.json()).response, 'Grounded summary'); assert.equal(calls, 1);
  });
});
test('bridge bounds concurrent calls and releases capacity after completion', async () => {
  let release, entered;
  const ready = new Promise(resolve => { entered = resolve; });
  await withServer({ async *stream() { entered(); await new Promise(resolve => { release = resolve; }); yield { done: true, response: 'Summary' }; } }, async base => {
    const first = request(base); await ready;
    assert.equal((await request(base)).status, 429);
    release(); assert.equal((await first).status, 200);
    assert.equal((await (await fetch(`${base}/health`)).json()).busy, false);
  });
});
test('model failure returns no invented summary or sensitive diagnostics', async () => {
  await withServer({ async *stream() { throw new Error('private transcript and credentials'); } }, async base => {
    const response = await request(base); assert.equal(response.status, 502);
    assert.doesNotMatch(await response.text(), /private transcript|credentials/);
  });
});
test('slow request bodies time out and release the bridge', async () => {
  await withServer({ async *stream() { throw new Error('must not run'); } }, async base => {
    const status = await new Promise((resolve, reject) => {
      const req = http.request(`${base}/complete`, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'content-length': '200' } }, res => { res.resume(); resolve(res.statusCode); });
      req.on('error', reject); req.write('{');
    });
    assert.equal(status, 504);
    await new Promise(resolve => setTimeout(resolve, 10));
    assert.equal((await (await fetch(`${base}/health`)).json()).busy, false);
  }, 40);
});
