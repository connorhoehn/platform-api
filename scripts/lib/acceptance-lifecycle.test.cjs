const { test } = require('node:test');
const assert = require('node:assert/strict');
const http = require('node:http');
const { cleanupAll, listen, closeServer, forwardingServer } = require('./acceptance-lifecycle.cjs');

test('cleanup attempts remaining resources after both rejection and a stalled close', async () => {
  const closed = [];
  const result = await cleanupAll([
    ['rejected', async () => { throw new Error('offline'); }],
    ['stalled', () => new Promise(() => {})],
    ['remaining', async () => { closed.push('remaining'); }],
  ], 10);
  assert.deepEqual(result.map(row => row.closed), [false, false, true]);
  assert.deepEqual(closed, ['remaining']);
});
test('listen rejects a bind failure without waiting for the run deadline', async () => {
  const server = http.createServer();
  const initialListeners = server.listenerCount('listening');
  server.listen = function () { process.nextTick(() => this.emit('error', new Error('bind denied'))); return this; };
  await assert.rejects(listen(server), /bind denied/);
  assert.equal(server.listenerCount('listening'), initialListeners);
});
test('closing an owned proxy releases an in-flight upstream connection', async () => {
  let upstreamClosed;
  const closed = new Promise(resolve => { upstreamClosed = resolve; });
  let reached;
  const arrived = new Promise(resolve => { reached = resolve; });
  const upstream = http.createServer((req, res) => { res.on('close', upstreamClosed); reached(); });
  await listen(upstream);
  const proxy = forwardingServer(`http://127.0.0.1:${upstream.address().port}`, [], new AbortController().signal);
  await listen(proxy);
  try {
    const pending = fetch(`http://127.0.0.1:${proxy.address().port}`, { method: 'POST', body: 'owned fixture' }).catch(() => null);
    await arrived;
    await closeServer(proxy);
    await closed;
    await pending;
  } finally { await closeServer(proxy); await closeServer(upstream); }
});
