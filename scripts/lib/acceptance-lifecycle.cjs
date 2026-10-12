const http = require('node:http');

// Keep every cleanup independent: one unavailable dependency must not prevent
// the browser, remaining tables or listening sockets from being released.
async function cleanupAll(steps, timeoutMs = 10_000) {
  const results = [];
  for (const [name, close] of steps) {
    let timer;
    try {
      await Promise.race([
        Promise.resolve().then(close),
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error('Cleanup timed out')), timeoutMs); }),
      ]);
      results.push({ name, closed: true });
    } catch (error) {
      results.push({ name, closed: false, error: error.message });
    } finally { clearTimeout(timer); }
  }
  return results;
}
function listen(server) {
  return new Promise((resolve, reject) => {
    const fail = error => { server.removeListener('listening', ready); reject(error); };
    const ready = () => { server.removeListener('error', fail); resolve(); };
    server.once('error', fail); server.once('listening', ready);
    server.listen(0, '127.0.0.1');
  });
}
function closeServer(server) {
  return new Promise((resolve, reject) => {
    server.close(error => error && error.code !== 'ERR_SERVER_NOT_RUNNING' ? reject(error) : resolve());
    server.closeAllConnections();
  });
}
function forwardingServer(upstream, seen, signal) {
  return http.createServer((req, res) => {
    if (req.method !== 'POST') { res.writeHead(404).end(); return; }
    seen.push({ method: req.method, at: new Date().toISOString() });
    const request = http.request(upstream, { method: req.method, headers: req.headers, signal }, response => {
      res.writeHead(response.statusCode, response.headers); response.pipe(res);
    });
    request.setTimeout(180_000, () => request.destroy(new Error('Inference request timed out')));
    const cancel = () => { if (!res.writableFinished) request.destroy(); };
    res.once('close', cancel);
    req.once('aborted', () => request.destroy());
    request.on('error', () => { if (!res.headersSent) res.writeHead(502).end(); else res.destroy(); });
    req.pipe(request);
  });
}
module.exports = { cleanupAll, listen, closeServer, forwardingServer };
