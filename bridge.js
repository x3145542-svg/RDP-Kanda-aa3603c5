// ═══════════════════════════════════════════════════════
// KANDA RDP BRIDGE v2 — runner pool + keepalive ping/pong + client queue
//   - HTTP (PORT env, Railway domain)  : WebSocket /ws — runner (GitHub Actions) kết nối vào
//   - TCP  (RDP_PORT, Railway TCP Proxy): raw TCP — RDP client (mstsc) kết nối vào
// v2 fix lỗi 0x7 "connection was lost":
//   1. Ping/pong 20s mọi runner WS -> phát hiện + kick WS chết (half-open) ngay
//   2. Mỗi runner ws có cờ _assigned -> không bao giờ ghép 2 client vào 1 ws
//   3. Client tới mà chưa có runner rảnh -> chờ tới 8s (runner respawn 1.5s) thay vì destroy ngay
// ═══════════════════════════════════════════════════════
const net = require('net');
const http = require('http');
const { WebSocketServer } = require('ws');

const HTTP_PORT = parseInt(process.env.PORT || '8080', 10);     // Railway HTTP domain
const RDP_PORT  = parseInt(process.env.RDP_PORT || '3389', 10); // Railway TCP Proxy -> app port
const PING_MS = 20000;        // ping mỗi runner 20s
const WAIT_RUNNER_MS = 8000;  // mstsc tới mà chưa có runner -> chờ tối đa 8s

const runners = new Set();

// ─── HTTP + WS (runner side) ───
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') { res.writeHead(200); res.end('OK'); return; }
  res.writeHead(200, {'Content-Type': 'text/plain'});
  res.end('KANDA RDP BRIDGE v2');
});

const wss = new WebSocketServer({ server: httpServer, path: '/ws' });
wss.on('connection', (ws) => {
  ws._alive = true;
  ws._assigned = false;
  runners.add(ws);
  console.log(`[bridge] runner joined (pool=${runners.size})`);
  ws.on('pong', () => { ws._alive = true; });
  ws.on('close', () => { runners.delete(ws); console.log(`[bridge] runner left (pool=${runners.size})`); });
  ws.on('error', () => { try { ws.terminate(); } catch (e) {} });
});

// Keepalive: ping mọi runner WS mỗi 20s; WS nào 1 chu kỳ không pong -> kick (chống half-open)
setInterval(() => {
  for (const ws of runners) {
    if (ws.readyState !== 1) { runners.delete(ws); continue; }
    if (!ws._alive) {
      console.log('[bridge] runner missed pong -> kick (half-open WS)');
      runners.delete(ws);
      try { ws.terminate(); } catch (e) {}
      continue;
    }
    ws._alive = false;
    try { ws.ping(); } catch (e) {}
  }
}, PING_MS);

function pickRunner() {
  for (const ws of runners) {
    if (!ws._assigned && ws.readyState === 1) return ws;
  }
  return null;
}

// ─── TCP server (RDP client side) ───
const tcpServer = net.createServer((sock) => {
  console.log('[bridge] RDP client connected');
  let paired = false;
  const started = Date.now();
  const t = setInterval(() => {
    const ws = pickRunner();
    if (ws) {
      clearInterval(t);
      paired = true;
      ws._assigned = true;
      console.log('[bridge] pairing client <-> runner');
      ws.on('message', (data) => {
        try {
          if (!sock.destroyed) sock.write(Buffer.isBuffer(data) ? data : Buffer.from(data));
        } catch (e) {}
      });
      const cleanup = () => { try { if (ws.readyState === 1) ws.close(); } catch (e) {} };
      sock.on('data', (buf) => { try { if (ws.readyState === 1) ws.send(buf); } catch (e) {} });
      sock.on('close', cleanup);
      sock.on('error', cleanup);
      ws.on('close', () => { try { sock.destroy(); } catch (e) {} });
      return;
    }
    if (Date.now() - started > WAIT_RUNNER_MS && !paired) {
      clearInterval(t);
      console.log('[bridge] no runner available -> reject client');
      sock.destroy();
    }
  }, 150);
});

tcpServer.on('error', (e) => console.log('[bridge] tcp server err: ' + e.message));

httpServer.listen(HTTP_PORT, '0.0.0.0', () => console.log(`[bridge] HTTP+WS on :${HTTP_PORT}`));
tcpServer.listen(RDP_PORT, '0.0.0.0', () => console.log(`[bridge] TCP RDP on :${RDP_PORT}`));
