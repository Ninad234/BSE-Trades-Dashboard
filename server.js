'use strict';

const http = require('http');
const path = require('path');
const express = require('express');
const { WebSocketServer, WebSocket } = require('ws');

const config = require('./lib/config');
const { TradeStore } = require('./lib/db');
const { PullWorker } = require('./lib/worker');

const store = new TradeStore(config.dataFile);
const worker = new PullWorker(config);

const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const server = http.createServer(app);
const wss = new WebSocketServer({ server, path: '/ws' });

// ---------------------------------------------------------------------------
// WebSocket fan-out
// ---------------------------------------------------------------------------

function broadcast(message) {
  const payload = JSON.stringify(message);
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(payload);
    }
  }
}

wss.on('connection', (socket) => {
  socket.isAlive = true;
  socket.on('pong', () => {
    socket.isAlive = true;
  });
  socket.on('error', (err) => {
    console.error('[ws] client error:', err.message);
  });

  // A (re)connecting dashboard is brought fully up to date from the database.
  socket.send(
    JSON.stringify({
      type: 'snapshot',
      trades: store.latest(config.snapshotLimit),
      stats: store.stats(),
      job: worker.current(),
      config: {
        speedUp: config.speedUp,
        pullDurationMs: config.pullDurationMs,
        chunkIntervalMs: config.chunkIntervalMs,
        totalTrades: config.totalTrades,
      },
    })
  );
});

// Protocol-level pings keep the connection active well inside the 30 second network limit.
const heartbeat = setInterval(() => {
  for (const client of wss.clients) {
    if (client.isAlive === false) {
      client.terminate();
      continue;
    }
    client.isAlive = false;
    client.ping();
  }
}, config.wsHeartbeatMs);

// ---------------------------------------------------------------------------
// Worker -> persist -> push pipeline
// ---------------------------------------------------------------------------

worker.on('started', (job) => {
  console.log(`[worker] job ${job.id} started: ${job.totalTrades} trades in ${job.totalChunks} chunks over ${job.durationMs} ms`);
  broadcast({ type: 'job_started', job });
});

worker.on('chunk', ({ job, trades }) => {
  store.insertTrades(trades); // persist first ...
  broadcast({ type: 'trades', jobId: job.id, trades, stats: store.stats(), job }); // ... then push
});

worker.on('completed', (job) => {
  console.log(`[worker] job ${job.id} completed: ${job.deliveredTrades} trades`);
  broadcast({ type: 'job_completed', job, stats: store.stats() });
});

worker.on('failed', (job) => {
  console.error(`[worker] job ${job.id} failed: ${job.error}`);
  broadcast({ type: 'job_failed', job, stats: store.stats() });
});

worker.on('cancelled', (job) => {
  console.log(`[worker] job ${job.id} cancelled after ${job.deliveredTrades} trades`);
  broadcast({ type: 'job_cancelled', job, stats: store.stats() });
});

// ---------------------------------------------------------------------------
// Mock BSE API
// ---------------------------------------------------------------------------

// Requests a full trade pull. Returns 202 immediately; data arrives asynchronously.
app.post('/api/bse/pull', (req, res) => {
  const { job, alreadyRunning } = worker.start();
  res
    .status(202)
    .location(`/api/bse/pull/${job.id}`)
    .json({
      accepted: true,
      alreadyRunning,
      message: alreadyRunning
        ? 'A pull is already running; attached to the existing job.'
        : 'Pull accepted. Trades will be streamed over the WebSocket as they are received.',
      job,
      statusUrl: `/api/bse/pull/${job.id}`,
      websocketUrl: '/ws',
    });
});

app.get('/api/bse/pull', (req, res) => {
  res.json({ jobs: worker.list() });
});

app.get('/api/bse/pull/:id', (req, res) => {
  const job = worker.get(req.params.id);
  if (!job) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }
  res.json({ job });
});

app.delete('/api/bse/pull/:id', (req, res) => {
  const result = worker.cancel(req.params.id);
  if (!result.found) {
    res.status(404).json({ error: 'Job not found' });
    return;
  }
  if (!result.cancelled) {
    res.status(409).json({ error: 'Job is not running', job: result.job });
    return;
  }
  res.json({ cancelled: true, job: result.job });
});

// ---------------------------------------------------------------------------
// Backend REST endpoints
// ---------------------------------------------------------------------------

app.get('/api/trades', (req, res) => {
  const limit = Math.min(Math.max(parseInt(req.query.limit, 10) || 100, 1), 1000);
  const offset = Math.max(parseInt(req.query.offset, 10) || 0, 0);
  const jobId = typeof req.query.jobId === 'string' && req.query.jobId ? req.query.jobId : null;
  res.json({ ...store.page({ jobId, limit, offset }), limit, offset });
});

app.get('/api/stats', (req, res) => {
  res.json(store.stats());
});

app.delete('/api/trades', (req, res) => {
  if (worker.activeJobId) {
    res.status(409).json({ error: 'Cannot clear trades while a pull is running' });
    return;
  }
  store.clear();
  broadcast({ type: 'cleared', stats: store.stats() });
  res.json({ cleared: true });
});

app.get('/health', (req, res) => {
  res.json({
    status: 'ok',
    uptimeSeconds: Math.round(process.uptime()),
    websocketClients: wss.clients.size,
    storedTrades: store.totals.totalTrades,
    activeJobId: worker.activeJobId,
  });
});

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

server.listen(config.port, () => {
  console.log(`Dashboard:   http://localhost:${config.port}`);
  console.log(`WebSocket:   ws://localhost:${config.port}/ws`);
  console.log(
    `Pull profile: ${config.speedUp ? 'SPEED_UP' : 'default'} | ${config.totalTrades} trades | ` +
      `${config.pullDurationMs} ms total | chunk every ${config.chunkIntervalMs} ms`
  );
});

let shuttingDown = false;
async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }
  shuttingDown = true;
  console.log(`\n${signal} received, shutting down...`);

  const forceExit = setTimeout(() => process.exit(1), 5000);
  forceExit.unref();

  clearInterval(heartbeat);
  worker.stopAll();
  for (const client of wss.clients) {
    client.close(1001, 'Server shutting down');
  }
  await store.close();
  wss.close();
  server.close(() => process.exit(0));
  server.closeAllConnections();
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));


// server.js
// package.json
// lib/config.js
// lib/tradeGenerator.js
// lib/worker.js
// lib/db.js
// public/index.html
// ARCHITECTURE_NOTE.md
// README.md
// .gitignore