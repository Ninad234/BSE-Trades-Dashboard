'use strict';

const path = require('path');

function readInt(name, fallback) {
  const parsed = parseInt(process.env[name], 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const speedUp =
  String(process.env.SPEED_UP || 'false').toLowerCase() === 'true' ||
  process.argv.includes('--speed-up');

module.exports = Object.freeze({
  port: readInt('PORT', 3000),
  speedUp,
  // Default architecture: a pull lasts 15 minutes, delivered as a chunk every 5 seconds.
  // SPEED_UP=true: a pull lasts 30 seconds, delivered as a chunk every 500 ms.
  pullDurationMs: readInt('PULL_DURATION_MS', speedUp ? 30 * 1000 : 15 * 60 * 1000),
  chunkIntervalMs: readInt('CHUNK_INTERVAL_MS', speedUp ? 500 : 5 * 1000),
  totalTrades: readInt('TOTAL_TRADES', speedUp ? 3000 : 9000),
  seed: readInt('SEED', 20260101),
  dataFile: process.env.DATA_FILE || path.join(__dirname, '..', 'data', 'trades.ndjson'),
  // Must stay well below the 30 second idle/connection limit enforced by the network.
  wsHeartbeatMs: readInt('WS_HEARTBEAT_MS', 15 * 1000),
  snapshotLimit: readInt('SNAPSHOT_LIMIT', 500),
});
