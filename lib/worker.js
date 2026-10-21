'use strict';

const { EventEmitter } = require('events');
const crypto = require('crypto');
const { TradeGenerator } = require('./tradeGenerator');

/**
 * Simulates the long-running BSE pull. start() returns immediately; the stream of
 * trade chunks is produced on timers and announced through events:
 *   started   (job)
 *   chunk     ({ job, trades })
 *   completed (job)
 *   failed    (job)
 *   cancelled (job)
 */
class PullWorker extends EventEmitter {
  constructor(config) {
    super();
    this.config = config;
    this.jobs = new Map();
    this.timers = new Map();
    this.activeJobId = null;
    this.lastJobId = null;
    this.runCounter = 0;
  }

  snapshot(job) {
    return { ...job };
  }

  current() {
    const id = this.activeJobId || this.lastJobId;
    return id ? this.snapshot(this.jobs.get(id)) : null;
  }

  get(id) {
    const job = this.jobs.get(id);
    return job ? this.snapshot(job) : null;
  }

  list() {
    return Array.from(this.jobs.values())
      .map((job) => this.snapshot(job))
      .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
  }

  start() {
    const active = this.activeJobId ? this.jobs.get(this.activeJobId) : null;
    if (active && active.status === 'running') {
      return { job: this.snapshot(active), alreadyRunning: true };
    }

    const { pullDurationMs, chunkIntervalMs, totalTrades } = this.config;
    const job = {
      id: crypto.randomUUID(),
      status: 'running',
      createdAt: new Date().toISOString(),
      finishedAt: null,
      durationMs: pullDurationMs,
      chunkIntervalMs,
      totalChunks: Math.max(1, Math.ceil(pullDurationMs / chunkIntervalMs)),
      totalTrades,
      chunksDelivered: 0,
      deliveredTrades: 0,
      error: null,
    };

    this.jobs.set(job.id, job);
    this.activeJobId = job.id;
    this.lastJobId = job.id;
    this.runCounter += 1;

    const seed = this.config.seed + this.runCounter - 1;
    setImmediate(() => this.run(job, seed));
    return { job: this.snapshot(job), alreadyRunning: false };
  }

  run(job, seed) {
    if (job.status !== 'running') {
      return;
    }

    const generator = new TradeGenerator(seed);
    const baseCount = Math.floor(job.totalTrades / job.totalChunks);
    const remainder = job.totalTrades % job.totalChunks;
    const startedAt = Date.now();

    this.emit('started', this.snapshot(job));

    const step = (index) => {
      this.timers.delete(job.id);
      if (job.status !== 'running') {
        return;
      }

      try {
        const count = baseCount + (index < remainder ? 1 : 0);
        const trades = generator.nextChunk({
          jobId: job.id,
          count,
          startSeq: job.deliveredTrades + 1,
          windowMs: job.chunkIntervalMs,
        });
        job.chunksDelivered = index + 1;
        job.deliveredTrades += trades.length;

        // Listeners persist and broadcast synchronously; a throw here fails the job.
        this.emit('chunk', { job: this.snapshot(job), trades });

        if (job.chunksDelivered >= job.totalChunks) {
          this.finish(job, 'completed', null);
          return;
        }

        const nextDue = startedAt + (index + 2) * job.chunkIntervalMs;
        this.schedule(job, step, index + 1, Math.max(0, nextDue - Date.now()));
      } catch (err) {
        this.finish(job, 'failed', err && err.message ? err.message : String(err));
      }
    };

    this.schedule(job, step, 0, job.chunkIntervalMs);
  }

  schedule(job, step, index, delayMs) {
    const timer = setTimeout(() => step(index), delayMs);
    this.timers.set(job.id, timer);
  }

  finish(job, status, error) {
    const timer = this.timers.get(job.id);
    if (timer) {
      clearTimeout(timer);
      this.timers.delete(job.id);
    }
    job.status = status;
    job.error = error;
    job.finishedAt = new Date().toISOString();
    if (this.activeJobId === job.id) {
      this.activeJobId = null;
    }
    this.emit(status, this.snapshot(job));
  }

  cancel(id) {
    const job = this.jobs.get(id);
    if (!job) {
      return { found: false, cancelled: false };
    }
    if (job.status !== 'running') {
      return { found: true, cancelled: false, job: this.snapshot(job) };
    }
    this.finish(job, 'cancelled', null);
    return { found: true, cancelled: true, job: this.snapshot(job) };
  }

  stopAll() {
    for (const timer of this.timers.values()) {
      clearTimeout(timer);
    }
    this.timers.clear();
    for (const job of this.jobs.values()) {
      if (job.status === 'running') {
        job.status = 'cancelled';
        job.finishedAt = new Date().toISOString();
      }
    }
    this.activeJobId = null;
  }
}

module.exports = { PullWorker };
