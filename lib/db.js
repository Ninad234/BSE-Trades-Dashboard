'use strict';

const fs = require('fs');
const path = require('path');

/**
 * Self-contained trade store: an in-memory array for fast reads plus an append-only
 * NDJSON file so trades survive restarts. Aggregates are maintained incrementally.
 */
class TradeStore {
  constructor(file) {
    this.file = file;
    this.trades = [];
    this.resetStats();

    fs.mkdirSync(path.dirname(file), { recursive: true });
    this.load();
    this.stream = fs.createWriteStream(file, { flags: 'a' });
    this.stream.on('error', (err) => {
      console.error('[store] write stream error:', err.message);
    });
  }

  resetStats() {
    this.totals = { totalTrades: 0, totalVolume: 0, totalTurnover: 0, buyCount: 0, sellCount: 0 };
    this.bySymbol = new Map();
  }

  load() {
    if (!fs.existsSync(this.file)) {
      return;
    }
    const lines = fs.readFileSync(this.file, 'utf8').split('\n');
    let skipped = 0;
    for (const line of lines) {
      if (!line.trim()) {
        continue;
      }
      try {
        const trade = JSON.parse(line);
        this.trades.push(trade);
        this.accumulate(trade);
      } catch (err) {
        skipped += 1;
      }
    }
    console.log(`[store] loaded ${this.trades.length} trades from ${this.file}` + (skipped ? ` (${skipped} unreadable lines skipped)` : ''));
  }

  accumulate(trade) {
    this.totals.totalTrades += 1;
    this.totals.totalVolume += trade.quantity;
    this.totals.totalTurnover += trade.value;
    if (trade.side === 'BUY') {
      this.totals.buyCount += 1;
    } else {
      this.totals.sellCount += 1;
    }

    let entry = this.bySymbol.get(trade.symbol);
    if (!entry) {
      entry = {
        symbol: trade.symbol,
        scripCode: trade.scripCode,
        count: 0,
        volume: 0,
        turnover: 0,
        open: trade.price,
        high: trade.price,
        low: trade.price,
        last: trade.price,
      };
      this.bySymbol.set(trade.symbol, entry);
    }
    entry.count += 1;
    entry.volume += trade.quantity;
    entry.turnover += trade.value;
    entry.high = Math.max(entry.high, trade.price);
    entry.low = Math.min(entry.low, trade.price);
    entry.last = trade.price;
  }

  insertTrades(trades) {
    if (!trades.length) {
      return;
    }
    const receivedAt = new Date().toISOString();
    let payload = '';
    for (const trade of trades) {
      trade.receivedAt = receivedAt;
      this.trades.push(trade);
      this.accumulate(trade);
      payload += JSON.stringify(trade) + '\n';
    }
    this.stream.write(payload);
  }

  latest(limit) {
    return this.trades.slice(Math.max(0, this.trades.length - limit));
  }

  page({ jobId, limit, offset }) {
    const source = jobId ? this.trades.filter((t) => t.jobId === jobId) : this.trades;
    return { total: source.length, items: source.slice(offset, offset + limit) };
  }

  stats() {
    return {
      totalTrades: this.totals.totalTrades,
      totalVolume: this.totals.totalVolume,
      totalTurnover: Math.round(this.totals.totalTurnover * 100) / 100,
      buyCount: this.totals.buyCount,
      sellCount: this.totals.sellCount,
      symbols: Array.from(this.bySymbol.values())
        .map((s) => ({ ...s, turnover: Math.round(s.turnover * 100) / 100 }))
        .sort((a, b) => b.turnover - a.turnover),
    };
  }

  clear() {
    this.trades = [];
    this.resetStats();
    fs.writeFileSync(this.file, '');
  }

  close() {
    return new Promise((resolve) => {
      this.stream.end(resolve);
    });
  }
}

module.exports = { TradeStore };
