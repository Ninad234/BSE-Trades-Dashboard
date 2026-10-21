'use strict';

// Deterministic PRNG so a given SEED always produces the same trade stream.
function mulberry32(seed) {
  let a = seed >>> 0;
  return function next() {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const INSTRUMENTS = Object.freeze([
  { scripCode: '500325', symbol: 'RELIANCE', basePrice: 2900 },
  { scripCode: '532540', symbol: 'TCS', basePrice: 3900 },
  { scripCode: '500180', symbol: 'HDFCBANK', basePrice: 1650 },
  { scripCode: '500209', symbol: 'INFY', basePrice: 1500 },
  { scripCode: '532174', symbol: 'ICICIBANK', basePrice: 1200 },
  { scripCode: '500696', symbol: 'HINDUNILVR', basePrice: 2500 },
  { scripCode: '500112', symbol: 'SBIN', basePrice: 800 },
  { scripCode: '532454', symbol: 'BHARTIARTL', basePrice: 1400 },
  { scripCode: '500875', symbol: 'ITC', basePrice: 450 },
  { scripCode: '500510', symbol: 'LT', basePrice: 3500 },
]);

class TradeGenerator {
  constructor(seed) {
    this.rand = mulberry32(seed);
    this.prices = new Map(INSTRUMENTS.map((i) => [i.symbol, i.basePrice]));
  }

  nextTrade(jobId, seq, windowMs) {
    const instrument = INSTRUMENTS[Math.floor(this.rand() * INSTRUMENTS.length)];
    const drift = (this.rand() - 0.5) * 0.004;
    const price = Math.round(this.prices.get(instrument.symbol) * (1 + drift) * 100) / 100;
    this.prices.set(instrument.symbol, price);

    const quantity = 1 + Math.floor(this.rand() * this.rand() * 1000);
    const side = this.rand() < 0.5 ? 'BUY' : 'SELL';
    const tradeTime = new Date(Date.now() - Math.floor(this.rand() * windowMs)).toISOString();

    return {
      id: `${jobId}-${String(seq).padStart(7, '0')}`,
      jobId,
      seq,
      scripCode: instrument.scripCode,
      symbol: instrument.symbol,
      side,
      price,
      quantity,
      value: Math.round(price * quantity * 100) / 100,
      tradeTime,
    };
  }

  nextChunk({ jobId, count, startSeq, windowMs }) {
    const trades = new Array(count);
    for (let i = 0; i < count; i += 1) {
      trades[i] = this.nextTrade(jobId, startSeq + i, windowMs);
    }
    return trades;
  }
}

module.exports = { TradeGenerator, INSTRUMENTS };
