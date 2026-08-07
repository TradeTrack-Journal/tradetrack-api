import type { Candle } from './types';

/**
 * Folds candles into a coarser timeframe by UTC-aligned buckets of `targetSeconds` — a verbatim
 * port of the main app's aggregate-candles.ts (BacktestReplay/utils), which itself matches
 * dukascopy-node's fold. Keep the three in lockstep: chunks built here must equal chunks the
 * main app would have built for the same minutes.
 */
export function aggregateCandles(candles: Candle[], targetSeconds: number): Candle[] {
	const spanMs = targetSeconds * 1000;
	const out: Candle[] = [];
	let cur: Candle | null = null;
	let bucket = 0;
	// Volumes are fractional floats; dukascopy-node's own fold rounds the sum to 4 decimals —
	// match it so server-aggregated candles are byte-identical to lib-folded ones (no FP dust).
	const flush = (candle: Candle) => {
		candle.v = Number(candle.v.toFixed(4));
		out.push(candle);
	};
	for (const c of candles) {
		const b = Math.floor(c.t / spanMs) * spanMs;
		if (!cur || b !== bucket) {
			if (cur) flush(cur);
			bucket = b;
			cur = { t: b, o: c.o, h: c.h, l: c.l, c: c.c, v: c.v };
		} else {
			cur.h = Math.max(cur.h, c.h);
			cur.l = Math.min(cur.l, c.l);
			cur.c = c.c;
			cur.v += c.v;
		}
	}
	if (cur) flush(cur);
	return out;
}
