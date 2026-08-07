/**
 * Chart candle in the main app's replay format (`src/feature/BacktestReplay/types/replay.ts`).
 * A `CandleChunk.candles` column is `JSON.stringify` of an array of these. Key ORDER matters
 * for byte-parity with chunks written from Vercel: build objects as {t, o, h, l, c, v}.
 */
export interface Candle {
	t: number; // open time, unix ms
	o: number;
	h: number;
	l: number;
	c: number;
	v: number;
}

/** Resolutions the replay serves; chunk bucket granularity differs per resolution (see main app). */
export type ResolutionKey = '1m' | '5m' | '15m' | '1h' | '4h' | '1d';
