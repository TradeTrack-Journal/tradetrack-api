import { decompressFile } from 'lzma-purejs-requirejs';

import type { Candle } from './types';

/**
 * Dukascopy `*_candles_*.bi5` record: 24 bytes big-endian —
 * u32 seconds since file start, u32 open, u32 close, u32 low, u32 high (all price × the
 * instrument's decimalFactor), f32 volume. Verified against the live archive 2026-08-07
 * (tick-aggregated candles reproduce these files bit-for-bit).
 */
const CANDLE_RECORD_BYTES = 24;

/**
 * Decode one LZMA-compressed candle file into replay candles.
 *
 * - Flat filler records (volume === 0, carrying the previous close through closed minutes)
 *   are DROPPED — dukascopy-node's default `ignoreFlats` does the same, so chunks built here
 *   match chunks the main app built through dukascopy-node.
 * - Volume is kept as the raw float32 value (no rounding) — again matching dukascopy-node,
 *   which passes the file's volume through untouched for the file's native timeframe.
 * - Candle object keys are built in {t,o,h,l,c,v} order for JSON byte-parity (types.ts).
 */
export function decodeCandleFile(
	compressed: Buffer,
	fileStartMs: number,
	decimalFactor: number
): Candle[] {
	const data = decompressFile(compressed);
	if (data.length % CANDLE_RECORD_BYTES !== 0) {
		throw new Error(
			`bi5: decompressed size ${data.length} is not a multiple of ${CANDLE_RECORD_BYTES}`
		);
	}
	const out: Candle[] = [];
	for (let off = 0; off < data.length; off += CANDLE_RECORD_BYTES) {
		const v = data.readFloatBE(off + 20);
		if (v === 0) continue;
		out.push({
			t: fileStartMs + data.readUInt32BE(off) * 1000,
			o: data.readUInt32BE(off + 4) / decimalFactor,
			h: data.readUInt32BE(off + 16) / decimalFactor,
			l: data.readUInt32BE(off + 12) / decimalFactor,
			c: data.readUInt32BE(off + 8) / decimalFactor,
			v,
		});
	}
	return out;
}
