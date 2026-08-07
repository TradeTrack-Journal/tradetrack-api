export const MS_PER_DAY = 86_400_000;

/**
 * Bucket math for chunk spans, mirroring the main app's granularity mapping
 * (replay-candles.ts granularityFor): 1m chunks span a UTC day, 1h chunks a UTC month,
 * 1d chunks a UTC year. All boundaries are UTC-aligned like Dukascopy's file layout.
 */

export function nextMonthStartMs(ms: number): number {
	const d = new Date(ms);
	return Date.UTC(d.getUTCFullYear(), d.getUTCMonth() + 1, 1);
}

export function nextYearStartMs(ms: number): number {
	return Date.UTC(new Date(ms).getUTCFullYear() + 1, 0, 1);
}

/** UTC-day starts covering [fromMs, toMs) — the per-day archive files a span is built from. */
export function dayStartsIn(fromMs: number, toMs: number): number[] {
	const out: number[] = [];
	for (let d = Math.floor(fromMs / MS_PER_DAY) * MS_PER_DAY; d < toMs; d += MS_PER_DAY) {
		out.push(d);
	}
	return out;
}

/** S3 key of one day's BID m1 candle file. Months are zero-indexed in the archive; days are not. */
export function dayMinuteCandlesKey(s3Folder: string, dayStartMs: number): string {
	const d = new Date(dayStartMs);
	const yyyy = d.getUTCFullYear();
	const mm = String(d.getUTCMonth()).padStart(2, '0');
	const dd = String(d.getUTCDate()).padStart(2, '0');
	return `${s3Folder}/${yyyy}/${mm}/${dd}/BID_candles_min_1.bi5`;
}
