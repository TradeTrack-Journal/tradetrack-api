/**
 * Dukascopy publishes its full price archive in a public requester-pays S3 bucket (verified
 * 2026-08-07: 2706 instruments, per-day ready-made m1 candle files + tick files). The bucket
 * and region are fixed upstream; env vars DUKASCOPY_S3_BUCKET / DUKASCOPY_S3_REGION exist
 * only as escape hatches should Dukascopy ever move the archive.
 */
export const DUKASCOPY_S3_DEFAULT_BUCKET = 'cfg-public-proper-wallaby';
export const DUKASCOPY_S3_DEFAULT_REGION = 'eu-west-1';

/**
 * `CandleChunk.source` for buckets ingested by this service. The main app writes its rows
 * with the default 'dukascopy' (live datafeed via dukascopy-node) — distinguishing the S3
 * archive path keeps backfill audits and debugging honest.
 */
export const CHUNK_SOURCE = 'dukascopy-s3';

/**
 * Volumes are fractional floats; dukascopy-node's tick→candle fold rounds the sum to 4
 * decimals, and the main app's aggregation matches it (aggregate-candles.ts). Candles decoded
 * from the S3 archive must apply the same rounding so chunks are byte-identical regardless of
 * which path produced them.
 */
export const VOLUME_DECIMALS = 4;

export interface IngestInstrument {
	/** Top-level archive folder — the dukascopy-node instrument id, uppercased. */
	s3Folder: string;
	/** Price scale of the .bi5 integer prices — dukascopy-node's decimalFactor for the id. */
	decimalFactor: number;
}

/**
 * The replay instrument set, keyed by the app-facing symbol used in CandleChunk/CandleIngestJob
 * rows. MUST stay in sync with the main app's REPLAY_INSTRUMENTS (BacktestReplay/constants) —
 * decimalFactor values were extracted from dukascopy-node 1.46.4's instrument metadata, since
 * a wrong scale corrupts every price silently. The 2026-08-08 batch (NAS100…ADAUSD) was
 * additionally verified by decoding real 2026-08-05 archive files and eyeballing the prices.
 */
export const INGEST_INSTRUMENT_BY_SYMBOL: Record<string, IngestInstrument> = {
	EURUSD: { s3Folder: 'EURUSD', decimalFactor: 1e5 },
	GBPUSD: { s3Folder: 'GBPUSD', decimalFactor: 1e5 },
	USDJPY: { s3Folder: 'USDJPY', decimalFactor: 1e3 },
	AUDUSD: { s3Folder: 'AUDUSD', decimalFactor: 1e5 },
	USDCAD: { s3Folder: 'USDCAD', decimalFactor: 1e5 },
	XAUUSD: { s3Folder: 'XAUUSD', decimalFactor: 1e3 },
	US30: { s3Folder: 'USA30IDXUSD', decimalFactor: 1e3 },
	SPX500: { s3Folder: 'USA500IDXUSD', decimalFactor: 1e3 },
	GER40: { s3Folder: 'DEUIDXEUR', decimalFactor: 1e3 },
	NAS100: { s3Folder: 'USATECHIDXUSD', decimalFactor: 1e3 },
	USOIL: { s3Folder: 'LIGHTCMDUSD', decimalFactor: 1e3 },
	GBPAUD: { s3Folder: 'GBPAUD', decimalFactor: 1e5 },
	GBPJPY: { s3Folder: 'GBPJPY', decimalFactor: 1e3 },
	// Crypto trades 24/7: Saturday files exist in the archive and ingest like any other day.
	BTCUSD: { s3Folder: 'BTCUSD', decimalFactor: 1e1 },
	ADAUSD: { s3Folder: 'ADAUSD', decimalFactor: 1e3 },
};

/**
 * Only BASE resolutions are ever persisted as chunks — the main app aggregates 5m/15m/4h on
 * the fly from these and never writes them to Neon (replay-candles.ts BASE_RESOLUTION), so a
 * job for a derived resolution is a bug, not work. Values are the fold target in seconds.
 */
export const BASE_RESOLUTION_SECONDS: Record<string, number> = {
	'1m': 60,
	'1h': 3600,
	'1d': 86400,
};

// ---- Ingest worker tunables ----

/** Queue poll cadence. Mirrors the ~1.5s reconcile heartbeat of the other workers here. */
export const POLL_INTERVAL_MS = 1500;
/** Jobs processed concurrently per drain pass — S3 has no throttle, this bounds our own RAM/CPU. */
export const JOB_CONCURRENCY = 4;
/** Parallel per-day file GETs inside one month/year job. */
export const DAY_FETCH_CONCURRENCY = 8;
/** Claim attempts after which a job is FAILED instead of returned to PENDING. */
export const MAX_JOB_ATTEMPTS = 3;
/** A RUNNING job untouched this long is presumed orphaned (crashed worker) and reclaimable. */
export const STUCK_RUNNING_RECLAIM_MS = 10 * 60_000;
/**
 * The archive publishes a day's files ~2h after the UTC day ends (verified 2026-08-07), while
 * the main app finalizes buckets after 2h — a job can therefore arrive BEFORE its file exists.
 * Until the bucket end is at least this old, a missing file means "not published yet" (retry,
 * uncounted attempt), never "market closed" (permanent empty chunk).
 */
export const PUBLISH_SAFETY_MARGIN_MS = 6 * 60 * 60 * 1000;
