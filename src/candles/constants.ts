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
