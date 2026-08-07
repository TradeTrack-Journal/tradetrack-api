import { Injectable } from '@nestjs/common';

import { PrismaService } from '../prisma';

import { CHUNK_SOURCE } from './constants';

import type { Candle } from './types';

/**
 * Durable (L3) chunk writes — the same Neon `CandleChunk` rows the main app persists from
 * Vercel (`replay-candles.ts` writeChunk). Only Neon is written here: the main app reads
 * L1 memory → L2 Redis → L3 Neon and re-warms its own hot layers from L3 on a miss, so this
 * service never touches Redis and no TTL parity has to be maintained.
 */
@Injectable()
export class ChunkWriterService {
	constructor(private readonly prisma: PrismaService) {}

	/**
	 * Upsert one immutable historical bucket. THROWS on failure — unlike the Vercel writer
	 * there are no hot layers to fall back on here, so a swallowed error would silently drop
	 * the bucket; the ingest worker instead turns the throw into a failed (retryable) job.
	 */
	async writeChunk(
		symbol: string,
		resolution: string,
		bucketStartMs: number,
		candles: Candle[]
	): Promise<void> {
		const json = JSON.stringify(candles);
		const bucketStart = new Date(bucketStartMs);
		await this.prisma.candleChunk.upsert({
			where: { symbol_resolution_bucketStart: { symbol, resolution, bucketStart } },
			create: {
				symbol,
				resolution,
				bucketStart,
				candles: json,
				candleCount: candles.length,
				source: CHUNK_SOURCE,
			},
			update: { candles: json, candleCount: candles.length, source: CHUNK_SOURCE },
		});
	}
}
