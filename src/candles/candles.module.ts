import { Module } from '@nestjs/common';

import { ChunkWriterService } from './chunk-writer.service';

/**
 * Replay/backtest candle ingest — moves historical candle fetching off Vercel into this
 * always-on service. Phase 2 skeleton: providers only, nothing polls or fetches yet; the
 * job worker + S3 fetcher arrive in Phase 3 behind CANDLE_INGEST_ENABLED.
 */
@Module({
	providers: [ChunkWriterService],
})
export class CandlesModule {}
