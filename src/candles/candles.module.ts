import { Module } from '@nestjs/common';

import { ArchiveClientService } from './archive-client.service';
import { ChunkWriterService } from './chunk-writer.service';
import { IngestWorkerService } from './ingest-worker.service';

/**
 * Replay/backtest candle ingest — moves historical candle fetching off Vercel into this
 * always-on service: IngestWorkerService polls the CandleIngestJob queue (enqueued by the
 * main app on chunk misses), ArchiveClientService reads Dukascopy's S3 archive, and
 * ChunkWriterService persists permanent CandleChunk rows. The worker stays idle unless
 * CANDLE_INGEST_ENABLED=true and AWS credentials are present.
 */
@Module({
	providers: [ArchiveClientService, ChunkWriterService, IngestWorkerService],
})
export class CandlesModule {}
