import { Module } from '@nestjs/common';

import { BackfillService } from './backfill.service';
import { ConnectionManagerService } from './connection-manager.service';
import { TradeWriterService } from './trade-writer.service';

@Module({
	providers: [ConnectionManagerService, TradeWriterService, BackfillService],
})
export class MetaApiModule {}
