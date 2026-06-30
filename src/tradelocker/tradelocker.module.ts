import { Module } from '@nestjs/common';

import { ConnectionManagerService } from './connection-manager.service';
import { TradeWriterService } from './trade-writer.service';

@Module({
	providers: [ConnectionManagerService, TradeWriterService],
})
export class TradeLockerModule {}
