import { Module } from '@nestjs/common';

import { ConnectionManagerService } from './connection-manager.service';

@Module({
	providers: [ConnectionManagerService],
})
export class TradeLockerModule {}
