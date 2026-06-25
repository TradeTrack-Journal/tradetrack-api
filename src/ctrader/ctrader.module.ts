import { Module } from '@nestjs/common';

import { BackfillService } from './backfill.service';
import { CtraderConnectionManager } from './ctrader-connection-manager';
import { CtraderTradeWriter } from './ctrader-trade-writer';

@Module({
	providers: [CtraderConnectionManager, CtraderTradeWriter, BackfillService],
})
export class CtraderModule {}
