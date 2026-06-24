import { Module } from '@nestjs/common';

import { CtraderConnectionManager } from './ctrader-connection-manager';
import { CtraderTradeWriter } from './ctrader-trade-writer';

@Module({
	providers: [CtraderConnectionManager, CtraderTradeWriter],
})
export class CtraderModule {}
