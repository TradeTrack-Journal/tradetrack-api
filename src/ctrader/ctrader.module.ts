import { Module } from '@nestjs/common';

import { CtraderConnectionManager } from './ctrader-connection-manager';

@Module({
	providers: [CtraderConnectionManager],
})
export class CtraderModule {}
