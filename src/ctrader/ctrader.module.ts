import { Module } from '@nestjs/common';

import { CtraderPocService } from './ctrader-poc.service';

@Module({
	providers: [CtraderPocService],
})
export class CtraderModule {}
