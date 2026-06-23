import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';

import { validateEnv } from './config';
import { CtraderModule } from './ctrader';
import { HealthModule } from './health';
import { PrismaModule } from './prisma';

@Module({
	imports: [
		ConfigModule.forRoot({
			isGlobal: true,
			validate: validateEnv,
			envFilePath: ['.env'],
		}),
		PrismaModule,
		HealthModule,
		CtraderModule,
	],
})
export class AppModule {}
