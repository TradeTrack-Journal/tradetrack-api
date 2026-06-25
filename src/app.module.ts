import { Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { APP_FILTER } from '@nestjs/core';
import { SentryGlobalFilter, SentryModule } from '@sentry/nestjs/setup';

import { validateEnv } from './config';
import { CtraderModule } from './ctrader';
import { HealthModule } from './health';
import { PrismaModule } from './prisma';
import { TradeLockerModule } from './tradelocker';

@Module({
	imports: [
		// Must be the first import so Sentry can hook into the Nest request lifecycle.
		SentryModule.forRoot(),
		ConfigModule.forRoot({
			isGlobal: true,
			validate: validateEnv,
			envFilePath: ['.env'],
		}),
		PrismaModule,
		HealthModule,
		CtraderModule,
		TradeLockerModule,
	],
	providers: [
		// Catches exceptions thrown in routes/handlers and forwards them to Sentry. Since there is no
		// other global exception filter, this safely owns global error reporting.
		{ provide: APP_FILTER, useClass: SentryGlobalFilter },
	],
})
export class AppModule {}
