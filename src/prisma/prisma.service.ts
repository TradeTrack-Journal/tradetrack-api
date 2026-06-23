import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PrismaPg } from '@prisma/adapter-pg';
import { PrismaClient } from '@prisma/client';

import type { Env } from '../config';

/**
 * Single long-lived Prisma client for the whole process, wired to Neon Postgres through
 * @prisma/adapter-pg with a small persistent pool. One pool per process keeps the Postgres
 * connection count low — we never open a connection per account.
 */
@Injectable()
export class PrismaService extends PrismaClient implements OnModuleInit, OnModuleDestroy {
	private readonly logger = new Logger(PrismaService.name);

	constructor(config: ConfigService<Env, true>) {
		const connectionString = config.get('DATABASE_URL', { infer: true });
		const isDev = config.get('NODE_ENV', { infer: true }) === 'development';

		super({
			adapter: new PrismaPg({ connectionString, max: 5 }),
			log: isDev ? ['warn', 'error'] : ['error'],
		});
	}

	async onModuleInit(): Promise<void> {
		try {
			await this.$connect();
			this.logger.log('Connected to PostgreSQL via @prisma/adapter-pg');
		} catch (error) {
			// An always-on service must boot even if the DB is briefly unreachable: Prisma
			// reconnects lazily on the next query, and /health/ready reports the real status.
			this.logger.error(
				`Initial DB connection failed (continuing, will retry on demand): ${
					error instanceof Error ? error.message : String(error)
				}`
			);
		}
	}

	async onModuleDestroy(): Promise<void> {
		await this.$disconnect();
	}

	/** Lightweight readiness probe used by the health endpoint. */
	async isHealthy(): Promise<boolean> {
		try {
			await this.$queryRaw`SELECT 1`;
			return true;
		} catch {
			return false;
		}
	}
}
