import { Controller, Get, HttpCode, HttpStatus, ServiceUnavailableException } from '@nestjs/common';

import { PrismaService } from '../prisma';

@Controller('health')
export class HealthController {
	constructor(private readonly prisma: PrismaService) {}

	/**
	 * Liveness — 200 while the process is up. This is the probe Fly's HTTP health check hits;
	 * it deliberately does NOT touch the DB so a transient DB blip can't make Fly kill the machine.
	 */
	@Get()
	@HttpCode(HttpStatus.OK)
	live() {
		return {
			status: 'ok',
			service: 'tradetrack-api',
			uptime: Math.round(process.uptime()),
			timestamp: new Date().toISOString(),
		};
	}

	/** Readiness — verifies the DB is reachable. 200 when up, 503 when down. */
	@Get('ready')
	async ready() {
		const dbUp = await this.prisma.isHealthy();

		if (!dbUp) {
			throw new ServiceUnavailableException({ status: 'degraded', db: 'down' });
		}

		return { status: 'ok', db: 'up' };
	}
}
