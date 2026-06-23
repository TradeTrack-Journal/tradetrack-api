import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { CTraderConnection } from '@reiryoku/ctrader-layer';

import type { Env } from '../config';
import { decrypt } from '../crypto';
import { PrismaService } from '../prisma';

const HOSTS = {
	demo: 'demo.ctraderapi.com',
	live: 'live.ctraderapi.com',
} as const;

const CTRADER_PORT = 5035;
const HEARTBEAT_INTERVAL_MS = 10_000;

interface PocAccount {
	source: 'env' | 'db';
	accessToken: string;
	ctidTraderAccountId: number;
	environment: 'demo' | 'live';
	label: string;
}

/** Minimal cTrader execution payload — only the fields we log in the PoC. */
interface ExecutionEventPayload {
	executionType?: string | number;
	ctidTraderAccountId?: string | number;
	deal?: { dealId?: number; tradeSide?: string | number; volume?: number; symbolId?: number };
	order?: { orderId?: number };
	position?: { positionId?: number };
}

/**
 * Phase 1 proof-of-concept: open ONE persistent cTrader connection, authenticate the app and a
 * single account, keep it alive with heartbeats, and log every pushed ProtoOAExecutionEvent.
 * No reconnect, no token refresh, no DB writes here — those are Phase 2/3.
 */
@Injectable()
export class CtraderPocService implements OnModuleInit, OnModuleDestroy {
	private readonly logger = new Logger(CtraderPocService.name);
	private connection?: CTraderConnection;
	private heartbeat?: NodeJS.Timeout;
	private executionListenerId?: string;

	constructor(
		private readonly config: ConfigService<Env, true>,
		private readonly prisma: PrismaService
	) {}

	onModuleInit(): void {
		// Fire-and-forget so a slow/unreachable cTrader never blocks bootstrap (health stays up).
		void this.start();
	}

	onModuleDestroy(): void {
		this.cleanup();
	}

	/**
	 * Tear down heartbeat, listener and connection. Safe on partial init and reused by the start()
	 * failure path, so a half-open TLS socket is never left dangling after a failed auth.
	 */
	private cleanup(): void {
		if (this.heartbeat) {
			clearInterval(this.heartbeat);
			this.heartbeat = undefined;
		}
		if (this.connection && this.executionListenerId) {
			try {
				this.connection.removeEventListener(this.executionListenerId);
			} catch {
				// ignore
			}
			this.executionListenerId = undefined;
		}
		if (this.connection) {
			try {
				this.connection.close();
			} catch {
				// ignore close errors
			}
			this.connection = undefined;
		}
	}

	private async start(): Promise<void> {
		try {
			const clientId = this.config.get('CTRADER_CLIENT_ID', { infer: true });
			const clientSecret = this.config.get('CTRADER_CLIENT_SECRET', { infer: true });
			if (!clientId || !clientSecret) {
				this.logger.warn('CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET not set — PoC connection skipped.');
				return;
			}

			const account = await this.resolveAccount();
			if (!account) {
				this.logger.warn(
					'No cTrader account available (no env override and no active CtraderToken) — PoC skipped.'
				);
				return;
			}

			const host = HOSTS[account.environment];
			this.logger.log(
				`Connecting to ${host}:${CTRADER_PORT} for ${account.label} [${account.source}]`
			);

			const connection = new CTraderConnection({ host, port: CTRADER_PORT });
			this.connection = connection;

			await connection.open();
			this.logger.log('Socket open — authenticating application...');
			await connection.sendCommand('ProtoOAApplicationAuthReq', { clientId, clientSecret });

			this.logger.log('Application authenticated — authenticating account...');
			await connection.sendCommand('ProtoOAAccountAuthReq', {
				accessToken: account.accessToken,
				ctidTraderAccountId: account.ctidTraderAccountId,
			});

			this.logger.log(
				`Account ${account.ctidTraderAccountId} authenticated — subscribing to executions.`
			);
			this.executionListenerId = connection.on('ProtoOAExecutionEvent', (payload: unknown) => {
				this.handleExecutionEvent(payload);
			});

			this.startHeartbeat(connection);
			this.logger.log('PoC live: heartbeat running, waiting for ProtoOAExecutionEvent pushes...');
		} catch (error) {
			this.logger.error(`PoC connection failed: ${this.describe(error)}`);
			// Close any half-open socket so a failed auth never leaks a dangling TLS connection.
			this.cleanup();
		}
	}

	private startHeartbeat(connection: CTraderConnection): void {
		this.heartbeat = setInterval(() => {
			try {
				connection.sendHeartbeat();
			} catch (error) {
				this.logger.warn(`Heartbeat failed: ${this.describe(error)}`);
			}
		}, HEARTBEAT_INTERVAL_MS);
	}

	private handleExecutionEvent(event: unknown): void {
		// The library wraps the payload in a CTraderLayerEvent and calls listener(event); the real
		// ProtoOAExecutionEvent fields live on event.descriptor (fall back to the arg for safety).
		const wrapper = event as { descriptor?: unknown };
		const data = (wrapper?.descriptor ?? event ?? {}) as ExecutionEventPayload;
		const dealPart = data.deal
			? ` deal=${data.deal.dealId} side=${String(data.deal.tradeSide)} vol=${data.deal.volume} symbolId=${data.deal.symbolId}`
			: '';

		this.logger.log(
			`▶ ProtoOAExecutionEvent type=${String(data.executionType)} account=${String(data.ctidTraderAccountId)}${dealPart}`
		);
		this.logger.debug(`Full execution payload: ${JSON.stringify(data)}`);
	}

	/** Resolve the single PoC account: env override first, then the newest active CtraderToken. */
	private async resolveAccount(): Promise<PocAccount | undefined> {
		const envToken = this.config.get('CTRADER_POC_ACCESS_TOKEN', { infer: true });
		const envAccountId = this.config.get('CTRADER_POC_ACCOUNT_ID', { infer: true });
		if (envToken && envAccountId) {
			const id = this.parseAccountId(envAccountId);
			if (id === undefined) {
				this.logger.warn('CTRADER_POC_ACCOUNT_ID is not a valid numeric account id — PoC skipped.');
				return undefined;
			}
			const environment = this.config.get('CTRADER_POC_ENVIRONMENT', { infer: true }) ?? 'demo';
			return {
				source: 'env',
				accessToken: envToken,
				ctidTraderAccountId: id,
				environment,
				label: `env:${envAccountId}`,
			};
		}

		const token = await this.prisma.ctraderToken.findFirst({
			where: { isActive: true },
			orderBy: { updatedAt: 'desc' },
			select: {
				accessToken: true,
				ctidTraderAccountId: true,
				environment: true,
				tradingAccountId: true,
			},
		});
		if (!token?.ctidTraderAccountId) {
			return undefined;
		}

		const id = this.parseAccountId(token.ctidTraderAccountId);
		if (id === undefined) {
			this.logger.warn(
				`Stored ctidTraderAccountId '${token.ctidTraderAccountId}' is invalid — PoC skipped.`
			);
			return undefined;
		}

		return {
			source: 'db',
			accessToken: decrypt(token.accessToken),
			ctidTraderAccountId: id,
			environment: token.environment === 'live' ? 'live' : 'demo',
			label: `db:${token.tradingAccountId}`,
		};
	}

	private parseAccountId(raw: string | number): number | undefined {
		const id = Number(raw);
		return Number.isFinite(id) && id > 0 ? id : undefined;
	}

	private describe(error: unknown): string {
		if (error instanceof Error) {
			return error.message;
		}
		try {
			return JSON.stringify(error);
		} catch {
			return String(error);
		}
	}
}
