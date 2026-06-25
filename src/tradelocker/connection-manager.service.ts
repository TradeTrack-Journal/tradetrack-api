import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as Sentry from '@sentry/nestjs';

import type { Env } from '../config';
import { authenticateAccounts } from './client';
import { TradeLockerConnection, TradeLockerSubscribeError } from './connection';
import {
	RECONNECT_BASE_DELAY_MS,
	RECONNECT_MAX_DELAY_MS,
	TRADELOCKER_AUTH_HOSTS,
	TRADELOCKER_STREAM_HOSTS,
} from './constants';
import type { PocAccountConfig } from './types';

interface PocSession {
	config: PocAccountConfig;
	developerApiKey: string;
	connection?: TradeLockerConnection;
	reconnectAttempt: number;
	reconnectTimer?: NodeJS.Timeout;
	/** Bumped on every (re)connect; lets a superseded attempt detect it lost ownership. */
	generation: number;
}

/**
 * Phase 2 PoC: keeps ONE TradeLocker account streaming for the process lifetime, sourced from
 * TRADELOCKER_POC_* env vars (no DB yet). It re-authenticates fresh on every (re)connect — TradeLocker
 * has no refresh grant, re-login IS the refresh — opens the socket, SUBSCRIBEs, and logs stream
 * messages plus the SyncEnd boundary. No trades are persisted; that is Phase 3.
 *
 * Structure mirrors CtraderConnectionManager: fire-and-forget start so a slow broker never blocks
 * bootstrap, a generation guard after each await, and exponential reconnect/backoff owned here.
 */
@Injectable()
export class ConnectionManagerService implements OnApplicationBootstrap, OnApplicationShutdown {
	private readonly logger = new Logger(ConnectionManagerService.name);
	private session?: PocSession;
	private stopped = false;

	constructor(private readonly config: ConfigService<Env, true>) {}

	onApplicationBootstrap(): void {
		void this.start().catch((error) => {
			this.logger.error(`TradeLocker manager start failed: ${describe(error)}`);
			Sentry.captureException(error);
		});
	}

	onApplicationShutdown(): void {
		this.stopped = true;
		if (this.session?.reconnectTimer) {
			clearTimeout(this.session.reconnectTimer);
		}
		this.session?.connection?.close();
		this.session = undefined;
	}

	private async start(): Promise<void> {
		// Master switch — stays idle unless explicitly enabled. Kept OFF until our production Streams
		// API key is approved by TradeLocker, so the module ships but never connects meanwhile.
		if (this.config.get('TRADELOCKER_LIVE_SYNC_ENABLED', { infer: true }) !== 'true') {
			this.logger.log(
				'TradeLocker live-sync disabled (set TRADELOCKER_LIVE_SYNC_ENABLED=true to enable). Idle.'
			);
			return;
		}

		const developerApiKey = this.config.get('TRADELOCKER_DEVELOPER_API_KEY', { infer: true })?.trim();
		if (!developerApiKey) {
			this.logger.warn('TRADELOCKER_DEVELOPER_API_KEY not set — TradeLocker manager idle.');
			return;
		}

		const config = this.readPocConfig();
		if (!config) {
			this.logger.warn(
				'TRADELOCKER_POC_* not set — TradeLocker manager idle (PoC needs EMAIL/PASSWORD/SERVER).'
			);
			return;
		}

		this.session = { config, developerApiKey, reconnectAttempt: 0, generation: 0 };
		this.logger.log(
			`PoC mode — connecting one TradeLocker account (server=${config.server}, env=${config.environment})`
		);
		await this.connect();
	}

	private async connect(): Promise<void> {
		const session = this.session;
		if (!session || this.stopped) {
			return;
		}

		const generation = ++session.generation;
		const superseded = (): boolean =>
			this.stopped || this.session !== session || session.generation !== generation;

		// Declared outside the try so the catch closes THIS attempt's own connection — never the
		// shared session.connection, which a newer reconnect generation may already own.
		let connection: TradeLockerConnection | undefined;
		try {
			const authBaseUrl =
				this.config.get('TRADELOCKER_AUTH_BASE_URL', { infer: true })?.trim() ||
				TRADELOCKER_AUTH_HOSTS[session.config.environment];

			const tokens = await authenticateAccounts({
				baseUrl: authBaseUrl,
				email: session.config.email,
				password: session.config.password,
				server: session.config.server,
			});
			if (superseded()) {
				return;
			}
			if (tokens.length === 0) {
				throw new Error('auth returned no account tokens');
			}

			this.logger.log(
				`Auth ok — ${tokens.length} account token(s): [${tokens
					.map((t) => `${t.accountId}@${t.host ?? '?'}`)
					.join(', ')}]`
			);

			const wanted = session.config.accountId;
			const token = (wanted ? tokens.find((t) => t.accountId === wanted) : undefined) ?? tokens[0];
			if (wanted && token.accountId !== wanted) {
				this.logger.warn(
					`TRADELOCKER_POC_ACCOUNT_ID=${wanted} not in issued tokens — falling back to ${token.accountId}`
				);
			}

			const streamHost =
				this.config.get('TRADELOCKER_STREAM_BASE_URL', { infer: true })?.trim() ||
				TRADELOCKER_STREAM_HOSTS[session.config.environment];

			// The JWT's host claim must match the socket's environment, else SUBSCRIBE -> invalidJwt
			// "Host: <host> not recognized". Surface the pairing so a mismatch is obvious in the logs.
			this.logger.log(
				`Subscribing account ${token.accountId} (jwt host=${token.host ?? '?'}) via socket ${streamHost}`
			);
			if (token.host && streamHost.includes('api-dev') && !/dev|stg/i.test(token.host)) {
				this.logger.warn(
					`Likely environment mismatch: production-looking JWT host '${token.host}' on the DEV socket '${streamHost}'. Use wss://api.tradelocker.com with a PRODUCTION developer-api-key, or test with a dev account.`
				);
			}

			connection = new TradeLockerConnection(
				session.config.environment,
				streamHost,
				session.developerApiKey,
				this.logger,
				{
					onAccountStatus: (m) =>
						this.logger.log(
							`AccountStatus acc=${m.accountId} balance=${m.balance ?? '-'} equity=${m.equity ?? '-'} openPnls=${m.positionPnLs?.length ?? 0}`
						),
					onOpenOrder: (m) =>
						this.logger.log(
							`OpenOrder order=${m.orderId} pos=${m.positionId ?? '-'} ${m.side} ${m.instrument} isOpen=${m.isOpen ?? '-'}`
						),
					onPosition: (m) =>
						this.logger.log(
							`Position pos=${m.positionId} ${m.side} ${m.instrument} lots=${m.lots ?? '-'} open=${m.openPrice ?? '-'} at=${m.openDateTime ?? '-'}`
						),
					onClosePosition: (m) =>
						this.logger.log(
							`ClosePosition pos=${m.positionId} closePrice=${m.closePrice ?? '-'} at=${m.closeDateTime ?? '-'}`
						),
					onSyncEnd: () =>
						this.logger.log('SyncEnd — initial snapshot complete; realtime deltas follow'),
					onJwtInvalid: (reason) =>
						this.logger.warn(`JWT invalid: ${reason} — will re-auth on reconnect`),
					onLost: (reason) => this.handleLost(generation, reason),
				}
			);
			session.connection = connection;

			await connection.open();
			if (superseded()) {
				connection.close();
				return;
			}

			await connection.subscribe(token.accessToken, token.accountId, token.brandId);
			if (superseded()) {
				connection.close();
				return;
			}

			session.reconnectAttempt = 0;
			this.logger.log(`[${session.config.environment}] live — streaming account ${token.accountId}`);
		} catch (error) {
			// Always close THIS attempt's own connection (never the shared session ref, which a newer
			// reconnect generation may already own).
			connection?.close();
			if (superseded()) {
				return;
			}
			session.connection = undefined;

			if (error instanceof TradeLockerSubscribeError && error.fatal) {
				this.logger.error(`SUBSCRIBE rejected (${error.code}) — not reconnecting: ${error.message}`);
				Sentry.captureException(error);
				return;
			}

			this.logger.error(`connect failed: ${describe(error)}`);
			Sentry.captureException(error);
			this.scheduleReconnect();
		}
	}

	private handleLost(generation: number, reason: string): void {
		const session = this.session;
		if (!session || session.generation !== generation) {
			return; // a stale connection lost — the current attempt already moved on
		}
		this.logger.warn(`connection lost: ${reason}`);
		session.connection?.close();
		session.connection = undefined;
		this.scheduleReconnect();
	}

	private scheduleReconnect(): void {
		const session = this.session;
		if (this.stopped || !session || session.reconnectTimer) {
			return;
		}

		session.reconnectAttempt += 1;
		const delay = Math.min(
			RECONNECT_BASE_DELAY_MS * 2 ** (session.reconnectAttempt - 1),
			RECONNECT_MAX_DELAY_MS
		);
		this.logger.log(`reconnecting in ${delay}ms (attempt ${session.reconnectAttempt})`);

		session.reconnectTimer = setTimeout(() => {
			session.reconnectTimer = undefined;
			void this.connect().catch((error) => {
				this.logger.error(`reconnect attempt crashed: ${describe(error)}`);
				Sentry.captureException(error);
			});
		}, delay);
	}

	private readPocConfig(): PocAccountConfig | undefined {
		const email = this.config.get('TRADELOCKER_POC_EMAIL', { infer: true })?.trim();
		const password = this.config.get('TRADELOCKER_POC_PASSWORD', { infer: true });
		const server = this.config.get('TRADELOCKER_POC_SERVER', { infer: true })?.trim();
		const environment = this.config.get('TRADELOCKER_POC_ENVIRONMENT', { infer: true }) ?? 'demo';
		const accountId = this.config.get('TRADELOCKER_POC_ACCOUNT_ID', { infer: true })?.trim();

		if (!email || !password || !server) {
			return undefined;
		}
		return { email, password, server, environment, accountId: accountId || undefined };
	}
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
