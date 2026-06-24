import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import type { Env } from '../config';
import { decrypt, encrypt } from '../crypto';
import { PrismaService } from '../prisma';
import { refreshAccessToken } from './ctrader-client';
import { CtraderConnection } from './ctrader-connection';
import { CtraderTradeWriter } from './ctrader-trade-writer';
import {
	DEFAULT_TOKEN_LIFETIME_MS,
	RECONNECT_BASE_DELAY_MS,
	RECONNECT_MAX_DELAY_MS,
	TOKEN_REFRESH_SKEW_MS,
} from './ctrader.constants';
import type { CtraderEnvironment, ExecutionEventPayload, PoolAccount } from './types';

interface EnvSession {
	accounts: PoolAccount[];
	connection?: CtraderConnection;
	reconnectAttempt: number;
	reconnectTimer?: NodeJS.Timeout;
	/** Bumped on every (re)connect attempt; lets a superseded attempt detect it lost ownership. */
	generation: number;
}

/**
 * Keeps cTrader accounts connected for the lifetime of the process. One socket per environment
 * (demo/live) multiplexes all of that environment's accounts. Each socket reconnects with
 * exponential backoff, refreshes expired tokens (writing the new pair back, encrypted), and
 * re-authenticates every account on reconnect. Execution events are logged here — Phase 3 will
 * persist them.
 *
 * Concurrency: every connect attempt captures a generation id. After each await it checks whether
 * it has been superseded (shutdown, or a newer attempt took over) and bails without touching shared
 * state, so a connection lost mid-handshake can't clobber the reconnect that replaces it.
 */
@Injectable()
export class CtraderConnectionManager implements OnModuleInit, OnModuleDestroy {
	private readonly logger = new Logger(CtraderConnectionManager.name);
	private readonly sessions = new Map<CtraderEnvironment, EnvSession>();
	private readonly accountsByCtid = new Map<number, PoolAccount>();
	private stopped = false;

	constructor(
		private readonly config: ConfigService<Env, true>,
		private readonly prisma: PrismaService,
		private readonly writer: CtraderTradeWriter
	) {}

	onModuleInit(): void {
		// Fire-and-forget so a slow/unreachable cTrader never blocks bootstrap (health stays up).
		void this.start().catch((error) =>
			this.logger.error(`cTrader manager start failed: ${describe(error)}`)
		);
	}

	onModuleDestroy(): void {
		this.stopped = true;
		for (const session of this.sessions.values()) {
			if (session.reconnectTimer) {
				clearTimeout(session.reconnectTimer);
			}
			session.connection?.close();
		}
		this.sessions.clear();
		this.accountsByCtid.clear();
	}

	private async start(): Promise<void> {
		if (!this.appCredentials()) {
			this.logger.warn('CTRADER_CLIENT_ID / CTRADER_CLIENT_SECRET not set — cTrader manager idle.');
			return;
		}

		const accounts = await this.loadAccounts();
		if (accounts.length === 0) {
			this.logger.warn('No active cTrader accounts in DB — cTrader manager idle.');
			return;
		}

		for (const account of accounts) {
			this.accountsByCtid.set(account.ctidTraderAccountId, account);
			const session = this.sessions.get(account.environment) ?? {
				accounts: [],
				reconnectAttempt: 0,
				generation: 0,
			};
			session.accounts.push(account);
			this.sessions.set(account.environment, session);
		}

		for (const environment of this.sessions.keys()) {
			void this.connectEnv(environment).catch((error) =>
				this.logger.error(`[${environment}] connect attempt crashed: ${describe(error)}`)
			);
		}
	}

	private async connectEnv(environment: CtraderEnvironment): Promise<void> {
		if (this.stopped) {
			return;
		}
		const session = this.sessions.get(environment);
		if (!session) {
			return;
		}
		const credentials = this.appCredentials();
		if (!credentials) {
			return;
		}

		const generation = ++session.generation;
		const superseded = (): boolean => this.stopped || session.generation !== generation;

		const connection = new CtraderConnection(environment, this.logger, {
			onExecution: (ctid, payload) => this.handleExecution(environment, ctid, payload),
			onTokenInvalidated: (ctids, reason) =>
				this.handleTokenInvalidated(environment, generation, ctids, reason),
			onLost: (reason) => this.handleLost(environment, generation, reason),
		});
		session.connection = connection;

		try {
			this.logger.log(`[${environment}] opening connection for ${session.accounts.length} account(s)`);
			await connection.open();
			if (superseded()) {
				connection.close();
				return;
			}

			await connection.applicationAuth(credentials.clientId, credentials.clientSecret);
			if (superseded()) {
				connection.close();
				return;
			}

			let authenticated = 0;
			for (const account of session.accounts) {
				if (superseded()) {
					connection.close();
					return;
				}
				try {
					await this.ensureFreshToken(account);
					await connection.accountAuth(account.accessToken, account.ctidTraderAccountId);
					account.symbolNames = await this.fetchSymbolNames(
						connection,
						account.ctidTraderAccountId,
						environment
					);
					authenticated += 1;
					this.logger.log(`[${environment}] account ${account.ctidTraderAccountId} authenticated`);
				} catch (error) {
					// Isolate per-account failures (bad/revoked token) so one account can't take the
					// whole environment's socket down.
					this.logger.error(
						`[${environment}] account ${account.ctidTraderAccountId} auth failed (skipped): ${describe(error)}`
					);
				}
			}

			if (superseded()) {
				connection.close();
				return;
			}
			session.reconnectAttempt = 0;
			this.logger.log(
				`[${environment}] live — ${authenticated}/${session.accounts.length} account(s) authenticated, heartbeat + liveness running`
			);
		} catch (error) {
			if (superseded()) {
				connection.close();
				return;
			}
			this.logger.error(`[${environment}] connect failed: ${describe(error)}`);
			connection.close();
			session.connection = undefined;
			this.scheduleReconnect(environment);
		}
	}

	private handleLost(environment: CtraderEnvironment, generation: number, reason: string): void {
		const session = this.sessions.get(environment);
		if (!session || session.generation !== generation) {
			return; // a stale connection lost — the current one already moved on
		}
		this.logger.warn(`[${environment}] connection lost: ${reason}`);
		session.connection?.close();
		session.connection = undefined;
		this.scheduleReconnect(environment);
	}

	private handleTokenInvalidated(
		environment: CtraderEnvironment,
		generation: number,
		ctids: number[],
		reason: string
	): void {
		const session = this.sessions.get(environment);
		if (!session || session.generation !== generation) {
			return;
		}
		this.logger.warn(
			`[${environment}] token invalidated for [${ctids.join(', ')}]: ${reason} — forcing refresh + reconnect`
		);
		for (const account of session.accounts) {
			if (ctids.includes(account.ctidTraderAccountId)) {
				account.expiresAt = new Date(0); // force ensureFreshToken to refresh on reconnect
			}
		}
		this.handleLost(environment, generation, `token invalidated: ${reason}`);
	}

	private handleExecution(
		environment: CtraderEnvironment,
		ctid: number,
		payload: ExecutionEventPayload
	): void {
		const account = this.accountsByCtid.get(ctid);
		if (!account) {
			this.logger.warn(`[${environment}] execution event for unknown account ${ctid} — ignored`);
			return;
		}
		void this.writer
			.record(account, payload)
			.catch((error) =>
				this.logger.error(`[${environment}] trade write failed for ${ctid}: ${describe(error)}`)
			);
	}

	private async fetchSymbolNames(
		connection: CtraderConnection,
		ctid: number,
		environment: CtraderEnvironment
	): Promise<Map<number, string>> {
		const map = new Map<number, string>();
		try {
			const res = (await connection.request('ProtoOASymbolsListReq', {
				ctidTraderAccountId: ctid,
				includeArchivedSymbols: true,
			})) as {
				symbol?: Array<{ symbolId?: string | number; symbolName?: string }>;
				archivedSymbol?: Array<{ symbolId?: string | number; name?: string }>;
			};
			for (const sym of res.symbol ?? []) {
				const id = Number(sym.symbolId);
				if (Number.isFinite(id) && sym.symbolName) {
					map.set(id, sym.symbolName);
				}
			}
			for (const sym of res.archivedSymbol ?? []) {
				const id = Number(sym.symbolId);
				if (Number.isFinite(id) && sym.name) {
					map.set(id, sym.name);
				}
			}
		} catch (error) {
			this.logger.warn(
				`[${environment}] symbol list failed for ${ctid} (names fall back to id): ${describe(error)}`
			);
		}
		return map;
	}

	private scheduleReconnect(environment: CtraderEnvironment): void {
		if (this.stopped) {
			return;
		}
		const session = this.sessions.get(environment);
		if (!session || session.reconnectTimer) {
			return;
		}

		session.reconnectAttempt += 1;
		const delay = Math.min(
			RECONNECT_BASE_DELAY_MS * 2 ** (session.reconnectAttempt - 1),
			RECONNECT_MAX_DELAY_MS
		);
		this.logger.log(
			`[${environment}] reconnecting in ${delay}ms (attempt ${session.reconnectAttempt})`
		);

		session.reconnectTimer = setTimeout(() => {
			session.reconnectTimer = undefined;
			void this.connectEnv(environment).catch((error) =>
				this.logger.error(`[${environment}] connect attempt crashed: ${describe(error)}`)
			);
		}, delay);
	}

	/** Refresh the account's access token in place (and in the DB) when it is unknown or near expiry. */
	private async ensureFreshToken(account: PoolAccount): Promise<void> {
		// A null expiry is treated as "unknown — refresh now" so a stale stored token isn't used.
		const expiresInMs = account.expiresAt ? account.expiresAt.getTime() - Date.now() : -Infinity;
		if (expiresInMs > TOKEN_REFRESH_SKEW_MS) {
			return;
		}

		const credentials = this.appCredentials();
		if (!credentials) {
			return;
		}

		this.logger.log(`Refreshing access token for account ${account.ctidTraderAccountId}`);
		const tokens = await refreshAccessToken({
			clientId: credentials.clientId,
			clientSecret: credentials.clientSecret,
			refreshToken: account.refreshToken,
		});

		const lifetimeMs = tokens.expiresIn > 0 ? tokens.expiresIn * 1000 : DEFAULT_TOKEN_LIFETIME_MS;
		const expiresAt = new Date(Date.now() + lifetimeMs);

		await this.prisma.ctraderToken.update({
			where: { id: account.tokenId },
			data: {
				accessToken: encrypt(tokens.accessToken),
				refreshToken: encrypt(tokens.refreshToken),
				expiresAt,
			},
		});

		account.accessToken = tokens.accessToken;
		account.refreshToken = tokens.refreshToken;
		account.expiresAt = expiresAt;
	}

	private async loadAccounts(): Promise<PoolAccount[]> {
		const tokens = await this.prisma.ctraderToken.findMany({
			where: { isActive: true },
			select: {
				id: true,
				tradingAccountId: true,
				ctidTraderAccountId: true,
				accessToken: true,
				refreshToken: true,
				expiresAt: true,
				environment: true,
				tradingAccount: { select: { userId: true, nominal: true } },
			},
		});

		const accounts: PoolAccount[] = [];
		for (const token of tokens) {
			const ctid = Number(token.ctidTraderAccountId);
			if (!Number.isFinite(ctid) || ctid <= 0) {
				this.logger.warn(`Skipping token ${token.id}: invalid ctidTraderAccountId`);
				continue;
			}
			if (!token.tradingAccount) {
				this.logger.warn(`Skipping token ${token.id}: no linked trading account`);
				continue;
			}
			try {
				accounts.push({
					tokenId: token.id,
					tradingAccountId: token.tradingAccountId,
					ctidTraderAccountId: ctid,
					accessToken: decrypt(token.accessToken),
					refreshToken: decrypt(token.refreshToken),
					expiresAt: token.expiresAt,
					environment: token.environment === 'live' ? 'live' : 'demo',
					userId: token.tradingAccount.userId,
					nominal: token.tradingAccount.nominal,
				});
			} catch (error) {
				this.logger.error(`Skipping token ${token.id}: ${describe(error)}`);
			}
		}
		return accounts;
	}

	private appCredentials(): { clientId: string; clientSecret: string } | undefined {
		const clientId = this.config.get('CTRADER_CLIENT_ID', { infer: true });
		const clientSecret = this.config.get('CTRADER_CLIENT_SECRET', { infer: true });
		if (!clientId || !clientSecret) {
			return undefined;
		}
		return { clientId, clientSecret };
	}
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
