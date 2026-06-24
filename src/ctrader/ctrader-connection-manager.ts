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
	RECONCILE_INTERVAL_MS,
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
	/** Signature of the account set this session is connected for; a change triggers a reconnect. */
	signature: string;
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
	private readonly accountsByCtid = new Map<string, PoolAccount>();
	private stopped = false;
	private reconcileTimer?: NodeJS.Timeout;
	private reconciling = false;
	/** Last (count, max-updatedAt) of active tokens — a cheap gate before a full reload. */
	private lastProbe: { count: number; updatedAt: number } = { count: -1, updatedAt: 0 };

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
		if (this.reconcileTimer) {
			clearInterval(this.reconcileTimer);
			this.reconcileTimer = undefined;
		}
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

		const accounts = this.dedupeByCtid(await this.loadAccounts());
		if (accounts.length === 0) {
			// Don't bail — arm the reconcile loop so the first account a user connects is picked up.
			this.logger.warn('No active cTrader accounts in DB yet — waiting for one to be connected.');
		}

		for (const account of accounts) {
			this.accountsByCtid.set(this.ctidKey(account.environment, account.ctidTraderAccountId), account);
			const session = this.sessions.get(account.environment) ?? {
				accounts: [],
				reconnectAttempt: 0,
				generation: 0,
				signature: '',
			};
			session.accounts.push(account);
			this.sessions.set(account.environment, session);
		}

		for (const session of this.sessions.values()) {
			session.signature = this.computeSignature(session.accounts);
		}

		for (const environment of this.sessions.keys()) {
			void this.connectEnv(environment).catch((error) =>
				this.logger.error(`[${environment}] connect attempt crashed: ${describe(error)}`)
			);
		}

		this.reconcileTimer = setInterval(() => {
			void this.reconcile();
		}, RECONCILE_INTERVAL_MS);
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
			// Re-sync the signature from the (possibly token-refreshed) accounts so the manager's own
			// token refresh during this connect isn't mistaken for an external change next reconcile.
			session.signature = this.computeSignature(session.accounts);
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
		const account = this.accountsByCtid.get(this.ctidKey(environment, ctid));
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

	/**
	 * Periodically reconcile live connections with the DB so accounts a user connects (or removes)
	 * are picked up without a process restart. A cheap aggregate probe (count + max updatedAt of
	 * active tokens) gates the full reload, so the steady-state cost is one trivial query per tick.
	 */
	private async reconcile(): Promise<void> {
		if (this.stopped || this.reconciling) {
			return; // a slow tick must not overlap the next interval fire
		}
		this.reconciling = true;
		try {
			const probe = await this.prisma.ctraderToken.aggregate({
				where: { isActive: true },
				_count: { _all: true },
				_max: { updatedAt: true },
			});
			const count = probe._count._all;
			const updatedAt = probe._max.updatedAt?.getTime() ?? 0;
			if (count === this.lastProbe.count && updatedAt === this.lastProbe.updatedAt) {
				return; // nothing changed since the last tick
			}
			this.lastProbe = { count, updatedAt };

			const fresh = this.dedupeByCtid(await this.loadAccounts());
			if (this.stopped) {
				return;
			}
			this.applyReconcile(fresh);
		} catch (error) {
			this.logger.error(`reconcile failed: ${describe(error)}`);
		} finally {
			this.reconciling = false;
		}
	}

	/**
	 * Apply the freshly-loaded account set to the live sessions. Fully synchronous (no awaits) so the
	 * mutation of sessions / accountsByCtid is atomic relative to any suspended connectEnv. On a
	 * change it bumps the session generation (superseding any in-flight connect) and reuses the
	 * hardened reconnect path; the array reference is REPLACED, never mutated, so a connect loop that
	 * is iterating the old array finishes harmlessly before bailing on its superseded() check.
	 */
	private applyReconcile(fresh: PoolAccount[]): void {
		const freshByEnv = new Map<CtraderEnvironment, PoolAccount[]>();
		for (const account of fresh) {
			const list = freshByEnv.get(account.environment) ?? [];
			list.push(account);
			freshByEnv.set(account.environment, list);
		}

		const environments = new Set<CtraderEnvironment>([...this.sessions.keys(), ...freshByEnv.keys()]);

		for (const environment of environments) {
			const desired = freshByEnv.get(environment) ?? [];
			const session = this.sessions.get(environment);

			if (!session) {
				if (desired.length === 0) {
					continue;
				}
				// New environment: seed a session and connect it (connectEnv bumps generation to 1).
				this.sessions.set(environment, {
					accounts: desired,
					reconnectAttempt: 0,
					generation: 0,
					signature: this.computeSignature(desired),
				});
				this.setRouting(environment, desired);
				this.logger.log(
					`[${environment}] new environment with ${desired.length} account(s) — connecting`
				);
				void this.connectEnv(environment).catch((error) =>
					this.logger.error(`[${environment}] connect attempt crashed: ${describe(error)}`)
				);
				continue;
			}

			if (desired.length === 0) {
				// Environment went empty: tear the socket down and drop the session.
				this.logger.warn(`[${environment}] no active accounts remain — tearing down`);
				if (session.reconnectTimer) {
					clearTimeout(session.reconnectTimer);
					session.reconnectTimer = undefined;
				}
				session.generation += 1; // supersede any in-flight connect before we drop the session
				const connection = session.connection;
				session.connection = undefined; // clear before close so nothing sees a half-closed socket
				connection?.close();
				this.clearRouting(environment, session.accounts);
				this.sessions.delete(environment);
				continue;
			}

			const signature = this.computeSignature(desired);
			if (signature === session.signature) {
				continue; // no change for this environment
			}

			this.logger.log(`[${environment}] account set changed — reconnecting`);
			this.clearRouting(environment, session.accounts);
			session.accounts = desired; // replace the reference; never mutate the array a loop may hold
			this.setRouting(environment, desired);
			session.signature = signature;
			// Bump generation FIRST so any in-flight connect supersedes itself at its next await. Then
			// pass the NEW generation to handleLost so its guard PASSES and it closes the socket and
			// schedules the single reconnect that picks up `desired`. (Passing the old generation would
			// make handleLost no-op and the reconnect would never fire — this is intentional.)
			session.generation += 1;
			this.handleLost(environment, session.generation, 'reconcile: account set changed');
		}
	}

	private setRouting(environment: CtraderEnvironment, accounts: PoolAccount[]): void {
		for (const account of accounts) {
			this.accountsByCtid.set(this.ctidKey(environment, account.ctidTraderAccountId), account);
		}
	}

	private clearRouting(environment: CtraderEnvironment, accounts: PoolAccount[]): void {
		for (const account of accounts) {
			this.accountsByCtid.delete(this.ctidKey(environment, account.ctidTraderAccountId));
		}
	}

	/** Stable fingerprint of an environment's account set: a change means reconnect is needed. */
	private computeSignature(accounts: PoolAccount[]): string {
		return [...accounts]
			.sort((a, b) => a.ctidTraderAccountId - b.ctidTraderAccountId)
			.map((a) => `${a.ctidTraderAccountId}:${a.tokenId}:${a.expiresAt?.getTime() ?? 0}`)
			.join('|');
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

	/**
	 * A cTrader account (ctidTraderAccountId) must be managed by exactly one trading account — the
	 * product blocks duplicate connections at connect time, but defend against stale/duplicate
	 * tokens here too. If several accounts share a ctid, keep the one with the freshest token
	 * (latest expiry) and drop the rest, so we authenticate each ctid ONCE (no redundant
	 * accountAuth, no "[object Object]" noise) and the surviving account is the one whose token
	 * actually authenticates the socket (its symbol list resolves names instead of raw ids).
	 * Grouped by (environment, ctid) so a ctid is never collapsed across environments.
	 */
	private dedupeByCtid(accounts: PoolAccount[]): PoolAccount[] {
		const byCtid = new Map<string, PoolAccount[]>();
		for (const account of accounts) {
			const key = this.ctidKey(account.environment, account.ctidTraderAccountId);
			const list = byCtid.get(key) ?? [];
			list.push(account);
			byCtid.set(key, list);
		}

		const kept: PoolAccount[] = [];
		for (const [key, list] of byCtid) {
			if (list.length === 1) {
				kept.push(list[0]);
				continue;
			}
			// Freshest token wins; an unknown (null) expiry sorts as epoch 0, below any real token.
			const ranked = [...list].sort((a, b) => this.tokenFreshness(b) - this.tokenFreshness(a));
			const [winner, ...skipped] = ranked;
			kept.push(winner);
			this.logger.warn(
				`${key} linked by ${list.length} trading accounts — keeping ${winner.tradingAccountId} (user ${winner.userId}), skipping ${skipped.map((a) => a.tradingAccountId).join(', ')}`
			);
		}
		return kept;
	}

	/** Sort key for picking the freshest token among accounts sharing a ctid (null expiry = epoch 0). */
	private tokenFreshness(account: PoolAccount): number {
		return account.expiresAt?.getTime() ?? 0;
	}

	/** Key for accountsByCtid / dedup: a ctid is unique per environment, so scope it by environment. */
	private ctidKey(environment: CtraderEnvironment, ctid: number): string {
		return `${environment}:${ctid}`;
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
