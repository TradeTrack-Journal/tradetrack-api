import { Injectable, Logger, OnApplicationBootstrap, OnApplicationShutdown } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import * as Sentry from '@sentry/nestjs';

import type { Env } from '../config';
import { decrypt } from '../crypto';
import { PrismaService } from '../prisma';
import { authenticateAccounts, authenticateUser } from './client';
import { TradeLockerConnection, TradeLockerSubscribeError } from './connection';
import {
	CLOSE_TOPUP_MAX_RETRIES,
	PERIODIC_BACKFILL_INTERVAL_MS,
	PERIODIC_BACKFILL_MAX_PAGES,
	RECONCILE_INTERVAL_MS,
	RECONNECT_BASE_DELAY_MS,
	RECONNECT_MAX_DELAY_MS,
	REST_MIN_INTERVAL_MS,
	TRADELOCKER_AUTH_HOSTS,
	TRADELOCKER_STREAM_HOSTS,
	USER_TOKEN_TTL_MS,
} from './constants';
import { fetchClosedTrades, type FetchClosedTradesParams } from './history';
import { sendTelegramMessage } from './telegram';
import type { ClosePositionMessage, PositionMessage } from './stream.schema';
import { TradeWriterService } from './trade-writer.service';
import type {
	ClosedTrade,
	OpenTrade,
	TradeLockerAccountContext,
	TradeLockerAccountToken,
	TradeLockerEnvironment,
} from './types';

/** A TradeLocker account resolved from the DB — the identity + decrypted login the worker streams. */
interface ManagedAccount {
	tradingAccountId: string;
	userId: string;
	nominal: number;
	/** TradeLocker account id as the stream/token reports it (matches TradingAccount.terminalAccountId). */
	accountId: string;
	/** Account number for the REST `accNum` header. Required — closes can't be finalized without it. */
	accNum: string;
	server: string;
	environment: TradeLockerEnvironment;
	email: string;
	/** Decrypted login password. */
	password: string;
	/** Fingerprint of the identity/credentials; a change triggers a reconnect on reconcile. */
	signature: string;
}

interface AccountSession {
	account: ManagedAccount;
	token?: TradeLockerAccountToken;
	connection?: TradeLockerConnection;
	reconnectAttempt: number;
	reconnectTimer?: NodeJS.Timeout;
	/** Bumped on every (re)connect; lets a superseded attempt detect it lost ownership. */
	generation: number;
	/** Epoch ms of the last close-history backfill; gates the periodic safety-net scan. */
	lastBackfillAt?: number;
}

/**
 * Phase 3: keeps every enabled TradeLocker account streaming for the process lifetime and persists
 * its trades. Opens are taken from the live `Position` stream; closes are finalized from REST
 * `close-trades-history` (the live `ClosePosition` carries no realized money). The worker is the
 * single owner of live accounts — the main app's cron must exclude them.
 *
 * One stream connection per account (the Streams SUBSCRIBE is per-account). Accounts that share a
 * login (email/server/environment) authenticate once — a credential set returns one token per
 * account — and the right token is matched back to each account by id. Each account reconnects with
 * its own exponential backoff and a generation guard, so a drop mid-handshake can't clobber the
 * reconnect that replaces it. A reconcile loop adds/removes accounts as users connect or disconnect
 * them, without a restart.
 */
@Injectable()
export class ConnectionManagerService implements OnApplicationBootstrap, OnApplicationShutdown {
	private readonly logger = new Logger(ConnectionManagerService.name);
	private readonly sessions = new Map<string, AccountSession>();
	/** In-flight auth per login group, so a group's accounts don't each fire a separate login. */
	private readonly groupAuths = new Map<string, Promise<TradeLockerAccountToken[]>>();
	/** Cached user-level REST token per login group (the /trade/* endpoints reject the stream JWT). */
	private readonly userTokens = new Map<string, { token: string; fetchedAt: number }>();
	private readonly userAuthInflight = new Map<string, Promise<string>>();
	/** Closes the live stream reported before close-trades-history caught up; retried on each tick. */
	private readonly pendingCloses = new Map<
		string,
		{ session: AccountSession; positionId: string; attempts: number }
	>();
	/** Positions whose REST finalization is in flight, so a duplicate close can't double-fetch/write. */
	private readonly finalizingCloses = new Set<string>();
	/**
	 * Positions already finalized as closed this process (closeKey). Lets the periodic backfill skip
	 * re-upserting an unchanged realized row, and lets a live close short-circuit when a backfill
	 * already captured it. Grows with realized volume over the process lifetime — bounded in practice
	 * and reset on restart.
	 */
	private readonly closedSeen = new Set<string>();
	/** Global pacer for ALL close-history requests — one login's accounts share the 5-req/10s limit. */
	private readonly restPacer = createPacer(REST_MIN_INTERVAL_MS);
	private developerApiKey = '';
	private stopped = false;
	private reconcileTimer?: NodeJS.Timeout;
	private reconciling = false;
	/** Fire the "live close captured" Telegram ping only once per process. */
	private liveCloseNotified = false;
	/** Log the ONLY_USER_IDS scope note once, not on every reconcile tick's loadAccounts(). */
	private onlyUsersLogged = false;

	constructor(
		private readonly config: ConfigService<Env, true>,
		private readonly prisma: PrismaService,
		private readonly writer: TradeWriterService
	) {}

	onApplicationBootstrap(): void {
		void this.start().catch((error) => {
			this.logger.error(`TradeLocker manager start failed: ${describe(error)}`);
			Sentry.captureException(error);
		});
	}

	onApplicationShutdown(): void {
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
		this.pendingCloses.clear();
		this.closedSeen.clear();
	}

	private async start(): Promise<void> {
		// Master switch — stays idle unless explicitly enabled.
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
		this.developerApiKey = developerApiKey;

		const accounts = await this.loadAccounts();
		if (accounts.length === 0) {
			this.logger.warn(
				'No active TradeLocker accounts in DB yet — waiting for one to be connected (reconcile is armed).'
			);
		}
		for (const account of accounts) {
			this.startAccount(account);
		}

		this.reconcileTimer = setInterval(() => {
			void this.reconcile();
		}, RECONCILE_INTERVAL_MS);
	}

	private startAccount(account: ManagedAccount): void {
		// Seed the backfill clock so the periodic net waits a full interval — the initial SyncEnd
		// backfill covers the account's existing history right after it connects.
		const session: AccountSession = {
			account,
			reconnectAttempt: 0,
			generation: 0,
			lastBackfillAt: Date.now(),
		};
		this.sessions.set(account.tradingAccountId, session);
		this.logger.log(
			`connecting account ${account.accountId} (user ${account.userId}, env ${account.environment})`
		);
		void this.connect(session).catch((error) => {
			this.logger.error(`[${account.accountId}] connect attempt crashed: ${describe(error)}`);
			Sentry.captureException(error);
		});
	}

	private async connect(session: AccountSession): Promise<void> {
		if (this.stopped || this.sessions.get(session.account.tradingAccountId) !== session) {
			return;
		}
		const generation = ++session.generation;
		const superseded = (): boolean =>
			this.stopped ||
			this.sessions.get(session.account.tradingAccountId) !== session ||
			session.generation !== generation;

		const account = session.account;
		// Declared before the try so the catch closes THIS attempt's connection, never a newer one.
		let connection: TradeLockerConnection | undefined;
		try {
			const tokens = await this.authGroup(account);
			if (superseded()) {
				return;
			}
			const token = this.matchToken(tokens, account);
			if (!token) {
				// The login didn't return this account (removed upstream / wrong creds). Don't hot-loop —
				// reconcile or a credentials change will revive it.
				this.logger.warn(
					`[${account.accountId}] no matching token among ${tokens.length} issued — not connecting`
				);
				return;
			}
			session.token = token;

			const streamHost =
				this.config.get('TRADELOCKER_STREAM_BASE_URL', { infer: true })?.trim() ||
				TRADELOCKER_STREAM_HOSTS[account.environment];

			connection = new TradeLockerConnection(
				account.environment,
				streamHost,
				this.developerApiKey,
				this.logger,
				{
					onAccountStatus: () => undefined, // unrealized balances aren't persisted
					onOpenOrder: () => undefined, // positions, not orders, drive Trade rows
					onPosition: (m) => this.handlePosition(session, m),
					onClosePosition: (m) => this.handleClose(session, m),
					onSyncEnd: () => this.handleSyncEnd(session),
					onJwtInvalid: () => {
						session.token = undefined; // force a fresh login on the next reconnect
					},
					onLost: (reason) => this.handleLost(session, generation, reason),
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
			this.logger.log(`[${account.environment}] live — streaming account ${account.accountId}`);
		} catch (error) {
			connection?.close();
			if (superseded()) {
				return;
			}
			session.connection = undefined;

			if (error instanceof TradeLockerSubscribeError && error.fatal) {
				this.logger.error(
					`[${account.accountId}] SUBSCRIBE rejected (${error.code}) — not reconnecting: ${error.message}`
				);
				Sentry.captureException(error);
				return;
			}

			this.logger.error(`[${account.accountId}] connect failed: ${describe(error)}`);
			Sentry.captureException(error);
			this.scheduleReconnect(session);
		}
	}

	/**
	 * Authenticate one login group, de-duplicating concurrent calls so a group's accounts share a
	 * single login (a credential set returns one token per account it can access). The in-flight
	 * promise is cleared once settled, so a later reconnect re-logs in fresh — TradeLocker has no
	 * refresh grant, re-login IS the refresh.
	 */
	private authGroup(account: ManagedAccount): Promise<TradeLockerAccountToken[]> {
		const key = this.groupKey(account);
		const existing = this.groupAuths.get(key);
		if (existing) {
			return existing;
		}
		const baseUrl =
			this.config.get('TRADELOCKER_AUTH_BASE_URL', { infer: true })?.trim() ||
			TRADELOCKER_AUTH_HOSTS[account.environment];
		const inflight = authenticateAccounts({
			baseUrl,
			email: account.email,
			password: account.password,
			server: account.server,
		}).finally(() => {
			if (this.groupAuths.get(key) === inflight) {
				this.groupAuths.delete(key);
			}
		});
		this.groupAuths.set(key, inflight);
		return inflight;
	}

	private handlePosition(session: AccountSession, msg: PositionMessage): void {
		const open = mapOpenTrade(msg);
		void this.writer
			.recordOpen(this.context(session.account), open)
			.catch((error) =>
				this.logger.error(`[${session.account.accountId}] open write failed: ${describe(error)}`)
			);
	}

	private handleClose(session: AccountSession, msg: ClosePositionMessage): void {
		void this.topUpClose(session, msg.positionId).catch((error) =>
			this.logger.error(
				`[${session.account.accountId}] close top-up failed for ${msg.positionId}: ${describe(error)}`
			)
		);
	}

	private handleSyncEnd(session: AccountSession): void {
		this.logger.log(`[${session.account.accountId}] SyncEnd — backfilling recent closed trades`);
		// Catch closes that happened during downtime (the open snapshot only carries OPEN positions).
		// This resets the periodic clock — a fresh reconnect just did a full scan.
		session.lastBackfillAt = Date.now();
		void this.backfillCloses(session).catch((error) =>
			this.logger.error(`[${session.account.accountId}] close backfill failed: ${describe(error)}`)
		);
	}

	/** Fetch the realized row for a position and finalize the Trade. Returns true once written. */
	private async finalizeClose(session: AccountSession, positionId: string): Promise<boolean> {
		const key = this.closeKey(session, positionId);
		if (this.closedSeen.has(key)) {
			this.pendingCloses.delete(key);
			return true; // a backfill already finalized this position — nothing left to fetch/write
		}
		if (this.finalizingCloses.has(key)) {
			return false; // a finalize is already running for this position (duplicate close / retry overlap)
		}
		// Claim the position synchronously (no await between has() and add()) so two concurrent finalizes
		// — a live close top-up and a retry tick — can't both slip past the guard and double-fetch/write.
		this.finalizingCloses.add(key);
		try {
			const rest = await this.restParamsFor(session);
			if (!rest) {
				return false;
			}
			const closed = await fetchClosedTrades({ ...rest, untilPositionId: positionId });
			const match = closed.find((c) => c.positionId === positionId);
			if (!match) {
				return false;
			}
			await this.writer.recordClosed(this.context(session.account), match);
			this.closedSeen.add(key);
			this.pendingCloses.delete(key);
			// finalizeClose only ever runs for a LIVE close (ClosePosition event or its retry) — backfill
			// writes directly — so this is the signal that the live close path worked end-to-end.
			this.notifyFirstLiveClose(session.account, match);
			return true;
		} finally {
			this.finalizingCloses.delete(key);
		}
	}

	/** Finalize a just-closed position; if the report hasn't caught up, queue a bounded retry. */
	private async topUpClose(session: AccountSession, positionId: string): Promise<void> {
		if (await this.finalizeClose(session, positionId)) {
			return;
		}
		const key = this.closeKey(session, positionId);
		if (!this.pendingCloses.has(key)) {
			this.pendingCloses.set(key, { session, positionId, attempts: 0 });
		}
		this.logger.warn(
			`[${session.account.accountId}] close-history has no row for ${positionId} yet — will retry`
		);
	}

	/** Re-attempt closes the report lagged on; drop after a bounded number of tries. */
	private async retryPendingCloses(): Promise<void> {
		for (const [key, pending] of [...this.pendingCloses]) {
			const { session, positionId } = pending;
			// Session replaced/stopped → its next SyncEnd backfill is the backstop; stop tracking here.
			if (this.sessions.get(session.account.tradingAccountId) !== session) {
				this.pendingCloses.delete(key);
				continue;
			}
			// Don't burn an attempt while a finalize is already running, or mid-reconnect (no token yet) —
			// these are transient, not a genuine "report has no row".
			if (this.finalizingCloses.has(key) || !session.token) {
				continue;
			}
			try {
				if (await this.finalizeClose(session, positionId)) {
					continue;
				}
			} catch (error) {
				this.logger.error(
					`[${session.account.accountId}] close retry failed for ${positionId}: ${describe(error)}`
				);
			}
			pending.attempts += 1;
			if (pending.attempts >= CLOSE_TOPUP_MAX_RETRIES) {
				this.pendingCloses.delete(key);
				this.logger.warn(
					`[${session.account.accountId}] giving up close top-up for ${positionId} after ${pending.attempts} tries`
				);
			}
		}
	}

	/**
	 * Bounded pull of recent realized trades, finalizing each. Skips positions already finalized this
	 * process so the periodic net doesn't re-upsert an unchanged report page every few minutes.
	 * `maxPages` bounds the walk — the periodic scan passes 1 (only the newest closes can be recent
	 * misses); SyncEnd uses the deeper default to also cover closes that happened during downtime.
	 */
	private async backfillCloses(session: AccountSession, maxPages?: number): Promise<void> {
		const rest = await this.restParamsFor(session);
		if (!rest) {
			return;
		}
		const closed = await fetchClosedTrades(maxPages ? { ...rest, maxPages } : rest);
		const ctx = this.context(session.account);
		let written = 0;
		for (const trade of closed) {
			const key = this.closeKey(session, trade.positionId);
			if (this.closedSeen.has(key) || this.finalizingCloses.has(key)) {
				// Already finalized (authoritative row won't change), or a live finalize is mid-flight for
				// it — skip to avoid a duplicate upsert. If that finalize fails it re-queues via
				// pendingCloses / the next periodic scan, so skipping here never drops a close.
				continue;
			}
			await this.writer.recordClosed(ctx, trade);
			this.closedSeen.add(key);
			this.pendingCloses.delete(key);
			written += 1;
		}
		if (written > 0) {
			this.logger.log(
				`[${session.account.accountId}] backfilled ${written} closed trade(s) from REST`
			);
		}
	}

	private closeKey(session: AccountSession, positionId: string): string {
		return `${session.account.tradingAccountId}:${positionId}`;
	}

	/**
	 * Post a one-off "live close captured" Telegram ping (same bot/chat as the main app) the first time
	 * the live close path finalizes a trade — confirmation that the stream→REST→write chain works in
	 * prod. Best-effort and fire-and-forget; claims the flag synchronously to avoid a double-send race
	 * and resets it only if the send fails, so a transient failure still notifies on the next close.
	 */
	private notifyFirstLiveClose(account: ManagedAccount, closed: ClosedTrade): void {
		if (this.liveCloseNotified) {
			return;
		}
		this.liveCloseNotified = true;

		const botToken = this.config.get('TELEGRAM_BOT_TOKEN', { infer: true })?.trim();
		const chatId = this.config.get('TELEGRAM_FEEDBACK_CHAT_ID', { infer: true })?.trim();
		if (!botToken || !chatId) {
			this.logger.log('Live close captured (Telegram not configured — ping skipped).');
			return; // stays "notified" so we don't recheck every close
		}

		const net = closed.netProfit ?? closed.grossProfit + closed.commission + closed.swap;
		const text =
			`✅ <b>TradeLocker live-sync</b>\n` +
			`Перше живе закриття опрацьовано та записано в Trade.\n` +
			`Account: <code>${account.accountId}</code> (user <code>${account.userId}</code>)\n` +
			`Position: <code>${closed.positionId}</code>\n` +
			`${closed.side} ${closed.symbol} qty=${closed.quantity}\n` +
			`PnL(net): ${net}`;

		void sendTelegramMessage({ botToken, chatId, text }).then((ok) => {
			if (ok) {
				this.logger.log(`Live close captured — Telegram ping sent (pos ${closed.positionId}).`);
			} else {
				this.liveCloseNotified = false; // let the next live close retry the notification
				this.logger.warn('Live close Telegram ping failed — will retry on the next live close.');
			}
		});
	}

	private async restParamsFor(
		session: AccountSession
	): Promise<FetchClosedTradesParams | undefined> {
		const { account } = session;
		if (this.sessions.get(account.tradingAccountId) !== session) {
			return undefined; // session superseded/stopped — drop the work
		}
		const baseUrl =
			this.config.get('TRADELOCKER_AUTH_BASE_URL', { infer: true })?.trim() ||
			TRADELOCKER_AUTH_HOSTS[account.environment];
		// REST uses a USER-LEVEL token (not the per-account stream JWT, which /trade/* rejects with 400).
		const accessToken = await this.userAuth(account, baseUrl);
		return {
			baseUrl,
			accessToken,
			accNum: account.accNum,
			developerApiKey: this.developerApiKey,
			throttle: this.restPacer,
		};
	}

	/** User-level REST token for the account's login, cached per group with a TTL and in-flight dedup. */
	private userAuth(account: ManagedAccount, baseUrl: string): Promise<string> {
		const key = this.groupKey(account);
		const cached = this.userTokens.get(key);
		if (cached && Date.now() - cached.fetchedAt < USER_TOKEN_TTL_MS) {
			return Promise.resolve(cached.token);
		}
		const existing = this.userAuthInflight.get(key);
		if (existing) {
			return existing;
		}
		const inflight = authenticateUser({
			baseUrl,
			email: account.email,
			password: account.password,
			server: account.server,
			developerApiKey: this.developerApiKey,
		})
			.then((token) => {
				this.userTokens.set(key, { token, fetchedAt: Date.now() });
				return token;
			})
			.finally(() => {
				if (this.userAuthInflight.get(key) === inflight) {
					this.userAuthInflight.delete(key);
				}
			});
		this.userAuthInflight.set(key, inflight);
		return inflight;
	}

	private handleLost(session: AccountSession, generation: number, reason: string): void {
		if (
			this.sessions.get(session.account.tradingAccountId) !== session ||
			session.generation !== generation
		) {
			return; // a stale connection lost — the current attempt already moved on
		}
		this.logger.warn(`[${session.account.accountId}] connection lost: ${reason}`);
		session.connection?.close();
		session.connection = undefined;
		this.scheduleReconnect(session);
	}

	private scheduleReconnect(session: AccountSession): void {
		if (this.stopped || session.reconnectTimer) {
			return;
		}
		if (this.sessions.get(session.account.tradingAccountId) !== session) {
			return;
		}
		session.reconnectAttempt += 1;
		const delay = Math.min(
			RECONNECT_BASE_DELAY_MS * 2 ** (session.reconnectAttempt - 1),
			RECONNECT_MAX_DELAY_MS
		);
		this.logger.log(
			`[${session.account.accountId}] reconnecting in ${delay}ms (attempt ${session.reconnectAttempt})`
		);
		session.reconnectTimer = setTimeout(() => {
			session.reconnectTimer = undefined;
			void this.connect(session).catch((error) => {
				this.logger.error(`[${session.account.accountId}] reconnect crashed: ${describe(error)}`);
				Sentry.captureException(error);
			});
		}, delay);
	}

	/** Reconcile live sessions with the DB so connect/disconnect is picked up without a restart. */
	private async reconcile(): Promise<void> {
		if (this.stopped || this.reconciling) {
			return;
		}
		this.reconciling = true;
		try {
			await this.retryPendingCloses();

			const desired = new Map<string, ManagedAccount>();
			for (const account of await this.loadAccounts()) {
				desired.set(account.tradingAccountId, account);
			}
			if (this.stopped) {
				return;
			}

			// Removed or deactivated accounts: tear down.
			for (const [id, session] of this.sessions) {
				if (!desired.has(id)) {
					this.logger.log(`[${session.account.accountId}] no longer active — stopping`);
					this.stopSession(session);
				}
			}

			// New or changed accounts: (re)connect.
			for (const [id, account] of desired) {
				const session = this.sessions.get(id);
				if (!session) {
					this.startAccount(account);
				} else if (session.account.signature !== account.signature) {
					this.logger.log(`[${account.accountId}] account/credentials changed — reconnecting`);
					this.stopSession(session);
					this.startAccount(account);
				}
			}

			// Safety-net backfill: independently of reconnect/SyncEnd, re-scan the newest close-history
			// page for each live session so a close whose live `ClosePosition` never arrived (silent
			// socket, no drop → no reconnect) is still captured. Bounded to one page and funnelled
			// through restPacer; closedSeen makes it a no-op when nothing was missed. Fire-and-forget so
			// a slow login can't stall the reconcile loop; the clock is set up-front so the next tick
			// won't double-fire.
			const now = Date.now();
			for (const session of this.sessions.values()) {
				if (now - (session.lastBackfillAt ?? 0) < PERIODIC_BACKFILL_INTERVAL_MS) {
					continue;
				}
				session.lastBackfillAt = now;
				void this.backfillCloses(session, PERIODIC_BACKFILL_MAX_PAGES).catch((error) =>
					this.logger.error(
						`[${session.account.accountId}] periodic backfill failed: ${describe(error)}`
					)
				);
			}
		} catch (error) {
			this.logger.error(`reconcile failed: ${describe(error)}`);
		} finally {
			this.reconciling = false;
		}
	}

	private stopSession(session: AccountSession): void {
		session.generation += 1; // supersede any in-flight connect
		if (session.reconnectTimer) {
			clearTimeout(session.reconnectTimer);
			session.reconnectTimer = undefined;
		}
		session.connection?.close();
		session.connection = undefined;
		this.sessions.delete(session.account.tradingAccountId);
		for (const [key, pending] of [...this.pendingCloses]) {
			if (pending.session === session) {
				this.pendingCloses.delete(key);
			}
		}
	}

	private async loadAccounts(): Promise<ManagedAccount[]> {
		const onlyUsers = this.config
			.get('TRADELOCKER_ONLY_USER_IDS', { infer: true })
			?.split(',')
			.map((id) => id.trim())
			.filter(Boolean);
		if (onlyUsers?.length && !this.onlyUsersLogged) {
			this.onlyUsersLogged = true;
			this.logger.warn(`TRADELOCKER_ONLY_USER_IDS set — managing only ${onlyUsers.length} user(s)`);
		}

		const rows = await this.prisma.tradingAccount.findMany({
			where: {
				terminalType: 'tradelocker',
				...(onlyUsers?.length ? { userId: { in: onlyUsers } } : {}),
			},
			select: {
				id: true,
				userId: true,
				nominal: true,
				terminalAccountId: true,
				terminalAccNum: true,
				terminalServer: true,
				terminalEnvironment: true,
				terminalIntegrationStatus: true,
				terminalCredentials: { select: { email: true, encryptedPassword: true } },
			},
		});

		const accounts: ManagedAccount[] = [];
		for (const row of rows) {
			// 'deactivated' = TradeLocker auth/account no longer valid. Active is null OR any other value.
			// Filtered in code (not the query) because Prisma's `{ not: 'deactivated' }` drops NULL rows —
			// SQL three-valued logic: `NULL != 'deactivated'` is NULL, not true — which would exclude
			// every active (null-status) account.
			if (row.terminalIntegrationStatus === 'deactivated') {
				continue;
			}
			const creds = row.terminalCredentials;
			if (!creds || !row.terminalAccountId || !row.terminalServer) {
				continue; // not connectable without creds + identity
			}
			let password: string;
			try {
				password = decrypt(creds.encryptedPassword);
			} catch (error) {
				this.logger.error(`Skipping account ${row.id}: password decrypt failed (${describe(error)})`);
				continue;
			}
			const accNum = row.terminalAccNum?.trim();
			if (!accNum) {
				// Without accNum the REST close-history top-up is impossible, so a streamed close could
				// never finalize and the trade would stay open forever. Don't manage it — surface why.
				this.logger.warn(
					`Skipping account ${row.id} (${row.terminalAccountId}): no terminalAccNum — cannot finalize closes`
				);
				continue;
			}
			const environment: TradeLockerEnvironment = row.terminalEnvironment === 'live' ? 'live' : 'demo';
			accounts.push({
				tradingAccountId: row.id,
				userId: row.userId,
				nominal: row.nominal,
				accountId: row.terminalAccountId,
				accNum,
				server: row.terminalServer,
				environment,
				email: creds.email,
				password,
				signature: `${row.terminalAccountId}:${accNum}:${row.terminalServer}:${environment}:${creds.email}`,
			});
		}
		return accounts;
	}

	private matchToken(
		tokens: TradeLockerAccountToken[],
		account: ManagedAccount
	): TradeLockerAccountToken | undefined {
		const exact = new Set([account.accountId, account.accNum].filter(Boolean));
		const normalized = new Set([...exact].map(normalizeAccountId));
		return tokens.find(
			(t) => exact.has(t.accountId) || normalized.has(normalizeAccountId(t.accountId))
		);
	}

	private context(account: ManagedAccount): TradeLockerAccountContext {
		return {
			userId: account.userId,
			tradingAccountId: account.tradingAccountId,
			nominal: account.nominal,
		};
	}

	private groupKey(account: ManagedAccount): string {
		return `${account.environment}::${account.server}::${account.email}`;
	}
}

/** Map a live `Position` stream message to the writer's open-trade shape (quantity in UNITS). */
function mapOpenTrade(msg: PositionMessage): OpenTrade {
	// quantity is UNITS. The stream usually sends `units` directly; if only `lots` + `lotSize` are
	// present, derive units = lots * lotSize. NEVER treat a bare lot count as units (off by lotSize).
	// Either way the close finalization later SETs the authoritative units from close-trades-history.
	const units =
		msg.units !== undefined
			? Number(msg.units)
			: msg.lots !== undefined && msg.lotSize !== undefined
				? Number(msg.lots) * Number(msg.lotSize)
				: Number.NaN;
	const entryMs = msg.openDateTime ? Date.parse(msg.openDateTime) : Number.NaN;
	return {
		positionId: msg.positionId,
		symbol: msg.instrument,
		side: msg.side,
		quantity: Number.isFinite(units) ? units : 0,
		entryDate: Number.isFinite(entryMs) ? new Date(entryMs) : new Date(),
	};
}

/** Strip a leading env prefix (D#/L#) and leading zeros so id formats compare equal across endpoints. */
function normalizeAccountId(value: string | undefined): string {
	if (!value) {
		return '';
	}
	return value
		.replace(/^[A-Za-z]+#/, '')
		.replace(/^0+/, '')
		.trim()
		.toLowerCase();
}

/**
 * A min-interval pacer: every returned promise resolves at least `minIntervalMs` after the previous
 * one, serializing arbitrarily many concurrent callers into a steady drip. Used to keep all of a
 * login's close-history requests under TradeLocker's shared rate limit.
 */
function createPacer(minIntervalMs: number): () => Promise<void> {
	let last = 0;
	let chain: Promise<void> = Promise.resolve();
	return () => {
		chain = chain.then(async () => {
			const wait = last + minIntervalMs - Date.now();
			if (wait > 0) {
				await new Promise((resolve) => setTimeout(resolve, wait));
			}
			last = Date.now();
		});
		return chain;
	};
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
