import { Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { Prisma } from '@prisma/client';
import MetaApi from 'metaapi.cloud-sdk';
import type { MetatraderAccount } from 'metaapi.cloud-sdk';

import { decrypt } from '../crypto';
import { PrismaService } from '../prisma';
import { BackfillService } from './backfill.service';
import { classifyMetaApiError } from './client';
import {
	accountName,
	LIVE_STATES,
	RECONCILE_INTERVAL_MS,
	RECONNECT_BASE_DELAY_MS,
	RECONNECT_MAX_DELAY_MS,
	STATE_UNDEPLOYED,
	STATUS,
	SYNC_METHOD,
} from './constants';
import { MetaApiSyncListener } from './sync-listener';
import { TradeWriterService } from './trade-writer.service';
import type { ManagedAccount } from './types';

interface Session {
	generation: number;
	/** Remembered so a vanished row can still have its billing terminal torn down. */
	metaApiAccountId: string | null;
	close: () => Promise<void>;
}

/**
 * Sole owner of the MetaApi lifecycle: create (which IS the deploy — it starts a cloud terminal and
 * bills a 6-hour minimum), stream, undeploy, delete. The web app never calls MetaApi; it writes intent
 * into TradingAccount columns and this reconcile loop acts on it within RECONCILE_INTERVAL_MS.
 *
 * Status contract (the design spec's lifecycle invariant):
 *   'connecting'    -> idempotent create + stream + backfill -> status = null
 *   null            -> healthy, streaming
 *   'disconnecting' -> undeploy -> delete -> clear terminalAccountId -> clear marker -> 'deactivated'
 *   'deactivated' / 'paused_inactive' -> excluded from the selection, and only ever set by this
 *                                        service AFTER a confirmed undeploy.
 *
 * The invariant: no status that hides a row from `loadAccounts` is written before the cloud terminal
 * has been stopped, and terminalAccountId is cleared only after a confirmed delete. Break it and you
 * strand a deployed, billing terminal that nothing can ever reach again.
 */
@Injectable()
export class ConnectionManagerService implements OnApplicationBootstrap, OnModuleDestroy {
	private readonly logger = new Logger(ConnectionManagerService.name);
	private readonly sessions = new Map<string, Session>();
	private readonly backoff = new Map<string, number>();
	private readonly retryTimers = new Set<NodeJS.Timeout>();
	/** Guards against two concurrent open() calls for one account (reconcile vs. a retry timer). */
	private readonly opening = new Set<string>();
	private generation = 0;
	private reconcileTimer: NodeJS.Timeout | null = null;
	private reconciling = false;
	private stopped = false;
	private api: MetaApi | null = null;

	constructor(
		private readonly prisma: PrismaService,
		private readonly config: ConfigService,
		private readonly writer: TradeWriterService,
		private readonly backfill: BackfillService
	) {}

	onApplicationBootstrap(): void {
		const enabled = this.config.get<string>('METAAPI_LIVE_SYNC_ENABLED') === 'true';
		if (!enabled) {
			this.logger.log('METAAPI_LIVE_SYNC_ENABLED is not "true" — idle');
			return;
		}
		const token = this.config.get<string>('METAAPI_TOKEN');
		if (!token) {
			this.logger.warn('METAAPI_TOKEN missing — idle');
			return;
		}
		this.api = new MetaApi(token);
		// Fire-and-forget: a slow MetaApi must never block Nest boot.
		void this.start();
	}

	async onModuleDestroy(): Promise<void> {
		this.stopped = true;
		if (this.reconcileTimer) clearInterval(this.reconcileTimer);
		for (const timer of this.retryTimers) clearTimeout(timer);
		this.retryTimers.clear();
		await Promise.all([...this.sessions.values()].map((session) => session.close()));
		this.sessions.clear();
	}

	private async start(): Promise<void> {
		await this.reconcile();
		this.reconcileTimer = setInterval(() => {
			void this.reconcile();
		}, RECONCILE_INTERVAL_MS);
	}

	private async reconcile(): Promise<void> {
		if (this.stopped || this.reconciling) return;
		this.reconciling = true;
		try {
			const accounts = await this.loadAccounts();
			const seen = new Set(accounts.map((account) => account.tradingAccountId));

			// A row can leave the selection without ever passing through 'disconnecting': it may be
			// archived, or another writer may clobber the shared terminalSyncState Json and wipe our
			// syncMethod marker (the EA's ingest handler overwrites that column wholesale). Closing the
			// socket is NOT enough — the cloud terminal keeps running and billing, and once the row is
			// out of the selection nothing will ever reach it again. Tear the terminal down here, using
			// the accountId the session remembered.
			for (const [id, session] of this.sessions) {
				if (seen.has(id)) continue;
				this.sessions.delete(id);
				await session.close();
				if (session.metaApiAccountId) {
					this.logger.warn(`${id} left the selection while deployed — tearing its terminal down`);
					await this.destroyCloudAccount(id, session.metaApiAccountId);
				}
			}

			// Launch per account without awaiting: one slow broker (createAccount can poll for minutes
			// while MetaApi auto-detects its settings) must not head-of-line-block the other accounts, nor
			// the teardown of a 'disconnecting' row. The `opening` guard keeps a single flight per account.
			for (const account of accounts) {
				if (this.stopped) return;
				if (account.status === STATUS.disconnecting) {
					void this.teardown(account);
					continue;
				}
				if (this.sessions.has(account.tradingAccountId)) continue;
				void this.open(account);
			}
		} catch (error) {
			this.logger.error('reconcile failed', error as Error);
		} finally {
			this.reconciling = false;
		}
	}

	/**
	 * Status is filtered IN CODE, not in the Prisma where-clause. terminalIntegrationStatus is nullable
	 * and an active account has NULL there; Prisma's `not`/`notIn` drops NULL rows (SQL three-valued
	 * logic: `NULL != x` is NULL, not true), so a query-side filter would silently exclude every active
	 * account. TradeLocker's manager does the same, with the same warning.
	 */
	private async loadAccounts(): Promise<ManagedAccount[]> {
		const onlyUsers = (process.env.METAAPI_ONLY_USER_IDS ?? '')
			.split(',')
			.map((id) => id.trim())
			.filter(Boolean);

		const rows = await this.prisma.tradingAccount.findMany({
			where: {
				terminalType: 'mt5',
				terminalSyncState: { path: ['syncMethod'], equals: SYNC_METHOD },
				// Archiving hides an account from normal management, but a MetaApi account being archived
				// still owns a deployed, billing cloud terminal. The app marks it 'disconnecting' when it
				// archives, and we must keep seeing that row until teardown finishes.
				OR: [{ isArchived: false }, { terminalIntegrationStatus: STATUS.disconnecting }],
				...(onlyUsers.length ? { userId: { in: onlyUsers } } : {}),
			},
			select: {
				id: true,
				userId: true,
				nominal: true,
				terminalAccountId: true,
				terminalServer: true,
				terminalIntegrationStatus: true,
				terminalSyncState: true,
				terminalCredentials: { select: { email: true, encryptedPassword: true } },
			},
		});

		const managed: ManagedAccount[] = [];
		for (const row of rows) {
			const status = row.terminalIntegrationStatus;
			// 'deactivated' / 'paused_inactive' are terminal here — not ours to touch.
			if (status !== null && status !== STATUS.connecting && status !== STATUS.disconnecting) {
				continue;
			}
			if (!row.terminalCredentials || !row.terminalServer) {
				this.logger.warn(`${row.id} marked metaapi but has no credentials/server — skipping`);
				continue;
			}

			const syncState = (row.terminalSyncState ?? {}) as Record<string, unknown>;
			const backfillFromRaw = syncState.backfillFrom;
			const backfillFrom =
				typeof backfillFromRaw === 'string' && !Number.isNaN(Date.parse(backfillFromRaw))
					? new Date(backfillFromRaw)
					: null;

			managed.push({
				tradingAccountId: row.id,
				userId: row.userId,
				nominal: row.nominal,
				metaApiAccountId: row.terminalAccountId,
				login: row.terminalCredentials.email,
				password: decrypt(row.terminalCredentials.encryptedPassword),
				server: row.terminalServer,
				status,
				backfillFrom,
				syncState,
			});
		}
		return managed;
	}

	private async open(account: ManagedAccount): Promise<void> {
		const api = this.api;
		if (!api || this.opening.has(account.tradingAccountId)) return;
		this.opening.add(account.tradingAccountId);

		const generation = ++this.generation;
		// Placeholder session claims the slot for this generation; replaced once the stream is live.
		this.sessions.set(account.tradingAccountId, {
			generation,
			metaApiAccountId: account.metaApiAccountId,
			close: () => Promise.resolve(),
		});

		/**
		 * What we know about the cloud account AS WE LEARN IT. Load-bearing for billing: the moment
		 * `ensureAccount` returns, a cloud terminal exists and is running. Handing `handleFailure` the
		 * original `account` (whose metaApiAccountId is still null) would make it conclude nothing was
		 * ever created, skip the undeploy, and then write a status that hides the row. The terminal would
		 * bill forever with no handle left to stop it.
		 */
		let resolved: ManagedAccount = account;

		try {
			const metaAccount = await this.ensureAccount(api, account);
			resolved = { ...account, metaApiAccountId: metaAccount.id };
			this.rememberAccountId(account.tradingAccountId, generation, metaAccount.id);
			if (this.abandon(account.tradingAccountId, generation)) return;

			// createAccount resolves in DEPLOYING, not DEPLOYED. Only a genuinely UNDEPLOYED account (the
			// pause/resume case) needs a deploy(); re-issuing it on one that is already coming up is
			// pointless churn.
			if (metaAccount.state === STATE_UNDEPLOYED) {
				await metaAccount.deploy();
			}
			await metaAccount.waitConnected();
			if (this.abandon(account.tradingAccountId, generation)) return;

			const rpc = metaAccount.getRPCConnection();
			await rpc.connect();
			await rpc.waitSynchronized();
			await this.backfill.run(rpc, resolved);
			if (this.abandon(account.tradingAccountId, generation)) {
				await rpc.close();
				return;
			}

			const ctx = {
				userId: account.userId,
				tradingAccountId: account.tradingAccountId,
				nominal: account.nominal,
			};
			const stream = metaAccount.getStreamingConnection();
			stream.addSynchronizationListener(
				new MetaApiSyncListener({
					writer: this.writer,
					ctx,
					logger: this.logger,
					clampFrom: account.backfillFrom,
					fetchPositionDeals: async (positionId) =>
						(await rpc.getDealsByPosition(positionId)).deals ?? [],
				})
			);
			await stream.connect();
			await stream.waitSynchronized();

			// Last gate. Everything below mutates shared state, so a session that was superseded (or a
			// shutdown that already ran) must not resurrect itself.
			if (this.stopped || this.isStale(account.tradingAccountId, generation)) {
				await stream.close();
				await rpc.close();
				return;
			}

			this.sessions.set(account.tradingAccountId, {
				generation,
				metaApiAccountId: metaAccount.id,
				close: async () => {
					// The RPC connection is registry-cached and captured by the listener closure. Closing the
					// stream alone leaks one RpcMetaApiConnectionInstance per reconnect.
					await stream.close();
					await rpc.close();
				},
			});
			this.backoff.delete(account.tradingAccountId);

			// Success. Clear 'connecting' so the UI can stop spinning and show "connected".
			await this.prisma.tradingAccount.update({
				where: { id: account.tradingAccountId },
				data: {
					terminalIntegrationStatus: null,
					terminalAccountId: metaAccount.id,
					lastSyncAt: new Date(),
					terminalSyncState: this.mergeSyncState(account, {
						deployedAt: new Date().toISOString(),
						lastError: null,
					}),
				},
			});
			this.logger.log(`streaming ${account.tradingAccountId} (metaapi ${metaAccount.id})`);
		} catch (error) {
			this.sessions.delete(account.tradingAccountId);
			await this.handleFailure(resolved, error);
		} finally {
			this.opening.delete(account.tradingAccountId);
		}
	}

	/**
	 * Give up this open() attempt because a newer generation owns the account, or we are shutting down.
	 * The cloud terminal stays deployed on purpose: its id is already persisted, so the next reconcile
	 * adopts it. If the row instead vanished from the selection, the session-vanished branch in
	 * reconcile() tears it down using the id we remembered.
	 */
	private abandon(tradingAccountId: string, generation: number): boolean {
		return this.stopped || this.isStale(tradingAccountId, generation);
	}

	private rememberAccountId(tradingAccountId: string, generation: number, id: string): void {
		const session = this.sessions.get(tradingAccountId);
		if (session && session.generation === generation) {
			session.metaApiAccountId = id;
		}
	}

	/**
	 * Idempotent create. `createAccount` IS the deploy — never follow it with undeploy+deploy, that
	 * bills two 6-hour minimums.
	 *
	 * The dedup lookup matches on our deterministic NAME, not on login+server. Two TradingAccount rows
	 * can legitimately carry the same MT5 login (a re-added broker account, a shared investor login),
	 * and the token may be shared with another app — matching on login+server would cross-wire two rows
	 * onto one cloud account or adopt somebody else's.
	 *
	 * The lookup also recovers the orphan case: createAccount succeeded cloud-side but its response was
	 * lost, so terminalAccountId was never persisted. Without it the retry would create a SECOND billable
	 * terminal.
	 */
	private async ensureAccount(api: MetaApi, account: ManagedAccount): Promise<MetatraderAccount> {
		if (account.metaApiAccountId) {
			return api.metatraderAccountApi.getAccount(account.metaApiAccountId);
		}

		const adopted = await this.findByName(api, account.tradingAccountId);
		if (adopted) {
			this.logger.warn(
				`adopting existing MetaApi account ${adopted.id} for ${account.tradingAccountId}`
			);
			await this.persistAccountId(account.tradingAccountId, adopted.id);
			return adopted;
		}

		// The SDK's httpClient already polls MetaApi's 202 "broker settings detection in progress"
		// responses internally, so there is no retry loop to write here. It surfaces a TimeoutError (no
		// status) if detection never finishes; that classifies as transient and the next reconcile
		// re-adopts by name rather than creating a duplicate.
		const created = await api.metatraderAccountApi.createAccount({
			name: accountName(account.tradingAccountId),
			type: 'cloud-g2',
			login: account.login,
			password: account.password,
			server: account.server,
			platform: 'mt5',
			magic: 0,
			...(this.config.get<string>('METAAPI_REGION')
				? { region: this.config.get<string>('METAAPI_REGION') }
				: {}),
		});
		// Persist immediately: a crash between create and this write orphans a billing account, and only
		// the name lookup can then find it again.
		await this.persistAccountId(account.tradingAccountId, created.id);
		return created;
	}

	/** Find the cloud account this trading account owns, by its deterministic name. */
	private async findByName(
		api: MetaApi,
		tradingAccountId: string
	): Promise<MetatraderAccount | null> {
		const wanted = accountName(tradingAccountId);
		try {
			const accounts = await api.metatraderAccountApi.getAccountsWithInfiniteScrollPagination({
				query: wanted,
			});
			return accounts.find((candidate) => candidate.name === wanted) ?? null;
		} catch (error) {
			this.logger.error(`name lookup failed for ${tradingAccountId}`, error as Error);
			throw error;
		}
	}

	private async persistAccountId(tradingAccountId: string, metaApiAccountId: string): Promise<void> {
		await this.prisma.tradingAccount.update({
			where: { id: tradingAccountId },
			data: { terminalAccountId: metaApiAccountId },
		});
	}

	/**
	 * 'disconnecting' is deliberately INCLUDED in loadAccounts so this is reachable at all — and so a
	 * worker restart mid-teardown resumes it. Every step is idempotent: on failure the row keeps its
	 * status and the next reconcile retries.
	 */
	private async teardown(account: ManagedAccount): Promise<void> {
		const api = this.api;
		if (!api) return;

		const session = this.sessions.get(account.tradingAccountId);
		if (session) {
			this.sessions.delete(account.tradingAccountId);
			await session.close();
		}

		// The row may have no stored id because a create crashed before persisting it. Resolve by name,
		// or an orphaned terminal bills forever.
		let cloudId = account.metaApiAccountId;
		if (!cloudId) {
			try {
				cloudId = (await this.findByName(api, account.tradingAccountId))?.id ?? null;
			} catch {
				return; // lookup failed — stay 'disconnecting' and retry next reconcile
			}
		}

		if (cloudId && !(await this.destroyCloudAccount(account.tradingAccountId, cloudId))) return;

		await this.prisma.tradingAccount.update({
			where: { id: account.tradingAccountId },
			data: {
				terminalAccountId: null,
				terminalIntegrationStatus: STATUS.deactivated,
				autoSyncEnabled: false,
				terminalSyncState: this.mergeSyncState(account, { syncMethod: null, deployedAt: null }),
			},
		});
		this.logger.log(`torn down ${account.tradingAccountId}`);
	}

	/**
	 * Stop the billing meter and delete the cloud account. Returns false only when the resource may STILL
	 * be alive and billing — the caller must then leave the row visible and retry.
	 */
	private async destroyCloudAccount(tradingAccountId: string, cloudId: string): Promise<boolean> {
		const api = this.api;
		if (!api) return false;

		try {
			const metaAccount = await api.metatraderAccountApi.getAccount(cloudId);
			// Undeploy for DEPLOYING too: that terminal is coming up and already on the meter.
			if (LIVE_STATES.includes(metaAccount.state)) {
				await metaAccount.undeploy();
			}
			await metaAccount.remove();
			return true;
		} catch (error) {
			const kind = classifyMetaApiError(error);
			if (kind === 'not-found') {
				// Already gone (a previous teardown removed it and crashed before the DB write). Finalizing
				// is correct; returning false here would strand the row in 'disconnecting' forever.
				this.logger.log(`cloud account ${cloudId} already absent — treating teardown as done`);
				return true;
			}
			if (kind === 'global-auth') {
				this.logger.error(
					`METAAPI_TOKEN rejected while tearing down ${tradingAccountId} — the cloud terminal is STILL DEPLOYED and billing. Rotate the token; the next reconcile retries.`
				);
			} else {
				this.logger.error(`teardown failed for ${tradingAccountId}`, error as Error);
			}
			return false;
		}
	}

	private async handleFailure(account: ManagedAccount, error: unknown): Promise<void> {
		const kind = classifyMetaApiError(error);

		if (kind === 'global-auth') {
			// OUR token, not the account. Deactivating here would delete every healthy live account.
			this.logger.error(
				'MetaApi returned 401 — METAAPI_TOKEN is expired or revoked. Rotate the secret. No accounts were changed.',
				error as Error
			);
			return;
		}

		if (kind === 'not-found') {
			// The stored id points at a cloud account that no longer exists. Clear it and let the next
			// reconcile re-adopt by name or create afresh. Not the account's fault; do not deactivate.
			this.logger.warn(
				`stored MetaApi account for ${account.tradingAccountId} is gone — clearing the id and retrying`
			);
			await this.prisma.tradingAccount.update({
				where: { id: account.tradingAccountId },
				data: { terminalAccountId: null },
			});
			return;
		}

		if (kind === 'fatal-account') {
			this.logger.error(`fatal for ${account.tradingAccountId}, deactivating`, error as Error);
			// A cloud account may already exist and be deployed (createAccount deploys). Stop the meter
			// BEFORE writing a status that hides the row, or we can never reach it again.
			if (account.metaApiAccountId) {
				const destroyed = await this.destroyCloudAccount(
					account.tradingAccountId,
					account.metaApiAccountId
				);
				if (!destroyed) return; // still billing — stay visible and retry next reconcile
			}

			await this.prisma.tradingAccount.update({
				where: { id: account.tradingAccountId },
				data: {
					terminalAccountId: null,
					terminalIntegrationStatus: STATUS.deactivated,
					autoSyncEnabled: false,
					terminalSyncState: this.mergeSyncState(account, {
						lastError: this.errorMessage(error),
						deployedAt: null,
					}),
				},
			});
			return;
		}

		const attempts = (this.backoff.get(account.tradingAccountId) ?? 0) + 1;
		this.backoff.set(account.tradingAccountId, attempts);
		const delay = Math.min(RECONNECT_BASE_DELAY_MS * 2 ** (attempts - 1), RECONNECT_MAX_DELAY_MS);
		this.logger.warn(
			`transient failure for ${account.tradingAccountId} (attempt ${attempts}), retrying in ${delay}ms`
		);
		// The reconcile loop would pick this up anyway; the timer just retries sooner. `opening` keeps it
		// from racing a concurrent reconcile-driven open of the same account.
		const timer = setTimeout(() => {
			this.retryTimers.delete(timer);
			if (!this.stopped && !this.sessions.has(account.tradingAccountId)) void this.open(account);
		}, delay);
		this.retryTimers.add(timer);
	}

	/**
	 * terminalSyncState is a SHARED Json column: the EA writes `eaLastSeenAt`/`backfillRequested` and
	 * TradeLocker a `closedTrades` cursor. Always spread the row's existing value — replacing it would
	 * silently destroy another integration's state.
	 */
	private mergeSyncState(
		account: ManagedAccount,
		patch: Record<string, unknown>
	): Prisma.InputJsonObject {
		return { ...account.syncState, ...patch } as Prisma.InputJsonObject;
	}

	private errorMessage(error: unknown): string {
		if (error instanceof Error && error.message) return error.message;
		return 'MetaApi rejected the account';
	}

	private isStale(tradingAccountId: string, generation: number): boolean {
		return this.sessions.get(tradingAccountId)?.generation !== generation;
	}
}
