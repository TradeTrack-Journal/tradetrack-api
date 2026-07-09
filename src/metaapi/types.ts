import type { MetatraderDeal, MetatraderPosition } from 'metaapi.cloud-sdk';

/**
 * A MetaApi-managed MT5 account loaded from the DB, with its investor password already decrypted.
 * `syncState` is the RAW terminalSyncState object: it is a shared Json column (the EA writes
 * `eaLastSeenAt`/`backfillRequested`, TradeLocker a `closedTrades` cursor), so every write back to it
 * must spread this value rather than replace it.
 */
export interface ManagedAccount {
	tradingAccountId: string;
	userId: string;
	nominal: number;
	/** MetaApi accountId; null until the worker has created the cloud account. */
	metaApiAccountId: string | null;
	login: string;
	password: string;
	server: string;
	/** null = active; otherwise 'connecting' | 'disconnecting'. */
	status: string | null;
	/** Floor for the backfill window — the EA-migration clamp. Null when the account never used the EA. */
	backfillFrom: Date | null;
	syncState: Record<string, unknown>;
}

/** The subset the trade writer needs in order to persist a row. */
export interface MetaApiAccountContext {
	userId: string;
	tradingAccountId: string;
	nominal: number;
}

/** One MT5 position reconstructed from its deals. */
export interface AggregatedPosition {
	positionId: string;
	symbol: string;
	side: 'BUY' | 'SELL';
	quantity: number;
	entryDate: Date;
	/** null while the position is still open. */
	exitDate: Date | null;
	grossProfit: number;
	commission: number;
	swap: number;
}

/**
 * The slice of MetaApi's RPC connection the backfill needs. Declared structurally so the service does
 * not depend on the SDK's concrete connection class.
 */
export interface RpcLike {
	getDealsByTimeRange(startTime: Date, endTime: Date): Promise<{ deals: MetatraderDeal[] }>;
	getPositions(): Promise<MetatraderPosition[]>;
}

/**
 * MetatraderPosition.id is a NUMBER while MetatraderDeal.positionId is a STRING. Comparing them
 * directly always yields false, which would silently misclassify every position as closed. Every
 * comparison must go through this.
 */
export function positionKey(value: number | string): string {
	return String(value);
}
