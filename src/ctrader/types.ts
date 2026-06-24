export type CtraderEnvironment = 'demo' | 'live';

/** A single account the pool keeps connected. Tokens here are already decrypted (in memory). */
export interface PoolAccount {
	tokenId: string;
	tradingAccountId: string;
	ctidTraderAccountId: number;
	accessToken: string;
	refreshToken: string;
	expiresAt: Date | null;
	environment: CtraderEnvironment;
	/** Owner + account size, needed to write trades. */
	userId: string;
	nominal: number;
	/** symbolId → symbolName, resolved once per connection (best-effort). */
	symbolNames?: Map<number, string>;
}

// cTrader encodes int64 fields as strings in JSON, so numeric fields arrive as string | number.
type Int64 = string | number;

export interface CtraderTradeData {
	symbolId?: Int64;
	volume?: Int64;
	tradeSide?: string;
	openTimestamp?: Int64;
}

export interface CtraderClosePositionDetail {
	entryPrice?: number;
	grossProfit?: Int64;
	swap?: Int64;
	commission?: Int64;
	balance?: Int64;
	closedVolume?: Int64;
}

export interface CtraderEventDeal {
	dealId?: Int64;
	positionId?: Int64;
	symbolId?: Int64;
	volume?: Int64;
	tradeSide?: string;
	executionTimestamp?: Int64;
	closePositionDetail?: CtraderClosePositionDetail | null;
}

export interface CtraderEventPosition {
	positionId?: Int64;
	positionStatus?: string;
	tradeData?: CtraderTradeData;
}

/** Shape of a ProtoOAExecutionEvent payload (the fields we read). */
export interface ExecutionEventPayload {
	executionType?: string | number;
	ctidTraderAccountId?: Int64;
	position?: CtraderEventPosition;
	deal?: CtraderEventDeal | null;
}
