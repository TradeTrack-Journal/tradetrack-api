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
}

/** Minimal shape of a ProtoOAExecutionEvent payload (only the fields we currently read). */
export interface ExecutionEventPayload {
	executionType?: string | number;
	ctidTraderAccountId?: string | number;
	deal?: { dealId?: number; tradeSide?: string | number; volume?: number; symbolId?: number };
	order?: { orderId?: number };
	position?: { positionId?: number };
}
