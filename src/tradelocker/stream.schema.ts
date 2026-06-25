import { z } from 'zod';

/**
 * Inbound `stream` message schemas, mirrored from the official stream-api-example
 * (schemas/stream.schema.js). Two things to know:
 *  - every numeric field arrives as a DECIMAL STRING, never a JS number — parse with Number() only
 *    at the persistence boundary (Phase 3);
 *  - id fields (accountId / positionId / orderId) are strings.
 * Unknown extra keys are stripped by zod (not rejected), so forward-compatible fields are tolerated.
 */
const decimal = z.string().regex(/^[+-]?\d+(\.\d+)?$/);
const side = z.enum(['BUY', 'SELL']);

export const PositionPnLSchema = z.object({
	positionId: z.string(),
	pnl: decimal,
});

export const AccountStatusMessageSchema = z.object({
	type: z.literal('AccountStatus'),
	accountId: z.string(),
	currency: z.string().optional(),
	balance: decimal.optional(),
	marginAvailable: decimal.optional(),
	marginUsed: decimal.optional(),
	blockedBalance: decimal.optional(),
	credit: decimal.optional(),
	equity: decimal.optional(),
	positionPnLs: z.array(PositionPnLSchema).optional(),
});

export const OpenOrderMessageSchema = z.object({
	type: z.literal('OpenOrder'),
	accountId: z.string(),
	orderId: z.string(),
	positionId: z.string().nullable().optional(),
	instrument: z.string(),
	side,
	orderType: z.string().optional(),
	amount: decimal.optional(),
	lotSize: decimal.optional(),
	averageFilledPrice: decimal.optional(),
	filledAmount: z.string().nullable().optional(),
	price: z.string().nullable().optional(),
	status: z.string().nullable().optional(),
	tif: z.string().optional(),
	createdDateTime: z.string().optional(),
	lastUpdate: z.string().optional(),
	isOpen: z.boolean().optional(),
});

export const PositionMessageSchema = z.object({
	type: z.literal('Position'),
	accountId: z.string(),
	positionId: z.string(),
	instrument: z.string(),
	side,
	lots: decimal.optional(),
	lotSize: decimal.optional(),
	units: decimal.optional(),
	openPrice: decimal.optional(),
	openDateTime: z.string().optional(),
	maintMargin: decimal.optional(),
	openOrderId: z.string().nullable().optional(),
	stopLossOrderId: z.string().nullable().optional(),
	takeProfitOrderId: z.string().nullable().optional(),
});

export const ClosePositionMessageSchema = z.object({
	type: z.literal('ClosePosition'),
	positionId: z.string(),
	closePrice: z.string().nullable().optional(),
	closeDateTime: z.string().optional(),
});

export const PropertyMessageSchema = z.object({
	type: z.literal('Property'),
	name: z.enum(['SyncEnd', 'Disconnected']),
});

export const StreamMessageSchema = z.discriminatedUnion('type', [
	AccountStatusMessageSchema,
	OpenOrderMessageSchema,
	PositionMessageSchema,
	ClosePositionMessageSchema,
	PropertyMessageSchema,
]);

/** Reads `type` only — lets us log the kind of an otherwise-unparseable frame. */
export const TypedMessageSchema = z.object({ type: z.string() });

/** SUBSCRIBE/UNSUBSCRIBE ack and pushed status frames. Success is { status:'ok', code:'connected' }. */
export const StatusMessageSchema = z.object({
	status: z.enum(['ok', 'error']),
	code: z.string().optional(),
	message: z.string().optional(),
	remainingRequests: z.number().optional(),
});

export type AccountStatusMessage = z.infer<typeof AccountStatusMessageSchema>;
export type OpenOrderMessage = z.infer<typeof OpenOrderMessageSchema>;
export type PositionMessage = z.infer<typeof PositionMessageSchema>;
export type ClosePositionMessage = z.infer<typeof ClosePositionMessageSchema>;
export type PropertyMessage = z.infer<typeof PropertyMessageSchema>;
export type StreamMessage = z.infer<typeof StreamMessageSchema>;
export type StatusMessage = z.infer<typeof StatusMessageSchema>;
