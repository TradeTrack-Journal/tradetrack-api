import { Injectable, Logger } from '@nestjs/common';

import { PrismaService } from '../prisma';
import {
	computeProfitPercentageFromMoney,
	computeTradeRr,
	DEFAULT_IMPORTED_RISK_PERCENTAGE,
	getTradeResult,
	roundMoney,
} from '../trades';
import { POSITION_KEY_PREFIX, TERMINAL_NAME } from './constants';
import type { AggregatedPosition, MetaApiAccountContext } from './types';

/**
 * Persists MetaApi MT5 positions into the Trade table — one row per position, keyed by
 * terminalTradeId = `metaapi_pos_{positionId}` (mirrors cTrader's prefix convention). The distinct
 * terminalName keeps @@unique([userId, terminalName, terminalTradeId]) from colliding with the EA
 * importer's `mt5` rows.
 *
 * `profit` stores the GROSS figure and `pnl` the net, matching cTrader and the main app's EA importer,
 * so a row written here is numerically identical to one written by the cron.
 *
 * Writes for one position are serialized so a fast open->close cannot race two upserts onto the same
 * unique key, and the open path never clears an existing close.
 */
@Injectable()
export class TradeWriterService {
	private readonly logger = new Logger(TradeWriterService.name);
	private readonly chains = new Map<string, Promise<unknown>>();

	constructor(private readonly prisma: PrismaService) {}

	async recordPosition(ctx: MetaApiAccountContext, position: AggregatedPosition): Promise<void> {
		await this.serialize(ctx.userId, position.positionId, () => this.writePosition(ctx, position));
	}

	/** Refresh the floating P&L of an OPEN position. Update-only: never creates, never touches a close. */
	async recordUnrealized(
		ctx: MetaApiAccountContext,
		positionId: string,
		unrealizedPl: number
	): Promise<void> {
		await this.serialize(ctx.userId, positionId, () =>
			this.writeUnrealized(ctx, positionId, unrealizedPl)
		);
	}

	/** Run task on the per-position serialization chain so concurrent writes cannot clobber each other. */
	private serialize(userId: string, positionId: string, task: () => Promise<void>): Promise<void> {
		const key = `${userId}:${positionId}`;
		const previous = this.chains.get(key) ?? Promise.resolve();
		const current = previous.then(task);
		const tail: Promise<unknown> = current
			.catch(() => undefined)
			.finally(() => {
				if (this.chains.get(key) === tail) {
					this.chains.delete(key);
				}
			});
		this.chains.set(key, tail);
		return current;
	}

	private async writePosition(
		ctx: MetaApiAccountContext,
		position: AggregatedPosition
	): Promise<void> {
		const terminalTradeId = `${POSITION_KEY_PREFIX}${position.positionId}`;

		// Risk % is a user-owned input (terminals never report it), so a hand-edited value MUST survive
		// re-finalization — every backfill re-runs this. Preserve it, default only a brand-new row. The
		// read runs inside the serialize chain, so no other write for this position interleaves.
		const existing = await this.prisma.trade.findUnique({
			where: this.whereKey(ctx.userId, terminalTradeId),
			select: { riskPercentage: true },
		});
		const riskPercentage = existing?.riskPercentage ?? DEFAULT_IMPORTED_RISK_PERCENTAGE;

		if (position.exitDate === null) {
			// OPEN: create the row or refresh its entry data. Never clobber a close that already landed.
			await this.prisma.trade.upsert({
				where: this.whereKey(ctx.userId, terminalTradeId),
				create: {
					symbol: position.symbol,
					type: position.side,
					quantity: position.quantity,
					entryDate: position.entryDate,
					exitDate: null,
					tags: [],
					fromTerminal: true,
					terminalName: TERMINAL_NAME,
					terminalTradeId,
					riskPercentage,
					user: { connect: { id: ctx.userId } },
					tradingAccount: { connect: { id: ctx.tradingAccountId } },
				},
				update: {
					symbol: position.symbol,
					type: position.side,
					quantity: position.quantity,
					entryDate: position.entryDate,
				},
			});
			this.logger.log(`open ${position.positionId} ${position.side} ${position.symbol}`);
			return;
		}

		const grossProfit = roundMoney(position.grossProfit);
		const commission = roundMoney(position.commission);
		const swap = roundMoney(position.swap);
		const pnl = roundMoney(grossProfit + commission + swap);

		// %/RR are derived from the GROSS figure and the result from the NET one — exactly as
		// ctrader-trade-writer.ts:192-202 and the main app's buildCreatePayload do. Deriving them from
		// pnl instead would silently shrink every R-multiple on any trade carrying commission or swap,
		// so the same fill would read differently depending on which terminal imported it.
		const profitPercentage = computeProfitPercentageFromMoney(grossProfit, ctx.nominal) ?? undefined;
		const rr =
			computeTradeRr({
				profitMoney: grossProfit,
				profitPercentage: profitPercentage ?? null,
				nominal: ctx.nominal,
				riskPercentage,
			}) ?? undefined;
		const result = getTradeResult(pnl, ctx.nominal);

		const data = {
			symbol: position.symbol,
			type: position.side,
			quantity: position.quantity,
			entryDate: position.entryDate,
			exitDate: position.exitDate,
			profit: grossProfit,
			grossProfit,
			commission,
			swap,
			pnl,
			profitPercentage,
			rr,
			result,
			riskPercentage,
			fromTerminal: true,
			terminalName: TERMINAL_NAME,
			terminalTradeId,
		};

		// SETs absolute realized totals, so re-running the backfill on every reconnect converges.
		await this.prisma.trade.upsert({
			where: this.whereKey(ctx.userId, terminalTradeId),
			create: {
				...data,
				tags: [],
				user: { connect: { id: ctx.userId } },
				tradingAccount: { connect: { id: ctx.tradingAccountId } },
			},
			update: data,
		});
		this.logger.log(`closed ${position.positionId} pnl=${pnl} result=${result ?? '-'}`);
	}

	private async writeUnrealized(
		ctx: MetaApiAccountContext,
		positionId: string,
		unrealizedPl: number
	): Promise<void> {
		const terminalTradeId = `${POSITION_KEY_PREFIX}${positionId}`;
		const existing = await this.prisma.trade.findUnique({
			where: this.whereKey(ctx.userId, terminalTradeId),
			select: { exitDate: true, riskPercentage: true },
		});
		// No open row yet (the position handler owns creation), or the realized close already won.
		if (!existing || existing.exitDate !== null) return;

		const riskPercentage = existing.riskPercentage ?? DEFAULT_IMPORTED_RISK_PERCENTAGE;
		const profitPercentage = computeProfitPercentageFromMoney(unrealizedPl, ctx.nominal) ?? null;
		const rr =
			computeTradeRr({
				profitMoney: unrealizedPl,
				profitPercentage,
				nominal: ctx.nominal,
				riskPercentage,
			}) ?? null;

		// exitDate:null in the where atomically skips a close that landed between the read and here, and
		// updateMany no-ops (rather than throwing) if the row just vanished.
		await this.prisma.trade.updateMany({
			where: {
				userId: ctx.userId,
				terminalName: TERMINAL_NAME,
				terminalTradeId,
				exitDate: null,
			},
			data: { pnl: unrealizedPl, profitPercentage, rr, riskPercentage },
		});
	}

	private whereKey(userId: string, terminalTradeId: string) {
		return {
			userId_terminalName_terminalTradeId: {
				userId,
				terminalName: TERMINAL_NAME,
				terminalTradeId,
			},
		};
	}
}
