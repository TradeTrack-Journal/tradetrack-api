import { Injectable, Logger } from '@nestjs/common';

import { PrismaService } from '../prisma';
import {
	computeProfitPercentageFromMoney,
	computeTradeRr,
	DEFAULT_IMPORTED_RISK_PERCENTAGE,
	getTradeResult,
	roundMoney,
} from '../trades';
import { TERMINAL_NAME } from './constants';
import type { ClosedTrade, OpenTrade, TradeLockerAccountContext } from './types';

/**
 * Persists TradeLocker positions into the Trade table, one row per position keyed by
 * terminalTradeId = the RAW positionId (no prefix — matches the main app's REST importer, unlike
 * cTrader's `ctrader_pos_` keys). The OPEN side comes from the live `Position` stream and creates an
 * open row (exitDate null); the CLOSE side comes from REST `close-trades-history` (the live
 * `ClosePosition` carries no realized money) and finalizes that same row.
 *
 * Close finalization SETs absolute realized totals — `close-trades-history` reports each position's
 * full realized P&L, so re-running the top-up on every reconnect / `ClosePosition` is idempotent and
 * converges, exactly like cTrader's backfill. Writes for one position are serialized so a fast
 * open→close can't race two upserts onto the same unique key, and the open path never clears a close,
 * so an out-of-order open-after-close still leaves the trade closed.
 *
 * Money math is the vendored `trades/` helpers, so a row written here is numerically identical to one
 * the main app's cron writes.
 */
@Injectable()
export class TradeWriterService {
	private readonly logger = new Logger(TradeWriterService.name);
	private readonly chains = new Map<string, Promise<unknown>>();

	constructor(private readonly prisma: PrismaService) {}

	/** Upsert the open side of a position from a live `Position` message. Never clears an existing close. */
	async recordOpen(ctx: TradeLockerAccountContext, open: OpenTrade): Promise<void> {
		await this.serialize(ctx.userId, open.positionId, () => this.writeOpen(ctx, open));
	}

	/** Finalize a position as closed from realized REST data. Idempotent (SETs absolute totals). */
	async recordClosed(ctx: TradeLockerAccountContext, closed: ClosedTrade): Promise<void> {
		await this.serialize(ctx.userId, closed.positionId, () => this.writeClosed(ctx, closed));
	}

	/**
	 * Refresh the unrealized (floating) P&L of an OPEN position from a live `AccountStatus.positionPnLs`
	 * entry. Update-only: never CREATES a row (the stream pnl entry has no symbol/side/qty to build one —
	 * the `Position` handler owns creation) and never touches a CLOSED row (the realized close wins).
	 * Mirrors the main app's open-position merge so the row is numerically identical to a cron-written one.
	 */
	async recordUnrealized(
		ctx: TradeLockerAccountContext,
		positionId: string,
		unrealizedPl: number
	): Promise<void> {
		await this.serialize(ctx.userId, positionId, () =>
			this.writeUnrealized(ctx, positionId, unrealizedPl)
		);
	}

	/** Run task on the per-position serialization chain so concurrent writes can't clobber each other. */
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

	private async writeOpen(ctx: TradeLockerAccountContext, open: OpenTrade): Promise<void> {
		await this.prisma.trade.upsert({
			where: this.whereKey(ctx.userId, open.positionId),
			create: {
				symbol: open.symbol,
				type: open.side,
				quantity: open.quantity,
				entryDate: open.entryDate,
				exitDate: null,
				tags: [],
				fromTerminal: true,
				terminalName: TERMINAL_NAME,
				terminalTradeId: open.positionId,
				riskPercentage: DEFAULT_IMPORTED_RISK_PERCENTAGE,
				user: { connect: { id: ctx.userId } },
				tradingAccount: { connect: { id: ctx.tradingAccountId } },
			},
			// Refresh entry data only — never clobber a close that may have already landed.
			update: {
				symbol: open.symbol,
				type: open.side,
				quantity: open.quantity,
				entryDate: open.entryDate,
			},
		});
		this.logger.log(`open ${open.positionId} ${open.side} ${open.symbol} ${open.quantity}`);
	}

	private async writeClosed(ctx: TradeLockerAccountContext, closed: ClosedTrade): Promise<void> {
		const grossProfit = roundMoney(closed.grossProfit);
		const commission = roundMoney(closed.commission);
		const swap = roundMoney(closed.swap);
		// Net realized P&L — prefer the report's native net, else derive it. This is the basis the main
		// app's TradeLocker importer persists as `profit` AND `pnl`, and derives %/R/result from (its
		// adapter sets profit = pnl = net), UNLIKE cTrader where `profit` is the gross figure.
		const pnl = roundMoney(closed.netProfit ?? grossProfit + commission + swap);
		// Risk % is a user-owned input (terminals never report it) — a manually-set value MUST survive
		// re-finalization (every backfill/live re-close runs this). Preserve an existing value and only
		// default a brand-new row, exactly like the main app's buildUpdatePayload. On upsert-create
		// `existing` is null ⇒ DEFAULT; on update it's the stored (possibly hand-edited) value. RR is
		// then derived from the PRESERVED risk so it stays consistent. Read runs inside the per-position
		// serialize chain, so no other write for this position interleaves.
		const existing = await this.prisma.trade.findUnique({
			where: this.whereKey(ctx.userId, closed.positionId),
			select: { riskPercentage: true },
		});
		const riskPercentage = existing?.riskPercentage ?? DEFAULT_IMPORTED_RISK_PERCENTAGE;
		const profitPercentage = computeProfitPercentageFromMoney(pnl, ctx.nominal) ?? undefined;
		const rr =
			computeTradeRr({
				profitMoney: pnl,
				profitPercentage: profitPercentage ?? null,
				nominal: ctx.nominal,
				riskPercentage,
			}) ?? undefined;
		const result = getTradeResult(pnl, ctx.nominal);

		const data = {
			symbol: closed.symbol,
			type: closed.side,
			quantity: closed.quantity,
			entryDate: closed.entryDate,
			exitDate: closed.exitDate,
			profit: pnl,
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
			terminalTradeId: closed.positionId,
		};

		await this.prisma.trade.upsert({
			where: this.whereKey(ctx.userId, closed.positionId),
			// close-trades-history carries authoritative open data (units + open time), so the finalized
			// row SETs quantity/entryDate on update too — matching the main app's importer, which
			// overwrites them, and correcting any best-effort values the live open row may have held.
			create: {
				...data,
				tags: [],
				user: { connect: { id: ctx.userId } },
				tradingAccount: { connect: { id: ctx.tradingAccountId } },
			},
			update: data,
		});
		this.logger.log(
			`closed ${closed.positionId} ${closed.side} ${closed.symbol} pnl=${pnl} result=${result ?? '-'}`
		);
	}

	private async writeUnrealized(
		ctx: TradeLockerAccountContext,
		positionId: string,
		unrealizedPl: number
	): Promise<void> {
		// Risk % is user-owned — read the existing row to preserve it (and to confirm the row exists and is
		// still open before writing). No open row yet ⇒ the `Position` handler hasn't created it; skip and
		// the next AccountStatus tick catches it once it exists.
		const existing = await this.prisma.trade.findUnique({
			where: this.whereKey(ctx.userId, positionId),
			select: { exitDate: true, riskPercentage: true },
		});
		if (!existing || existing.exitDate !== null) {
			return; // no open row to update, or already closed (the realized close is authoritative)
		}
		// Store the raw unrealized figure (already in account currency) and derive %/RR from it — exactly
		// like the main app's mergeOpenPositions, so the floating R-multiple matches a cron-written row.
		const riskPercentage = existing.riskPercentage ?? DEFAULT_IMPORTED_RISK_PERCENTAGE;
		const profitPercentage = computeProfitPercentageFromMoney(unrealizedPl, ctx.nominal) ?? null;
		const rr =
			computeTradeRr({
				profitMoney: unrealizedPl,
				profitPercentage,
				nominal: ctx.nominal,
				riskPercentage,
			}) ?? null;
		// exitDate:null in the where makes the write atomically skip a close that landed between the read
		// and here, and updateMany no-ops (rather than throwing) if the row just vanished.
		await this.prisma.trade.updateMany({
			where: {
				userId: ctx.userId,
				terminalName: TERMINAL_NAME,
				terminalTradeId: positionId,
				exitDate: null,
			},
			data: { pnl: unrealizedPl, profitPercentage, rr, riskPercentage },
		});
	}

	private whereKey(userId: string, positionId: string) {
		return {
			userId_terminalName_terminalTradeId: {
				userId,
				terminalName: TERMINAL_NAME,
				terminalTradeId: positionId,
			},
		};
	}
}
