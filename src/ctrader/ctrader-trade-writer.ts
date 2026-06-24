import { Injectable, Logger } from '@nestjs/common';

import { PrismaService } from '../prisma';
import {
	computeProfitPercentageFromMoney,
	computeTradeRr,
	DEFAULT_IMPORTED_RISK_PERCENTAGE,
	getTradeResult,
	roundMoney,
} from '../trades';
import type { ExecutionEventPayload, PoolAccount } from './types';

const TERMINAL_NAME = 'ctrader';

/** cTrader volume unit → lots (100 = 0.01 lot, 10000 = 1 lot). */
const VOLUME_PER_LOT = 10_000;
/** Monetary integers are scaled by moneyDigits (2) → divide by 100 for account currency. */
const MONEY_SCALE = 100;

/**
 * Persists cTrader execution events into the Trade table. One Trade row per position, keyed by
 * terminalTradeId = `ctrader_pos_{positionId}`, so the OPEN fill creates an open row (exitDate
 * null) and the CLOSE fill updates that same row to closed. The realized money math mirrors the
 * main app exactly so a backend-written trade is identical to a cron-written one.
 *
 * Writes for the same position are serialized so a fast open→close can't race two concurrent
 * upserts onto the same unique key. The open path never clears exitDate, so even an out-of-order
 * open-after-close leaves the trade closed.
 */
@Injectable()
export class CtraderTradeWriter {
	private readonly logger = new Logger(CtraderTradeWriter.name);
	private readonly chains = new Map<string, Promise<unknown>>();

	constructor(private readonly prisma: PrismaService) {}

	async record(account: PoolAccount, payload: ExecutionEventPayload): Promise<void> {
		const positionId = String(payload.deal?.positionId ?? payload.position?.positionId ?? '');
		if (!payload.deal || !positionId) {
			return; // ORDER_ACCEPTED and friends carry no deal — nothing to persist
		}

		const key = `${account.userId}:ctrader_pos_${positionId}`;
		const previous = this.chains.get(key) ?? Promise.resolve();
		const current = previous.then(() => this.write(account, payload, positionId));
		const tail: Promise<unknown> = current
			.catch(() => undefined)
			.finally(() => {
				if (this.chains.get(key) === tail) {
					this.chains.delete(key);
				}
			});
		this.chains.set(key, tail);
		await current;
	}

	private async write(
		account: PoolAccount,
		payload: ExecutionEventPayload,
		positionId: string
	): Promise<void> {
		const deal = payload.deal;
		if (!deal) {
			return;
		}

		const terminalTradeId = `ctrader_pos_${positionId}`;
		const symbolId = Number(deal.symbolId ?? payload.position?.tradeData?.symbolId ?? 0);
		const symbol = account.symbolNames?.get(symbolId) ?? String(symbolId);
		// The position direction is the real trade side; the closing deal's tradeSide is inverted.
		const direction = String(payload.position?.tradeData?.tradeSide ?? deal.tradeSide ?? 'BUY');
		const quantity = Number(deal.volume ?? payload.position?.tradeData?.volume ?? 0) / VOLUME_PER_LOT;
		const entryDate = this.toDate(
			payload.position?.tradeData?.openTimestamp ?? deal.executionTimestamp
		);

		// A deal carrying closePositionDetail is a closing deal (partial OR full). The opening fill
		// has closePositionDetail null. Accumulate realized P&L across all closing deals (writes are
		// serialized per position, so the read-modify-write is safe); the trade stays open until the
		// position is fully closed.
		const detail = deal.closePositionDetail;
		if (!detail) {
			await this.upsertOpen(account, terminalTradeId, { symbol, direction, quantity, entryDate });
			return;
		}

		await this.applyClose(account, terminalTradeId, {
			symbol,
			direction,
			quantity,
			entryDate,
			exitDate: this.toDate(deal.executionTimestamp),
			grossProfit: roundMoney(Number(detail.grossProfit ?? 0) / MONEY_SCALE),
			commission: roundMoney(Number(detail.commission ?? 0) / MONEY_SCALE),
			swap: roundMoney(Number(detail.swap ?? 0) / MONEY_SCALE),
			fullyClosed: payload.position?.positionStatus === 'POSITION_STATUS_CLOSED',
		});
	}

	private async upsertOpen(
		account: PoolAccount,
		terminalTradeId: string,
		t: { symbol: string; direction: string; quantity: number; entryDate: Date }
	): Promise<void> {
		await this.prisma.trade.upsert({
			where: this.whereKey(account.userId, terminalTradeId),
			create: {
				symbol: t.symbol,
				type: t.direction,
				quantity: t.quantity,
				entryDate: t.entryDate,
				exitDate: null,
				tags: [],
				fromTerminal: true,
				terminalName: TERMINAL_NAME,
				terminalTradeId,
				riskPercentage: DEFAULT_IMPORTED_RISK_PERCENTAGE,
				user: { connect: { id: account.userId } },
				tradingAccount: { connect: { id: account.tradingAccountId } },
			},
			// Refresh entry data only — never clobber a close that may have already landed.
			update: {
				symbol: t.symbol,
				type: t.direction,
				quantity: t.quantity,
				entryDate: t.entryDate,
			},
		});
		this.logger.log(`open ${terminalTradeId} ${t.direction} ${t.symbol} ${t.quantity}`);
	}

	private async applyClose(
		account: PoolAccount,
		terminalTradeId: string,
		t: {
			symbol: string;
			direction: string;
			quantity: number;
			entryDate: Date;
			exitDate: Date;
			grossProfit: number;
			commission: number;
			swap: number;
			fullyClosed: boolean;
		}
	): Promise<void> {
		// Add this closing deal's realized values onto whatever the row already has (partial closes
		// arrive as separate deals). Safe to read-then-write: writes for one position are serialized.
		const existing = await this.prisma.trade.findUnique({
			where: this.whereKey(account.userId, terminalTradeId),
			select: { grossProfit: true, commission: true, swap: true },
		});

		const grossProfit = roundMoney((existing?.grossProfit ?? 0) + t.grossProfit);
		const commission = roundMoney((existing?.commission ?? 0) + t.commission);
		const swap = roundMoney((existing?.swap ?? 0) + t.swap);
		const pnl = roundMoney(grossProfit + commission + swap);
		const profitPercentage =
			computeProfitPercentageFromMoney(grossProfit, account.nominal) ?? undefined;
		const rr =
			computeTradeRr({
				profitMoney: grossProfit,
				profitPercentage: profitPercentage ?? null,
				nominal: account.nominal,
				riskPercentage: DEFAULT_IMPORTED_RISK_PERCENTAGE,
			}) ?? undefined;
		// Stay open (no exitDate / result) until the position is fully closed.
		const exitDate = t.fullyClosed ? t.exitDate : null;
		const result = t.fullyClosed ? getTradeResult(pnl, account.nominal) : null;

		const data = {
			symbol: t.symbol,
			type: t.direction,
			exitDate,
			profit: grossProfit,
			grossProfit,
			commission,
			swap,
			pnl,
			profitPercentage,
			rr,
			result,
			riskPercentage: DEFAULT_IMPORTED_RISK_PERCENTAGE,
			fromTerminal: true,
			terminalName: TERMINAL_NAME,
			terminalTradeId,
		};

		await this.prisma.trade.upsert({
			where: this.whereKey(account.userId, terminalTradeId),
			// quantity + entryDate only on create — never clobber the open row's entry data.
			create: {
				...data,
				quantity: t.quantity,
				entryDate: t.entryDate,
				tags: [],
				user: { connect: { id: account.userId } },
				tradingAccount: { connect: { id: account.tradingAccountId } },
			},
			update: data,
		});
		this.logger.log(
			`${t.fullyClosed ? 'closed' : 'partial'} ${terminalTradeId} ${t.direction} ${t.symbol} pnl=${pnl} result=${result ?? '-'}`
		);
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

	private toDate(timestamp: string | number | undefined): Date {
		const ms = Number(timestamp ?? 0);
		return ms > 0 ? new Date(ms) : new Date();
	}
}
