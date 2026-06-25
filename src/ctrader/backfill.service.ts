import { Injectable, Logger } from '@nestjs/common';

import { PrismaService } from '../prisma';
import type { CtraderConnection } from './ctrader-connection';
import {
	BACKFILL_LOOKBACK_MS,
	BACKFILL_MAX_ROWS,
	BACKFILL_WATERMARK_BUFFER_MS,
} from './ctrader.constants';
import type { BackfillPosition } from './ctrader-trade-writer';
import { CtraderTradeWriter } from './ctrader-trade-writer';
import type { PoolAccount } from './types';

const TERMINAL_NAME = 'ctrader';
const VOLUME_PER_LOT = 10_000;
const MONEY_SCALE = 100;

interface RawCloseDetail {
	grossProfit?: number | string;
	commission?: number | string;
	swap?: number | string;
}

interface RawDeal {
	positionId?: number | string;
	symbolId?: number | string;
	volume?: number | string;
	filledVolume?: number | string;
	tradeSide?: number | string;
	executionTimestamp?: number | string;
	createTimestamp?: number | string;
	closePositionDetail?: RawCloseDetail;
}

interface RawPosition {
	positionId?: number | string;
	tradeData?: {
		symbolId?: number | string;
		volume?: number | string;
		tradeSide?: number | string;
		openTimestamp?: number | string;
	};
}

interface NormalizedDeal {
	symbolId: number;
	volume: number;
	filledVolume: number;
	side: string;
	executionTimestamp: number;
	createTimestamp: number;
	close?: { grossProfit: number; commission: number; swap: number };
}

interface OpenPosition {
	positionId: string;
	symbolId: number;
	volume: number;
	side: string;
	openTimestamp: number;
}

/**
 * On (re)connect, reconstructs an account's trades from cTrader history so deals that happened while
 * the backend was down aren't lost, and currently-open positions show up immediately:
 *   - ProtoOAReconcileReq gives the authoritative set of OPEN positions (with their entry data).
 *   - ProtoOADealListReq (from a moving watermark) gives recent closed history.
 * A position is "closed" iff it is NOT in the open set, so a position opened before the deal window
 * is classified correctly. Closed positions are written with absolute realized values (SET), which
 * is idempotent across the per-(re)connect re-runs; open positions go through the open path, which
 * never clears a live-set close. Realized P&L of partial closes on still-open positions is the one
 * remaining gap (live events carry it forward from connect time).
 */
@Injectable()
export class BackfillService {
	private readonly logger = new Logger(BackfillService.name);
	private readonly inFlight = new Set<number>();

	constructor(
		private readonly prisma: PrismaService,
		private readonly writer: CtraderTradeWriter
	) {}

	async backfillAccount(connection: CtraderConnection, account: PoolAccount): Promise<void> {
		const ctid = account.ctidTraderAccountId;
		if (this.inFlight.has(ctid)) {
			return; // a backfill for this account is already running (rapid reconnects)
		}
		this.inFlight.add(ctid);
		try {
			await this.run(connection, account);
		} finally {
			this.inFlight.delete(ctid);
		}
	}

	private async run(connection: CtraderConnection, account: PoolAccount): Promise<void> {
		const ctid = account.ctidTraderAccountId;
		const fromTimestamp = await this.watermark(account);
		const toTimestamp = Date.now();

		// 1) Authoritative open positions. If this fails we fall back to volume-based inference and
		//    skip positions we can't classify, rather than risk mis-closing an open one.
		const open = await this.fetchOpenPositions(connection, ctid);
		const openById = new Map(open.positions.map((p) => [p.positionId, p]));

		// 2) Recent closed history.
		const dealsByPosition = await this.fetchDeals(connection, ctid, fromTimestamp, toTimestamp);

		let written = 0;
		const handled = new Set<string>();

		for (const [positionId, deals] of dealsByPosition) {
			let position: BackfillPosition | null;
			if (open.ok) {
				if (openById.has(positionId)) {
					position = null; // open positions are written from reconcile data below
				} else {
					// Closed per reconcile — needs a closing deal to compute realized P&L; skip anomalies.
					position = deals.some((d) => d.close)
						? this.reconstructClosed(account, positionId, deals)
						: null;
				}
			} else {
				position = this.reconstructWithoutReconcile(account, positionId, deals);
			}
			if (!position) {
				continue;
			}
			if (await this.write(account, position)) {
				written += 1;
			}
			handled.add(positionId);
		}

		// 3) Open positions from reconcile (authoritative entry data; covers ones opened before the
		//    deal window, and ones with no deals in the window).
		if (open.ok) {
			for (const [positionId, p] of openById) {
				if (handled.has(positionId)) {
					continue;
				}
				if (await this.write(account, this.mapOpen(account, p))) {
					written += 1;
				}
			}
		}

		this.logger.log(
			`[${ctid}] backfill: ${written} position(s) written (${dealsByPosition.size} from deals, ${openById.size} open) since ${new Date(fromTimestamp).toISOString()}`
		);
	}

	private async write(account: PoolAccount, position: BackfillPosition): Promise<boolean> {
		try {
			await this.writer.backfillPosition(account, position);
			return true;
		} catch (error) {
			this.logger.error(
				`[${account.ctidTraderAccountId}] backfill write failed for ${position.positionId}: ${describe(error)}`
			);
			return false;
		}
	}

	/** Pull-from timestamp: just before the last closed trade (with overlap), bounded by the lookback.
	 *  Keyed on closed history only — an old still-open position must not drag the window back, and its
	 *  eventual close arrives as a live event anyway. */
	private async watermark(account: PoolAccount): Promise<number> {
		const defaultFrom = Date.now() - BACKFILL_LOOKBACK_MS;
		const lastClosed = await this.prisma.trade.findFirst({
			where: {
				userId: account.userId,
				tradingAccountId: account.tradingAccountId,
				fromTerminal: true,
				terminalName: TERMINAL_NAME,
				exitDate: { not: null },
			},
			orderBy: { exitDate: 'desc' },
			select: { exitDate: true },
		});
		if (!lastClosed?.exitDate) {
			return defaultFrom;
		}
		return Math.max(defaultFrom, lastClosed.exitDate.getTime() - BACKFILL_WATERMARK_BUFFER_MS);
	}

	private async fetchOpenPositions(
		connection: CtraderConnection,
		ctid: number
	): Promise<{ ok: boolean; positions: OpenPosition[] }> {
		try {
			const res = (await connection.request('ProtoOAReconcileReq', {
				ctidTraderAccountId: ctid,
			})) as { position?: RawPosition[] };
			const positions: OpenPosition[] = [];
			for (const raw of res.position ?? []) {
				const positionId = String(raw.positionId ?? '');
				if (!positionId) {
					continue;
				}
				positions.push({
					positionId,
					symbolId: Number(raw.tradeData?.symbolId ?? 0),
					volume: Number(raw.tradeData?.volume ?? 0),
					side: Number(raw.tradeData?.tradeSide) === 2 ? 'SELL' : 'BUY',
					openTimestamp: Number(raw.tradeData?.openTimestamp ?? 0),
				});
			}
			return { ok: true, positions };
		} catch (error) {
			this.logger.warn(`[${ctid}] reconcile (open positions) failed: ${describe(error)}`);
			return { ok: false, positions: [] };
		}
	}

	private async fetchDeals(
		connection: CtraderConnection,
		ctid: number,
		fromTimestamp: number,
		toTimestamp: number
	): Promise<Map<string, NormalizedDeal[]>> {
		const byPosition = new Map<string, NormalizedDeal[]>();
		let response: { deal?: RawDeal[] };
		try {
			response = (await connection.request('ProtoOADealListReq', {
				ctidTraderAccountId: ctid,
				fromTimestamp,
				toTimestamp,
				maxRows: BACKFILL_MAX_ROWS,
			})) as { deal?: RawDeal[] };
		} catch (error) {
			this.logger.warn(`[${ctid}] backfill deal list failed: ${describe(error)}`);
			return byPosition;
		}
		for (const raw of response.deal ?? []) {
			const positionId = String(raw.positionId ?? '');
			if (!positionId) {
				continue;
			}
			const list = byPosition.get(positionId) ?? [];
			list.push(this.normalize(raw));
			byPosition.set(positionId, list);
		}
		return byPosition;
	}

	/** A position absent from the open set is fully closed → write absolute realized values. */
	private reconstructClosed(
		account: PoolAccount,
		positionId: string,
		deals: NormalizedDeal[]
	): BackfillPosition {
		const closing = deals.filter((d) => d.close);
		const openDeal = deals.find((d) => !d.close);
		const firstClose = closing[0];

		// The opening deal's side IS the position direction; a closing deal's side is the opposite, so
		// invert it when the opening deal is older than the window (cTrader closes a long with a sell).
		const direction = openDeal ? openDeal.side : firstClose ? invert(firstClose.side) : 'BUY';
		const symbolId = openDeal?.symbolId ?? firstClose?.symbolId ?? 0;
		const openVolume = openDeal
			? openDeal.volume
			: closing.reduce((sum, d) => sum + d.filledVolume, 0);

		return {
			positionId,
			symbol: account.symbolNames?.get(symbolId) ?? String(symbolId),
			direction,
			quantity: openVolume / VOLUME_PER_LOT,
			entryDate: this.toDate(
				openDeal ? openDeal.executionTimestamp || openDeal.createTimestamp : firstClose?.createTimestamp
			),
			fullyClosed: true,
			exitDate: this.toDate(Math.max(0, ...closing.map((d) => d.executionTimestamp))),
			grossProfit: closing.reduce((s, d) => s + (d.close?.grossProfit ?? 0), 0) / MONEY_SCALE,
			commission: closing.reduce((s, d) => s + (d.close?.commission ?? 0), 0) / MONEY_SCALE,
			swap: closing.reduce((s, d) => s + (d.close?.swap ?? 0), 0) / MONEY_SCALE,
		};
	}

	/** Reconcile unavailable: only classify positions whose opening deal is in the window; skip the
	 *  rest rather than guess (a wrong guess could mis-close an open position). */
	private reconstructWithoutReconcile(
		account: PoolAccount,
		positionId: string,
		deals: NormalizedDeal[]
	): BackfillPosition | null {
		const openDeal = deals.find((d) => !d.close);
		if (!openDeal) {
			return null;
		}
		const closing = deals.filter((d) => d.close);
		const closedVolume = closing.reduce((sum, d) => sum + d.filledVolume, 0);
		if (closedVolume >= openDeal.volume && closing.length > 0) {
			return this.reconstructClosed(account, positionId, deals);
		}
		return {
			positionId,
			symbol: account.symbolNames?.get(openDeal.symbolId) ?? String(openDeal.symbolId),
			direction: openDeal.side,
			quantity: openDeal.volume / VOLUME_PER_LOT,
			entryDate: this.toDate(openDeal.executionTimestamp || openDeal.createTimestamp),
			fullyClosed: false,
			exitDate: null,
			grossProfit: 0,
			commission: 0,
			swap: 0,
		};
	}

	private mapOpen(account: PoolAccount, p: OpenPosition): BackfillPosition {
		return {
			positionId: p.positionId,
			symbol: account.symbolNames?.get(p.symbolId) ?? String(p.symbolId),
			direction: p.side,
			quantity: p.volume / VOLUME_PER_LOT,
			entryDate: this.toDate(p.openTimestamp),
			fullyClosed: false,
			exitDate: null,
			grossProfit: 0,
			commission: 0,
			swap: 0,
		};
	}

	private normalize(d: RawDeal): NormalizedDeal {
		const detail = d.closePositionDetail;
		return {
			symbolId: Number(d.symbolId ?? 0),
			volume: Number(d.volume ?? 0),
			filledVolume: Number(d.filledVolume ?? d.volume ?? 0),
			side: Number(d.tradeSide) === 2 ? 'SELL' : 'BUY',
			executionTimestamp: Number(d.executionTimestamp ?? 0),
			createTimestamp: Number(d.createTimestamp ?? 0),
			close: detail
				? {
						grossProfit: Number(detail.grossProfit ?? 0),
						commission: Number(detail.commission ?? 0),
						swap: Number(detail.swap ?? 0),
					}
				: undefined,
		};
	}

	private toDate(timestamp: number | undefined): Date {
		const ms = Number(timestamp ?? 0);
		return ms > 0 ? new Date(ms) : new Date();
	}
}

function invert(side: string): string {
	return side === 'BUY' ? 'SELL' : 'BUY';
}

function describe(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}
