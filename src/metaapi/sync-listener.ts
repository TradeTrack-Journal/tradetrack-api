import type { Logger } from '@nestjs/common';
import { SynchronizationListener } from 'metaapi.cloud-sdk';
import type { MetatraderDeal, MetatraderPosition } from 'metaapi.cloud-sdk';

import { DEAL_TYPE_BALANCE, POSITION_TYPE_BUY } from './constants';
import { aggregateDeals } from './deal-aggregator';
import { TradeWriterService } from './trade-writer.service';
import { positionKey, type MetaApiAccountContext } from './types';

export interface SyncListenerDeps {
	writer: TradeWriterService;
	ctx: MetaApiAccountContext;
	logger: Logger;
	/**
	 * Fetch every deal belonging to one position (RPC `getDealsByPosition`).
	 *
	 * Required for correctness: a live `onDealAdded` delivers ONE deal. A closing (DEAL_ENTRY_OUT) deal
	 * on its own carries no side, entry time or volume — those live on the opening deal — so
	 * aggregating it alone would skip the position entirely and the close would only appear after the
	 * next reconnect backfill. Re-fetching the position's full deal set finalizes it immediately.
	 */
	fetchPositionDeals: (positionId: string) => Promise<MetatraderDeal[]>;
	/** EA-migration clamp; deals at or before it were already booked by the EA importer. */
	clampFrom: Date | null;
}

/**
 * Bridges MetaApi's streaming callbacks to the trade writer. A plain class (no @Injectable) because
 * the connection manager constructs one instance per streamed account.
 *
 * Method names must match MetaApi's SynchronizationListener exactly — extending the SDK's base class
 * makes a typo a compile error rather than a silent no-op.
 */
export class MetaApiSyncListener extends SynchronizationListener {
	/** String keys: MetatraderPosition.id is a number, MetatraderDeal.positionId is a string. */
	private readonly openPositionIds = new Set<string>();

	/**
	 * False until the SDK finishes replaying deal history.
	 *
	 * On connect the websocket client pushes the account's ENTIRE deal history through `onDealAdded`,
	 * one call per historical deal. Treating those as live would fire one `getDealsByPosition` RPC per
	 * historical deal — hundreds of round-trips, right while the synchronization throttler is busy —
	 * redoing work the backfill already did. History is the backfill's job; this listener only handles
	 * what happens after.
	 */
	private dealsSynchronized = false;

	constructor(private readonly deps: SyncListenerDeps) {
		super();
	}

	/** Fires once the SDK has finished replaying deal history. Everything after this is live. */
	override onDealsSynchronized(): Promise<void> {
		this.dealsSynchronized = true;
		return Promise.resolve();
	}

	/** Full snapshot delivered at (re)synchronization. Reseeds the open-position set. */
	override async onPositionsReplaced(
		_instanceIndex: string,
		positions: MetatraderPosition[]
	): Promise<void> {
		this.openPositionIds.clear();
		for (const position of positions) {
			this.openPositionIds.add(positionKey(position.id));
			await this.writeOpen(position);
		}
	}

	override async onPositionUpdated(
		_instanceIndex: string,
		position: MetatraderPosition
	): Promise<void> {
		this.openPositionIds.add(positionKey(position.id));
		await this.writeOpen(position);
		if (typeof position.unrealizedProfit === 'number') {
			await this.deps.writer.recordUnrealized(
				this.deps.ctx,
				positionKey(position.id),
				position.unrealizedProfit
			);
		}
	}

	/**
	 * The position left the terminal. Drop it from the open set FIRST, so the closing deal that follows
	 * is aggregated as closed rather than open.
	 */
	/**
	 * The position left the terminal. Drop it from the open set FIRST so any closing deal is aggregated
	 * as closed, then finalize immediately — the SDK does not guarantee that the closing deal arrives in
	 * the same packet, and if it arrived earlier the row would sit open until the next reconnect.
	 */
	override async onPositionRemoved(_instanceIndex: string, positionId: string): Promise<void> {
		this.openPositionIds.delete(positionKey(positionId));
		if (!this.dealsSynchronized) return;
		await this.finalize(positionKey(positionId));
	}

	override async onDealAdded(_instanceIndex: string, deal: MetatraderDeal): Promise<void> {
		if (deal.type === DEAL_TYPE_BALANCE || !deal.positionId) return;
		// History replay — the backfill owns it.
		if (!this.dealsSynchronized) return;
		await this.finalize(deal.positionId);
	}

	/** Re-fetch the position's whole deal set so the opening deal (side/entry/volume) is present. */
	private async finalize(positionId: string): Promise<void> {
		try {
			const deals = await this.deps.fetchPositionDeals(positionId);
			if (!deals.length) return;
			const positions = aggregateDeals(deals, this.openPositionIds, this.deps.clampFrom);
			for (const position of positions) {
				await this.deps.writer.recordPosition(this.deps.ctx, position);
			}
		} catch (error) {
			// Never let a stream callback throw — the next reconnect's backfill is the backstop.
			this.deps.logger.error(`finalize failed for position ${positionId}`, error as Error);
		}
	}

	private async writeOpen(position: MetatraderPosition): Promise<void> {
		await this.deps.writer.recordPosition(this.deps.ctx, {
			positionId: positionKey(position.id),
			symbol: position.symbol,
			side: position.type === POSITION_TYPE_BUY ? 'BUY' : 'SELL',
			quantity: position.volume,
			entryDate: position.time instanceof Date ? position.time : new Date(position.time),
			exitDate: null,
			// Realized money only exists once the position closes; the closing deal supplies it.
			grossProfit: 0,
			commission: 0,
			swap: 0,
		});
	}
}
