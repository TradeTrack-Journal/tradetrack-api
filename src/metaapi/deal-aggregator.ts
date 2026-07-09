import type { MetatraderDeal } from 'metaapi.cloud-sdk';

import { DEAL_ENTRY_IN, DEAL_ENTRY_OUT, DEAL_TYPE_BALANCE, DEAL_TYPE_BUY } from './constants';
import type { AggregatedPosition } from './types';

function toDate(value: Date | string): Date {
	return value instanceof Date ? value : new Date(value);
}

/**
 * Rebuild MT5 positions from their deals. In MT5 a round-turn position is several deals sharing a
 * `positionId`: one DEAL_ENTRY_IN opening it and one or more DEAL_ENTRY_OUT closing it (a partial
 * close produces several OUT deals).
 *
 * Money: `deal.profit` is MT5's GROSS figure — commission and swap are separate fields. MetaApi's own
 * model docs only call it "deal profit" and never state this, so the semantics come from the MT5
 * platform and are confirmed empirically by the Phase 0 spike. Net = gross + commission + swap, both
 * of the latter being negative. We persist `profit = gross`, matching the EA importer and cTrader.
 *
 * Classification mirrors ctrader/backfill.service.ts:
 *   - present in getPositions()                      -> OPEN   (exitDate null)
 *   - absent, with a closing deal inside the window  -> CLOSED
 *   - absent, but its opening deal fell outside the window -> SKIPPED. We would have to invent the
 *     side, entry time and volume. A wider window or a live event will pick it up instead.
 *
 * `openPositionIds` must contain STRING keys — MetatraderPosition.id is a number, so callers pass it
 * through `positionKey()`. Balance operations (deposits/withdrawals) are not trades.
 */
export function aggregateDeals(
	deals: MetatraderDeal[],
	openPositionIds: ReadonlySet<string>,
	clampFrom: Date | null = null
): AggregatedPosition[] {
	const groups = new Map<string, MetatraderDeal[]>();
	for (const deal of deals) {
		if (deal.type === DEAL_TYPE_BALANCE) continue;
		if (!deal.positionId) continue;
		const bucket = groups.get(deal.positionId);
		if (bucket) bucket.push(deal);
		else groups.set(deal.positionId, [deal]);
	}

	const clampMs = clampFrom ? clampFrom.getTime() : null;
	const positions: AggregatedPosition[] = [];

	for (const [positionId, group] of groups) {
		const sorted = [...group].sort((a, b) => toDate(a.time).getTime() - toDate(b.time).getTime());
		const opening = sorted.find((deal) => deal.entryType === DEAL_ENTRY_IN);

		// No opening deal in this window: side / entryDate / volume are unknowable. Skip, never guess.
		if (!opening || !opening.symbol || opening.volume == null) continue;

		const closing = sorted.filter((deal) => deal.entryType === DEAL_ENTRY_OUT);
		const isOpen = openPositionIds.has(positionId);
		const exitDate = isOpen || closing.length === 0 ? null : toDate(closing[closing.length - 1].time);

		// EA-migration clamp. The EA books money per CLOSING deal (`mt5_{posId}_{closeTicket}`), so a
		// position that was partially closed before the migration already has that partial's P&L stored
		// under terminalName='mt5'. Summing every OUT deal here would count it a second time — the unique
		// key cannot dedupe across a different terminalName. Anything realized at or before the clamp
		// belongs to the EA; only money booked after it is ours.
		if (clampMs !== null) {
			if (exitDate && exitDate.getTime() <= clampMs) continue; // fully closed pre-migration: EA owns it
		}
		const owned =
			clampMs === null ? sorted : sorted.filter((deal) => toDate(deal.time).getTime() > clampMs);

		const grossProfit = owned.reduce((sum, deal) => sum + (deal.profit ?? 0), 0);
		const commission = owned.reduce((sum, deal) => sum + (deal.commission ?? 0), 0);
		const swap = owned.reduce((sum, deal) => sum + (deal.swap ?? 0), 0);

		positions.push({
			positionId,
			symbol: opening.symbol,
			side: opening.type === DEAL_TYPE_BUY ? 'BUY' : 'SELL',
			quantity: opening.volume,
			entryDate: toDate(opening.time),
			exitDate,
			grossProfit,
			commission,
			swap,
		});
	}

	return positions;
}
