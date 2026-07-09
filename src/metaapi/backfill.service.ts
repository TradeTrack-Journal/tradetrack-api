import { Injectable, Logger } from '@nestjs/common';

import { PrismaService } from '../prisma';
import {
	BACKFILL_LOOKBACK_DAYS,
	BACKFILL_WATERMARK_BUFFER_DAYS,
	POSITION_KEY_PREFIX,
	TERMINAL_NAME,
} from './constants';
import { aggregateDeals } from './deal-aggregator';
import { TradeWriterService } from './trade-writer.service';
import { positionKey, type ManagedAccount, type RpcLike } from './types';

const DAY_MS = 24 * 60 * 60 * 1_000;

/**
 * Reconstructs everything that happened while we were not streaming. Runs on every (re)connect, so a
 * worker restart or a dropped socket never silently loses trades.
 *
 * The window starts at the newest closed trade we already stored, minus a buffer (a broker can report
 * a deal slightly out of order), floored by the 90-day lookback, and floored again by
 * `account.backfillFrom` — the EA-migration clamp.
 *
 * That clamp is load-bearing. A user migrating from the EA to MetaApi already has those trades stored
 * under terminalName='mt5'; ours land under 'metaapi'. The unique key cannot dedupe across a different
 * terminalName, so without the clamp up to 90 days of trades would be counted twice and every P&L
 * statistic would be inflated.
 */
@Injectable()
export class BackfillService {
	private readonly logger = new Logger(BackfillService.name);

	constructor(
		private readonly prisma: PrismaService,
		private readonly writer: TradeWriterService
	) {}

	async run(rpc: RpcLike, account: ManagedAccount): Promise<void> {
		const now = new Date();
		const from = await this.resolveWatermark(account, now);
		this.logger.log(
			`backfill ${account.tradingAccountId} from=${from.toISOString()} to=${now.toISOString()}`
		);

		const [history, openPositions] = await Promise.all([
			rpc.getDealsByTimeRange(from, now),
			rpc.getPositions(),
		]);

		// MetatraderPosition.id is a number; deal.positionId is a string. Normalize or nothing matches.
		const openIds = new Set(openPositions.map((position) => positionKey(position.id)));
		// The clamp is applied INSIDE the aggregator: it must drop the individual pre-migration closing
		// deals, not just whole pre-migration positions. A position partially closed before the migration
		// and fully closed after it would otherwise carry the EA's already-booked partial P&L as well.
		const positions = aggregateDeals(history.deals ?? [], openIds, account.backfillFrom);

		const ctx = {
			userId: account.userId,
			tradingAccountId: account.tradingAccountId,
			nominal: account.nominal,
		};

		for (const position of positions) {
			await this.writer.recordPosition(ctx, position);
		}
		this.logger.log(`backfill ${account.tradingAccountId} wrote ${positions.length} positions`);
	}

	private async resolveWatermark(account: ManagedAccount, now: Date): Promise<Date> {
		const newest = await this.prisma.trade.findFirst({
			where: {
				userId: account.userId,
				terminalName: TERMINAL_NAME,
				terminalTradeId: { startsWith: POSITION_KEY_PREFIX },
				exitDate: { not: null },
			},
			orderBy: { exitDate: 'desc' },
			select: { exitDate: true },
		});

		const lookbackFloor = new Date(now.getTime() - BACKFILL_LOOKBACK_DAYS * DAY_MS);
		const watermark = newest?.exitDate
			? new Date(newest.exitDate.getTime() - BACKFILL_WATERMARK_BUFFER_DAYS * DAY_MS)
			: lookbackFloor;

		// Never reach further back than the lookback floor, nor past the EA-migration clamp.
		const candidates = [watermark, lookbackFloor];
		if (account.backfillFrom) candidates.push(account.backfillFrom);
		return new Date(Math.max(...candidates.map((date) => date.getTime())));
	}
}
