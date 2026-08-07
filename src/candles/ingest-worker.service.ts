import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { PrismaService } from '../prisma';

import { aggregateCandles } from './aggregate';
import { ArchiveClientService } from './archive-client.service';
import { dayStartsIn, MS_PER_DAY, nextMonthStartMs, nextYearStartMs } from './buckets';
import { ChunkWriterService } from './chunk-writer.service';
import {
	BASE_RESOLUTION_SECONDS,
	DAY_FETCH_CONCURRENCY,
	INGEST_INSTRUMENT_BY_SYMBOL,
	JOB_CONCURRENCY,
	MAX_JOB_ATTEMPTS,
	POLL_INTERVAL_MS,
	PUBLISH_SAFETY_MARGIN_MS,
	STUCK_RUNNING_RECLAIM_MS,
} from './constants';

import type { Env } from '../config';
import type { Candle } from './types';
import type { CandleIngestJob } from '@prisma/client';

/** A job that can never succeed (bad symbol/resolution) — FAILED immediately, no retries. */
class TerminalJobError extends Error {}

/** Sentinel for "the archive hasn't published this span yet" — retried without burning an attempt. */
const NOT_PUBLISHED = Symbol('not-published');

interface IngestResult {
	candles: Candle[];
	files: number;
}

interface JobKey {
	symbol: string;
	resolution: string;
	bucketStart: Date;
}

/**
 * Single-owner queue worker: polls `CandleIngestJob` (enqueued by the main app on chunk
 * misses), fetches the span's per-day m1 files from the Dukascopy S3 archive, folds derived
 * spans (1h/1d) with the shared aggregation, and persists permanent CandleChunk rows. Runs
 * only on the one Fly machine, so plain optimistic claims are enough — the `updatedAt` guard
 * in the claim exists for the crashed-deploy reclaim path, not for peer workers.
 */
@Injectable()
export class IngestWorkerService implements OnModuleInit, OnModuleDestroy {
	private readonly logger = new Logger(IngestWorkerService.name);
	private timer: NodeJS.Timeout | null = null;
	private draining = false;

	constructor(
		private readonly config: ConfigService<Env, true>,
		private readonly prisma: PrismaService,
		private readonly archive: ArchiveClientService,
		private readonly chunkWriter: ChunkWriterService
	) {}

	onModuleInit(): void {
		if (this.config.get('CANDLE_INGEST_ENABLED', { infer: true }) !== 'true') {
			this.logger.log('CANDLE_INGEST_ENABLED is not "true" — candle ingest worker idle');
			return;
		}
		if (
			!this.config.get('AWS_ACCESS_KEY_ID', { infer: true }) ||
			!this.config.get('AWS_SECRET_ACCESS_KEY', { infer: true })
		) {
			this.logger.warn('AWS credentials missing — candle ingest worker idle');
			return;
		}
		this.timer = setInterval(() => void this.drain(), POLL_INTERVAL_MS);
		this.logger.log(`Candle ingest worker polling every ${POLL_INTERVAL_MS}ms`);
	}

	onModuleDestroy(): void {
		if (this.timer) clearInterval(this.timer);
	}

	/** One pass: keep claiming batches until the queue is empty. Re-entry is a no-op. */
	private async drain(): Promise<void> {
		if (this.draining) return;
		this.draining = true;
		try {
			// Keys already claimed in THIS pass. A job that bounces back to PENDING (transient
			// failure, unpublished span) must wait for the next interval tick — without this the
			// loop would re-claim it immediately and burn all its retries within milliseconds.
			const seen: JobKey[] = [];
			for (;;) {
				const jobs: CandleIngestJob[] = [];
				for (let i = 0; i < JOB_CONCURRENCY; i++) {
					const job = await this.claimNext(seen);
					if (!job) break;
					seen.push({
						symbol: job.symbol,
						resolution: job.resolution,
						bucketStart: job.bucketStart,
					});
					jobs.push(job);
				}
				if (jobs.length === 0) return;
				await Promise.all(jobs.map((job) => this.runJob(job)));
			}
		} catch (err) {
			// runJob never throws; this guards the claim path (e.g. DB down) from killing the loop.
			this.logger.error(`drain pass failed: ${err instanceof Error ? err.message : String(err)}`);
		} finally {
			this.draining = false;
		}
	}

	/**
	 * Optimistically claim the oldest runnable job: PENDING, or RUNNING but untouched long
	 * enough that its worker must have died mid-job (deploys restart the machine; the row would
	 * stay RUNNING forever otherwise). The updatedAt equality in the guard makes the reclaim
	 * race-safe against the original owner finishing at the same moment.
	 */
	private async claimNext(exclude: JobKey[]): Promise<CandleIngestJob | null> {
		const candidate = await this.prisma.candleIngestJob.findFirst({
			where: {
				OR: [
					{ status: 'PENDING' },
					{ status: 'RUNNING', updatedAt: { lt: new Date(Date.now() - STUCK_RUNNING_RECLAIM_MS) } },
				],
				NOT: exclude,
			},
			orderBy: { createdAt: 'asc' },
		});
		if (!candidate) return null;
		const claimed = await this.prisma.candleIngestJob.updateMany({
			where: {
				symbol: candidate.symbol,
				resolution: candidate.resolution,
				bucketStart: candidate.bucketStart,
				status: candidate.status,
				updatedAt: candidate.updatedAt,
			},
			data: { status: 'RUNNING', attempts: { increment: 1 } },
		});
		if (claimed.count !== 1) return null; // lost the race; the next pass sees fresh state
		return { ...candidate, status: 'RUNNING', attempts: candidate.attempts + 1 };
	}

	/** Runs one claimed job to a terminal state. Never throws — every path settles the row. */
	private async runJob(job: CandleIngestJob): Promise<void> {
		const label = `${job.symbol}/${job.resolution}/${job.bucketStart.toISOString().slice(0, 10)}`;
		const startedAt = Date.now();
		try {
			const result = await this.ingest(job);
			if (result === NOT_PUBLISHED) {
				// Not a failure: the file simply isn't in the archive yet. Return the attempt so a
				// slow publish can never exhaust MAX_JOB_ATTEMPTS.
				await this.settle(job, {
					status: 'PENDING',
					attempts: { decrement: 1 },
					lastError: 'archive has not published this span yet',
				});
				return;
			}
			await this.chunkWriter.writeChunk(
				job.symbol,
				job.resolution,
				job.bucketStart.getTime(),
				result.candles
			);
			await this.settle(job, { status: 'DONE', lastError: null });
			this.logger.log(
				`ingested ${label}: ${result.candles.length} candles from ${result.files} file(s) in ${Date.now() - startedAt}ms`
			);
		} catch (err) {
			const message = err instanceof Error ? err.message : String(err);
			const terminal = err instanceof TerminalJobError || job.attempts >= MAX_JOB_ATTEMPTS;
			await this.settle(job, { status: terminal ? 'FAILED' : 'PENDING', lastError: message });
			this.logger[terminal ? 'error' : 'warn'](
				`${terminal ? 'failed' : 'will retry'} ${label} (attempt ${job.attempts}): ${message}`
			);
		}
	}

	/** Persist a job outcome; a lost row (janitor/manual delete) is logged, never thrown. */
	private async settle(
		job: CandleIngestJob,
		data: {
			status: 'PENDING' | 'DONE' | 'FAILED';
			lastError: string | null;
			attempts?: { decrement: number };
		}
	): Promise<void> {
		try {
			await this.prisma.candleIngestJob.updateMany({
				where: { symbol: job.symbol, resolution: job.resolution, bucketStart: job.bucketStart },
				data,
			});
		} catch (err) {
			this.logger.error(
				`could not settle job ${job.symbol}/${job.resolution}: ${err instanceof Error ? err.message : String(err)}`
			);
		}
	}

	/** Fetch + decode + (for 1h/1d) fold one job's span. */
	private async ingest(job: CandleIngestJob): Promise<IngestResult | typeof NOT_PUBLISHED> {
		const instrument = INGEST_INSTRUMENT_BY_SYMBOL[job.symbol];
		if (!instrument) throw new TerminalJobError(`unknown replay symbol '${job.symbol}'`);
		const targetSeconds = BASE_RESOLUTION_SECONDS[job.resolution];
		if (!targetSeconds) {
			throw new TerminalJobError(
				`'${job.resolution}' is not a base resolution — only 1m/1h/1d chunks are persisted`
			);
		}
		const bucketMs = job.bucketStart.getTime();

		if (job.resolution === '1m') {
			const candles = await this.archive.fetchDayMinuteCandles(
				instrument.s3Folder,
				bucketMs,
				instrument.decimalFactor
			);
			if (candles === null) {
				if (Date.now() - (bucketMs + MS_PER_DAY) < PUBLISH_SAFETY_MARGIN_MS) return NOT_PUBLISHED;
				// Settled day with no file = market closed (Saturday/holiday) → a permanent empty
				// chunk, mirroring the main app's closed-day behavior, so it is never re-enqueued.
				return { candles: [], files: 0 };
			}
			return { candles, files: 1 };
		}

		// 1h (month span) / 1d (year span): fold the span's per-day m1 files. The span must be
		// fully published — enqueue only happens for finalized buckets, but a just-ended month
		// hits the publish lag, and folding a span with its tail missing would persist a
		// permanently short chunk.
		const spanEndMs =
			job.resolution === '1h' ? nextMonthStartMs(bucketMs) : nextYearStartMs(bucketMs);
		if (Date.now() - spanEndMs < PUBLISH_SAFETY_MARGIN_MS) return NOT_PUBLISHED;

		const days = dayStartsIn(bucketMs, spanEndMs);
		const perDay = await mapWithConcurrency(days, DAY_FETCH_CONCURRENCY, (dayMs) =>
			this.archive.fetchDayMinuteCandles(instrument.s3Folder, dayMs, instrument.decimalFactor)
		);
		// Missing days inside a settled span are closed market days, not gaps — skip them.
		const minutes: Candle[] = [];
		for (const day of perDay) if (day) minutes.push(...day);
		return { candles: aggregateCandles(minutes, targetSeconds), files: days.length };
	}
}

/** Order-preserving concurrent map (worker-pool style, no dependency). */
async function mapWithConcurrency<T, R>(
	items: T[],
	limit: number,
	fn: (item: T) => Promise<R>
): Promise<R[]> {
	const out = new Array<R>(items.length);
	let next = 0;
	const lanes = Array.from({ length: Math.min(limit, items.length) }, async () => {
		for (;;) {
			const i = next++;
			if (i >= items.length) return;
			out[i] = await fn(items[i]);
		}
	});
	await Promise.all(lanes);
	return out;
}
