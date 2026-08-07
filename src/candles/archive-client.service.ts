import { GetObjectCommand, NoSuchKey, S3Client } from '@aws-sdk/client-s3';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

import { decodeCandleFile } from './bi5';
import { dayMinuteCandlesKey } from './buckets';
import { DUKASCOPY_S3_DEFAULT_BUCKET, DUKASCOPY_S3_DEFAULT_REGION } from './constants';

import type { Env } from '../config';
import type { Candle } from './types';

/**
 * Read access to Dukascopy's public price archive (requester-pays S3 bucket — every GET is
 * billed to OUR AWS account, which is why fetch results must always land in a permanent
 * CandleChunk row: pay for a file once, ever). Credentials come from the standard AWS env
 * vars via the SDK's default provider chain; the worker stays idle when they're absent.
 */
@Injectable()
export class ArchiveClientService {
	private readonly client: S3Client;
	private readonly bucket: string;

	constructor(config: ConfigService<Env, true>) {
		this.bucket = config.get('DUKASCOPY_S3_BUCKET', { infer: true }) ?? DUKASCOPY_S3_DEFAULT_BUCKET;
		this.client = new S3Client({
			region: config.get('DUKASCOPY_S3_REGION', { infer: true }) ?? DUKASCOPY_S3_DEFAULT_REGION,
		});
	}

	/**
	 * One UTC day of BID m1 candles for an instrument, decoded and flat-filtered.
	 * `null` = the archive has no file for that day — either a closed market day or a day whose
	 * file is not published yet; the CALLER must tell those apart (PUBLISH_SAFETY_MARGIN_MS).
	 */
	async fetchDayMinuteCandles(
		s3Folder: string,
		dayStartMs: number,
		decimalFactor: number
	): Promise<Candle[] | null> {
		let body: Uint8Array;
		try {
			const res = await this.client.send(
				new GetObjectCommand({
					Bucket: this.bucket,
					Key: dayMinuteCandlesKey(s3Folder, dayStartMs),
					RequestPayer: 'requester',
				})
			);
			if (!res.Body) return null;
			body = await res.Body.transformToByteArray();
		} catch (err) {
			if (err instanceof NoSuchKey) return null;
			throw err;
		}
		return decodeCandleFile(Buffer.from(body), dayStartMs, decimalFactor);
	}
}
