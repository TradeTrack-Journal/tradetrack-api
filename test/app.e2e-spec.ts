import { INestApplication } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
import request from 'supertest';
import { App } from 'supertest/types';

import { AppModule } from './../src/app.module';

// AppModule validates env at boot, so DATABASE_URL must be set for this suite to run.
describe('HealthController (e2e)', () => {
	let app: INestApplication<App>;

	beforeAll(async () => {
		const moduleFixture: TestingModule = await Test.createTestingModule({
			imports: [AppModule],
		}).compile();

		app = moduleFixture.createNestApplication();
		await app.init();
	});

	afterAll(async () => {
		await app.close();
	});

	it('/health (GET) returns ok', () => {
		return request(app.getHttpServer())
			.get('/health')
			.expect(200)
			.expect((res) => {
				const body = res.body as { status?: string };
				if (body.status !== 'ok') {
					throw new Error(`unexpected body: ${JSON.stringify(body)}`);
				}
			});
	});
});
