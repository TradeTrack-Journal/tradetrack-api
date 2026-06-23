import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';

import { AppModule } from './app.module';

async function bootstrap() {
	const app = await NestFactory.create(AppModule);

	// Let Nest lifecycle hooks (e.g. PrismaService.onModuleDestroy) run on SIGTERM from Fly.
	app.enableShutdownHooks();

	const port = process.env.PORT ?? 3001;
	await app.listen(port, '0.0.0.0');

	new Logger('Bootstrap').log(`tradetrack-api listening on http://0.0.0.0:${port}`);
}

void bootstrap();
