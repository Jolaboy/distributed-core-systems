import Fastify from 'fastify';
import { Kafka, logLevel } from 'kafkajs';
import { createTelemetryClient, ingest } from './grpc-client.js';

const PORT = Number(process.env.PORT ?? 3000);
const HOST = process.env.HOST ?? '0.0.0.0';
const KAFKA_BOOTSTRAP = process.env.KAFKA_BOOTSTRAP ?? '';
const KAFKA_TOPIC = process.env.KAFKA_TOPIC ?? 'telemetry.frames';
const KAFKA_GROUP = process.env.KAFKA_GROUP ?? 'node-consumer';
const GRPC_ADDR = process.env.GRPC_ADDR ?? '';

// In-memory rollups exposed via the HTTP status endpoint.
const stats = {
  startedAt: new Date().toISOString(),
  kafkaConnected: false,
  messagesConsumed: 0,
  elementsProcessed: 0,
  lastEventId: null,
};

const app = Fastify({ logger: true });

app.get('/healthz', async () => ({ status: 'ok' }));
app.get('/stats', async () => stats);

// Demonstrates the Node.js -> Rust gRPC path: forwards a frame and returns the Ack.
app.post('/forward', async (request, reply) => {
  if (!GRPC_ADDR) {
    return reply.code(503).send({ error: 'GRPC_ADDR not configured' });
  }
  const frame = {
    event_id: request.body?.event_id ?? `node_${Date.now()}`,
    metric_signature: request.body?.metric_signature ?? 'node_forward',
    data_points: request.body?.data_points ?? [1, 2, 3],
  };
  const client = createTelemetryClient(GRPC_ADDR);
  try {
    const ack = await ingest(client, frame);
    return { forwarded: frame.event_id, ack };
  } catch (err) {
    request.log.error({ err }, 'gRPC forward failed');
    return reply.code(502).send({ error: String(err) });
  } finally {
    client.close();
  }
});

async function startKafkaConsumer() {
  if (!KAFKA_BOOTSTRAP) {
    app.log.warn('KAFKA_BOOTSTRAP not set; Kafka consumer disabled');
    return;
  }

  const kafka = new Kafka({
    clientId: 'node-consumer',
    brokers: KAFKA_BOOTSTRAP.split(',').map((b) => b.trim()).filter(Boolean),
    logLevel: logLevel.NOTHING,
    retry: { retries: 8 },
  });

  const consumer = kafka.consumer({ groupId: KAFKA_GROUP });

  try {
    await consumer.connect();
    await consumer.subscribe({ topic: KAFKA_TOPIC, fromBeginning: false });
    stats.kafkaConnected = true;
    app.log.info(`subscribed to kafka topic "${KAFKA_TOPIC}"`);

    await consumer.run({
      eachMessage: async ({ message }) => {
        stats.messagesConsumed += 1;
        try {
          const frame = JSON.parse(message.value?.toString() ?? '{}');
          stats.elementsProcessed += Array.isArray(frame.data_points)
            ? frame.data_points.length
            : 0;
          stats.lastEventId = frame.event_id ?? stats.lastEventId;
        } catch {
          // Ignore malformed payloads; keep the consumer healthy.
        }
      },
    });
  } catch (err) {
    stats.kafkaConnected = false;
    app.log.error({ err }, 'kafka consumer error; continuing in degraded mode');
  }

  const shutdown = async () => {
    try {
      await consumer.disconnect();
    } catch {
      /* noop */
    }
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

async function main() {
  await startKafkaConsumer();
  await app.listen({ port: PORT, host: HOST });
  app.log.info(`node-consumer listening on http://${HOST}:${PORT}`);
}

main().catch((err) => {
  app.log.error({ err }, 'fatal startup error');
  process.exit(1);
});
