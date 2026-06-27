import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';
import grpc from '@grpc/grpc-js';
import protoLoader from '@grpc/proto-loader';

const __dirname = dirname(fileURLToPath(import.meta.url));
const PROTO_PATH = join(__dirname, '..', 'proto', 'telemetry.proto');

const packageDef = protoLoader.loadSync(PROTO_PATH, {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
});

const proto = grpc.loadPackageDefinition(packageDef).telemetry.v1;

/**
 * Creates a gRPC client for the Rust TelemetryService.
 * @param {string} target host:port of the gRPC server (e.g. "rust-api:50051").
 */
export function createTelemetryClient(target) {
  return new proto.TelemetryService(target, grpc.credentials.createInsecure());
}

/**
 * Forwards a single telemetry frame over gRPC.
 * @param {grpc.Client} client
 * @param {object} frame { event_id, metric_signature, data_points }
 * @returns {Promise<object>} the Ack response.
 */
export function ingest(client, frame) {
  return new Promise((resolve, reject) => {
    client.Ingest(frame, (err, response) => {
      if (err) reject(err);
      else resolve(response);
    });
  });
}
