// Node-based load generator using autocannon — an alternative to k6 that needs
// no system install (npm install && npm run load).
//
//   TARGET=http://localhost:8080 CONNECTIONS=200 PIPELINING=10 DURATION=20 npm run load
import autocannon from 'autocannon';

const TARGET = process.env.TARGET || 'http://localhost:8080';

const instance = autocannon(
  {
    url: `${TARGET}/api/v1/telemetry`,
    method: 'POST',
    connections: Number(process.env.CONNECTIONS || 200),
    pipelining: Number(process.env.PIPELINING || 10),
    duration: Number(process.env.DURATION || 20),
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({
      event_id: 'tx_loadtest',
      metric_signature: 'v8_stable',
      data_points: [1.05, 99.4, 40.2],
    }),
  },
  (err, result) => {
    if (err) {
      console.error('load test failed:', err);
      process.exit(1);
    }
    console.log(
      `\nRequests/sec: avg ${result.requests.average} | p99 latency ${result.latency.p99} ms | errors ${result.errors}`,
    );
  },
);

autocannon.track(instance, { renderProgressBar: true });
