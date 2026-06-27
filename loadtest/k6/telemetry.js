import http from 'k6/http';
import { check } from 'k6';

// Load profile aimed at the 45k req/sec ceiling. Throughput is hardware- and
// network-dependent; tune `rate`, `preAllocatedVUs`, and `maxVUs` to your box.
//
//   k6 run loadtest/k6/telemetry.js
//
// Override the target:  k6 run -e TARGET=http://localhost:8080 loadtest/k6/telemetry.js
export const options = {
  scenarios: {
    telemetry_ingest: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 45000), // iterations per timeUnit
      timeUnit: '1s',
      duration: __ENV.DURATION || '30s',
      preAllocatedVUs: Number(__ENV.VUS || 500),
      maxVUs: Number(__ENV.MAX_VUS || 2000),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],   // <1% errors
    http_req_duration: ['p(95)<25'],  // 95th percentile under 25ms
  },
};

const TARGET = __ENV.TARGET || 'http://localhost:8080';
const URL = `${TARGET}/api/v1/telemetry`;
const PARAMS = { headers: { 'Content-Type': 'application/json' } };

export default function () {
  const payload = JSON.stringify({
    event_id: `tx_${__VU}_${__ITER}`,
    metric_signature: 'v8_stable',
    data_points: [1.05, 99.4, 40.2],
  });

  const res = http.post(URL, payload, PARAMS);
  check(res, {
    'status is 200': (r) => r.status === 200,
    'ack received': (r) => r.body && r.body.includes('ACK_RECEIVED_SUCCESS'),
  });
}
