// 純壓力測試：100 個 VU 各自扮演一台 device，連續送 1 分鐘。
// 用法：k6 run loadtest/k6.js
//      k6 run -e BASE_URL=http://localhost:8090 -e SLEEP=1 loadtest/k6.js   （SLEEP=1 模擬每秒 1 筆）
import http from 'k6/http';
import { check, sleep } from 'k6';

const BASE_URL = __ENV.BASE_URL || 'http://localhost:8090';
const SLEEP = Number(__ENV.SLEEP || 0);

export const options = {
  vus: 100,
  duration: '1m',
  thresholds: {
    http_req_failed: ['rate==0'], // 驗收條件：無錯誤
    checks: ['rate==1'],
  },
};

function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    return (c === 'x' ? r : (r & 0x3) | 0x8).toString(16);
  });
}

export default function () {
  const deviceId = `device-${String(__VU).padStart(3, '0')}`; // __VU 為 1..100

  const payload = JSON.stringify({
    uni_id: uuidv4(),
    device_id: deviceId,
    seq: __ITER + 1,
    record_time: new Date().toISOString(),
    freq: 1000 + (__ITER % 100),
  });

  const res = http.post(`${BASE_URL}/v1/readings`, payload, {
    headers: { 'Content-Type': 'application/json' },
  });

  check(res, { 'status is 202': (r) => r.status === 202 });

  if (SLEEP > 0) sleep(SLEEP);
}
