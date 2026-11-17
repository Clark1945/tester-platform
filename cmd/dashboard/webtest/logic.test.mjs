// 監控頁面判讀邏輯的單元測試。
// 執行：node --test cmd/dashboard/webtest/
import test from 'node:test';
import assert from 'node:assert/strict';
import { createRequire } from 'node:module';

const M = createRequire(import.meta.url)('../web/logic.js');
const base = M.DEMOS.normal.s;
const assess = (patch = {}) => M.assess({ ...base, ...patch });

test('堵塞的門檻', () => {
  const c = (ready, growth = 0) => M.congestion({ ready, growth });
  assert.equal(c(0), 'ok');
  assert.equal(c(99), 'ok');
  assert.equal(c(100), 'warn', '100 筆開始算積壓');
  assert.equal(c(999), 'warn');
  assert.equal(c(1000), 'bad', '1000 筆算堵塞');
  assert.equal(c(150, 50), 'bad', '筆數不多但持續增加，也算堵塞');
  assert.equal(c(150, 5), 'warn', '增加速度剛好在門檻上不算');
  assert.equal(c(50, 500), 'ok', '還沒到 100 筆，成長再快也不算');
  assert.equal(c(null), 'off', '讀不到 queue 深度時是未知，不是正常');
});

test('queue 裡的點數：對數刻度，有資料就至少一顆，不超過上限', () => {
  assert.equal(M.pileCount(0), 0);
  assert.equal(M.pileCount(null), 0);
  assert.equal(M.pileCount(1), 4);
  assert.ok(M.pileCount(100) < M.pileCount(1000));
  assert.ok(M.pileCount(1000) < M.pileCount(48708));
  assert.equal(M.pileCount(50000), M.PILE_SLOTS);
  assert.equal(M.pileCount(9e9), M.PILE_SLOTS, '再多也不會超過格數');
  for (let n = 1; n < 60000; n += 997) assert.ok(M.pileCount(n) >= 1 && M.pileCount(n) <= M.PILE_SLOTS);
});

test('資料點的密度：沒有流量就沒有點，流量越大越密但有上限', () => {
  assert.equal(M.particleRate(0), 0);
  assert.equal(M.particleRate(0.01), 0);
  assert.ok(M.particleRate(1) > 0);
  assert.ok(M.particleRate(100) > M.particleRate(10));
  assert.ok(M.particleRate(1e9) <= 7);
});

test('正常與待命', () => {
  const a = assess();
  assert.equal(a.level, 'ok');
  assert.equal(a.title, '資料正常流動');
  assert.match(a.detail, /100 台設備每秒送出 100 筆/);
  assert.equal(a.focus, null);
  assert.equal(a.pile, 0);
  for (const e of Object.values(a.edges).slice(0, 4)) {
    assert.equal(e.state, 'ok');
    assert.ok(e.particles > 0, '四段主線都要有資料點在流動');
  }
  assert.equal(a.edges.rcvDlq.particles, 0, '沒有訊息進 DLQ 時那一段不該有點');

  const idle = M.assess(M.DEMOS.idle.s);
  assert.equal(idle.level, 'ok');
  assert.equal(idle.title, '系統待命中');
  assert.equal(idle.edges.devApi.particles, 0);
});

test('堵塞：上游照常流動，出口標示消化不及，焦點在 queue', () => {
  const a = M.assess(M.DEMOS.jam.s);
  assert.equal(a.level, 'bad');
  assert.equal(a.title, 'Queue 堵塞');
  assert.match(a.detail, /48,708 筆在等待，而且還在增加/);
  assert.equal(a.focus, 'mq');
  assert.equal(a.nodes.mq.state, 'bad');
  assert.equal(a.edges.devApi.state, 'ok');
  assert.equal(a.edges.apiMq.state, 'ok');
  assert.equal(a.edges.mqRcv.note, '消化不及');
  assert.ok(a.edges.mqRcv.particles > 0, '出口仍然有資料流出，只是比較慢');
  assert.ok(a.pile > 50, '接近滿槽');
});

test('積壓是注意，不是異常', () => {
  const a = M.assess(M.DEMOS.backlog.s);
  assert.equal(a.level, 'warn');
  assert.equal(a.title, 'Queue 開始積壓');
  assert.equal(a.nodes.mq.state, 'warn');
});

test('資料庫停機：只有最後一段斷掉，API 那一段不受影響', () => {
  const a = M.assess(M.DEMOS.db.s);
  assert.equal(a.level, 'bad');
  assert.equal(a.title, '資料庫連不上', '根本原因優先於它造成的堵塞');
  assert.match(a.detail, /Queue 堵塞/, '連帶的問題列在說明裡');
  assert.equal(a.focus, 'db');
  assert.equal(a.nodes.db.state, 'bad');
  assert.equal(a.edges.rcvDb.state, 'bad');
  assert.equal(a.edges.rcvDb.particles, 0, '斷掉的那一段不能有資料點');
  assert.equal(a.edges.devApi.state, 'ok');
  assert.ok(a.edges.apiMq.particles > 0);
});

test('Receiver 停機：資料庫狀態是未知，不是正常也不是異常', () => {
  const a = M.assess(M.DEMOS.receiver.s);
  assert.equal(a.title, 'Receiver 停機');
  assert.equal(a.nodes.rcv.state, 'bad');
  assert.equal(a.nodes.db.state, 'off');
  assert.equal(a.edges.mqRcv.note, '沒人取走');
  assert.equal(a.edges.mqRcv.particles, 0);
  assert.equal(a.edges.rcvDb.state, 'off');
});

test('Broker 斷線', () => {
  const a = M.assess(M.DEMOS.broker.s);
  assert.equal(a.level, 'bad');
  assert.equal(a.title, 'RabbitMQ 沒有回應');
  assert.equal(a.nodes.mq.state, 'bad');
  assert.equal(a.edges.apiMq.state, 'bad');
  assert.equal(a.edges.apiMq.particles, 0);
  assert.equal(a.edges.devApi.note, '部分被退回');
  assert.equal(a.pile, 0, 'broker 沒回應時不知道 queue 裡有多少，不畫點');
});

test('API 停機', () => {
  const a = assess({ apiUp: false, brokerOk: null, inRate: 0, devActive: null });
  assert.equal(a.title, 'API 沒有回應');
  assert.equal(a.nodes.api.state, 'bad');
  assert.equal(a.edges.devApi.note, '送不進去');
  assert.equal(a.nodes.dev.value, '–');
});

test('DLQ 有訊息時要提醒，但其他部分照常', () => {
  const a = M.assess(M.DEMOS.dlq.s);
  assert.equal(a.level, 'warn');
  assert.equal(a.title, '有訊息進了 DLQ');
  assert.equal(a.focus, 'dlq');
  assert.equal(a.nodes.dlq.state, 'warn');
  assert.equal(a.nodes.mq.state, 'ok');
});

test('多個問題同時發生：異常優先於注意，並全部列出', () => {
  const a = assess({ dlq: 3, rcvUp: false, dbOk: null, ready: 5000, growth: 100 });
  assert.equal(a.level, 'bad');
  assert.equal(a.title, 'Receiver 停機');
  assert.match(a.detail, /Queue 堵塞/);
  assert.match(a.detail, /有訊息進了 DLQ/);
});

test('監控資料中斷：全部是未知，沒有任何資料點在動', () => {
  const a = M.offline('22:15:05');
  assert.equal(a.level, 'off');
  assert.match(a.detail, /22:15:05/);
  assert.deepEqual(Object.keys(a.nodes), ['dev', 'api', 'mq', 'rcv', 'db', 'dlq']);
  for (const n of Object.values(a.nodes)) assert.equal(n.state, 'off');
  for (const e of Object.values(a.edges)) assert.equal(e.particles, 0);
  assert.equal(a.pile, 0);
  assert.doesNotMatch(M.offline().detail, /undefined|null/);
});

test('每一種預覽狀態都產生完整的畫面資料', () => {
  for (const [key, demo] of Object.entries(M.DEMOS)) {
    const a = M.assess(demo.s);
    assert.ok(['ok', 'warn', 'bad'].includes(a.level), key);
    assert.ok(a.title && a.detail, key);
    assert.deepEqual(Object.keys(a.nodes), ['dev', 'api', 'mq', 'rcv', 'db', 'dlq'], key);
    assert.deepEqual(Object.keys(a.edges), ['devApi', 'apiMq', 'mqRcv', 'rcvDb', 'rcvDlq'], key);
    for (const n of Object.values(a.nodes)) {
      assert.ok(['ok', 'warn', 'bad', 'off'].includes(n.state), key);
      assert.ok(typeof n.value === 'string' && !/undefined|NaN/.test(n.value), `${key}: ${n.value}`);
    }
    assert.ok(!/undefined|NaN/.test(a.title + a.detail), `${key}: ${a.detail}`);
  }
});
