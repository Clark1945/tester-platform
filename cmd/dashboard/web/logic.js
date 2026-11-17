// 監控頁面的判讀邏輯：把一組量測值（snapshot）轉成「畫面該長什麼樣子」。
// 這裡沒有任何 DOM 操作，所以能用 node --test 直接測（見 cmd/dashboard/webtest）。
(function (root) {
  'use strict';

  // 堵塞的定義：進來的比消化的快，東西堆在 queue。
  const BACKLOG_WARN = 100;   // 等待中 ≥ 100 筆：積壓（約等於 1 秒的正常流量）
  const BACKLOG_BAD = 1000;   // ≥ 1000 筆：堵塞
  const GROWING = 5;          // 每秒增加超過 5 筆算「仍在增加」

  const PILE_SLOTS = 60;      // queue 裡最多畫幾顆點
  const PILE_FULL = 50000;    // 多少筆算滿槽（對數刻度）

  const num = (n, digits = 0) =>
    n == null || Number.isNaN(n) ? '–' : Number(n).toLocaleString('zh-TW', { maximumFractionDigits: digits });
  const perSec = (r) => num(r, r < 10 ? 1 : 0) + ' 筆/秒';

  function congestion(s) {
    if (s.ready == null) return 'off';
    if (s.ready >= BACKLOG_BAD || (s.ready >= BACKLOG_WARN && s.growth > GROWING)) return 'bad';
    if (s.ready >= BACKLOG_WARN) return 'warn';
    return 'ok';
  }

  // queue 裡要畫幾顆點。對數刻度：1 筆就看得到 1 顆，PILE_FULL 筆時畫滿。
  function pileCount(ready) {
    if (!ready || ready <= 0) return 0;
    const ratio = Math.log10(ready + 1) / Math.log10(PILE_FULL + 1);
    return Math.max(1, Math.min(PILE_SLOTS, Math.round(ratio * PILE_SLOTS)));
  }

  // 每秒在一段線上放出幾顆資料點。取對數，避免 10 與 3000 筆/秒看起來差太多。
  function particleRate(rate) {
    if (!(rate > 0.05)) return 0;
    return Math.min(7, 0.8 + Math.log10(rate + 1) * 1.7);
  }

  // assess 回傳畫面需要的一切：標題、說明、每個節點與每段線的狀態。
  // 狀態只有四種：ok（正常）、warn（注意）、bad（異常）、off（未知）。
  function assess(s) {
    const jam = congestion(s);
    const brokerDown = !s.mqUp || s.brokerOk === false;
    const problems = [];   // { level, title, text, focus }
    const add = (level, title, text, focus) => problems.push({ level, title, text, focus });

    if (!s.apiUp) add('bad', 'API 沒有回應', '設備送不進資料，會自動重送。', 'api');
    if (!s.mqUp) add('bad', 'RabbitMQ 沒有回應', 'API 收不了資料，設備會自動重送。', 'mq');
    else if (s.apiUp && s.brokerOk === false) add('bad', 'API 連不上 RabbitMQ', '設備的請求被退回，會自動重送。', 'api');
    if (!s.rcvUp) add('bad', 'Receiver 停機', '資料只進不出，先留在 Queue，恢復後會補寫。', 'rcv');
    if (s.dbOk === false) add('bad', '資料庫連不上', 'Receiver 持續重試，資料留在 Queue，恢復後會補寫。', 'db');

    if (jam === 'bad') {
      add('bad', 'Queue 堵塞',
        `${num(s.ready)} 筆在等待` + (s.growth > GROWING ? '，而且還在增加。' : '，正在消化。') + '資料不會遺失。', 'mq');
    } else if (jam === 'warn') {
      add('warn', 'Queue 開始積壓', `${num(s.ready)} 筆在等待，消化速度跟不上。`, 'mq');
    }
    if (s.apiUp && !brokerDown && s.failRate > 0) {
      add('warn', '部分請求未獲確認', `每秒 ${num(s.failRate, 1)} 筆被退回，設備會重送。`, 'api');
    }
    if (s.rcvUp && s.dbOk !== false && s.retryRate > 0) add('warn', '寫入失敗，重試中', 'Receiver 寫入資料庫時遇到暫時性錯誤。', 'rcv');
    if (s.dlq > 0) add('warn', '有訊息進了 DLQ', `${num(s.dlq)} 筆無法寫入，需要人工檢視。`, 'dlq');

    // 最嚴重的問題當標題；同級時先發現的優先（上面的順序就是由上游到下游）
    const worst = problems.find((p) => p.level === 'bad') || problems[0];
    let level = 'ok', title, detail, focus = null;
    if (worst) {
      level = worst.level;
      title = worst.title;
      focus = worst.focus;
      detail = [worst.text].concat(problems.filter((p) => p !== worst).map((p) => p.title + '。')).join(' ');
    } else if (s.inRate > 0.05) {
      title = '資料正常流動';
      detail = `${num(s.devActive)} 台設備每秒送出 ${num(s.inRate, s.inRate < 10 ? 1 : 0)} 筆，每秒寫入 ${num(s.writeRate, s.writeRate < 10 ? 1 : 0)} 筆。`;
    } else {
      title = '系統待命中';
      detail = '各服務正常，目前沒有設備在送資料。';
    }

    const arriving = (s.inRate || 0) + (s.failRate || 0) + (s.rejRate || 0);
    const settled = (s.writeRate || 0) + (s.dupRate || 0) + (s.dlqRate || 0);

    const nodes = {
      dev: { state: 'ok', value: s.devActive == null ? '–' : `${num(s.devActive)} 台在線` },
      api: !s.apiUp ? { state: 'bad', value: '沒有回應' }
        : s.brokerOk === false ? { state: 'warn', value: '無法發佈' }
        : { state: 'ok', value: s.inRate > 0.05 ? perSec(s.inRate) + ' 進入' : '待命' },
      mq: !s.mqUp ? { state: 'bad', value: '沒有回應' }
        : { state: jam, value: s.ready == null ? '–' : `${num(s.ready)} 筆等待中` },
      rcv: !s.rcvUp ? { state: 'bad', value: '停機' }
        : { state: s.retryRate > 0 ? 'warn' : 'ok', value: s.retryRate > 0 ? '重試中' : s.writeRate > 0.05 ? perSec(s.writeRate) + ' 寫入' : '待命' },
      db: s.dbOk == null ? { state: 'off', value: '狀態未知' }
        : s.dbOk ? { state: 'ok', value: '可寫入' } : { state: 'bad', value: '連不上' },
      dlq: { state: s.dlq > 0 ? 'warn' : 'ok', value: s.dlq == null ? '–' : `${num(s.dlq)} 筆` },
    };

    const edge = (rate, state, note) => ({ rate, state, note: note || '', particles: state === 'bad' ? 0 : particleRate(rate) });
    const edges = {
      devApi: !s.apiUp ? edge(0, 'bad', '送不進去')
        : s.failRate > 0 ? edge(arriving, 'warn', '部分被退回') : edge(arriving, 'ok'),
      apiMq: !s.apiUp ? edge(0, 'off')
        : brokerDown ? edge(0, 'bad', '連線中斷') : edge(s.inRate, 'ok'),
      mqRcv: !s.rcvUp ? edge(0, 'bad', '沒人取走')
        : !s.mqUp ? edge(0, 'off')
        : jam === 'bad' ? edge(settled, 'warn', '消化不及')
        : jam === 'warn' ? edge(settled, 'warn', '消化偏慢') : edge(settled, 'ok'),
      rcvDb: !s.rcvUp ? edge(0, 'off')
        : s.dbOk === false ? edge(0, 'bad', '寫不進去')
        : s.retryRate > 0 ? edge(s.writeRate, 'warn', '重試中') : edge(s.writeRate, 'ok'),
      rcvDlq: edge(s.rcvUp ? s.dlqRate : 0, s.dlqRate > 0 ? 'warn' : 'ok'),
    };

    return { level, title, detail, focus, jam, nodes, edges, pile: s.mqUp ? pileCount(s.ready) : 0 };
  }

  // offline：連監控資料都拿不到時的畫面。所有節點都是「未知」，
  // 不能沿用上一次的數字——那會讓畫面看起來一切正常。
  function offline(lastOkText) {
    const unknown = () => ({ state: 'off', value: '–' });
    const still = () => ({ rate: 0, state: 'off', note: '', particles: 0 });
    return {
      level: 'off',
      title: '無法取得監控資料',
      detail: (lastOkText ? `最後一次更新在 ${lastOkText}。` : '') + '在恢復之前，無法判斷系統的狀態。請確認 Prometheus 是否正常。',
      focus: null, jam: 'off', pile: 0,
      nodes: { dev: unknown(), api: unknown(), mq: unknown(), rcv: unknown(), db: unknown(), dlq: unknown() },
      edges: { devApi: still(), apiMq: still(), mqRcv: still(), rcvDb: still(), rcvDlq: still() },
    };
  }

  // ---- 預覽用的假資料：不必真的弄壞系統就能看每一種畫面 ----
  const healthy = {
    apiUp: true, rcvUp: true, mqUp: true, brokerOk: true, dbOk: true,
    inRate: 100, failRate: 0, rejRate: 0, writeRate: 100, dupRate: 0, dlqRate: 0, retryRate: 0,
    ready: 0, unacked: 2, dlq: 0, growth: 0,
    devTotal: 100, devActive: 100,
    errRej: 0, errPub: 0, errDlq: 0, errRetry: 0,
    apiP95: 0.004, rcvP95: 0.009, sinceWrite: 0.4,
  };
  const DEMOS = {
    normal:   { label: '正常', s: healthy },
    idle:     { label: '沒有流量', s: { ...healthy, inRate: 0, writeRate: 0, devActive: 0, unacked: 0, sinceWrite: 312 } },
    backlog:  { label: '積壓', s: { ...healthy, inRate: 420, writeRate: 400, ready: 380, unacked: 50, growth: 2 } },
    jam:      { label: '堵塞', s: { ...healthy, inRate: 3367, writeRate: 508, ready: 48708, unacked: 50, growth: 2850, rcvP95: 0.041 } },
    db:       { label: '資料庫停機', s: { ...healthy, dbOk: false, writeRate: 0, retryRate: 0.7, ready: 1942, unacked: 50, growth: 100, errRetry: 28, sinceWrite: 21 } },
    receiver: { label: 'Receiver 停機', s: { ...healthy, rcvUp: false, dbOk: null, writeRate: 0, ready: 1420, unacked: 0, growth: 100, sinceWrite: null, rcvP95: null } },
    broker:   { label: 'Broker 斷線', s: { ...healthy, mqUp: false, brokerOk: false, inRate: 0, failRate: 100, writeRate: 0, ready: null, unacked: null, dlq: null, errPub: 1240, sinceWrite: 14 } },
    dlq:      { label: '有訊息進 DLQ', s: { ...healthy, dlq: 7, dlqRate: 0, errDlq: 7 } },
  };

  const api = { congestion, pileCount, particleRate, assess, offline, num, perSec, DEMOS, PILE_SLOTS, BACKLOG_WARN, BACKLOG_BAD };
  root.Monitor = api;
  if (typeof module !== 'undefined' && module.exports) module.exports = api;
})(typeof globalThis !== 'undefined' ? globalThis : this);
