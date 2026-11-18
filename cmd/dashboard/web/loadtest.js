'use strict';
const $ = id => document.getElementById(id);
const states = {idle:'尚未開始',running:'測試中',draining:'等待最後回應',stopping:'停止中',stopped:'已停止',completed:'已完成'};
const busy = r => ['running','draining','stopping'].includes(r.state);
let latest, pending = false, revision = 0, connectionFailed = false;
const num = n => Number(n || 0).toLocaleString('zh-TW');
function estimate() {
  $('estimate').textContent = `預計排程 ${num($('rps').value * $('duration').value)} 筆；每台平均 ${Number($('rps').value / $('devices').value).toFixed(2)} 筆 / 秒。`;
}
function render(r) {
  latest = r;
  const active = busy(r);
  if (active) {
    $('rps').value = r.config.rps;
    $('devices').value = r.config.devices;
    $('duration').value = r.config.duration_seconds;
    estimate();
  }
  $('state').textContent = states[r.state] || r.state;
  $('start').disabled = active || pending;
  $('stop').disabled = !active || r.state === 'stopping' || pending;
  for (const id of ['rps','devices','duration']) $(id).disabled = active || pending;
  $('download').disabled = !r.id || active;
  $('progress').value = r.config?.duration_seconds ? Math.min(100, r.elapsed_seconds / r.config.duration_seconds * 100) : 0;
  $('run-info').textContent = r.id ? `${r.config.rps} RPS · ${r.config.devices} 台 · ${r.config.duration_seconds} 秒｜已經過 ${r.elapsed_seconds.toFixed(1)} 秒｜${new Date(r.started_at).toLocaleString('zh-TW')}` : '設定參數後即可開始。';
  $('actual').textContent = Number(r.actual_rps || 0).toFixed(1);
  $('sent').textContent = `${num(r.sent)} / ${num(r.completed)}`;
  $('accepted').textContent = num(r.accepted);
  $('http-errors').textContent = `${num(r.client_errors)} / ${num(r.server_errors)}`;
  $('network').textContent = `${num(r.network_errors)} / ${num(r.canceled)}`;
  $('skipped').textContent = num(r.skipped);
  $('avg').textContent = `${Number(r.average_latency_ms || 0).toFixed(1)} ms`;
  $('max').textContent = `${Number(r.max_latency_ms || 0).toFixed(1)} ms`;
  $('other').textContent = `其他 HTTP 回應：${num(r.other_responses)}。停止時取消的請求可能已被 API 接收。`;
}
async function request(path, options) {
  const response = await fetch(path, {cache:'no-store', ...options});
  const data = await response.json();
  if (!response.ok) throw new Error(data.error || `HTTP ${response.status}`);
  return data;
}
async function poll() {
  const observedRevision = revision;
  try {
    if (!pending) {
      const result = await request('/api/loadtest');
      if (!pending && observedRevision === revision) {
        render(result);
        if (connectionFailed) $('message').textContent = '';
        connectionFailed = false;
      }
    }
  } catch (e) {
    if (pending || observedRevision !== revision) return;
    connectionFailed = true;
    $('state').textContent = '無法取得狀態';
    $('message').textContent = `${e.message}；目前數字為上次取得的資料。`;
    $('start').disabled = true;
  } finally { setTimeout(poll, 1000); }
}
async function mutate(path, body) {
  revision++;
  pending = true;
  $('start').disabled = $('stop').disabled = true;
  try {
    const r = await request(path, {method:'POST', headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
    pending = false; render(r); $('message').textContent = '';
  } catch (e) {
    pending = false;
    if (latest) render(latest);
    $('message').textContent = e.message;
  }
}
$('load-form').addEventListener('submit', e => {
  e.preventDefault();
  mutate('/api/loadtest', {rps:Number($('rps').value),devices:Number($('devices').value),duration_seconds:Number($('duration').value)});
});
$('stop').addEventListener('click', () => mutate('/api/loadtest/stop', {}));
for (const id of ['rps','devices','duration']) $(id).addEventListener('input', estimate);
$('download').addEventListener('click', () => {
  if (!latest || busy(latest)) return;
  const url = URL.createObjectURL(new Blob([JSON.stringify(latest,null,2)],{type:'application/json'}));
  const a = document.createElement('a'); a.href=url; a.download=`loadtest-${latest.id}.json`; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
});
estimate(); poll();
