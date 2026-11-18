(() => {
  'use strict';
  const el = id => document.getElementById(`readings-${id}`);
  let rows = [], page = 1, loadedAt = null, busy = false, failed = false;
  const date = new Intl.DateTimeFormat('zh-TW', {year:'numeric',month:'2-digit',day:'2-digit',hour:'2-digit',minute:'2-digit',second:'2-digit',fractionalSecondDigits:3,hour12:false});
  function render() {
    const size = Number(el('size').value);
    const pages = Math.max(1, Math.ceil(rows.length / size));
    page = Math.max(1, Math.min(page,pages));
    const offset = (page-1)*size;
    const body = el('body');
    const elements = rows.slice(offset,offset+size).map(row => {
      const tr = document.createElement('tr');
      const t = new Date(row.record_time);
      const values = [row.device_id, row.device_name || '—', row.seq, Number.isNaN(t.getTime()) ? row.record_time : date.format(t), row.freq, row.uni_id];
      for (const value of values) { const td = document.createElement('td'); td.textContent = String(value); tr.append(td); }
      tr.children[3].title = row.record_time;
      return tr;
    });
    if (!elements.length) {
      const tr = document.createElement('tr'), td = document.createElement('td'); td.colSpan=6;
      td.textContent = busy ? '載入中…' : failed ? '無法讀取資料，請重新整理。' : '尚無已寫入的資料。'; tr.append(td); elements.push(tr);
    }
    body.replaceChildren(...elements);
    el('page').replaceChildren(...Array.from({length:pages},(_,i)=>new Option(String(i+1),String(i+1))));
    el('page').value = String(page);
    el('pages').textContent = `／ ${pages} 頁`;
    el('range').textContent = rows.length ? `第 ${offset+1}–${Math.min(offset+size, rows.length)} 筆，共 ${rows.length} 筆` : '共 0 筆';
    el('prev').disabled = busy || page <= 1;
    el('next').disabled = busy || page >= pages;
    el('page').disabled = busy || !rows.length;
    el('limit').disabled = el('refresh').disabled = busy;
    el('size').disabled = busy;
    el('status').classList.toggle('error',failed);
    if (busy) el('status').textContent = '正在讀取最新資料…';
    else if (failed) el('status').textContent = `讀取失敗${loadedAt ? '；目前保留的是上次取得的資料' : ''}，請按重新整理重試。`;
    else el('status').textContent = `已取得 ${rows.length} 筆，最多顯示最新 ${el('limit').value} 筆 · 更新於 ${date.format(loadedAt)}。不自動更新，避免翻頁時資料移動。`;
  }
  async function load() {
    if (busy) return;
    busy=true; failed=false; render();
    const controller = new AbortController();
    const timer = setTimeout(()=>controller.abort(),8000);
    try {
      const resp = await fetch(`/api/readings?order=desc&limit=${el('limit').value}`,{cache:'no-store',signal:controller.signal});
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (!Array.isArray(data.readings)) throw new Error('Invalid response');
      rows=data.readings; loadedAt=new Date(); page=1;
    } catch { failed=true; }
    finally {clearTimeout(timer);busy=false;render();}
  }
  el('refresh').addEventListener('click',load);
  el('limit').addEventListener('change',load);
  el('size').addEventListener('change',()=>{page=1;render();});
  el('page').addEventListener('change',()=>{page=Number(el('page').value);render();});
  el('prev').addEventListener('click',()=>{page--;render();});
  el('next').addEventListener('click',()=>{page++;render();});
  load();
})();
