<!-- FILE: site/app.js (v3) -->
// ---- Config ----
const DEFAULT_DATA = new URLSearchParams(location.search).get('data') || './data/latest/teina230.json';

const el = (id) => document.getElementById(id);
const dataUrlInput = el('dataUrl');
const seriesKeySelect = el('seriesKey');
const msg = el('msg');
const meta = el('meta');
const tableWrap = el('tableWrap');
let CHART;

function setMessage(text, kind='info'){
  msg.innerHTML = text ? `<div class="${kind==='error'?'error':'foot'}">${text}</div>` : '';
}
async function fetchText(url){
  const res = await fetch(url, {cache:'no-store'});
  if(!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.text();
}

// CSV → array of objects
function parseCSV(text){
  const lines = text.trim().split(/\r?\n/);
  if (!lines.length) return [];
  const headers = lines.shift().split(',').map(h=>h.trim());
  return lines.filter(Boolean).map(line=>{
    const cols = line.split(',');
    const obj = {};
    headers.forEach((h,i)=> obj[h] = cols[i]!==undefined ? cols[i].trim() : '');
    return obj;
  });
}

function tryParse(text){
  try { return { kind:'json', json: JSON.parse(text) }; }
  catch(e){ return { kind:'csv', rows: parseCSV(text) }; }
}

// Eurostat SDMX flatten (very light)
function sdmxToRows(sdmx){
  if (!sdmx || !sdmx.structure || !Array.isArray(sdmx.dataSets)) return null;
  const ds = sdmx.dataSets[0] || {};
  const dimsObs = sdmx.structure?.dimensions?.observation || [];
  const dimsTime = sdmx.structure?.dimensions?.time || [];
  const timeDim = dimsObs.find(d=>/time/i.test(d.id)) || dimsTime[0];
  const timeLabels = timeDim?.values || [];
  const toTime = (idx)=>{
    const v = timeLabels[idx];
    return v?.name ?? v?.label ?? v ?? idx;
  };
  const out = [];
  if (ds.observations) {
    Object.entries(ds.observations).forEach(([key, arr])=>{
      const parts = key.split(':');
      const tIdx = Number(parts[parts.length-1]);
      const time = toTime(tIdx);
      const value = Array.isArray(arr) ? arr[0] : arr;
      if (time!==undefined && value!=null) out.push({ time, value:Number(value) });
    });
  } else if (ds.series) {
    Object.values(ds.series).forEach(s => {
      const obs = s?.observations || {};
      Object.entries(obs).forEach(([idx, arr])=>{
        const time = toTime(Number(idx));
        const value = Array.isArray(arr) ? arr[0] : arr;
        if (time!==undefined && value!=null) out.push({ time, value:Number(value) });
      });
    });
  }
  return out.length ? out : null;
}

function objectJsonToRows(obj){
  if (Array.isArray(obj)) return obj;
  if (Array.isArray(obj?.records)) return obj.records;
  for (const k of ['data','rows','items','result']) {
    if (Array.isArray(obj?.[k])) return obj[k];
  }
  const sdmx = sdmxToRows(obj);
  if (sdmx) return sdmx;
  return null;
}

function detectKeys(rows){
  if(!rows.length) return { timeKey:null, countryKey:null, numericKeys:[] };
  const sample = rows[0];
  const keys = Object.keys(sample);
  const timeKey = keys.find(k=> ['time','TIME_PERIOD','date','period','year'].includes(k)) || keys.find(k=>/time|period|date|year/i.test(k));
  const countryKey = keys.find(k=> ['country','geo','country_code','GEO'].includes(k)) || keys.find(k=>/country|geo/i.test(k));
  const numericKeys = keys.filter(k=> rows.some(r=> !isNaN(parseFloat(String(r[k]).replace(',','.'))) ));
  // Prefer common value names
  const preferred = ['value_pct_gdp','value','OBS_VALUE'];
  const preferredExisting = preferred.filter(k => numericKeys.includes(k));
  return { timeKey, countryKey, numericKeys: numericKeys.filter(k=>k!==timeKey && k!==countryKey), preferredExisting };
}

// Parse many time formats
function parseTime(v){
  if (v==null) return null; const s = String(v).trim(); let m;
  m = s.match(/^(\d{4})-(\d{1,2})$/); if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  m = s.match(/^(\d{4})[-\/.](\d{1,2})(?:[-\/.](\d{1,2}))?$/); if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-${String(m[3]||'01').padStart(2,'0')}T00:00:00Z`);
  m = s.match(/^(\d{4})$/); if (m) return new Date(`${m[1]}-01-01T00:00:00Z`);
  m = s.match(/^(\d{4})-?Q([1-4])$/) || s.match(/^Q([1-4])-(\d{4})$/); if (m) { const year = m[2]?m[2]:m[1]; const q = m[2]?m[1]:m[2]; const month = (Number(q)-1)*3 + 1; return new Date(`${year}-${String(month).padStart(2,'0')}-01T00:00:00Z`); }
  m = s.match(/^(\d{4})M(\d{1,2})$/); if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  m = s.match(/^(\d{4})(\d{2})$/); if (m) return new Date(`${m[1]}-${m[2]}-01T00:00:00Z`);
  const d = new Date(s); return isNaN(d) ? null : d;
}

function toNum(v){ if (typeof v === 'string') return Number(v.replace(',','.')); return Number(v); }

function summarizeShape(rows, timeKey, countryKey){
  const times = new Set(); const geos = new Set();
  rows.forEach(r=>{ if(timeKey && r[timeKey]!=null) times.add(String(r[timeKey])); if(countryKey && r[countryKey]!=null) geos.add(String(r[countryKey])); });
  return { uniqueTimes: [...times], uniqueGeos: [...geos] };
}

function buildBarData(rows, countryKey, valueKey){
  // Sort desc by value; label by country or code
  const items = rows.map(r=>({ label: r[countryKey] ?? r.country_code ?? r.geo ?? '?', y: toNum(r[valueKey]) }))
                   .filter(p=> !isNaN(p.y));
  items.sort((a,b)=> b.y - a.y);
  return items;
}

function buildTimeSeries(rows, timeKey, valueKey, countryKey){
  // Choose a representative geo (prefer EU27_2020, EA20, EA19), else first
  let chosen = null;
  const prio = ['EU27_2020','EA20','EA19','EU28','EU27'];
  if (countryKey) {
    const all = new Set(rows.map(r=> String(r[countryKey])));
    chosen = prio.find(x=> all.has(x)) || [...all][0];
  }
  const filtered = countryKey ? rows.filter(r=> String(r[countryKey]) === chosen) : rows;
  const pts = filtered.map(r=> ({ x: parseTime(r[timeKey]), y: toNum(r[valueKey]) }))
                      .filter(p=> p.x instanceof Date && !isNaN(p.x) && !isNaN(p.y))
                      .sort((a,b)=> a.x - b.x);
  return { pts, chosenGeo: chosen };
}

async function ensureTimeAdapter(){
  if (window._chartTimeLoaded) return; await new Promise((resolve)=>{ const s = document.createElement('script'); s.src = 'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3'; s.onload=resolve; document.head.appendChild(s); }); window._chartTimeLoaded = true;
}

function renderBarChart(items, label){
  const ctx = document.getElementById('chart');
  const data = { labels: items.map(i=>i.label), datasets: [{ label, data: items.map(i=>i.y) }] };
  const options = { responsive:true, scales:{ x:{ ticks:{ autoSkip:false, maxRotation:60, minRotation:0 } }, y:{ beginAtZero:false } }, plugins:{ legend:{ display:true } } };
  if (CHART) CHART.destroy(); CHART = new Chart(ctx, { type:'bar', data, options });
}

function renderLineChart(points, label){
  const ctx = document.getElementById('chart');
  const data = { datasets: [{ label, data: points.map(p=>({x:p.x, y:p.y})), tension:.2 }] };
  const options = { responsive:true, parsing:false, scales:{ x:{ type:'time', time:{ unit:'month' } }, y:{ beginAtZero:false } }, plugins:{ legend:{ display:true } } };
  if (CHART) CHART.destroy(); CHART = new Chart(ctx, { type:'line', data, options });
}

function renderTableFromBars(items){
  let html = '<table><thead><tr><th>country</th><th>value</th></tr></thead><tbody>';
  html += items.map(i=> `<tr><td>${i.label}</td><td>${i.y}</td></tr>`).join('');
  html += '</tbody></table>'; tableWrap.innerHTML = html;
}
function renderTableFromPoints(points){
  const fmt = new Intl.DateTimeFormat('en-CA',{year:'numeric',month:'2-digit',day:'2-digit'});
  let html = '<table><thead><tr><th>period</th><th>value</th></tr></thead><tbody>';
  html += points.map(p=> `<tr><td>${fmt.format(p.x).slice(0,7)}</td><td>${p.y}</td></tr>`).join('');
  html += '</tbody></table>'; tableWrap.innerHTML = html;
}

async function load(url){
  setMessage('Laden…');
  try{
    const raw = await fetchText(url);
    const parsed = tryParse(raw);
    const rows = parsed.kind==='csv' ? parsed.rows : objectJsonToRows(parsed.json);
    if(!Array.isArray(rows) || rows.length===0) throw new Error('Lege of ongeldige dataset.');

    const { timeKey, countryKey, numericKeys, preferredExisting } = detectKeys(rows);
    if (!numericKeys.length) throw new Error('Geen numerieke kolommen gevonden.');
    const defaultValueKey = preferredExisting[0] || numericKeys[0];

    // UI: populate value dropdown (once per load)
    seriesKeySelect.innerHTML = numericKeys.map(k=>`<option value="${k}" ${k===defaultValueKey?'selected':''}>${k}</option>`).join('');
    const valueKey = seriesKeySelect.value || defaultValueKey;

    const { uniqueTimes, uniqueGeos } = summarizeShape(rows, timeKey, countryKey);

    // Decide mode
    if (timeKey && uniqueTimes.length === 1 && countryKey) {
      // Single period, multiple countries → BAR
      const bars = buildBarData(rows, countryKey, valueKey);
      renderBarChart(bars, `${uniqueTimes[0]} · ${valueKey}`);
      renderTableFromBars(bars);
      meta.innerHTML = `Bron: <code>${url}</code><br>Records: <b>${rows.length}</b> · Periode: <b>${uniqueTimes[0]||'n/a'}</b> · Waarde: <code>${valueKey}</code>`;
      setMessage('');
      return;
    }

    // Otherwise: Time series (choose a geo if multiple)
    if (timeKey) {
      const { pts, chosenGeo } = buildTimeSeries(rows, timeKey, valueKey, countryKey);
      if (pts.length) {
        await ensureTimeAdapter();
        renderLineChart(pts, `${chosenGeo||'all'} · ${valueKey}`);
        renderTableFromPoints(pts);
        meta.innerHTML = `Bron: <code>${url}</code><br>Records: <b>${rows.length}</b> · Geo: <b>${chosenGeo||'n/a'}</b> · Waarde: <code>${valueKey}</code>`;
        setMessage('');
        return;
      }
    }

    throw new Error('Kon geen tijdreeks of landenranglijst renderen (controleer kolomnamen).');
  }catch(err){ console.error(err); setMessage('Fout: '+ err.message, 'error'); }
}

// Wire
document.getElementById('loadBtn').addEventListener('click', ()=> load(dataUrlInput.value || DEFAULT_DATA));
seriesKeySelect.addEventListener('change', ()=> document.getElementById('loadBtn').click());

// Boot
dataUrlInput.value = DEFAULT_DATA; load(DEFAULT_DATA);
