// ---- Defaults & wiring ----
const DEFAULT_DATA = new URLSearchParams(location.search).get('data') || './data/latest/teina230.json';
const Y_BASELINE = { beginAtZero: true, suggestedMin: 0 };

const el = (id) => document.getElementById(id);
const dataUrlInput = el('dataUrl');
const seriesKeySelect = el('seriesKey');
const msg = el('msg');
const meta = el('meta');
const tableWrap = el('tableWrap');
let CHART;

// ---- Helpers ----
function setMessage(text, kind='info'){
  if (!msg) return;
  msg.innerHTML = text ? `<div class="${kind==='error'?'error':'foot'}">${text}</div>` : '';
}
async function fetchText(url){
  const res = await fetch(url, { cache:'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
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

// JSON/CSV tolerant parser
function tryParse(jsonOrCsv){
  try {
    const obj = JSON.parse(jsonOrCsv);
    // Common shapes
    if (Array.isArray(obj)) return { rows: obj };
    if (Array.isArray(obj?.records)) return { rows: obj.records };
    for (const k of ['data','rows','items','result']){
      if (Array.isArray(obj?.[k])) return { rows: obj[k] };
    }
    return { rows: [] };
  } catch(e){ /* not JSON */ }
  return { rows: parseCSV(jsonOrCsv) };
}

// Robust number parsing (handles "1 234,56")
function toNum(v){
  if (v == null) return NaN;
  if (typeof v === 'number') return v;
  if (typeof v !== 'string') return Number(v);
  const s = v.replace(/\s+/g,'').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}

// Guess keys
function detectKeys(rows){
  if (!rows.length) return { timeKey:null, countryKey:null, numericKeys:[] };

  const sample = rows[0] || {};
  const keys = Object.keys(sample);

  const timeKey =
    keys.find(k=> ['time','TIME_PERIOD','period','date','year'].includes(k)) ||
    keys.find(k=> /time|period|date|year/i.test(k)) || null;

  const countryKey =
    keys.find(k=> ['country','country_name','name','geo_label','country_code','geo','GEO'].includes(k)) ||
    keys.find(k=> /country|geo/i.test(k)) || null;

  const numericKeys = keys.filter(k => rows.some(r => Number.isFinite(toNum(r[k])) ));
  const filteredNumeric = numericKeys.filter(k => k !== timeKey && k !== countryKey);

  return { timeKey, countryKey, numericKeys: filteredNumeric };
}

// Time parsing
function parseTime(v){
  if (v==null) return null;
  const s = String(v).trim();
  let m;
  // YYYY-Qn or Qn-YYYY
  m = s.match(/^(\d{4})-?Q([1-4])$/) || s.match(/^Q([1-4])-(\d{4})$/);
  if (m){
    const year = m[2]?m[2]:m[1];
    const q = m[2]?m[1]:m[2];
    const month = (Number(q)-1)*3 + 1;
    return new Date(`${year}-${String(month).padStart(2,'0')}-01T00:00:00Z`);
  }
  // YYYY-MM
  m = s.match(/^(\d{4})-(\d{1,2})$/);
  if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  // YYYYMn
  m = s.match(/^(\d{4})M(\d{1,2})$/);
  if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  // YYYYMM
  m = s.match(/^(\d{4})(\d{2})$/);
  if (m) return new Date(`${m[1]}-${m[2]}-01T00:00:00Z`);
  // YYYY
  m = s.match(/^(\d{4})$/);
  if (m) return new Date(`${m[1]}-01-01T00:00:00Z`);

  const d = new Date(s);
  return isNaN(d) ? null : d;
}

// Label helpers
function labelForRow(r){
  return r.country ?? r.country_name ?? r.name ?? r.geo_label ?? r.country_code ?? r.geo ?? '?';
}
const PRETTY_NAME = {
  value_pct_gdp: 'General government gross debt (% of GDP)',
  value: 'Value',
  OBS_VALUE: 'Observed value'
};
function prettyMetricName(key){
  return PRETTY_NAME[key] || String(key).replace(/_/g,' ').replace(/\b\w/g, c=>c.toUpperCase());
}

// Build series
function buildTimeSeries(rows, timeKey, valueKey, countryKey){
  const filtered = countryKey ? rows.filter(r => r[countryKey] != null) : rows;
  const pts = filtered.map(r => ({ x: parseTime(r[timeKey]), y: toNum(r[valueKey]) }))
    .filter(p => p.x instanceof Date && !isNaN(p.x) && Number.isFinite(p.y))
    .sort((a,b) => a.x - b.x);
  return pts;
}
function buildBarRows(rows, countryKey, valueKey){
  return rows.map(r => ({ label: labelForRow(r), y: toNum(r[valueKey]) }))
    .filter(p => Number.isFinite(p.y) && p.label)
    .sort((a,b) => b.y - a.y);
}

// Chart renderers
async function ensureTimeAdapter(){
  if (window._chartTimeLoaded) return;
  await new Promise((resolve)=>{
    const s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3';
    s.onload = resolve;
    document.head.appendChild(s);
  });
  window._chartTimeLoaded = true;
}
function renderLine(points, valueKey){
  const ctx = document.getElementById('chart');
  const data = { datasets: [{ label: prettyMetricName(valueKey), data: points.map(p=>({x:p.x, y:p.y})), tension:.2 }] };
  const options = {
    responsive: true, parsing: false,
    scales: { x: { type:'time', time:{ unit:'month' } }, y: { ...Y_BASELINE } },
    plugins: { legend:{ display:false }, title:{ display:true, text: prettyMetricName(valueKey) } }
  };
  if (CHART) CHART.destroy();
  CHART = new Chart(ctx, { type:'line', data, options });
}
function renderBar(items, valueKey, periodText){
  const ctx = document.getElementById('chart');
  const data = { labels: items.map(i=>i.label), datasets: [{ label: prettyMetricName(valueKey), data: items.map(i=>i.y) }] };
  const options = {
    responsive: true,
    scales: { x:{ ticks:{ autoSkip:false, maxRotation:60, minRotation:0 } }, y:{ ...Y_BASELINE } },
    plugins: { legend:{ display:false }, title:{ display:true, text: `${prettyMetricName(valueKey)} — ${periodText||''}`.trim() } }
  };
  if (CHART) CHART.destroy();
  CHART = new Chart(ctx, { type:'bar', data, options });
}

// Tables
function renderTableFromPoints(points){
  const fmt = new Intl.DateTimeFormat('en-CA',{year:'numeric',month:'2-digit',day:'2-digit'});
  let html = '<table><thead><tr><th>periode</th><th>waarde</th></tr></thead><tbody>';
  html += points.map(p=> `<tr><td>${fmt.format(p.x).slice(0,7)}</td><td>${p.y}</td></tr>`).join('');
  html += '</tbody></table>';
  tableWrap.innerHTML = html;
}
function renderTableFromBars(items){
  let html = '<table><thead><tr><th>land</th><th>waarde</th></tr></thead><tbody>';
  html += items.map(i=> `<tr><td>${i.label}</td><td>${i.y}</td></tr>`).join('');
  html += '</tbody></table>';
  tableWrap.innerHTML = html;
}

// Shape summary
function unique(values){ return [...new Set(values)]; }
function summarizeShape(rows, timeKey){
  const times = unique(rows.map(r => timeKey ? String(r[timeKey]) : '').filter(Boolean));
  return { times, singlePeriod: times.length === 1, period: times[0] || null };
}

// ---- Main loader ----
async function load(url){
  setMessage('Laden…');
  try{
    const raw = await fetchText(url);
    const { rows } = tryParse(raw);
    if (!Array.isArray(rows) || rows.length === 0) throw new Error('Lege of ongeldige dataset.');

    const { timeKey, countryKey, numericKeys } = detectKeys(rows);
    if (!numericKeys.length) throw new Error('Geen numerieke kolommen gevonden.');

    // Dropdown (voorkeur voor veelgebruikte sleutels)
    const preferred = ['value_pct_gdp','value','OBS_VALUE'];
    const defaultKey = numericKeys.find(k => preferred.includes(k)) || numericKeys[0];
    seriesKeySelect.innerHTML = numericKeys.map(k => `<option value="${k}" ${k===defaultKey?'selected':''}>${k}</option>`).join('');
    const valueKey = seriesKeySelect.value || defaultKey;

    // Bepaal of het 1 periode (cross-section) of meerdere (timeseries) is
    const { times, singlePeriod, period } = summarizeShape(rows, timeKey);

    if (timeKey && singlePeriod && countryKey){
      // BAR: landenranglijst voor die periode
      const bars = buildBarRows(rows, countryKey, valueKey);
      if (!bars.length) throw new Error('Geen waarden om te tonen (bar).');
      renderBar(bars, valueKey, period);
      renderTableFromBars(bars);
      meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Periode: <b>${period}</b> · Waarde: <code>${valueKey}</code>`;
      setMessage('');
      return;
    }

    // Anders: LINE — tijdreeks
    if (timeKey){
      const pts = buildTimeSeries(rows, timeKey, valueKey, countryKey);
      if (!pts.length) throw new Error('Kon geen tijdreeks afleiden uit de data.');
      await ensureTimeAdapter();
      renderLine(pts, valueKey);
      renderTableFromPoints(pts);
      meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Waarde: <code>${valueKey}</code>`;
      setMessage('');
      return;
    }

    throw new Error('Geen tijdsleutel gevonden.');
  }catch(err){
    console.error(err);
    setMessage('Fout: ' + err.message, 'error');
  }
}

// ---- Wire & boot ----
document.addEventListener('DOMContentLoaded', ()=>{
  if (dataUrlInput) dataUrlInput.value = DEFAULT_DATA;
  document.getElementById('loadBtn')?.addEventListener('click', ()=> load(dataUrlInput.value || DEFAULT_DATA));
  seriesKeySelect?.addEventListener('change', ()=> document.getElementById('loadBtn').click());
  load(DEFAULT_DATA);
});
