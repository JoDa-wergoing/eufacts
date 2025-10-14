// ==== Defaults ====
const DEFAULT_DATA = new URLSearchParams(location.search).get('data') || './data/latest/teina230.json';
const Y_BASELINE = { beginAtZero: true, suggestedMin: 0 };

// ==== Elements ====
const el = (id) => document.getElementById(id);
const dataUrlInput = el('dataUrl');
const seriesKeySelect = el('seriesKey');
const msg = el('msg');
const meta = el('meta');
const tableWrap = el('tableWrap');
let CHART;

// ==== Utils ====
function setMessage(text, kind='info'){
  if (!msg) return;
  msg.innerHTML = text ? `<div class="${kind==='error'?'error':'foot'}">${text}</div>` : '';
}
async function fetchText(url){
  const res = await fetch(url, { cache:'no-store' });
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
  return await res.text();
}
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
function toNum(v){
  if (v == null) return NaN;
  if (typeof v === 'number') return v;
  if (typeof v !== 'string') return Number(v);
  const s = v.replace(/\s+/g,'').replace(',', '.');
  const n = Number(s);
  return Number.isFinite(n) ? n : NaN;
}

// Time parsing: YYYY-Qn, Qn-YYYY, YYYY-MM, YYYYMn, YYYYMM, YYYY
function parseTime(v){
  if (v==null) return null;
  const s = String(v).trim();
  let m;
  m = s.match(/^(\d{4})-?Q([1-4])$/) || s.match(/^Q([1-4])-(\d{4})$/);
  if (m){ const year = m[2]?m[2]:m[1]; const q = m[2]?m[1]:m[2]; const month = (Number(q)-1)*3 + 1; return new Date(`${year}-${String(month).padStart(2,'0')}-01T00:00:00Z`); }
  m = s.match(/^(\d{4})-(\d{1,2})$/); if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  m = s.match(/^(\d{4})M(\d{1,2})$/); if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
  m = s.match(/^(\d{4})(\d{2})$/);    if (m) return new Date(`${m[1]}-${m[2]}-01T00:00:00Z`);
  m = s.match(/^(\d{4})$/);           if (m) return new Date(`${m[1]}-01-01T00:00:00Z`);
  const d = new Date(s);
  return isNaN(d) ? null : d;
}

// ==== JSON -> rows (flatten) ====
// Supports:
// A) {records:[{time, value_*, country*}, ...]}          (cross-section)
// B) {records:[{country*, series:[{time, value_*},..]}]} (timeseries per land)
// C) arrays or {data|rows|items|result: [...]}
function objectJsonToRows(obj){
  if (!obj) return null;

  // C) array / common containers
  if (Array.isArray(obj)) return obj;
  for (const k of ['data','rows','items','result']){
    if (Array.isArray(obj?.[k])) return obj[k];
  }

  if (Array.isArray(obj.records)){
    const first = obj.records[0] || {};

    // B) nested series per land
    if (Array.isArray(first.series)){
      const out = [];
      for (const rec of obj.records){
        const base = {
          country: rec.country ?? rec.name ?? rec.geo_label ?? rec.country_name ?? undefined,
          country_code: rec.country_code ?? rec.geo ?? undefined,
          unit: rec.unit ?? obj.unit
        };
        for (const pt of rec.series || []){
          const row = { ...base, time: pt.time };

          // 1) kopieer alle keys van series punt
          for (const k of Object.keys(pt)){
            if (k !== 'time') row[k] = pt[k];
          }
          // 2) zorg dat we altijd een generieke 'value' hebben
          if (row.value === undefined){
            if (pt.value_pct_gdp !== undefined) row.value = pt.value_pct_gdp;
            else if (pt.OBS_VALUE !== undefined) row.value = pt.OBS_VALUE;
            else if (pt.value !== undefined) row.value = pt.value;
            else {
              // vind 1 bruikbare numerieke key als fallback
              const numKey = Object.keys(row).find(k => k !== 'time' && Number.isFinite(toNum(row[k])));
              if (numKey) row.value = row[numKey];
            }
          }
          out.push(row);
        }
      }
      return out;
    }

    // A) vlakke records
    return obj.records.map(r=>{
      const o = { ...r };
      if (o.value === undefined){
        if (o.value_pct_gdp !== undefined) o.value = o.value_pct_gdp;
        else if (o.OBS_VALUE !== undefined) o.value = o.OBS_VALUE;
      }
      return o;
    });
  }

  return null;
}

// Parser (JSON/CSV) → altijd {rows: [...]}
function tryParse(jsonOrCsv){
  try{
    const obj = JSON.parse(jsonOrCsv);
    const rows = objectJsonToRows(obj);
    return { rows: Array.isArray(rows) ? rows : [] };
  }catch(e){
    return { rows: parseCSV(jsonOrCsv) };
  }
}

// ==== Key detection over alle rijen ====
function detectKeys(rows){
  if (!rows.length) return { timeKey:null, countryKey:null, numericKeys:[] };

  // verzamel alle keys die voorkomen
  const keySet = new Set();
  for (const r of rows){ Object.keys(r || {}).forEach(k => keySet.add(k)); }
  const keys = [...keySet];

  const timeKey =
    keys.find(k=> ['time','TIME_PERIOD','period','date','year'].includes(k)) ||
    keys.find(k=> /time|period|date|year/i.test(k)) || null;

  const countryKey =
    keys.find(k=> ['country','country_name','name','geo_label','country_code','geo','GEO'].includes(k)) ||
    keys.find(k=> /country|geo/i.test(k)) || null;

  // numerieke keys (minstens 1 bruikbaar datapunt in de hele set)
  const numericKeys = keys.filter(k => k !== timeKey && k !== countryKey && rows.some(r => Number.isFinite(toNum(r?.[k])) ));

  return { timeKey, countryKey, numericKeys };
}

function unique(arr){ return [...new Set(arr)]; }
function summarizeShape(rows, timeKey){
  const times = unique(rows.map(r => timeKey ? String(r[timeKey]) : '').filter(Boolean));
  return { times, singlePeriod: times.length === 1, period: times[0] || null };
}
function labelForRow(r){
  return r.country ?? r.country_name ?? r.name ?? r.geo_label ?? r.country_code ?? r.geo ?? '?';
}
function prettyMetricName(key){
  const map = { value_pct_gdp: 'General government gross debt (% of GDP)', value: 'Value', OBS_VALUE: 'Observed value' };
  return map[key] || String(key).replace(/_/g,' ').replace(/\b\w/g, c=>c.toUpperCase());
}

// Eén geo kiezen (voorkeur EU27/EA) als er meerdere landen zijn
function pickGeo(rows, countryKey){
  if (!countryKey) return null;
  const geos = unique(rows.map(r => r[countryKey]).filter(Boolean).map(String));
  const prio = ['EU27_2020','EA20','EA19','EU28','EU27'];
  return prio.find(p => geos.includes(p)) || geos[0] || null;
}

// ==== Build datasets ====
function buildTimeSeries(rows, timeKey, valueKey, countryKey){
  let useRows = rows;
  if (countryKey){
    const chosen = pickGeo(rows, countryKey);
    if (chosen) useRows = rows.filter(r => String(r[countryKey]) === chosen);
  }
  const pts = useRows
    .map(r => ({ x: parseTime(r[timeKey]), y: toNum(r[valueKey]) }))
    .filter(p => p.x instanceof Date && !isNaN(p.x) && Number.isFinite(p.y))
    .sort((a,b) => a.x - b.x);
  return pts;
}
function buildBarRows(rows, countryKey, valueKey){
  return rows.map(r => ({ label: labelForRow(r), y: toNum(r[valueKey]) }))
    .filter(p => Number.isFinite(p.y) && p.label)
    .sort((a,b) => b.y - a.y);
}

// ==== Charts ====
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

// ==== Tables ====
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

// ==== Main loader ====
async function load(url){
  setMessage('Laden…');
  try{
    const raw = await fetchText(url);
    const { rows } = tryParse(raw);
    if (!Array.isArray(rows) || !rows.length) throw new Error('Lege of ongeldige dataset.');

    const { timeKey, countryKey, numericKeys } = detectKeys(rows);
    if (!timeKey) throw new Error('Geen tijdsleutel gevonden.');
    if (!numericKeys.length) throw new Error('Geen numerieke kolommen gevonden.');

    // kies default metric (voorkeur value_pct_gdp) of enige numerieke key
    const preferred = ['value_pct_gdp','value','OBS_VALUE'];
    let defaultKey = numericKeys.find(k => preferred.includes(k)) || null;
    if (!defaultKey && numericKeys.length === 1) defaultKey = numericKeys[0];
    if (!defaultKey) defaultKey = numericKeys[0];

    seriesKeySelect.innerHTML = numericKeys.map(k => `<option value="${k}" ${k===defaultKey?'selected':''}>${k}</option>`).join('');
    const valueKey = seriesKeySelect.value || defaultKey;

    const { times, singlePeriod, period } = summarizeShape(rows, timeKey);

    if (singlePeriod && countryKey){
      const bars = buildBarRows(rows, countryKey, valueKey);
      if (!bars.length) throw new Error('Geen waarden om te tonen (bar).');
      renderBar(bars, valueKey, period);
      renderTableFromBars(bars);
      meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Periode: <b>${period}</b> · Waarde: <code>${valueKey}</code>`;
      setMessage('');
      return;
    }

    await ensureTimeAdapter();
    const pts = buildTimeSeries(rows, timeKey, valueKey, countryKey);
    if (!pts.length) throw new Error('Kon geen tijdreeks afleiden uit de data.');
    renderLine(pts, valueKey);
    renderTableFromPoints(pts);
    meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Waarde: <code>${valueKey}</code>`;
    setMessage('');
  }catch(err){
    console.error(err);
    setMessage('Fout: ' + err.message, 'error');
  }
}

// ==== Wire & boot ====
document.addEventListener('DOMContentLoaded', ()=>{
  if (dataUrlInput) dataUrlInput.value = DEFAULT_DATA;
  document.getElementById('loadBtn')?.addEventListener('click', ()=> load(dataUrlInput.value || DEFAULT_DATA));
  seriesKeySelect?.addEventListener('change', ()=> document.getElementById('loadBtn').click());
  load(DEFAULT_DATA);
});
