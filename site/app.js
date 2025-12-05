// ==== Defaults ====
const DEFAULT_DATA = new URLSearchParams(location.search).get('data') || './data/latest/teina230-timeseries.json';
const Y_BASELINE = { beginAtZero: true, suggestedMin: 0 };

// ==== Elements ====
const el = (id) => document.getElementById(id);
const dataUrlInput = el('dataUrl');
const geoSelect = el('geoSelect');
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
// CSV → objects
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

// ======================================================
// ✅ FIXED parseTime() — correcte jaar/kwartaal parsing
// ======================================================
function parseTime(v){
  if (v == null) return null;
  const s = String(v).trim();
  let m;

  // YYYY-Qn or YYYYQn
  m = s.match(/^(\d{4})-?Q([1-4])$/);
  if (m) {
    const year = m[1];
    const q = m[2];
    const month = (Number(q) - 1) * 3 + 1;
    return new Date(`${year}-${String(month).padStart(2,'0')}-01T00:00:00Z`);
  }

  // Qn-YYYY
  m = s.match(/^Q([1-4])-(\d{4})$/);
  if (m) {
    const q = m[1];
    const year = m[2];
    const month = (Number(q) - 1) * 3 + 1;
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
// ======================================================
// Einde Bugfix
// ======================================================

function inferTimeUnit(points){
  return points.length && points.every(p => p.x instanceof Date && p.x.getUTCMonth() === 0)
    ? 'year' : 'month';
}

// ==== JSON → rows (flatten) ====
function objectJsonToRows(obj){
  if (!obj) return null;

  if (Array.isArray(obj)) return obj;
  for (const k of ['data','rows','items','result']){
    if (Array.isArray(obj?.[k])) return obj[k];
  }

  if (Array.isArray(obj.records)){
    const first = obj.records[0] || {};

    // B) geneste series per land
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
          for (const k of Object.keys(pt)){ if (k !== 'time') row[k] = pt[k]; }
          if (row.value === undefined){
            if (pt.value_pct_gdp !== undefined) row.value = pt.value_pct_gdp;
            else if (pt.OBS_VALUE !== undefined) row.value = pt.OBS_VALUE;
            else if (pt.value !== undefined) row.value = pt.value;
            else {
              const numKey = Object.keys(row).find(k =>
                k !== 'time' && Number.isFinite(toNum(row[k]))
              );
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

function tryParse(jsonOrCsv){
  try{
    const obj = JSON.parse(jsonOrCsv);
    const rows = objectJsonToRows(obj);
    return { rows: Array.isArray(rows) ? rows : [] };
  }catch(e){
    return { rows: parseCSV(jsonOrCsv) };
  }
}

// ==== Key detection ====
function detectKeys(rows){
  if (!rows.length) return { timeKey:null, countryKey:null, numericKeys:[], geos:[] };

  const keySet = new Set();
  for (const r of rows){ Object.keys(r || {}).forEach(k => keySet.add(k)); }
  const keys = [...keySet];

  const timeKey =
    keys.find(k=> ['time','TIME_PERIOD','period','date','year'].includes(k)) ||
    keys.find(k=> /time|period|date|year/i.test(k)) || null;

  const countryKey =
    keys.find(k=> ['country','country_name','name','geo_label','country_code','geo','GEO'].includes(k)) ||
    keys.find(k=> /country|geo/i.test(k)) || null;

  const numericKeys = keys.filter(k => k !== timeKey && k !== countryKey &&
    rows.some(r => Number.isFinite(toNum(r?.[k])) ));

  const geos = countryKey ? [...new Set(rows.map(r => r[countryKey]).filter(Boolean).map(String))] : [];

  return { timeKey, countryKey, numericKeys, geos };
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

// ==== Build datasets ====
function buildMultiSeries(rows, timeKey, valueKey, countryKey, selectedGeos){
  let useRows = rows;
  if (countryKey && selectedGeos && selectedGeos.length){
    const set = new Set(selectedGeos.map(String));
    useRows = rows.filter(r => set.has(String(r[countryKey])));
  }
  const byGeo = new Map();
  for (const r of useRows){
    const geo = countryKey ? String(r[countryKey]) : 'ALL';
    if (!byGeo.has(geo)) byGeo.set(geo, []);
    const x = parseTime(r[timeKey]);
    const y = toNum(r[valueKey]);
    if (x instanceof Date && !isNaN(x) && Number.isFinite(y)) byGeo.get(geo).push({x,y});
  }
  const datasets = [];
  for (const [geo, pts] of byGeo.entries()){
    if (!pts.length) continue;
    pts.sort((a,b)=> a.x - b.x);
    datasets.push({ label: geo, data: pts.map(p=>({x:p.x, y:p.y})), tension: .2 });
  }
  return datasets;
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

function renderLineMulti(datasets, timeUnit, valueKey){
  const ctx = document.getElementById('chart');

  if (!datasets || !datasets.length) {
    console.warn('renderLineMulti: geen datasets');
    return;
  }

  // Gebruik de eerste dataset als basis voor de X-as
  const first = datasets[0];
  if (!first.data || !first.data.length) {
    console.warn('renderLineMulti: eerste dataset heeft geen data');
    return;
  }

  // Maak nette labels van de Date → "YYYY-Qn"
  const labels = first.data.map(pt => {
    const d = pt.x;
    if (!(d instanceof Date) || isNaN(d)) return '';
    const year = d.getUTCFullYear();
    const month = d.getUTCMonth(); // 0=jan, 3=apr, 6=jul, 9=okt
    const quarter = Math.floor(month / 3) + 1;
    return `${year}-Q${quarter}`;
  });

  // Zet data om naar simpele number-arrays per land
  const simpleDatasets = datasets.map(ds => ({
    label: ds.label,
    data: ds.data.map(pt => pt.y),
    tension: ds.tension ?? 0.2,
    // Eventueel kun je hier borderColor/backgroundColor zetten,
    // maar Chart.js geeft zelf al prima default kleuren.
  }));

  const data = {
    labels,
    datasets: simpleDatasets
  };

  const options = {
    responsive: true,
    scales: {
      x: {
        type: 'category',
        ticks: {
          autoSkip: false,
          maxRotation: 0,
          minRotation: 0
        }
      },
      y: {
        ...Y_BASELINE   // beginAtZero: true, suggestedMin: 0
      }
    },
    plugins: {
      legend: { display: true },
      title: {
        display: true,
        text: prettyMetricName(valueKey)
      }
    }
  };

  if (CHART) CHART.destroy();
  CHART = new Chart(ctx, { type: 'line', data, options });
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
function renderTableFromDatasets(datasets){
  let rows = [];
  for (const ds of datasets){
    if (!ds.data.length) continue;
    const last = ds.data[ds.data.length-1];
    const ym = new Intl.DateTimeFormat('en-CA',{year:'numeric',month:'2-digit',day:'2-digit'}).format(last.x).slice(0,7);
    rows.push({ geo: ds.label, period: ym, value: last.y });
  }
  rows.sort((a,b)=> b.value - a.value);
  let html = '<table><thead><tr><th>geo</th><th>periode (laatste)</th><th>waarde</th></tr></thead><tbody>';
  html += rows.map(r=> `<tr><td>${r.geo}</td><td>${r.period}</td><td>${r.value}</td></tr>`).join('');
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

    const { timeKey, countryKey, numericKeys, geos } = detectKeys(rows);
    if (!timeKey) throw new Error('Geen tijdsleutel gevonden.');
    if (!numericKeys.length) throw new Error('Geen numerieke kolommen gevonden.');

    // Serie-keuze
    const preferred = ['value_pct_gdp','value','OBS_VALUE'];
    let defaultKey = numericKeys.find(k => preferred.includes(k)) || numericKeys[0];
    seriesKeySelect.innerHTML = numericKeys
      .map(k => `<option value="${k}" ${k===defaultKey?'selected':''}>${k}</option>`)
      .join('');
    const valueKey = seriesKeySelect.value || defaultKey;

    // Geo-keuze
    const prevSelection = new Set([...geoSelect.selectedOptions].map(o=>o.value));
    geoSelect.innerHTML = geos.map(g => `<option value="${g}" ${prevSelection.has(g)?'selected':''}>${g}</option>`).join('');

    const { times, singlePeriod, period } = summarizeShape(rows, timeKey);

    // ==== BAR ====
    if (singlePeriod && countryKey){
      const bars = buildBarRows(rows, countryKey, valueKey);
      if (!bars.length) throw new Error('Geen waarden om te tonen (bar).');
      renderBar(bars, valueKey, period);
      renderTableFromBars(bars);
      meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Periode: <b>${period}</b> · Kolom: <code>${valueKey}</code>`;
      setMessage('');
      return;
    }

    // ==== LINE ====
    await ensureTimeAdapter();

    const selectedGeos = [...geoSelect.selectedOptions].map(o=>o.value);
    let datasets = buildMultiSeries(rows, timeKey, valueKey, countryKey, selectedGeos);

    // fallback auto-select
    if (!datasets.length){
      const prio = ['EU27_2020','EA20','EA19'];
      const auto = geos.length ? (prio.filter(p=>geos.includes(p)).concat(geos)).slice(0,5) : [];
      geoSelect.innerHTML = auto.map(g => `<option value="${g}" selected>${g}</option>`).join('');
      datasets = buildMultiSeries(rows, timeKey, valueKey, countryKey, auto);
    }

    if (!datasets.length) throw new Error('Kon geen tijdreeks afleiden uit de data.');

    const allPoints = datasets.flatMap(ds => ds.data);
    const unit = inferTimeUnit(allPoints);
    renderLineMulti(datasets, unit, valueKey);
    renderTableFromDatasets(datasets);

    meta.innerHTML = `Bron: <code>${url}</code> · Records: <b>${rows.length}</b> · Kolom: <code>${valueKey}</code> · Reeksen: <b>${datasets.length}</b>`;
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
  geoSelect?.addEventListener('change', ()=> document.getElementById('loadBtn').click());
  load(DEFAULT_DATA);
});
