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

    // CSV → rows
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

    // JSON/CSV tolerant parser (supports {records:[...]}, {data:[...]}, etc.)
    function tryParse(jsonOrCsv){
      try {
        const obj = JSON.parse(jsonOrCsv);
        if (Array.isArray(obj)) return { kind:'json', rows: obj };
        if (Array.isArray(obj?.records)) return { kind:'json', rows: obj.records };
        for (const k of ['data','rows','items','result']){
          if (Array.isArray(obj?.[k])) return { kind:'json', rows: obj[k] };
        }
        return { kind:'json', rows: [] };
      } catch(e){ /* not json */ }
      return { kind:'csv', rows: parseCSV(jsonOrCsv) };
    }

    function detectKeys(rows){
      if(!rows.length) return { timeKey:null, numericKeys:[] };
      const sample = rows[0] || {};
      const keys = Object.keys(sample);
      // guess time key
      const timeCandidates = ['time','period','date','TIME','TIME_PERIOD','year'];
      const timeKey = keys.find(k=> timeCandidates.includes(k)) || keys.find(k=>/time|period|date|year/i.test(k));
      // robust numeric detection (commas & spaces)
      const toNum = (v)=>{
        if (v==null) return NaN;
        if (typeof v === 'number') return v;
        if (typeof v !== 'string') return Number(v);
        const s = v.replace(/\s+/g,'').replace(',', '.');   // "1 234,56" → "1234.56"
        const n = Number(s);
        return Number.isFinite(n) ? n : NaN;
      };
      const numericKeys = keys.filter(k=> rows.some(r=> Number.isFinite(toNum(r[k])) ));
      return { timeKey, numericKeys: numericKeys.filter(k=>k!==timeKey) };
    }

    // Parse times like YYYY, YYYY-MM, YYYY-Qn, Qn-YYYY, YYYYMn, YYYYMM
    function normalize(rows, timeKey, seriesKey){
      const toDate = (v)=>{
        const s = String(v ?? '').trim();
        if (/^\d{4}-\d{2}$/.test(s)) return new Date(`${s}-01T00:00:00Z`);
        if (/^\d{4}$/.test(s)) return new Date(`${s}-01-01T00:00:00Z`);
        let m = s.match(/^(\d{4})-?Q([1-4])$/) || s.match(/^Q([1-4])-(\d{4})$/);
        if (m){ const year = m[2]?m[2]:m[1]; const q = m[2]?m[1]:m[2]; const month = (Number(q)-1)*3 + 1; return new Date(`${year}-${String(month).padStart(2,'0')}-01T00:00:00Z`); }
        m = s.match(/^(\d{4})M(\d{1,2})$/);
        if (m) return new Date(`${m[1]}-${String(m[2]).padStart(2,'0')}-01T00:00:00Z`);
        m = s.match(/^(\d{4})(\d{2})$/);
        if (m) return new Date(`${m[1]}-${m[2]}-01T00:00:00Z`);
        const d = new Date(s);
        return isNaN(d) ? null : d;
      };
      const toNum = (v)=>{
        if (v==null) return NaN;
        if (typeof v === 'number') return v;
        if (typeof v !== 'string') return Number(v);
        const s = v.replace(/\s+/g,'').replace(',', '.');
        const n = Number(s);
        return Number.isFinite(n) ? n : NaN;
      };
      const pts = rows.map(r=>({ x: toDate(r[timeKey]), y: toNum(r[seriesKey]), raw:r }))
                      .filter(p=> p.x instanceof Date && !isNaN(p.x) && Number.isFinite(p.y))
                      .sort((a,b)=> a.x - b.x);
      return pts;
    }

    function renderChart(points, label){
      const ctx = document.getElementById('chart');
      const data = { datasets: [{ label, data: points.map(p=>({x:p.x, y:p.y})), tension:.2 }] };
      const options = {
        responsive:true, parsing:false,
        scales:{ x:{ type:'time', time:{ unit:'month' } }, y:{ beginAtZero:true, suggestedMin:0 } },
        plugins:{ legend:{ display:true }, tooltip:{ callbacks:{ label:(c)=> `${c.parsed.y}` } } }
      };
      if(!window._chartTimeLoaded){
        const s = document.createElement('script');
        s.src = 'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3';
        document.head.appendChild(s);
        window._chartTimeLoaded = true;
      }
      if (CHART) CHART.destroy();
      CHART = new Chart(ctx, { type:'line', data, options });
    }

    function renderTable(points){
      const fmt = new Intl.DateTimeFormat('en-CA',{year:'numeric',month:'2-digit',day:'2-digit'});
      let html = '<table><thead><tr><th>period</th><th>value</th></tr></thead><tbody>';
      html += points.map(p=> `<tr><td>${fmt.format(p.x).slice(0,7)}</td><td>${p.y}</td></tr>`).join('');
      html += '</tbody></table>';
      tableWrap.innerHTML = html;
    }

    async function load(url){
      setMessage('Laden…');
      try{
        const raw = await fetchText(url);
        const parsed = tryParse(raw);
        const rows = parsed.rows;
        if(!Array.isArray(rows) || rows.length===0) throw new Error('Lege of ongeldige dataset.');

        const { timeKey, numericKeys } = detectKeys(rows);
        if (!numericKeys.length) throw new Error('Geen numerieke kolommen gevonden.');

        // Dropdown (voorkeur voor value_pct_gdp / value / OBS_VALUE)
        const preferred = ['value_pct_gdp','value','OBS_VALUE'];
        const defaultKey = numericKeys.find(k=> preferred.includes(k)) || numericKeys[0];
        seriesKeySelect.innerHTML = numericKeys.map(k=>`<option value="${k}" ${k===defaultKey?'selected':''}>${k}</option>`).join('');
        const chosen = seriesKeySelect.value || defaultKey;

        const pts = normalize(rows, timeKey, chosen);
        if (!pts.length) throw new Error('Kon geen tijdreeks afleiden uit de data.');

        renderChart(pts, chosen);
        renderTable(pts);
        meta.innerHTML = `Bron: <code>${url}</code><br>Records: <b>${rows.length}</b> · Tijdskey: <code>${timeKey||'n/a'}</code> · Waardekey: <code>${chosen}</code>`;
        setMessage('');
      }catch(err){
        console.error(err);
        setMessage('Fout: '+ err.message, 'error');
      }
    }

    // UI
    document.getElementById('loadBtn').addEventListener('click', ()=> load(dataUrlInput.value || DEFAULT_DATA));
    seriesKeySelect.addEventListener('change', ()=> document.getElementById('loadBtn').click());

    // Boot
    dataUrlInput.value = DEFAULT_DATA;
    load(DEFAULT_DATA);