<!-- FILE: site/app.js -->
const ctx = document.getElementById('chart');
const data = {
datasets: [{ label, data: points.map(p=>({x:p.x, y:p.y})), tension:.2 }]
};
const options = {
responsive:true,
parsing:false,
scales:{ x:{ type:'time', time:{ unit:'month' } }, y:{ beginAtZero:false } },
plugins:{ legend:{ display:true }, tooltip:{ callbacks:{ label:(c)=> `${c.parsed.y}` } } }
};
// Lazy-load time adapter for Chart.js (date-fns)
if(!window._chartTimeLoaded){
const s = document.createElement('script');
s.src = 'https://cdn.jsdelivr.net/npm/chartjs-adapter-date-fns@3';
document.head.appendChild(s);
window._chartTimeLoaded = true;
}
if(CHART){ CHART.destroy(); }
CHART = new Chart(ctx, { type:'line', data, options });
}


function renderTable(points, timeFmt='yyyy-MM'){
const fmt = new Intl.DateTimeFormat('en-CA', {year:'numeric', month:'2-digit', day:'2-digit'});
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


// populate series dropdown once
seriesKeySelect.innerHTML = numericKeys.map(k=>`<option value="${k}">${k}</option>`).join('');
const chosen = seriesKeySelect.value || numericKeys[0];


const pts = normalize(rows, timeKey, chosen);
if(!pts.length) throw new Error('Kon geen tijdreeks afleiden uit de data.');


renderChart(pts, chosen);
renderTable(pts);
meta.innerHTML = `Bron: <code>${url}</code><br>Records: <b>${rows.length}</b> · Tijdskey: <code>${timeKey}</code> · Waardekey: <code>${chosen}</code>`;
setMessage('');
}catch(err){
console.error(err);
setMessage('Fout: '+ err.message, 'error');
}
}


// Wire UI
document.getElementById('loadBtn').addEventListener('click', ()=> load(dataUrlInput.value || DEFAULT_DATA));
seriesKeySelect.addEventListener('change', ()=> document.getElementById('loadBtn').click());


// Boot
dataUrlInput.value = DEFAULT_DATA;
load(DEFAULT_DATA);