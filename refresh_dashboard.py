"""
Humidity Ingress Dashboard — Data Refresh
==========================================
Queries Athena and regenerates dashboard.html with the latest data baked in.
No server needed — just open dashboard.html in any browser after running this.

Usage:
    python refresh_dashboard.py

Requirements:
    pip install pyathena pandas boto3 openpyxl
"""

# ── CONFIG — edit these ───────────────────────────────────────────────────────
S3_STAGING_DIR   = "s3://aws-athena-query-results-687061330321-ap-southeast-2/"
AWS_REGION       = "ap-southeast-2"
ATHENA_WORKGROUP = "general_research_workgroup"
GROUP_RESAMPLE_MIN  = 5    # group view: average window in minutes
SERIAL_RESAMPLE_SEC_LOCAL  = 30   # for local dashboard.html (open directly in Chrome)
SERIAL_RESAMPLE_SEC_SHARE  = 300  # for index.html pushed to GitHub Pages (keeps file <20MB)

REP_START_DATES = {
    "REP-501": "2026-06-10",
    "REP-502": "2026-06-03",
    "REP-503": "2026-06-03",
    "REP-510": "2026-06-09",
    "REP-511": "2026-06-09",
}

SILENT_SERIAL_SUFFIXES = {"18536", "18542", "10207", "10516"}
# ─────────────────────────────────────────────────────────────────────────────

import json, re, time, sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

HERE         = Path(__file__).parent
MAPPING_FILE = HERE / "group_mapping.json"
OUT_HTML     = HERE / "Humidity Plots Interactive" / "dashboard.html"

# ── Load serial→group mapping ─────────────────────────────────────────────────
with open(MAPPING_FILE) as f:
    GROUP_MAPPING = json.load(f)["serials"]

REP_SERIALS = {}
for serial, info in GROUP_MAPPING.items():
    REP_SERIALS.setdefault(info["rep"], []).append(serial)

def is_silent(serial):
    return any(serial.endswith(s) for s in SILENT_SERIAL_SUFFIXES)

# ── Athena ────────────────────────────────────────────────────────────────────
ATHENA_SQL = """
SELECT
    dsm.filter_serial_number AS serial_number,
    from_unixtime(
        to_unixtime(dsm.filter_utc_timestamp)
        - CAST(JSON_EXTRACT_SCALAR(sample, '$.timestampOffsetS') AS BIGINT)
    ) AS sample_time,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.mainHumidityPct')       AS DOUBLE)          AS main_humidity_pct,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.cueHumidityPct')        AS DOUBLE)          AS cue_humidity_pct,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.mainHumidityTempDeciC') AS DOUBLE) / 10.0   AS main_humidity_temp_c,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.cueHumidityTempDeciC')  AS DOUBLE) / 10.0   AS cue_humidity_temp_c
FROM halter_aws_athena_v2.device_service_devicemetric AS dsm
CROSS JOIN UNNEST(
    CAST(JSON_EXTRACT(dsm.json,
        '$.metric.hardwareDiagnostics.environmentalMonitoring.samplesList'
    ) AS ARRAY(JSON))
) AS t(sample)
WHERE dsm.partition_metric_name = 'HARDWARE_DIAGNOSTICS'
  AND dsm.filter_serial_number IN ({serials})
  AND dsm.filter_utc_timestamp >= CAST('{since}' AS TIMESTAMP)
ORDER BY serial_number, sample_time
"""

def fetch_all() -> pd.DataFrame:
    try:
        from pyathena import connect as athena_connect
    except ImportError:
        sys.exit("\n[ERROR] pyathena not installed.\nRun:  pip install pyathena pandas boto3\n")

    conn = athena_connect(s3_staging_dir=S3_STAGING_DIR, region_name=AWS_REGION, work_group=ATHENA_WORKGROUP)
    frames = []
    t0 = time.time()
    for rep, since in REP_START_DATES.items():
        serials = REP_SERIALS.get(rep, [])
        if not serials:
            continue
        serial_list = ", ".join(f"'{s}'" for s in serials)
        sql = ATHENA_SQL.format(serials=serial_list, since=since)
        print(f"  {rep}: querying {len(serials)} serials from {since}…", flush=True)
        df = pd.read_sql(sql, conn)
        print(f"  {rep}: {len(df):,} rows", flush=True)
        frames.append(df)
    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"  Done in {time.time()-t0:.0f}s — {len(combined):,} rows total")
    return combined

# ── Process ───────────────────────────────────────────────────────────────────
def process(df: pd.DataFrame, serial_resample_sec: int = 300) -> dict:
    df["sample_time"] = pd.to_datetime(df["sample_time"], utc=True)
    df["group_code"]  = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("group_code", "UNKNOWN"))
    df["variant"]     = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("variant", "?"))
    df["rep"]         = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("rep", "?"))

    COLS        = ["main_humidity_pct", "cue_humidity_pct", "main_humidity_temp_c", "cue_humidity_temp_c"]
    group_rule  = f"{GROUP_RESAMPLE_MIN}min"
    serial_rule = f"{serial_resample_sec}s"

    groups_out, serials_out = {}, {}

    for (rep, group), g in df.groupby(["rep", "group_code"]):
        m = re.search(r"(V\d+|P)$", group)
        variant = m.group(1) if m else group
        g_active = g[~g["serial_number"].apply(is_silent)]
        if g_active.empty:
            g_active = g
        agg = g_active.set_index("sample_time")[COLS].resample(group_rule).mean().dropna()
        if agg.empty:
            continue
        groups_out.setdefault(rep, {})[group] = {
            "variant": variant,
            "times": agg.index.strftime("%Y-%m-%dT%H:%M").tolist(),
            "mh": agg["main_humidity_pct"].round(2).tolist(),
            "ch": agg["cue_humidity_pct"].round(2).tolist(),
            "mt": agg["main_humidity_temp_c"].round(2).tolist(),
            "ct": agg["cue_humidity_temp_c"].round(2).tolist(),
        }

    for serial, s in df.groupby("serial_number"):
        group   = GROUP_MAPPING.get(serial, {}).get("group_code", "UNKNOWN")
        variant = GROUP_MAPPING.get(serial, {}).get("variant", "?")
        rep     = GROUP_MAPPING.get(serial, {}).get("rep", "?")
        agg = s.set_index("sample_time")[COLS].resample(serial_rule).mean().dropna()
        if agg.empty:
            continue
        serials_out.setdefault(rep, {})[serial] = {
            "group": group, "variant": variant, "silent": is_silent(serial),
            "times": agg.index.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
            "mh": agg["main_humidity_pct"].round(2).tolist(),
            "ch": agg["cue_humidity_pct"].round(2).tolist(),
            "mt": agg["main_humidity_temp_c"].round(2).tolist(),
            "ct": agg["cue_humidity_temp_c"].round(2).tolist(),
        }

    return {
        "groups": groups_out, "serials": serials_out,
        "refreshed_at": datetime.now(timezone.utc).isoformat(),
    }

# ── Build HTML ────────────────────────────────────────────────────────────────
# Use plain string + substitution to avoid f-string/JS-brace conflicts
HTML_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>Humidity Ingress Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.32.0.min.js"></script>
<style>
* { box-sizing: border-box; }
body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #f0f2f5; }
#header {
  background: #fff; border-bottom: 1px solid #ddd;
  padding: 10px 16px; display: flex; align-items: center; gap: 12px; flex-wrap: wrap;
  position: sticky; top: 0; z-index: 10; box-shadow: 0 1px 4px rgba(0,0,0,0.08);
}
h1 { margin: 0; font-size: 14px; font-weight: 700; color: #1a1a2e; white-space: nowrap; }
.sep { width: 1px; height: 26px; background: #e0e0e0; flex-shrink: 0; }
.cg { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; }
.cl { font-size: 10px; font-weight: 700; color: #888; text-transform: uppercase; letter-spacing: 0.6px; white-space: nowrap; }
.chip {
  cursor: pointer; padding: 3px 9px; border-radius: 12px;
  font-size: 11px; font-weight: 600; border: 1.5px solid transparent;
  transition: all 0.12s; user-select: none; white-space: nowrap;
}
.chip.on  { color: #fff; }
.chip.off { background: #fff !important; color: #999 !important; border-color: #ccc !important; }
.vbtn {
  cursor: pointer; padding: 4px 11px; font-size: 11px; font-weight: 600;
  border: 1.5px solid #bbb; background: #fff; color: #666; border-radius: 5px; transition: all 0.12s;
}
.vbtn.active { background: #1a1a2e; color: #fff; border-color: #1a1a2e; }
#ts { font-size: 10px; color: #aaa; margin-left: auto; white-space: nowrap; }
#update-btn {
  cursor: pointer; padding: 4px 12px; font-size: 11px; font-weight: 700;
  border: 1.5px solid #2ca02c; background: #2ca02c; color: #fff;
  border-radius: 5px; transition: all 0.15s; white-space: nowrap; flex-shrink: 0;
}
#update-btn:disabled { opacity: 0.6; cursor: default; }
#update-btn.offline { background: #fff; color: #aaa; border-color: #ccc; font-weight: 600; }
#update-status { font-size: 10px; white-space: nowrap; }
#rep-info {
  background: #f4f6f9; border-bottom: 1px solid #e0e0e0;
  padding: 6px 16px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
  min-height: 36px;
}
.rep-badge {
  display: flex; align-items: center; gap: 7px;
  background: #fff; border: 1.5px solid #ddd; border-radius: 7px;
  padding: 4px 12px 4px 6px; font-size: 12px; box-shadow: 0 1px 2px rgba(0,0,0,0.05);
}
.rep-badge .rep-label {
  font-weight: 800; border-radius: 4px; padding: 2px 7px; color: #fff; font-size: 11px; letter-spacing: 0.3px;
}
.rep-badge .rep-title { color: #222; font-weight: 500; }
.rep-badge a {
  color: #888; text-decoration: none; font-size: 13px; margin-left: 4px; transition: color 0.1s;
}
.rep-badge a:hover { color: #0055cc; }
#plot-wrap { background: #fff; margin: 10px; border-radius: 10px; border: 1px solid #ddd; overflow: hidden; }
#variant-key {
  background: #fafbfc; border-bottom: 1px solid #e8e8e8;
  padding: 5px 16px; display: flex; gap: 8px; flex-wrap: wrap; align-items: center;
}
.vk-badge {
  display: flex; align-items: center; gap: 5px; font-size: 11px;
  padding: 3px 9px 3px 4px; border-radius: 5px; border: 1px solid #ddd; background: #fff;
}
.vk-label { font-weight: 800; color: #fff; border-radius: 3px; padding: 1px 5px; font-size: 10px; }
.vk-mat { color: #333; font-weight: 500; }
.vk-des { color: #888; font-size: 10px; margin-left: 2px; }
#plot { width: 100%; height: calc(100vh - 152px); }
</style>
</head>
<body>
<div id="header">
  <h1>📊 Humidity Ingress Dashboard</h1>
  <div class="sep"></div>
  <div class="cg">
    <span class="cl">View</span>
    <button class="vbtn active" id="btn-group"  onclick="setView('group')">By Group (5m avg)</button>
    <button class="vbtn"        id="btn-serial" onclick="setView('serial')">By Serial</button>
  </div>
  <div class="sep"></div>
  <div class="cg"><span class="cl">REP</span><div id="rep-chips" class="cg"></div></div>
  <div class="sep"></div>
  <div class="cg"><span class="cl">Variant</span><div id="var-chips" class="cg"></div></div>
  <div class="sep"></div>
  <div class="cg"><span class="cl">Series</span><div id="ser-chips" class="cg"></div></div>
  <div class="sep"></div>
  <div class="cg">
    <span class="cl">Silent serials</span>
    <button class="vbtn" id="btn-silent" onclick="toggleSilent()" title="Serials reporting infrequently">&#9888; Show</button>
  </div>
  <div id="ts">Data as of __TIMESTAMP__</div>
  <div class="sep"></div>
  <button id="update-btn" class="offline" onclick="triggerUpdate()" title="Requires updater.py running in Terminal">&#128257; Update Data</button>
  <span id="update-status"></span>
</div>
<div id="rep-info"></div>
<div id="variant-key"></div>
<div id="plot-wrap"><div id="plot"></div></div>

<script>
const DATA = __DATA_JSON__;

const VARIANT_COLOR = {
  'P':'#0066CC','V1':'#00A040','V2':'#E07800','V3':'#CC1100','V5':'#7733BB','V6':'#008FA8'
};
const VARIANT_KEY = {
  'P': {material:'Desmopan 9380AU TPU', desiccant:'No desiccant', note:'Production baseline'},
  'V1':{material:'Desmopan 786E TPU',   desiccant:'No desiccant'},
  'V2':{material:'Desmopan 786E TPU',   desiccant:'5g desiccant'},
  'V3':{material:'Desmopan 786E TPU',   desiccant:'13g desiccant'},
  'V5':{material:'Thermolast TC8MUZ SEBS', desiccant:'13g desiccant'},
  'V6':{material:'Thermolast TC8MUZ SEBS', desiccant:'No desiccant', note:'Replaces V4'},
};
(function buildVariantKey() {
  const div = document.getElementById('variant-key');
  const lbl = document.createElement('span');
  lbl.className = 'cl'; lbl.textContent = 'Variant Key';
  div.appendChild(lbl);
  for (const [v, info] of Object.entries(VARIANT_KEY)) {
    const color = VARIANT_COLOR[v] || '#888';
    const badge = document.createElement('div');
    badge.className = 'vk-badge';
    badge.title = info.note || '';
    badge.innerHTML =
      '<span class="vk-label" style="background:'+color+'">'+v+'</span>' +
      '<span class="vk-mat">'+info.material+'</span>' +
      '<span class="vk-des">· '+info.desiccant+'</span>' +
      (info.note ? '<span class="vk-des" style="font-style:italic"> ('+info.note+')</span>' : '');
    div.appendChild(badge);
  }
})();
function varColor(v) { return VARIANT_COLOR[v] || '#888'; }
function fade(hex, a) {
  const r=parseInt(hex.slice(1,3),16),g=parseInt(hex.slice(3,5),16),b=parseInt(hex.slice(5,7),16);
  return `rgba(${r},${g},${b},${a})`;
}

const REP_DASH   = {'REP-501':'solid','REP-502':'dash','REP-503':'dot','REP-510':'dashdot','REP-511':'longdash'};
const SERIES_CFG = {
  mh:{label:'Main Humidity %',yaxis:'y', lw:2.5,alpha:1.0},
  ch:{label:'Cue Humidity %', yaxis:'y', lw:2.0,alpha:0.6},
  mt:{label:'Main Temp °C',   yaxis:'y2',lw:2.5,alpha:1.0},
  ct:{label:'Cue Temp °C',    yaxis:'y2',lw:2.0,alpha:0.6},
};

const SILENT_SUFFIXES = new Set(['18536','18542','10207','10516']);
function isSilent(key, d) {
  return d.silent || [...SILENT_SUFFIXES].some(s => key.endsWith(s));
}

function buildTraces(view) {
  const src = view==='group' ? DATA.groups : DATA.serials;
  const traces = [];
  for (const [rep, items] of Object.entries(src||{})) {
    for (const [key, d] of Object.entries(items)) {
      const variant = d.variant||'P';
      const silent  = view==='serial' && isSilent(key, d);
      const label   = silent ? `⚠ ${rep} ${key} (silent)` : `${rep} ${key}`;
      const color   = varColor(variant);
      const rdash   = REP_DASH[rep]||'solid';
      const alphaScale = silent ? 0.35 : 1.0;
      const lineDash   = silent ? 'dot' : rdash;
      let first = true;
      for (const [ser, cfg] of Object.entries(SERIES_CFG)) {
        traces.push({
          name: label, legendgroup: `${rep}__${key}`, showlegend: first,
          x: d.times, y: d[ser],
          type:'scatter', mode:'lines', yaxis: cfg.yaxis,
          line:{color:fade(color,cfg.alpha*alphaScale),width:cfg.lw,dash:lineDash},
          meta_rep:rep, meta_variant:variant, meta_series:ser, meta_silent:silent,
          hovertemplate:`%{x}<br>${cfg.label}: %{y:.1f}${silent?' ⚠ low-frequency':''}<extra>${label}</extra>`,
          visible:true,
        });
        first = false;
      }
    }
  }
  return traces;
}

let currentView    = 'group';
let activeREPs     = new Set(Object.keys(DATA.groups||{}));
let activeVariants = new Set(['P']);
let activeSeries   = new Set(['mh','ch','mt','ct']);
let showSilent     = false;
let groupTraces    = buildTraces('group');
let serialTraces   = buildTraces('serial');
function getTraces() { return currentView==='group' ? groupTraces : serialTraces; }

function toggleSilent() {
  showSilent = !showSilent;
  const btn = document.getElementById('btn-silent');
  btn.className = showSilent ? 'vbtn active' : 'vbtn';
  btn.textContent = showSilent ? '⚠ Shown' : '⚠ Show';
  applyFilters();
}

const LAYOUT = {
  xaxis:{title:'Time (UTC)',showgrid:true,gridcolor:'#eee',type:'date',autorange:true},
  yaxis:{title:'Relative Humidity (%)',titlefont:{color:'#1f77b4'},tickfont:{color:'#1f77b4'},showgrid:true,gridcolor:'#eee'},
  yaxis2:{title:'Temperature (°C)',titlefont:{color:'#d62728'},tickfont:{color:'#d62728'},overlaying:'y',side:'right',showgrid:false},
  legend:{bgcolor:'rgba(255,255,255,0.85)',bordercolor:'#ddd',borderwidth:1,font:{size:10}},
  hovermode:'x unified', plot_bgcolor:'white', paper_bgcolor:'white',
  margin:{t:20,b:50,l:65,r:65},
};
const CONFIG = {responsive:true,displaylogo:false,
  modeBarButtonsToRemove:['lasso2d','select2d'],
  toImageButtonOptions:{format:'png',scale:2}};

function visibleTrace(t) {
  if (t.meta_silent && !showSilent) return false;
  return activeREPs.has(t.meta_rep) && activeVariants.has(t.meta_variant) && activeSeries.has(t.meta_series);
}
function applyFilters() {
  Plotly.restyle('plot', {visible: getTraces().map(t => visibleTrace(t) ? true : 'legendonly')});
}
function initialPlot() {
  Plotly.react('plot', getTraces().map(t=>({...t,visible:visibleTrace(t)?true:'legendonly'})), LAYOUT, CONFIG);
  updateRepInfo();
}
function setView(v) {
  currentView = v;
  document.getElementById('btn-group').className  = 'vbtn'+(v==='group' ?' active':'');
  document.getElementById('btn-serial').className = 'vbtn'+(v==='serial'?' active':'');
  initialPlot();
}

function makeChip(label, color, active, onToggle) {
  const c = document.createElement('span');
  c.className = 'chip '+(active?'on':'off');
  c.textContent = label;
  if (active&&color) { c.style.background=color; c.style.borderColor=color; }
  c.onclick = onToggle;
  return c;
}
function toggleSet(set, key, chip, color) {
  if (set.has(key)) { set.delete(key); chip.className='chip off'; chip.style.background=''; chip.style.borderColor=''; }
  else              { set.add(key);    chip.className='chip on';  if(color){chip.style.background=color;chip.style.borderColor=color;} }
  applyFilters();
}

const repColors = {'REP-501':'#1f77b4','REP-502':'#2ca02c','REP-503':'#d62728','REP-510':'#9467bd','REP-511':'#ff7f0e'};

const REP_META = {
  'REP-501':{title:'28 Day Gravimetric Test — Humidity Ingress',url:'https://app.notion.com/p/28-Day-Gravimetric-Test-Humidity-Ingress-3720fbb13ddb80fda036e8a7b576aae2'},
  'REP-502':{title:'IEC 60068-2-30 Damp Heat Cyclic — Humidity Ingress Collars',url:'https://app.notion.com/p/IEC-60068-2-30-Damp-Heat-Cyclic-Humidity-Ingress-Collars-3720fbb13ddb80a0b475d362fbe8fe42'},
  'REP-503':{title:'Tropical Rain Shock — Humidity Ingress 700-1145 Alt Materials + Desiccant',url:'https://app.notion.com/p/Tropical-Rain-Shock-Humidity-Ingress-700-1145-Alt-Materials-Desiccant-3720fbb13ddb80b39345e0bc7fe3231e'},
  'REP-510':{title:'AF3 + HM Profile Moisture Ingress Characterisation',url:'https://app.notion.com/p/AF3-HM-Profile-Moisture-Ingress-Characterisation-37a0fbb13ddb80b9b4e2df334da6721d'},
  'REP-511':{title:'Hot Soak 85°C / 95% RH — Accelerated Moisture Ingress Alternate 700-1145 Materials',url:'https://app.notion.com/p/Hot-Soak-85-C-95-RH-Accelerated-Moisture-Ingress-Alternate-700-1145-materials-37a0fbb13ddb8072b312fd248d571da1'},
};

function updateRepInfo() {
  const div = document.getElementById('rep-info');
  div.innerHTML = '';
  const order = ['REP-501','REP-502','REP-503','REP-510','REP-511'];
  for (const rep of order) {
    if (!activeREPs.has(rep)) continue;
    const meta = REP_META[rep] || {};
    const color = repColors[rep] || '#888';
    const badge = document.createElement('div');
    badge.className = 'rep-badge';
    badge.innerHTML =
      '<span class="rep-label" style="background:'+color+'">'+rep+'</span>' +
      '<span class="rep-title">'+(meta.title||rep)+'</span>' +
      (meta.url ? '<a href="'+meta.url+'" target="_blank" title="Open test page in Notion">↗</a>' : '');
    div.appendChild(badge);
  }
}

const repDiv = document.getElementById('rep-chips');
for (const rep of Object.keys(DATA.groups||{})) {
  const col = repColors[rep]||'#555';
  const chip = makeChip(rep, col, true, null);
  chip.onclick = () => { toggleSet(activeREPs, rep, chip, col); updateRepInfo(); };
  repDiv.appendChild(chip);
}
const varDiv = document.getElementById('var-chips');
for (const [v, col] of Object.entries(VARIANT_COLOR)) {
  const active = v==='P';
  const chip = makeChip(v, col, active, null);
  chip.onclick = () => toggleSet(activeVariants, v, chip, col);
  varDiv.appendChild(chip);
}
const serDiv = document.getElementById('ser-chips');
const serColors = {mh:'#1f77b4',ch:'#aec7e8',mt:'#d62728',ct:'#f5a0a0'};
for (const [ser, cfg] of Object.entries(SERIES_CFG)) {
  const col = serColors[ser];
  const chip = makeChip(cfg.label, col, true, null);
  chip.onclick = () => toggleSet(activeSeries, ser, chip, col);
  serDiv.appendChild(chip);
}

initialPlot();

// ── Update button ─────────────────────────────────────────────────────────────
const UPDATER = 'http://localhost:5051';
async function pingUpdater() {
  try { const r = await fetch(UPDATER+'/ping',{signal:AbortSignal.timeout(800)}); return r.ok; }
  catch { return false; }
}
async function triggerUpdate() {
  const btn=document.getElementById('update-btn'), status=document.getElementById('update-status');
  btn.disabled=true; btn.textContent='⏳ Updating…';
  status.style.color='#888'; status.textContent='Querying Athena…';
  try {
    const r = await fetch(UPDATER+'/refresh', {signal:AbortSignal.timeout(300000)});
    if (r.ok) { status.style.color='#2ca02c'; status.textContent='Done — reloading…'; setTimeout(()=>location.reload(),800); }
    else throw new Error();
  } catch {
    status.style.color='#d62728'; status.textContent='Failed — is updater.py running?';
    btn.disabled=false; btn.textContent='🔄 Update Data';
  }
}
(async()=>{
  const alive = await pingUpdater();
  const btn = document.getElementById('update-btn');
  if (alive) { btn.classList.remove('offline'); btn.title='Fetch latest data from Athena and reload'; }
  btn.textContent = '🔄 Update Data';
  if (!alive) { document.getElementById('update-status').style.color='#aaa'; document.getElementById('update-status').textContent='(run updater.py to enable)'; }
})();
</script>
</body>
</html>"""


def build_html(data: dict) -> str:
    ts        = datetime.now().strftime("%d %b %Y %H:%M")
    data_json = json.dumps(data, separators=(",", ":"))
    return HTML_TEMPLATE.replace("__DATA_JSON__", data_json).replace("__TIMESTAMP__", ts)


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n── Humidity Ingress Dashboard Refresh ──")
    print("Querying Athena…")
    df = fetch_all()

    if df.empty:
        sys.exit("[ERROR] No data returned from Athena.")

    # ── Local dashboard (30s serial resolution) ───────────────────────────────
    print(f"Processing at {SERIAL_RESAMPLE_SEC_LOCAL}s resolution (local)…")
    data_local = process(df, serial_resample_sec=SERIAL_RESAMPLE_SEC_LOCAL)
    html_local = build_html(data_local)
    OUT_HTML.write_text(html_local, encoding="utf-8")
    size_local = OUT_HTML.stat().st_size / 1024 / 1024
    print(f"    dashboard.html → {size_local:.1f} MB  (open locally in Chrome)")

    # ── Shareable dashboard (300s serial resolution for GitHub Pages) ─────────
    print(f"Processing at {SERIAL_RESAMPLE_SEC_SHARE}s resolution (share)…")
    data_share = process(df, serial_resample_sec=SERIAL_RESAMPLE_SEC_SHARE)
    html_share = build_html(data_share)
    OUT_INDEX = OUT_HTML.parent / "index.html"
    OUT_INDEX.write_text(html_share, encoding="utf-8")
    size_share = OUT_INDEX.stat().st_size / 1024 / 1024
    print(f"    index.html     → {size_share:.1f} MB  (push to GitHub Pages)")

    print(f"\n✅  Done — {OUT_HTML.parent}")
