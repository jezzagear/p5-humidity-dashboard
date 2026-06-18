"""
Humidity Ingress Dashboard — Local Server
==========================================
Queries AWS Athena for live humidity/temperature data and serves the
interactive dashboard at http://localhost:5050

Requirements:
    pip install flask pyathena pandas boto3

AWS credentials must be configured — either via:
  - AWS CLI:  aws configure
  - Env vars: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION

Config (edit the section below):
"""

# ── CONFIG — edit these ───────────────────────────────────────────────────────
S3_STAGING_DIR        = "s3://YOUR_BUCKET/athena-results/"   # Athena writes results here
AWS_REGION            = "ap-southeast-2"                     # your AWS region
GROUP_RESAMPLE_MIN    = 5     # group view: average window in minutes
SERIAL_RESAMPLE_SEC   = 30    # serial view: resolution in seconds (matches raw sample rate)

# Start date for each REP — data is pulled from this date up to now on each refresh
REP_START_DATES = {
    "REP-501": "2026-06-10",   # 28-day gravimetric test start
    "REP-502": "2026-06-03",
    "REP-503": "2026-06-03",
    "REP-510": "2026-06-09",
    "REP-511": "2026-06-09",
}

# Serials known to report infrequently — highlighted in the dashboard
# (trailing digits from Clank's analysis: 18536, 18542, 10207, 10516)
SILENT_SERIAL_SUFFIXES = {"18536", "18542", "10207", "10516"}
# ─────────────────────────────────────────────────────────────────────────────

import json, os, re, time, traceback
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from flask import Flask, jsonify, send_from_directory

try:
    from pyathena import connect as athena_connect
except ImportError:
    raise SystemExit("\n[ERROR] pyathena not installed.\nRun:  pip install flask pyathena pandas boto3\n")

# ── Paths ─────────────────────────────────────────────────────────────────────
HERE = Path(__file__).parent
MAPPING_FILE  = HERE / "group_mapping.json"
CACHE_FILE    = HERE / "data_cache.json"
DASHBOARD_FILE = HERE / "Humidity Plots Interactive" / "dashboard_live.html"

with open(MAPPING_FILE) as f:
    GROUP_MAPPING = json.load(f)["serials"]

ALL_SERIALS = list(GROUP_MAPPING.keys())

# ── Athena query ──────────────────────────────────────────────────────────────
ATHENA_SQL = """
SELECT
    dsm.filter_serial_number                                              AS serial_number,
    from_unixtime(
        to_unixtime(from_iso8601_timestamp(dsm.filter_utc_timestamp))
        - CAST(JSON_EXTRACT_SCALAR(sample, '$.timestampOffsetS') AS BIGINT)
    )                                                                     AS sample_time,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.mainHumidityPct')       AS DOUBLE) AS main_humidity_pct,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.cueHumidityPct')        AS DOUBLE) AS cue_humidity_pct,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.mainHumidityTempDeciC') AS DOUBLE) / 10.0 AS main_humidity_temp_c,
    CAST(JSON_EXTRACT_SCALAR(sample, '$.cueHumidityTempDeciC')  AS DOUBLE) / 10.0 AS cue_humidity_temp_c
FROM halter_aws_athena_v2.device_service_devicemetric AS dsm
CROSS JOIN UNNEST(
    CAST(
        JSON_EXTRACT(dsm.json,
            '$.metric.hardwareDiagnostics.environmentalMonitoring.samplesList'
        ) AS ARRAY(JSON)
    )
) AS t(sample)
WHERE dsm.partition_metric_name = 'HARDWARE_DIAGNOSTICS'
  AND dsm.filter_serial_number IN ({serials})
  AND dsm.filter_utc_timestamp >= '{since}'
ORDER BY serial_number, sample_time
"""

# Build a lookup: rep -> list of serial numbers
REP_SERIALS = {}
for serial, info in GROUP_MAPPING.items():
    rep = info["rep"]
    REP_SERIALS.setdefault(rep, []).append(serial)

def fetch_from_athena() -> pd.DataFrame:
    """Run one query per REP using its configured start date, then combine."""
    conn = athena_connect(s3_staging_dir=S3_STAGING_DIR, region_name=AWS_REGION)
    frames = []
    t0 = time.time()

    for rep, start_date in REP_START_DATES.items():
        serials = REP_SERIALS.get(rep, [])
        if not serials:
            print(f"[Athena] {rep}: no serials in mapping, skipping")
            continue
        serial_list = ", ".join(f"'{s}'" for s in serials)
        sql = ATHENA_SQL.format(serials=serial_list, since=start_date)
        print(f"[Athena] {rep}: querying {len(serials)} serials from {start_date} …")
        df = pd.read_sql(sql, conn)
        print(f"[Athena] {rep}: {len(df):,} rows")
        frames.append(df)

    combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    print(f"[Athena] All done in {time.time()-t0:.1f}s — {len(combined):,} total rows")
    return combined


# ── Data processing ───────────────────────────────────────────────────────────
def is_silent(serial: str) -> bool:
    """Return True if this serial is known to report infrequently."""
    return any(serial.endswith(sfx) for sfx in SILENT_SERIAL_SUFFIXES)

def process(df: pd.DataFrame) -> dict:
    df["sample_time"] = pd.to_datetime(df["sample_time"], utc=True)
    df["group_code"]  = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("group_code", "UNKNOWN"))
    df["variant"]     = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("variant", "?"))
    df["rep"]         = df["serial_number"].map(lambda s: GROUP_MAPPING.get(s, {}).get("rep", "?"))

    COLS         = ["main_humidity_pct", "cue_humidity_pct", "main_humidity_temp_c", "cue_humidity_temp_c"]
    group_rule   = f"{GROUP_RESAMPLE_MIN}min"
    serial_rule  = f"{SERIAL_RESAMPLE_SEC}s"

    groups_out  = {}
    serials_out = {}

    # ── by group (5-min avg, silent serials excluded from group average) ──────
    for (rep, group), g in df.groupby(["rep", "group_code"]):
        m = re.search(r"(V\d+|P)$", group)
        variant = m.group(1) if m else group
        # exclude silent serials from group average so they don't skew it
        g_active = g[~g["serial_number"].apply(is_silent)]
        if g_active.empty:
            g_active = g  # fall back to all if every serial in group is silent
        agg = g_active.set_index("sample_time")[COLS].resample(group_rule).mean().dropna()
        if agg.empty:
            continue
        if rep not in groups_out:
            groups_out[rep] = {}
        groups_out[rep][group] = {
            "variant": variant,
            "times": agg.index.strftime("%Y-%m-%dT%H:%M").tolist(),
            "mh":    agg["main_humidity_pct"].round(2).tolist(),
            "ch":    agg["cue_humidity_pct"].round(2).tolist(),
            "mt":    agg["main_humidity_temp_c"].round(2).tolist(),
            "ct":    agg["cue_humidity_temp_c"].round(2).tolist(),
        }

    # ── by serial (30s resolution) ────────────────────────────────────────────
    for serial, s in df.groupby("serial_number"):
        group   = GROUP_MAPPING.get(serial, {}).get("group_code", "UNKNOWN")
        variant = GROUP_MAPPING.get(serial, {}).get("variant", "?")
        rep     = GROUP_MAPPING.get(serial, {}).get("rep", "?")
        silent  = is_silent(serial)
        agg = s.set_index("sample_time")[COLS].resample(serial_rule).mean().dropna()
        if agg.empty:
            continue
        if rep not in serials_out:
            serials_out[rep] = {}
        serials_out[rep][serial] = {
            "group": group, "variant": variant, "silent": silent,
            "times": agg.index.strftime("%Y-%m-%dT%H:%M:%S").tolist(),
            "mh":    agg["main_humidity_pct"].round(2).tolist(),
            "ch":    agg["cue_humidity_pct"].round(2).tolist(),
            "mt":    agg["main_humidity_temp_c"].round(2).tolist(),
            "ct":    agg["cue_humidity_temp_c"].round(2).tolist(),
        }

    return {"groups": groups_out, "serials": serials_out,
            "refreshed_at": datetime.now(timezone.utc).isoformat()}


# ── Cache helpers ─────────────────────────────────────────────────────────────
def save_cache(data: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(data, f, separators=(",", ":"))
    print(f"[Cache] Saved ({os.path.getsize(CACHE_FILE)/1024/1024:.1f} MB)")

def load_cache() -> dict | None:
    if CACHE_FILE.exists():
        with open(CACHE_FILE) as f:
            return json.load(f)
    return None


# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__, static_folder=str(HERE / "Humidity Plots Interactive"))

@app.route("/")
def index():
    return send_from_directory(str(HERE / "Humidity Plots Interactive"), "dashboard_live.html")

@app.route("/data")
def get_data():
    """Return latest data from Athena (or cached if Athena unavailable)."""
    try:
        df = fetch_from_athena()
        data = process(df)
        save_cache(data)
        return jsonify(data)
    except Exception as e:
        traceback.print_exc()
        # Fall back to cache
        cached = load_cache()
        if cached:
            cached["error"] = f"Athena unavailable — showing cached data: {e}"
            return jsonify(cached)
        return jsonify({"error": str(e)}), 500

@app.route("/cached")
def get_cached():
    """Return cached data without hitting Athena."""
    cached = load_cache()
    if cached:
        return jsonify(cached)
    return jsonify({"error": "No cache yet — click Refresh to load from Athena"}), 404


if __name__ == "__main__":
    print("\n" + "="*55)
    print("  Humidity Ingress Dashboard")
    print("  Open:  http://localhost:5050")
    print("  Stop:  Ctrl+C")
    print("="*55 + "\n")

    # Serve cached data immediately if available, query Athena on demand
    if CACHE_FILE.exists():
        print(f"[Cache] Found existing cache — dashboard will load instantly")
    else:
        print("[Cache] No cache yet — click 'Refresh Data' in the dashboard")

    app.run(host="127.0.0.1", port=5050, debug=False)
