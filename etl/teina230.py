#!/usr/bin/env python3
"""
ETL voor Eurostat TEINA230 — General government gross debt (% of GDP), quarterly.

Functies:
- Haalt de laatste 12 kwartalen op via Eurostat 1.0 JSON API.
- Selecteert programmatisch het meest recente kwartaal voor 'latest'.
- Bewaart tevens alle 12 kwartalen als timeseries per land.
- Schrijft outputs:
  - data/latest/eu_debt.json                (latest, per land)
  - data/timeseries/eu_debt_12q.json        (12 kwartalen timeseries, per land)
  - data/snapshots/YYYY-MM-DD/eu_debt.json  (snapshot van 'latest')
  - data/latest/manifest.json               (metadata + hashes)
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
import requests

# ---------- Config ----------
DATASET_ID = "teina230"  # General government gross debt - quarterly
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
# We halen bewust 12 kwartalen en reduceren vervolgens naar het laatste kwartaal.
EUROSTAT_URL = f"{BASE_URL}/{DATASET_ID}?lastTimePeriod=12"

OUT_DIR = "data"
LATEST_DIR = os.path.join(OUT_DIR, "latest")
SNAP_DIR = os.path.join(OUT_DIR, "snapshots")
TS_DIR = os.path.join(OUT_DIR, "timeseries")

JSON_LATEST = os.path.join(LATEST_DIR, "eu_debt.json")
JSON_TS = os.path.join(TS_DIR, "eu_debt_12q.json")
MANIFEST_OUT = os.path.join(LATEST_DIR, "manifest.json")

TIMEOUT_S = 60

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("etl.teina230")

# ---------- Helpers ----------
def sha256_file(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

def ensure_dirs():
    os.makedirs(LATEST_DIR, exist_ok=True)
    os.makedirs(SNAP_DIR, exist_ok=True)
    os.makedirs(TS_DIR, exist_ok=True)

def fetch_json(url: str) -> Dict[str, Any]:
    log.info("Fetching Eurostat JSON: %s", url)
    r = requests.get(url, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()

def _linear_index_to_coords(idx: int, dims_sizes: List[int]) -> List[int]:
    """
    Zet lineaire index om naar coördinaten per dimensie (row-major).
    Eurostat JSON 'value' dict gebruikt lineaire indices.
    """
    coords = []
    for size in reversed(dims_sizes):
        coords.append(idx % size)
        idx //= size
    return list(reversed(coords))

def _build_dim_meta(cube: Dict[str, Any]) -> Tuple[List[str], List[int], Dict[str, Any]]:
    dim_ids: List[str] = cube["id"]        # bijv. ["unit", "geo", "time"]
    size: List[int] = cube["size"]         # bijv. [U, G, T]
    dimension = cube["dimension"]

    dim_meta: Dict[str, Any] = {}
    for d in dim_ids:
        cat = dimension[d]["category"]
        index_map: Dict[str, int] = cat.get("index", {})
        if not index_map:
            members = list(cat.get("label", {}).keys())
            index_map = {m: i for i, m in enumerate(members)}
        labels: Dict[str, str] = cat.get("label", {})
        inv_index = {pos: code for code, pos in index_map.items()}
        size_guess = dimension[d]["category"].get("length", len(index_map))
        dim_meta[d] = {
            "index": index_map,     # code -> pos
            "inv_index": inv_index, # pos -> code
            "labels": labels,       # code -> label
            "size": size_guess,
        }
    return dim_ids, size, dim_meta

def _choose_pct_unit(records: List[Dict[str, Any]]) -> str:
    """Kies unit-code die % of GDP representeert."""
    units_present = sorted({r["unit_code"] for r in records if r.get("unit_code")})
    # 1) voorkeurscodes
    for candidate in ("PC_GDP", "PCGDP", "PCT_GDP", "PCTGDP"):
        if candidate in units_present:
            return candidate
    # 2) label bevat '%'
    unit_labels = {}
    for r in records:
        uc = r.get("unit_code")
        if uc and uc not in unit_labels:
            unit_labels[uc] = r.get("unit_label")
    for code, label in unit_labels.items():
        if label and "%" in label:
            return code
    # 3) fallback: enige unit
    if len(units_present) == 1:
        return units_present[0]
    # 4) laatste fallback: None -> caller moet hiermee omgaan
    return None

def parse_latest_and_timeseries(cube: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Parse de kubus:
    - 'latest' records: enkel meest recente kwartaal (per geo, 1 unit)
    - 'timeseries' per geo: alle kwartalen beschikbaar (12), zelfde unit-filter

    Return:
        latest_output (dict voor schrijven),
        meta (dict met o.a. gekozen unit & laatste periode)
    """
    value_map: Dict[str, float] = cube.get("value", {})
    if not value_map:
        raise ValueError("Eurostat response bevat geen 'value'.")

    dim_ids, size, dim_meta = _build_dim_meta(cube)

    if "time" not in dim_meta:
        raise ValueError("Geen 'time' dimensie gevonden in Eurostat response.")
    time_size = dim_meta["time"]["size"]
    latest_time_pos = time_size - 1
    latest_time_code = dim_meta["time"]["inv_index"][latest_time_pos]
    latest_time_label = dim_meta["time"]["labels"].get(latest_time_code, latest_time_code)

    # Verzamel alle records (alle kwartalen), we filteren later op unit en op laatste kwartaal.
    all_records: List[Dict[str, Any]] = []
    for k, v in value_map.items():
        lin_idx = int(k)
        coords = _linear_index_to_coords(lin_idx, size)
        coord_map = {}
        for dim_name, pos in zip(dim_ids, coords):
            inv = dim_meta[dim_name]["inv_index"]
            code = inv[pos]
            label = dim_meta[dim_name]["labels"].get(code, code)
            coord_map[dim_name] = {"code": code, "label": label, "pos": pos}
        all_records.append({
            "value": v,
            "unit_code": coord_map.get("unit", {}).get("code"),
            "unit_label": coord_map.get("unit", {}).get("label"),
            "geo_code": coord_map.get("geo", {}).get("code"),
            "geo_label": coord_map.get("geo", {}).get("label"),
            "time_code": coord_map.get("time", {}).get("code"),
            "time_label": coord_map.get("time", {}).get("label"),
            "time_pos": coord_map.get("time", {}).get("pos"),
        })

    # Unit-keuze (percentage-of-GDP)
    chosen_unit = _choose_pct_unit(all_records)
    if chosen_unit:
        all_records = [r for r in all_records if r.get("unit_code") == chosen_unit]

    # ---- Latest (alleen laatste kwartaal) ----
    latest_records = [r for r in all_records if r.get("time_pos") == latest_time_pos]
    latest_records.sort(key=lambda x: (x["value"] is None, x["value"]), reverse=True)

    latest_output = []
    for r in latest_records:
        latest_output.append({
            "country_code": r["geo_code"],
            "country": r["geo_label"],
            "time": r["time_label"],
            "value_pct_gdp": r["value"],
        })

    # ---- Timeseries (12 kwartalen per land) ----
    # Structureer per geo_code een lijst met {time, value}
    series_by_geo: Dict[str, Dict[str, Any]] = {}
    # sorteer op time_pos zodat de tijdreeks netjes oploopt
    all_records_sorted = sorted(all_records, key=lambda x: (x["geo_code"], x["time_pos"]))
    for r in all_records_sorted:
        geo = r["geo_code"]
        if not geo:
            continue
        if geo not in series_by_geo:
            series_by_geo[geo] = {
                "country_code": geo,
                "country": r["geo_label"],
                "unit": "% of GDP",
                "series": []
            }
        series_by_geo[geo]["series"].append({
            "time": r["time_label"],
            "value_pct_gdp": r["value"],
        })

    # Meta
    units_present = sorted({r.get("unit_code") for r in all_records if r.get("unit_code")})
    meta = {
        "dim_ids": dim_ids,
        "units_present": units_present,
        "chosen_unit": chosen_unit,
        "latest_time_code": latest_time_code,
        "latest_time_label": latest_time_label,
    }

    return latest_output, series_by_geo, meta

def build_latest_object(latest_records: List[Dict[str, Any]], source_url: str, dataset_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    latest_time = meta.get("latest_time_label")
    return {
        "dataset": dataset_id.upper(),
        "description": "General government gross debt, % of GDP (quarterly, latest period)",
        "source_url": source_url,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "latest_period": latest_time,
        "unit": "% of GDP",
        "records": latest_records,
        "notes": [
            "Eurostat 1.0 API; 12 quarters retrieved and reduced to latest quarter in ETL.",
            "Values represent consolidated gross debt of general government.",
            "Figures may be revised by Eurostat; check source for metadata."
        ],
    }

def build_timeseries_object(series_by_geo: Dict[str, Any], source_url: str, dataset_id: str, meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "dataset": dataset_id.upper(),
        "description": "General government gross debt, % of GDP (quarterly, last 12 quarters)",
        "source_url": source_url,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "unit": "% of GDP",
        "latest_period": meta.get("latest_time_label"),
        "records": list(series_by_geo.values()),
        "notes": [
            "Eurostat 1.0 API; last 12 quarters as returned by API.",
            "Series sorted by time ascending per country."
        ],
    }

def write_json(path: str, obj: Dict[str, Any]):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    log.info("Wrote %s (%d bytes)", path, os.path.getsize(path))

def main():
    ensure_dirs()

    # 1) Fetch
    try:
        cube = fetch_json(EUROSTAT_URL)
    except Exception:
        log.exception("Fout bij ophalen Eurostat JSON")
        sys.exit(1)

    # 2) Parse (latest + timeseries)
    try:
        latest_records, series_by_geo, meta = parse_latest_and_timeseries(cube)
        log.info(
            "Parsed: latest countries=%d | unit_chosen=%s | latest=%s",
            len(latest_records),
            meta.get("chosen_unit"),
            meta.get("latest_time_label"),
        )
    except Exception:
        log.exception("Fout bij parsen van Eurostat JSON")
        sys.exit(2)

    if not latest_records:
        log.error("Geen records gevonden na filtering; ETL wordt afgebroken.")
        sys.exit(3)

    # 3) Build output objects
    latest_obj = build_latest_object(latest_records, EUROSTAT_URL, DATASET_ID, meta)
    ts_obj = build_timeseries_object(series_by_geo, EUROSTAT_URL, DATASET_ID, meta)

    # 4) Write latest + timeseries
    write_json(JSON_LATEST, latest_obj)
    write_json(JSON_TS, ts_obj)

    # 5) Snapshot (latest)
    today = datetime.now(timezone.utc).date().isoformat()
    snap_dir = os.path.join(SNAP_DIR, today)
    os.makedirs(snap_dir, exist_ok=True)
    snap_path = os.path.join(snap_dir, "eu_debt.json")
    write_json(snap_path, latest_obj)

    # 6) Manifest
    manifest = {
        "dataset": DATASET_ID.upper(),
        "latest_file": os.path.relpath(JSON_LATEST, start=OUT_DIR),
        "timeseries_file": os.path.relpath(JSON_TS, start=OUT_DIR),
        "snapshot_file": os.path.relpath(snap_path, start=OUT_DIR),
        "source_url": EUROSTAT_URL,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_count_latest": len(latest_obj["records"]),
        "record_count_timeseries": sum(len(x["series"]) for x in ts_obj["records"]),
        "latest_period": latest_obj.get("latest_period"),
        "hash_latest": sha256_file(JSON_LATEST),
        "hash_timeseries": sha256_file(JSON_TS),
        "hash_snapshot": sha256_file(snap_path),
        "schema_latest": {
            "fields": [
                {"name": "country_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "time", "type": "string"},
                {"name": "value_pct_gdp", "type": "number"},
            ]
        },
        "schema_timeseries": {
            "fields": [
                {"name": "country_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "unit", "type": "string"},
                {"name": "series", "type": "array"},  # lijst van {time, value_pct_gdp}
            ]
        },
        "notes": [
            "Unit filtered to percentage-of-GDP (heuristic on unit codes/labels).",
            "Retrieved last 12 quarters and reduced to latest quarter for 'latest'."
        ],
    }
    write_json(MANIFEST_OUT, manifest)
    log.info("Manifest written to %s", MANIFEST_OUT)

    log.info("Done.")

if __name__ == "__main__":
    main()
