#!/usr/bin/env python3
"""
Generieke ETL voor meerdere Eurostat-datasets (Eufacts).

Momenteel geconfigureerd voor:
- TEINA230      — General government gross debt (% of GDP), quarterly
- GOV_10A_TAXAG — Revenue from taxes and social contributions (% of GDP), annual

Voor elke dataset wordt het volgende gedaan:
- Haal de laatste N periodes op via Eurostat 1.0 JSON API (?lastTimePeriod=N).
- Bouw een 'latest' view (laatste periode per land, cross-section).
- Bouw een 'timeseries' view (N periodes per land).
- Schrijf outputs met canonieke naamgeving:

  data/snapshots/YYYY-MM-DD/<dataset>.json          (cross-section snapshot)
  data/timeseries/YYYY-MM-DD/<dataset>.json         (timeseries snapshot)
  data/latest/<dataset>.json                        (latest alias - cross-section)
  data/latest/<dataset>-timeseries.json             (latest alias - timeseries)

Daarnaast:
- data/latest/manifest.json bevat metadata over alle datasets.
"""

import sys
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
from pathlib import Path

import requests

# ---------- Config (algemeen) ----------
BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
TIMEOUT_S = 60

# ---------- Te verwerken datasets ----------
DATASETS = [
    {
        "id": "teina230",
        "description": "General government gross debt, % of GDP (quarterly)",
        "last_time_periods": 12,
        "notes_latest": [
            "Eurostat 1.0 API; 12 quarters retrieved and reduced to latest quarter in ETL.",
            "Values represent consolidated gross debt of general government.",
            "Figures may be revised by Eurostat; check source for metadata.",
        ],
        "notes_timeseries": [
            "Eurostat 1.0 API; last 12 quarters as returned by API.",
            "Series sorted by time ascending per country.",
        ],
    },
    {
        "id": "gov_10a_taxag",
        "description": "Total receipts from taxes and social contributions (% of GDP, general government, annual)",
        "last_time_periods": 5,
                "url": (
        f"{BASE_URL}/gov_10a_taxag"
        "?freq=A"
        "&unit=PC_GDP"
        "&sector=S13"
        "&na_item=D2_D5_D91_D61_M_D612_M_D614_M_D995"
        "&lastTimePeriod=5"
        ),
        "notes_latest": [
            "Eurostat 1.0 API; last 5 years retrieved and reduced to latest year in ETL.",
            "Values represent general government revenue from taxes and social contributions.",
            "Figures may be revised by Eurostat; check source for metadata.",
        ],
        "notes_timeseries": [
            "Eurostat 1.0 API; last 5 years as returned by API.",
            "Series sorted by time ascending per country.",
        ],
    },
]

# --- Paden ---
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent  # <repo> (ouder van etl/)
OUT_DIR = REPO_ROOT / "data"
LATEST_DIR = OUT_DIR / "latest"
SNAPSHOTS_DIR = OUT_DIR / "snapshots"
TIMESERIES_DIR = OUT_DIR / "timeseries"

DATE_TODAY = datetime.now(timezone.utc).date().isoformat()
SNAP_DIR = SNAPSHOTS_DIR / DATE_TODAY
TS_DIR = TIMESERIES_DIR / DATE_TODAY

MANIFEST_OUT = LATEST_DIR / "manifest.json"

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("etl.eurostat_multi")

# ---------- Helpers ----------
def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()

def fetch_json(url: str) -> Dict[str, Any]:
    log.info("Fetching Eurostat JSON: %s", url)
    r = requests.get(url, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()

def _linear_index_to_coords(idx: int, dims_sizes: List[int]) -> List[int]:
    """Zet lineaire index om naar coördinaten per dimensie (row-major)."""
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

def _choose_pct_unit(records: List[Dict[str, Any]]) -> str | None:
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
    # 4) laatste fallback
    return None

def parse_latest_and_timeseries(cube: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    """
    Parse de kubus:
    - 'latest' records: enkel meest recente periode voor elk geo (1 unit)
    - 'timeseries' per geo: alle (max N) periodes, zelfde unit-filter

    Return:
        latest_records (list),
        series_by_geo (dict),
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

    # Verzamel alle records (alle periodes)
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

    # ---- Latest (alleen laatste periode) ----
    latest_records = [r for r in all_records if r.get("time_pos") == latest_time_pos]
    latest_records.sort(key=lambda x: (x["value"] is None, x["value"]), reverse=True)

    latest_output: List[Dict[str, Any]] = []
    for r in latest_records:
        latest_output.append({
            "country_code": r["geo_code"],
            "country": r["geo_label"],
            "time": r["time_label"],
            "value_pct_gdp": r["value"],
        })

    # ---- Timeseries (N periodes per land) ----
    series_by_geo: Dict[str, Dict[str, Any]] = {}
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

    units_present = sorted({r.get("unit_code") for r in all_records if r.get("unit_code")})
    meta = {
        "dim_ids": dim_ids,
        "units_present": units_present,
        "chosen_unit": chosen_unit,
        "latest_time_code": latest_time_code,
        "latest_time_label": latest_time_label,
    }

    return latest_output, series_by_geo, meta

def build_latest_object(latest_records: List[Dict[str, Any]], source_url: str, dataset_cfg: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    latest_time = meta.get("latest_time_label")
    return {
        "dataset": dataset_cfg["id"].upper(),
        "description": f"{dataset_cfg['description']} (latest period)",
        "source_url": source_url,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "latest_period": latest_time,
        "unit": "% of GDP",
        "records": latest_records,
        "notes": dataset_cfg.get("notes_latest", []),
    }

def build_timeseries_object(series_by_geo: Dict[str, Any], source_url: str, dataset_cfg: Dict[str, Any], meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "dataset": dataset_cfg["id"].upper(),
        "description": f"{dataset_cfg['description']} (last {dataset_cfg['last_time_periods']} periods)",
        "source_url": source_url,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "unit": "% of GDP",
        "latest_period": meta.get("latest_time_label"),
        "records": list(series_by_geo.values()),
        "notes": dataset_cfg.get("notes_timeseries", []),
    }

def write_json(path: Path, obj: Dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Wrote %s (%d bytes)", path, path.stat().st_size)

# ---------- Hoofdlogica ----------
def process_dataset(dataset_cfg: Dict[str, Any]) -> Dict[str, Any]:
    dataset_id = dataset_cfg["id"]
    last_n = dataset_cfg["last_time_periods"]

    if "url" in dataset_cfg:
        eurostat_url = dataset_cfg["url"]
    else:
        eurostat_url = f"{BASE_URL}/{dataset_id}?lastTimePeriod={last_n}"
    log.info("=== Processing dataset %s (lastTimePeriod=%s) ===", dataset_id, last_n)

    # 1) Fetch
    try:
        cube = fetch_json(eurostat_url)
    except Exception:
        log.exception("Fout bij ophalen Eurostat JSON voor %s", dataset_id)
        raise

    # 2) Parse (latest + timeseries)
    try:
        latest_records, series_by_geo, meta = parse_latest_and_timeseries(cube)
        log.info(
            "[%s] Parsed: latest countries=%d | unit_chosen=%s | latest=%s",
            dataset_id,
            len(latest_records),
            meta.get("chosen_unit"),
            meta.get("latest_time_label"),
        )
    except Exception:
        log.exception("Fout bij parsen van Eurostat JSON voor %s", dataset_id)
        raise

    if not latest_records:
        raise RuntimeError(f"[{dataset_id}] Geen records gevonden na filtering; ETL wordt afgebroken.")

    # 3) Build output objects
    latest_obj = build_latest_object(latest_records, eurostat_url, dataset_cfg, meta)
    ts_obj = build_timeseries_object(series_by_geo, eurostat_url, dataset_cfg, meta)

    # 4) Paden
    snap_path = SNAP_DIR / f"{dataset_id}.json"
    ts_path = TS_DIR / f"{dataset_id}.json"

    latest_cross = LATEST_DIR / f"{dataset_id}.json"
    latest_ts_hyphen = LATEST_DIR / f"{dataset_id}-timeseries.json"

    # 5) Write snapshots
    write_json(snap_path, latest_obj)
    write_json(ts_path, ts_obj)

    # 6) Write latest aliases
    LATEST_DIR.mkdir(parents=True, exist_ok=True)
    write_json(latest_cross, latest_obj)
    write_json(latest_ts_hyphen, ts_obj)

    # 7) Hashes (nice-to-have)
    try:
        hash_snap = sha256_file(snap_path)
        hash_ts = sha256_file(ts_path)
        hash_latest = sha256_file(latest_cross)
        hash_latest_ts = sha256_file(latest_ts_hyphen)
    except Exception:
        hash_snap = hash_ts = hash_latest = hash_latest_ts = None

    manifest_entry = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "latest_period": latest_obj.get("latest_period"),
        "unit": latest_obj.get("unit") or ts_obj.get("unit"),
        "latest_snapshot": str(snap_path.relative_to(OUT_DIR)),
        "latest_timeseries": str(ts_path.relative_to(OUT_DIR)),
        "latest_files": {
            "cross_section": str(latest_cross.relative_to(OUT_DIR)),
            "timeseries_hyphen": str(latest_ts_hyphen.relative_to(OUT_DIR)),
        },
        "hashes": {
            "snapshot": hash_snap,
            "timeseries": hash_ts,
            "latest_cross": hash_latest,
            "latest_timeseries": hash_latest_ts,
        },
        "source_url": eurostat_url,
        "record_count_latest": len(latest_obj["records"]),
        "record_count_timeseries": sum(len(x["series"]) for x in ts_obj["records"]),
    }

    return dataset_id, manifest_entry

def main():
    all_manifest: Dict[str, Any] = {"datasets": {}}
    LATEST_DIR.mkdir(parents=True, exist_ok=True)

    for cfg in DATASETS:
        try:
            dataset_id, entry = process_dataset(cfg)
            all_manifest["datasets"][dataset_id] = entry
        except Exception as e:
            log.error("Dataset %s is mislukt: %s", cfg["id"], e)
            # Niet meteen hele ETL afbreken; ga door met andere datasets

    # Manifest schrijven
    write_json(MANIFEST_OUT, all_manifest)
    log.info("Manifest written to %s", MANIFEST_OUT)
    log.info("Done.")

if __name__ == "__main__":
    main()
