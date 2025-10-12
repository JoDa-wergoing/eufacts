#!/usr/bin/env python3
"""
ETL voor Eurostat TEINA230 — General government gross debt (% of GDP), quarterly.
- Haalt de meest recente kwartaalcijfers op via de Eurostat 1.0 JSON API
- Normaliseert naar een eenvoudig JSON-schema per land (laatste kwartaal)
- Schrijft naar data/latest/eu_debt.json + data/latest/manifest.json
- Maakt ook een gedateerde snapshot in data/snapshots/YYYY-MM-DD/

Opzet is defensief: probeert de 'unit' dimensie te herkennen (PC_GDP / % of GDP).
"""

import os
import sys
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import requests

# ---------- Config ----------
DATASET_ID = "teina230"  # Eurostat: General government gross debt - quarterly
# We willen de laatste kwartaalperiode; de 1.0 API ondersteunt 'time=Q' + 'lastTimePeriod=1'
EUROSTAT_URL = (
    f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    f"{DATASET_ID}?time=Q&lastTimePeriod=1"
)

OUT_DIR = "data"
LATEST_DIR = os.path.join(OUT_DIR, "latest")
SNAP_DIR = os.path.join(OUT_DIR, "snapshots")

JSON_OUT = os.path.join(LATEST_DIR, "eu_debt.json")
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


def fetch_json(url: str) -> Dict[str, Any]:
    log.info("Fetching Eurostat JSON: %s", url)
    r = requests.get(url, timeout=TIMEOUT_S)
    r.raise_for_status()
    return r.json()


def _linear_index_to_coords(idx: int, dims_sizes: List[int]) -> List[int]:
    """
    Zet een lineaire index om naar coördinaten per dimensie (row-major).
    Eurostat JSON 'value' dict gebruikt lineaire indices.
    """
    coords = []
    for size in reversed(dims_sizes):
        coords.append(idx % size)
        idx //= size
    return list(reversed(coords))


def parse_cube_last_period(cube: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse de Eurostat-kubus voor de laatste periode (we hebben lastTimePeriod=1 gevraagd).
    Geeft:
    - records: lijst dicts met geo, time, unit, value (alleen voor het 1 recente kwartaal)
    - meta: dimensies & labels
    """
    dimension = cube["dimension"]
    dim_ids: List[str] = cube["id"]  # bijv. ["unit", "geo", "time"]
    size: List[int] = cube["size"]   # bijv. [1, 27, 1] als we lastTimePeriod=1 vroegen
    value_map: Dict[str, float] = cube["value"]  # keys = lineaire index als string

    # Bouw dimension metadata:
    dim_meta = {}
    for d in dim_ids:
        cat = dimension[d]["category"]
        # index: map label->pos; label: map code->human
        # In 1.0 API is 'index' een dict met member_code -> positie
        index_map: Dict[str, int] = cat.get("index", {})
        # fallback: soms is er alleen 'label' zonder index; maak dan posities op volgorde
        if not index_map:
            members = list(cat.get("label", {}).keys())
            index_map = {m: i for i, m in enumerate(members)}
        labels: Dict[str, str] = cat.get("label", {})
        # inverse index voor makkelijke lookup: pos -> code
        inv_index = {pos: code for code, pos in index_map.items()}
        dim_meta[d] = {
            "index": index_map,
            "inv_index": inv_index,
            "labels": labels,
            "size": dimension[d]["category"].get("length", len(index_map)),
        }

    # We hebben lastTimePeriod=1, dus de time-dimensie heeft size 1 (laatste kwartaal).
    # We reconstrueren records uit value_map.
    records = []
    for k, v in value_map.items():
        lin_idx = int(k)
        coords = _linear_index_to_coords(lin_idx, size)

        coord_map = {}
        for dim_name, pos in zip(dim_ids, coords):
            inv = dim_meta[dim_name]["inv_index"]
            code = inv[pos]
            label = dim_meta[dim_name]["labels"].get(code, code)
            coord_map[dim_name] = {"code": code, "label": label, "pos": pos}

        # Bouw record (we willen 1 time, 1 unit, meerdere geo's)
        rec = {
            "value": v,
            "unit_code": coord_map.get("unit", {}).get("code"),
            "unit_label": coord_map.get("unit", {}).get("label"),
            "geo_code": coord_map.get("geo", {}).get("code"),
            "geo_label": coord_map.get("geo", {}).get("label"),
            "time_code": coord_map.get("time", {}).get("code"),
            "time_label": coord_map.get("time", {}).get("label"),
        }
        records.append(rec)

    # Filter voor de juiste unit als er meerdere units zijn.
    # In deze dataset willen we "% of GDP". In de 1.0 API heet de unit vaak 'PC_GDP' of heeft label met '%'.
    units_present = sorted({r["unit_code"] for r in records if r["unit_code"]})
    chosen_unit = None
    # 1) voorkeurscode
    for candidate in ("PC_GDP", "PCGDP", "PCT_GDP", "PCTGDP"):
        if candidate in units_present:
            chosen_unit = candidate
            break
    # 2) anders kies een met '%' in label
    if chosen_unit is None:
        unit_labels = {r["unit_code"]: r["unit_label"] for r in records if r["unit_code"]}
        for code, label in unit_labels.items():
            if label and "%" in label:
                chosen_unit = code
                break
    # 3) fallback: pak de enige unit als er maar 1 is
    if chosen_unit is None and len(units_present) == 1:
        chosen_unit = units_present[0]

    if chosen_unit:
        records = [r for r in records if r["unit_code"] == chosen_unit]

    # dedup per geo (zou al uniek moeten zijn voor lastTimePeriod=1)
    # sorteer op waarde desc om later makkelijk ranking te doen in frontend
    records.sort(key=lambda x: (x["value"] is None, x["value"]), reverse=True)

    meta = {
        "dim_ids": dim_ids,
        "units_present": units_present,
        "chosen_unit": chosen_unit,
    }
    return {"records": records, "meta": meta}


def build_output(records: List[Dict[str, Any]], source_url: str, dataset_id: str) -> Dict[str, Any]:
    # Bepaal de (enige) time_label aanwezig
    time_labels = sorted({r["time_label"] for r in records if r.get("time_label")})
    latest_time = time_labels[0] if time_labels else None

    simplified = []
    for r in records:
        simplified.append({
            "country_code": r["geo_code"],
            "country": r["geo_label"],
            "time": r["time_label"],
            "value_pct_gdp": r["value"],  # percentage of GDP
        })

    return {
        "dataset": dataset_id.upper(),
        "description": "General government gross debt, % of GDP (quarterly, latest period)",
        "source_url": source_url,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "latest_period": latest_time,
        "unit": "% of GDP",
        "records": simplified,
        "notes": [
            "Eurostat 1.0 API; only the most recent quarter is included here.",
            "Values represent consolidated gross debt of general government.",
            "Figures may be revised by Eurostat; check source for metadata."
        ],
    }


def write_json(path: str, obj: Dict[str, Any]):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)


def main():
    ensure_dirs()

    # 1) Fetch
    try:
        cube = fetch_json(EUROSTAT_URL)
    except Exception as e:
        log.exception("Fout bij ophalen Eurostat JSON")
        sys.exit(1)

    # 2) Parse
    try:
        parsed = parse_cube_last_period(cube)
        records = parsed["records"]
        meta = parsed["meta"]
        log.info("Records (latest quarter): %d | Units present: %s | chosen=%s",
                 len(records), meta.get("units_present"), meta.get("chosen_unit"))
    except Exception as e:
        log.exception("Fout bij parsen van Eurostat JSON")
        sys.exit(1)

    if not records:
        log.error("Geen records gevonden na filtering; ETL wordt afgebroken.")
        sys.exit(2)

    # 3) Build output object
    out_obj = build_output(records, EUROSTAT_URL, DATASET_ID)

    # 4) Write latest
    write_json(JSON_OUT, out_obj)
    log.info("Wrote %s", JSON_OUT)

    # 5) Snapshot
    today = datetime.now(timezone.utc).date().isoformat()
    snap_dir = os.path.join(SNAP_DIR, today)
    os.makedirs(snap_dir, exist_ok=True)
    snap_path = os.path.join(snap_dir, "eu_debt.json")
    write_json(snap_path, out_obj)
    log.info("Snapshot saved to %s", snap_path)

    # 6) Manifest
    manifest = {
        "dataset": DATASET_ID.upper(),
        "latest_file": os.path.relpath(JSON_OUT, start=OUT_DIR),
        "snapshot_file": os.path.relpath(snap_path, start=OUT_DIR),
        "source_url": EUROSTAT_URL,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "record_count": len(out_obj["records"]),
        "latest_period": out_obj.get("latest_period"),
        "hash_latest": sha256_file(JSON_OUT),
        "hash_snapshot": sha256_file(snap_path),
        "schema": {
            "fields": [
                {"name": "country_code", "type": "string"},
                {"name": "country", "type": "string"},
                {"name": "time", "type": "string"},
                {"name": "value_pct_gdp", "type": "number"},
            ]
        },
        "notes": [
            "Unit filtered to percentage-of-GDP (heuristic on unit codes/labels).",
            "One time period (latest quarter) requested via lastTimePeriod=1.",
        ],
    }
    write_json(MANIFEST_OUT, manifest)
    log.info("Manifest written to %s", MANIFEST_OUT)

    log.info("Done.")


if __name__ == "__main__":
    main()
