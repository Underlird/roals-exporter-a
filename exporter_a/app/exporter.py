#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — Final Production Core
Version: 2026.1.9-lks (Robust Data Mapping)
"""

from __future__ import annotations
import copy
import datetime as dt
import hashlib
import json
import os
import re
import sys
import urllib.request
import urllib.parse
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None  # type: ignore

# --- Constants ---
EXPORTER_VERSION = "2026.1.9-lks"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
ALLOWED_DOMAINS = ["budget", "cameras", "climate_eg", "climate_og", "energy", "events", "internet", "motion", "network", "prices", "security", "system", "weather"]

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    _LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    lvl = _LEVELS.get(level.upper(), 20)
    cfg = _LEVELS.get(cfg_level.upper(), 20)
    if lvl >= cfg:
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)

@dataclass(frozen=True)
class Settings:
    data_root: str
    timezone: str
    log_level: str
    run_mode: str
    target_date: Optional[str]
    exporter_domain: Optional[str]
    entities: List[str]
    registry_path: str

    @staticmethod
    def from_options(opts: Dict[str, Any]) -> "Settings":
        return Settings(
            data_root=str(opts.get("data_root", "/share/nara_data")).strip(),
            timezone=str(opts.get("timezone", "Asia/Manila")).strip(),
            log_level=str(opts.get("log_level", "INFO")).strip().upper(),
            run_mode=str(opts.get("run_mode", "skeleton")).strip(),
            target_date=opts.get("target_date"),
            exporter_domain=opts.get("exporter_domain"),
            entities=opts.get("entities", []),
            registry_path=str(opts.get("registry_path", "/share/nara_data/registry/entity_registry.json")).strip(),
        )

# --- Registry & Activity ---
def load_registry(path: str, cfg_level: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f: json.dump({}, f, indent=2)
        return {}
    with open(path, "r", encoding="utf-8") as f:
        try: return json.load(f)
        except: return {}

def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    try:
        since = dt.date.fromisoformat(str(meta.get("since"))) if meta.get("since") else dt.date(1970, 1, 1)
        if meta.get("until"):
            return since <= target < dt.date.fromisoformat(str(meta.get("until")))
        return target >= since
    except: return False

# --- API & Mapping ---
def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token or not entity_ids: return {eid: [] for eid in entity_ids}

    # Zeitfenster: Start - 5 Min bis Ende des Tages
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=5)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    params = {
        "filter_entity_id": ",".join(entity_ids),
        "end_time": end_iso,
        "minimal_response": "0",
        "no_attributes": "1",
        "significant_changes_only": "0"
    }
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            h_map = {eid.lower(): [] for eid in entity_ids}
            # HA API Mapping (Case-Insensitive)
            for entity_list in data:
                if entity_list:
                    eid_api = entity_list[0].get("entity_id", "").lower()
                    if eid_api in h_map:
                        h_map[eid_api] = entity_list
                        log("DEBUG", f"Fetched {len(entity_list)} events for {eid_api}", cfg_level)
            
            # Rückgabe-Map wieder auf Original-Keys mappen
            return {eid: h_map.get(eid.lower(), []) for eid in entity_ids}
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level)
        return {eid: [] for eid in entity_ids}

def map_history_to_slots(ts_iso: List[str], history: List[Dict]) -> List[Any]:
    events = []
    for h in history:
        t_str = h.get("last_updated")
        if t_str:
            dt_utc = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            events.append((dt_utc.replace(tzinfo=None), h.get("state")))
    
    events.sort(key=lambda x: x[0])
    values, last_val, e_idx = [], None, 0
    
    for slot_str in ts_iso:
        slot_dt_utc = dt.datetime.fromisoformat(slot_str).astimezone(dt.timezone.utc).replace(tzinfo=None)
        while e_idx < len(events) and events[e_idx][0] <= slot_dt_utc:
            state = events[e_idx][1]
            if state not in (None, "unknown", "unavailable", ""):
                try: last_val = float(state)
                except: last_val = state
            e_idx += 1
        values.append(last_val)
    return values

# --- Payload ---
def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities_meta: Dict[str, Any], cfg_level: str) -> Dict[str, Any]:
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    sorted_eids = sorted(entities_meta.keys())
    history_map = fetch_ha_history(sorted_eids, day, tz, cfg_level)
    
    timeseries, source_map = {"ts_iso": ts_iso}, {}
    for eid in sorted_eids:
        suffix = entities_meta[eid].get("metric", {}).get("column_unit_suffix", "")
        col_key = f"{re.sub(r'[^a-z0-9]+', '_', eid.lower()).strip('_')}{suffix}"
        source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []))
        
    payload = {"meta": {"version": EXPORTER_VERSION, "domain": domain, "date": day.isoformat(), "columns_source_map": source_map, "integrity_hash": ""}, "timeseries": timeseries}
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    return payload

def main() -> int:
    stats = {"written": 0, "skipped_exists": 0, "skipped_empty": 0, "failed": 0}
    try:
        with open("/data/options.json", "r") as f: cfg_opts = json.load(f)
        c = Settings.from_options(cfg_opts)
        tz = ZoneInfo(c.timezone)
        day = dt.date.fromisoformat(c.target_date) if c.target_date else dt.datetime.now(tz=tz).date()
        reg = load_registry(c.registry_path, c.log_level)
        
        if c.run_mode == "skeleton": return 0
        domains = sorted(ALLOWED_DOMAINS) if c.run_mode == "daily_all_domains" else [c.exporter_domain]

        log("INFO", f"Exporter {EXPORTER_VERSION} startet: Mode={c.run_mode}, Day={day}, TZ={c.timezone}", c.log_level)

        for dom in domains:
            if not dom: continue
            try:
                if c.run_mode == "daily_all_domains":
                    active = {eid: m for eid, m in reg.items() if m.get("exporter_domain") == dom and entity_is_active(m, day)}
                else:
                    active = {eid: reg.get(eid, {"metric": {}}) for eid in c.entities}

                if not active: stats["skipped_empty"] += 1; continue

                out = os.path.join(c.data_root, f"{day.year:04d}", f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
                if os.path.exists(out): stats["skipped_exists"] += 1; continue

                log("INFO", f"Verarbeite {dom} ({len(active)} Entities)...", c.log_level)
                payload = build_daily_payload(dom, day, tz, active, c.log_level)
                
                os.makedirs(os.path.dirname(out), exist_ok=True)
                tmp = out + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                    f.flush(); os.fsync(f.fileno())
                os.replace(tmp, out)
                stats["written"] += 1
            except Exception as e: log("ERROR", f"Fehler in Domain {dom}: {e}"); stats["failed"] += 1

        log("INFO", f"Run Summary: {stats}")
        return 1 if stats["failed"] > 0 else 0
    except Exception as e: log("ERROR", f"Globaler Fehler: {e}"); return 2

if __name__ == "__main__": sys.exit(main())
