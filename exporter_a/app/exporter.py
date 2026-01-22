#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — LKS FINAL PRODUCTION READY
Verbesserungen: Registry-Bootstrap, Unit-Suffix Support, Robustes LKS-Mapping.
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

# ----------------------------
# Constants & Contract
# ----------------------------
EXPORTER_VERSION = "2026.1.7-lks"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
ALLOWED_DOMAINS = [
    "budget", "cameras", "climate_eg", "climate_og", "energy", "events",
    "internet", "motion", "network", "prices", "security", "system", "weather"
]

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    _LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    lvl = _LEVELS.get(level.upper(), 20)
    cfg = _LEVELS.get(cfg_level.upper(), 20)
    if lvl >= cfg:
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)

# ----------------------------
# Registry Handling (Bootstrap & Loading)
# ----------------------------
def load_registry(path: str, cfg_level: str = "INFO") -> Dict[str, Any]:
    """Lädt die Registry oder erstellt ein leeres Bootstrap-File."""
    if not path or not os.path.exists(path):
        log("WARNING", f"Registry nicht gefunden unter {path}. Erstelle Bootstrap...", cfg_level)
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({}, f, indent=2)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log("ERROR", f"Registry Fehler: {e}", cfg_level)
        return {}

# ----------------------------
# Configuration
# ----------------------------
@dataclass(frozen=True)
class Settings:
    data_root: str
    timezone: str
    log_level: str
    run_mode: str
    target_date: Optional[str]
    exporter_domain: Optional[str]
    entities: List[str]
    registry_path: Optional[str]
    strict_registry: bool

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
            registry_path=opts.get("registry_path", "/share/nara_data/registry/entity_registry.json"),
            strict_registry=bool(opts.get("strict_registry", True)),
        )

# ----------------------------
# Data Sampling (Truth Extraction)
# ----------------------------
def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token or not entity_ids:
        return {eid: [] for eid in entity_ids}

    # Wir holen 5 Min Puffer vor Mitternacht für den Startwert
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=5)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    params = {
        "filter_entity_id": ",".join(entity_ids),
        "minimal_response": "0",
        "no_attributes": "1",
        "significant_changes_only": "0"
    }
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            history_map = {eid: [] for eid in entity_ids}
            
            # HA API liefert Liste von Listen
            for entity_list in data:
                if not entity_list: continue
                eid = entity_list[0].get("entity_id")
                if eid in history_map:
                    history_map[eid] = entity_list
            return history_map
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level)
        return {eid: [] for eid in entity_ids}

def map_history_to_slots(ts_iso: List[str], history: List[Dict]) -> List[Any]:
    """
    KORREKTUR: Verbessertes LKS-Sampling (Last Known State).
    """
    events = []
    for h in history:
        t_str = h.get("last_updated")
        if t_str:
            # HA Zeitstempel sind UTC
            ts_dt = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00"))
            events.append((ts_dt, h.get("state")))
    
    events.sort(key=lambda x: x[0])
    
    values = []
    last_val = None
    e_idx = 0
    
    for slot_str in ts_iso:
        slot_dt = dt.datetime.fromisoformat(slot_str)
        # Suche den letzten gültigen Wert VOR oder GLEICH dem Slot-Zeitpunkt
        while e_idx < len(events) and events[e_idx][0] <= slot_dt:
            state = events[e_idx][1]
            if state not in (None, "unknown", "unavailable", ""):
                try:
                    last_val = float(state)
                except ValueError:
                    last_val = state
            e_idx += 1
        values.append(last_val)
    return values

# ----------------------------
# Payload Builder
# ----------------------------

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities: List[str], registry: Dict[str, Any], cfg_level: str) -> Dict[str, Any]:
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    history_map = fetch_ha_history(entities, day, tz, cfg_level)
    
    timeseries = {"ts_iso": ts_iso}
    columns_source_map = {}
    
    for eid in sorted(entities):
        # KORREKTUR: Suffix aus Registry ziehen
        meta = registry.get(eid, {})
        suffix = meta.get("metric", {}).get("column_unit_suffix", "")
        
        # Sicherer Spaltenname: ID + Suffix
        base_key = re.sub(r"[^a-z0-9]+", "_", eid.lower()).strip("_")
        col_key = f"{base_key}{suffix}"
        
        columns_source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []))
        
    payload = {
        "meta": {
            "version": EXPORTER_VERSION, 
            "domain": domain, 
            "date": day.isoformat(),
            "columns_source_map": columns_source_map, 
            "integrity_hash": ""
        },
        "timeseries": timeseries
    }
    
    # Deterministic Hash
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    return payload

# ----------------------------
# Main Execution
# ----------------------------

def main() -> int:
    try:
        with open("/data/options.json", "r") as f:
            options = json.load(f)
            cfg = Settings.from_options(options)
        
        tz = ZoneInfo(cfg.timezone)
        day = dt.date.fromisoformat(cfg.target_date) if cfg.target_date else dt.datetime.now(tz=tz).date()
        
        # KORREKTUR: Registry laden (Bootstrap integriert)
        registry = load_registry(cfg.registry_path, cfg.log_level)
        
        log("INFO", f"Exporter {EXPORTER_VERSION} startet: Mode={cfg.run_mode}, Day={day}, TZ={cfg.timezone}", cfg.log_level)
        
        if cfg.run_mode == "skeleton":
            return 0
            
        # Entity-Auswahl (Registry vs. Options)
        if cfg.run_mode == "daily_all_domains":
            # Hier würde die Filter-Logik für alle Domains stehen
            domains = ALLOWED_DOMAINS
        else:
            domains = [cfg.exporter_domain]
        
        for dom in domains:
            if not dom: continue
            
            # Entities für diese Domain bestimmen
            if cfg.run_mode == "daily_all_domains":
                entities_to_use = [eid for eid, m in registry.items() if m.get("exporter_domain") == dom]
            else:
                entities_to_use = cfg.entities

            if not entities_to_use: continue

            out_path = os.path.join(cfg.data_root, str(day.year), f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
            if os.path.exists(out_path):
                log("WARNING", f"Datei existiert bereits: {out_path}", cfg.log_level)
                continue
                
            log("INFO", f"Generiere Payload fuer {dom} ({len(entities_to_use)} Entities)...", cfg.log_level)
            payload = build_daily_payload(dom, day, tz, entities_to_use, registry, cfg.log_level)
            
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            tmp_path = out_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, out_path)
            log("INFO", f"SUCCESS: {out_path}", cfg.log_level)

        return 0
    except Exception as e:
        log("ERROR", f"CRASH: {e}", "INFO")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
