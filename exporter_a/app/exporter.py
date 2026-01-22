#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — LKS FINAL PRODUCTION READY
Version: 2026.1.4-lks (Freeze 2026)

Status: PRODUCTION-READY
Garantien: Atomic Write, fsync Durability, Collision Guard, 
           Deterministic Hashing, Last-Known-State Sampling.
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
EXPORTER_VERSION = "2026.1.4-lks"
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
            registry_path=opts.get("registry_path"),
            strict_registry=bool(opts.get("strict_registry", True)),
        )

# ----------------------------
# Data Sampling (Truth Extraction)
# ----------------------------
def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log("ERROR", "SUPERVISOR_TOKEN fehlt! API-Zugriff unmöglich.", cfg_level)
        return {}

    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=5)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    # KRITISCHE ÄNDERUNG: Strings statt Booleans
    params = {
        "filter_entity_id": ",".join(entity_ids),
        "end_time": end_iso,
        "minimal_response": "0",              # String "0" statt Boolean False
        "no_attributes": "1",                 # String "1" statt Boolean True
        "significant_changes_only": "0"       # String "0" - dieser fehlte komplett
    }
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
    
    log("DEBUG", f"History API Request URL (ohne Token)", cfg_level)
    
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            # Debug: Gesamtanzahl
            total_items = sum(len(entity_list) for entity_list in data if isinstance(entity_list, list))
            log("DEBUG", f"History API Response: {total_items} total items", cfg_level)
            
            history_map = {eid: [] for eid in entity_ids}
            for entity_list in data:
                for event in entity_list:
                    eid = event.get("entity_id")
                    if eid in history_map:
                        history_map[eid].append(event)
            
            # Debug: Pro Entity
            for eid, events in history_map.items():
                log("DEBUG", f"Entity {eid}: {len(events)} Events geladen.", cfg_level)
                
            return history_map
            
    except Exception as e:
        log("ERROR", f"History API Fehler: {e}", cfg_level)
        return {}

def map_history_to_slots(ts_iso: List[str], history: List[Dict]) -> List[Any]:
    events = []
    for h in history:
        t_str = h.get("last_updated") or h.get("last_changed")
        if t_str:
            ts_dt = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00"))
            events.append((ts_dt, h.get("state")))
            
    events.sort(key=lambda x: x[0])
    
    values = []
    e_idx = 0
    last_val = None
    
    for slot_str in ts_iso:
        slot_dt = dt.datetime.fromisoformat(slot_str)
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

def entity_to_column_key(entity_id: str) -> str:
    s = entity_id.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities: List[str], cfg_level: str) -> Dict[str, Any]:
    # Slots werden IMMER in Manila-Zeit deklariert (ROALS Standard)
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    history_map = fetch_ha_history(entities, day, tz, cfg_level)
    
    timeseries = {"ts_iso": ts_iso}
    columns_source_map = {}
    col_counts = {}
    
    for eid in sorted(entities):
        base_key = entity_to_column_key(eid)
        col_key = f"{base_key}_{hashlib.sha256(eid.encode()).hexdigest()[:8]}" if base_key in col_counts else base_key
        col_counts[base_key] = True
        
        columns_source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []))
        
    payload = {
        "events": [], # ROALS Contract Konsistenz
        "meta": {
            "version": EXPORTER_VERSION, 
            "domain": domain, 
            "date": day.isoformat(),
            "timezone": "Asia/Manila", # Anchor Timezone
            "generated_at": dt.datetime.now(tz=tz).isoformat(),
            "columns_source_map": columns_source_map, 
            "integrity_hash": ""
        },
        "timeseries": timeseries
    }
    
    cloned = copy.deepcopy(payload)
    cloned["meta"]["integrity_hash"] = ""
    s = json.dumps(cloned, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    return payload

# ----------------------------
# Main Execution
# ----------------------------

def main() -> int:
    try:
        with open("/data/options.json", "r") as f:
            cfg = Settings.from_options(json.load(f))
        
        tz = ZoneInfo(cfg.timezone)
        day = dt.date.fromisoformat(cfg.target_date) if cfg.target_date else dt.datetime.now(tz=tz).date()
        
        if cfg.run_mode == "skeleton":
            log("INFO", "Skeleton Mode beendet.", cfg.log_level)
            return 0
        
        if "oneshot" in cfg.run_mode and not cfg.entities:
            log("ERROR", "Keine Entities konfiguriert für oneshot Mode.", cfg.log_level)
            return 1
            
        domains = [cfg.exporter_domain] if "oneshot" in cfg.run_mode else ALLOWED_DOMAINS
        entities_to_use = cfg.entities
        
        for dom in domains:
            if not dom: continue
            out_path = os.path.join(cfg.data_root, str(day.year), f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
            if os.path.exists(out_path):
                log("WARNING", f"Datei existiert bereits: {out_path}", cfg.log_level)
                continue
                
            os.makedirs(os.path.dirname(out_path), exist_ok=True)
            payload = build_daily_payload(dom, day, tz, entities_to_use, cfg.log_level)
            
            # Atomic Write mit fsync Durability
            tmp_path = out_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, out_path)
            
            # Best-effort directory fsync
            try:
                dir_fd = os.open(os.path.dirname(out_path), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError: pass

            log("INFO", f"SUCCESS: {out_path} ({len(entities_to_use)} Entities)", cfg.log_level)

        return 0
    except Exception as e:
        log("ERROR", f"CRASH: {e}", "INFO")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
