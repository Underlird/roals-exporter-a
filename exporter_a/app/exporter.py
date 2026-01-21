#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — SOFORT-LÖSUNG (HOTFIXED)
Version: 2026.1-LKS-Hotfix

Funktionen:
- Holt echte Daten via HA Supervisor API (History)
- Füllt 288 Slots (5 Min Raster) mittels Last-Known-State Logik
- Keine externen Abhängigkeiten (urllib statt requests)
- FIX: NameError bei col_key behoben
- FIX: LKS behält Werte auch bei 'unavailable'
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
# Constants
# ----------------------------

EXPORTER_VERSION = "2026.1-LKS-Hotfix"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
ALLOWED_DOMAINS = [
    "budget", "cameras", "climate_eg", "climate_og", "energy", "events",
    "internet", "motion", "network", "prices", "security", "system", "weather"
]
_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}


# ----------------------------
# Logging
# ----------------------------

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    lvl = _LEVELS.get(level.upper(), 20)
    cfg = _LEVELS.get(cfg_level.upper(), 20)
    if lvl >= cfg:
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)


# ----------------------------
# Settings
# ----------------------------

OPTIONS_PATH = "/data/options.json"

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
# Helpers
# ----------------------------

def sanitize_entity_id(entity_id: str) -> str:
    return entity_id.strip()

def entity_to_column_key(entity_id: str) -> str:
    s = entity_id.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s

def get_zoneinfo(tz_name: str) -> ZoneInfo:
    return ZoneInfo(tz_name)

def build_ts_iso_for_date(day: dt.date, tz: ZoneInfo) -> List[str]:
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    return [(start + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]

def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))

def attach_integrity_hash(payload: Dict[str, Any]) -> Dict[str, Any]:
    cloned = copy.deepcopy(payload)
    cloned["meta"]["integrity_hash"] = ""
    h = hashlib.sha256(canonical_dumps(cloned).encode("utf-8")).hexdigest()
    payload["meta"]["integrity_hash"] = h
    return payload

def atomic_write_json_write_once(path: str, payload: Dict[str, Any]) -> None:
    if os.path.exists(path):
        # Im Test-Modus erlauben wir KEIN Überschreiben -> Fail Fast
        raise FileExistsError(f"File exists: {path}")
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp.{os.getpid()}"
    
    with open(tmp_path, "w", encoding="utf-8") as f:
        f.write(canonical_dumps(payload))
        f.flush()
        os.fsync(f.fileno())
    
    os.replace(tmp_path, path)


# ----------------------------
# HA API & Sampling Logic
# ----------------------------

def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    """Holt Daten via urllib."""
    if not entity_ids:
        return {}
    
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log("ERROR", "SUPERVISOR_TOKEN fehlt! Keine Daten.", cfg_level)
        return {}

    # Start: 00:00 minus 5 Minuten (für Initialwert)
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=5)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    params = {
        "filter_entity_id": ",".join(entity_ids),
        "end_time": end_iso,
        "minimal_response": "false", # FIX: String "false" statt "0"
        "no_attributes": "1"
    }
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}"
    full_url = url + "?" + urllib.parse.urlencode(params)
    
    req = urllib.request.Request(full_url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    
    log("DEBUG", f"API Request: {full_url}", cfg_level)
    
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            history_map = {eid: [] for eid in entity_ids}
            
            for entity_list in data:
                for event in entity_list:
                    eid = event.get("entity_id")
                    if eid in history_map:
                        history_map[eid].append(event)
            
            for eid, events in history_map.items():
                log("DEBUG", f"Entity {eid}: {len(events)} Events geladen.", cfg_level)
                
            return history_map
            
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level)
        return {}


def map_history_to_slots(ts_iso: List[str], history: List[Dict]) -> List[Any]:
    """Wandelt Events in 288 Slots um (Last Known State)."""
    
    events = []
    for h in history:
        ts_str = h.get("last_changed")
        if ts_str:
            ts_dt = dt.datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            events.append((ts_dt, h.get("state")))
            
    events.sort(key=lambda x: x[0])
    
    values = []
    e_idx = 0
    last_val = None
    
    for slot_str in ts_iso:
        slot_dt = dt.datetime.fromisoformat(slot_str)
        
        # Spule Events vor
        while e_idx < len(events) and events[e_idx][0] <= slot_dt:
            state = events[e_idx][1]
            # FIX: Kein Reset bei unavailable, wir behalten den letzten validen Wert (LKS)
            if state not in (None, "unknown", "unavailable", ""):
                try:
                    last_val = float(state)
                except ValueError:
                    last_val = state 
            e_idx += 1
            
        values.append(last_val)
        
    return values


# ----------------------------
# Builder
# ----------------------------

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, tz_name: str, entities: List[str], cfg_level: str) -> Dict[str, Any]:
    ts_iso = build_ts_iso_for_date(day, tz)
    
    # 1. Daten holen
    history_map = fetch_ha_history(entities, day, tz, cfg_level)
    
    timeseries = {"ts_iso": ts_iso}
    columns_source_map = {}
    
    # 2. Spalten bauen
    col_counts = {}
    
    for eid in sorted(entities):
        base_key = entity_to_column_key(eid)
        if base_key in col_counts:
            # Konflikt -> Hash anhängen
            suffix = hashlib.sha256(eid.encode()).hexdigest()[:8]
            col_key = f"{base_key}_{suffix}"
        else:
            col_counts[base_key] = True
            col_key = base_key # FIX: Variable definiert
            
        columns_source_map[col_key] = {"ha_entity_id": eid}
        
        # 3. SAMPLING ANWENDEN
        events = history_map.get(eid, [])
        timeseries[col_key] = map_history_to_slots(ts_iso, events)
        
    payload = {
        "meta": {
            "version": EXPORTER_VERSION,
            "domain": domain,
            "date": day.isoformat(),
            "timezone": tz_name, # FIX: tz_name statt str(tz)
            "generated_at": dt.datetime.now(tz=tz).isoformat(),
            "columns_source_map": columns_source_map
        },
        "timeseries": timeseries
    }
    return attach_integrity_hash(payload)


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    try:
        # Config laden
        with open(OPTIONS_PATH, "r") as f:
            raw_opts = json.load(f)
        cfg = Settings.from_options(raw_opts)
        
        tz = ZoneInfo(cfg.timezone)
        
        # Datum bestimmen
        if cfg.target_date:
            day = dt.date.fromisoformat(cfg.target_date)
        else:
            day = dt.datetime.now(tz=tz).date()
            
        log("INFO", f"Starte Exporter {EXPORTER_VERSION} Mode={cfg.run_mode} Day={day}", cfg.log_level)

        if cfg.run_mode == "skeleton":
            log("INFO", "Skeleton check OK.", cfg.log_level)
            return 0
            
        # Domains bestimmen
        if "oneshot" in cfg.run_mode:
            domains = [cfg.exporter_domain]
            entities_to_use = cfg.entities
        else:
            # daily_all_domains - Im Testmodus nicht voll unterstützt ohne Registry
            # Fail-Safe für den Test:
            log("WARNING", "daily_all_domains im Test-Modus ohne volle Registry-Logik nicht empfohlen.", cfg.log_level)
            domains = ALLOWED_DOMAINS
            entities_to_use = [] 
            
        for dom in domains:
            if not dom: continue
            
            out_path = os.path.join(
                cfg.data_root, 
                str(day.year), 
                f"{day.month:02d}", 
                f"{day.isoformat()}_{dom}.json"
            )
            
            # Entities für diesen Lauf
            current_entities = entities_to_use
            
            if not current_entities:
                log("WARNING", f"Keine Entities für {dom} definiert. Überspringe.", cfg.log_level)
                continue

            log("INFO", f"Generiere Payload für {dom} ({len(current_entities)} Entities)...", cfg.log_level)
            
            # FIX: Übergebe cfg.timezone (String)
            payload = build_daily_payload(dom, day, tz, cfg.timezone, current_entities, cfg.log_level)
            atomic_write_json_write_once(out_path, payload)
            
            log("INFO", f"SUCCESS: Datei geschrieben: {out_path}", cfg.log_level)

        return 0
        
    except Exception as e:
        log("ERROR", f"CRASH: {e}", "INFO")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
