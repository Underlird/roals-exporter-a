#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — LKS FINAL PRODUCTION READY
Version: 2026.1.6-lks (Debug Edition - Fixed)

Status: PRODUCTION-READY mit vollständigem Debug-Logging
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
EXPORTER_VERSION = "2026.1.6-lks"
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
    """
    Holt History-Daten von HA Supervisor API.
    KRITISCH: significant_changes_only=0 um alle Events zu bekommen.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log("ERROR", "SUPERVISOR_TOKEN fehlt! API-Zugriff unmöglich.", cfg_level)
        return {}

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
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    
    log("DEBUG", f"History API Request (entity_ids: {len(entity_ids)})", cfg_level)
    
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            log("DEBUG", f"Response structure: type={type(data)}, len={len(data) if isinstance(data, list) else 'N/A'}", cfg_level)
            
            total_items = sum(len(entity_list) for entity_list in data if isinstance(entity_list, list))
            log("DEBUG", f"History API Response: {total_items} total items", cfg_level)
            
            if data and len(data) > 0:
                first_list = data[0]
                log("DEBUG", f"First list: type={type(first_list)}, len={len(first_list) if isinstance(first_list, list) else 'N/A'}", cfg_level)
                
                if isinstance(first_list, list) and len(first_list) > 0:
                    first_event = first_list[0]
                    sample_eid = first_event.get("entity_id", "N/A")
                    log("DEBUG", f"Sample entity_id from API: '{sample_eid}'", cfg_level)
                    if entity_ids:
                        log("DEBUG", f"Expected entity_id: '{entity_ids[0]}'", cfg_level)
            
            normalized_map = {}
            for eid in entity_ids:
                normalized_key = eid.strip().lower()
                normalized_map[normalized_key] = eid
            
            log("DEBUG", f"Normalized map has {len(normalized_map)} entries", cfg_level)
            
            history_map = {eid: [] for eid in entity_ids}
            matched_count = 0
            unmatched_count = 0
            
            for entity_list in data:
                log("DEBUG", f"Processing entity_list with {len(entity_list)} items", cfg_level)
                
                for event in entity_list:
                    eid_raw = event.get("entity_id")
                    if eid_raw:
                        eid_normalized = eid_raw.strip().lower()
                        
                        if matched_count == 0 and unmatched_count == 0:
                            log("DEBUG", f"First event: eid_raw='{eid_raw}', normalized='{eid_normalized}'", cfg_level)
                            log("DEBUG", f"In normalized_map? {eid_normalized in normalized_map}", cfg_level)
                        
                        if eid_normalized in normalized_map:
                            original_eid = normalized_map[eid_normalized]
                            history_map[original_eid].append(event)
                            matched_count += 1
                        else:
                            unmatched_count += 1
                            if unmatched_count == 1:
                                log("DEBUG", f"Unmatched entity_id: '{eid_raw}'", cfg_level)
            
            log("DEBUG", f"Matched: {matched_count}, Unmatched: {unmatched_count}", cfg_level)
            
            for eid, events in history_map.items():
                log("DEBUG", f"Entity {eid}: {len(events)} Events geladen.", cfg_level)
                
            return history_map
                
    except Exception as e:
        log("ERROR", f"History API Fehler: {e}", cfg_level)
        import traceback
        traceback.print_exc()
        return {}

def map_history_to_slots(ts_iso: List[str], history: List[Dict]) -> List[Any]:
    """
    LKS-Sampling: Last Known State für jeden Slot.
    """
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
    """Konvertiert Entity ID in sicheren Spaltennamen."""
    s = entity_id.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities: List[str], cfg_level: str) -> Dict[str, Any]:
    """
    Baut das tägliche JSON-Payload mit 288 Slots.
    """
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    history_map = fetch_ha_history(entities, day, tz, cfg_level)
    
    timeseries = {"ts_iso": ts_iso}
    columns_source_map = {}
    col_counts = {}
    
    for eid in sorted(entities):
        base_key = entity_to_column_key(eid)
        if base_key in col_counts:
            col_key = f"{base_key}_{hashlib.sha256(eid.encode()).hexdigest()[:8]}"
        else:
            col_key = base_key
        col_counts[base_key] = True
        
        columns_source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []))
        
    payload = {
        "events": [],
        "meta": {
            "version": EXPORTER_VERSION, 
            "domain": domain, 
            "date": day.isoformat(),
            "timezone": str(tz),
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
        with open("/
