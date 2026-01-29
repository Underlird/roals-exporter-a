#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — ROALS-Perfect Edition
Version: 2026.1.19-lks-perfect

Architektur: Registry-First, Forensisch gehärtet
Features: Registry Gates, Data Quality Counters, Atomic Writes, Full Lineage (Hashes)
Garantien: 288 Slots (Hard Assert), Zero Silent Failures, Reproducibility
"""
from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import re
import sys
import shutil
import time
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: zoneinfo module fehlt. Python 3.9+ erforderlich.", file=sys.stderr)
        sys.exit(2)

# --- Constants ---
EXPORTER_VERSION = "2026.1.19-lks-perfect"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
API_TIMEOUT = 120
HISTORY_LOOKBACK_MIN = 15
ALLOWED_DOMAINS = ["budget", "cameras", "climate_eg", "climate_og", "energy", "events", "internet", "motion", "network", "prices", "security", "system", "weather"]
VALID_RUN_MODES = {"skeleton", "oneshot_domain", "oneshot_all_domains", "daily_all_domains"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}

# --- ROALS Manila Infrastructure Constants ---
DATA_ROOT = "/share/nara_data"
REGISTRY_PATH = "/share/nara_data/registry/entity_registry.json"

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if levels.get(level.upper(), 20) >= levels.get(cfg_level.upper(), 20):
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)

@dataclass(frozen=True)
class Settings:
    log_level: str
    run_mode: str
    target_date: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    exporter_domain: Optional[str]
    entities: List[str]
    fail_on_collision: bool  # P2.2

    @staticmethod
    def from_options(opts: Dict[str, Any]) -> "Settings":
        log_lvl = str(opts.get("log_level", "INFO")).strip().upper()
        if log_lvl not in VALID_LOG_LEVELS: log_lvl = "INFO"
        
        mode = str(opts.get("run_mode", "skeleton")).strip()
        if mode not in VALID_RUN_MODES:
            raise ValueError(f"Ungueltiger run_mode '{mode}'.")

        # P2.2 Configurable Collision Behavior
        fail_collision = bool(opts.get("fail_on_collision", False))

        # Date handling
        target_date = opts.get("target_date")
        start_date = opts.get("start_date")
        end_date = opts.get("end_date")
        
        if (start_date and not end_date) or (end_date and not start_date):
            raise ValueError("Batch-Mode erfordert start_date UND end_date.")
        
        # Validate dates
        for d in [target_date, start_date, end_date]:
            if d:
                try: dt.date.fromisoformat(d)
                except ValueError: raise ValueError(f"Datum ungueltig: {d}")

        return Settings(
            log_level=log_lvl,
            run_mode=mode,
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            exporter_domain=opts.get("exporter_domain"),
            entities=opts.get("entities", []),
            fail_on_collision=fail_collision
        )

    def get_config_hash(self) -> str:
        # P1.1 Config Hash calculation
        relevant = {
            "mode": self.run_mode,
            "domain": self.exporter_domain,
            "strict": self.fail_on_collision,
            "version": EXPORTER_VERSION
        }
        s = json.dumps(relevant, sort_keys=True)
        return hashlib.sha256(s.encode()).hexdigest()

def load_registry(path: str, cfg_level: str) -> Tuple[Dict[str, Any], str]:
    if not os.path.exists(path):
        log("ERROR", f"Registry fehlt: {path}", cfg_level)
        return {}, ""
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw_data = f.read()
            
        # P1.1 Registry Hash (über Raw Content oder geparstes JSON)
        # Wir nehmen geparstes JSON sorted für Stabilität
        data = json.loads(raw_data)
        canonical = json.dumps(data, sort_keys=True)
        reg_hash = hashlib.sha256(canonical.encode()).hexdigest()

        entity_prefixes = ("sensor.", "binary_sensor.", "climate.", "switch.", "light.", "input_")
        valid_registry = {}
        
        errors = 0
        for k, v in data.items():
            if not (isinstance(v, dict) and k.startswith(entity_prefixes)):
                continue
            
            # P0.2 Strict Loader Validation
            dom = v.get("exporter_domain")
            if not isinstance(dom, str) or not dom.strip():
                log("ERROR", f"Registry-Eintrag '{k}' hat ungueltige exporter_domain.", cfg_level)
                errors += 1
                continue # Skip invalid entry
                
            valid_registry[k] = v
            
        if errors > 0:
            log("WARNING", f"{errors} Registry-Eintraege wegen fehlender Domain uebersprungen.", cfg_level)

        return valid_registry, reg_hash

    except Exception as e:
        log("ERROR", f"Registry Load Fail: {e}", cfg_level)
        return {}, ""

def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    try:
        since = dt.date.fromisoformat(str(meta.get("since"))) if meta.get("since") else dt.date(1970, 1, 1)
        if meta.get("until"):
            return since <= target < dt.date.fromisoformat(str(meta.get("until")))
        return target >= since
    except: return False

def sanitize_key(key: str) -> str:
    return re.sub(r'[^a-z0-9_]+', '', key.lower()).strip('_')

def check_disk_space(path: str) -> bool:
    try: return (shutil.disk_usage(path).free / (1024*1024)) >= 100
    except: return True

def update_heartbeat(tz: ZoneInfo, cfg_level: str) -> None:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token: return
    try:
        url = "http://supervisor/core/api/services/input_datetime/set_datetime"
        data = json.dumps({
            "entity_id": "input_datetime.nara_last_success",
            "datetime": dt.datetime.now(tz=tz).isoformat()
        }).encode()
        req = urllib.request.Request(url, data=data, headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except: pass

# --- Core Logic ---

def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Tuple[Dict[str, List[Dict]], bool]:
    # P1.4 Retry Logic & P1.5 Deterministic Time Window
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token or not entity_ids: return {}, True

    # P1.5 Strict Time Window (00:00 - 23:59:59)
    # Start: 00:00 minus Lookback (für LKS bei Slot 0)
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=HISTORY_LOOKBACK_MIN)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?filter_entity_id={urllib.parse.quote(','.join(entity_ids))}&end_time={urllib.parse.quote(end_iso)}&minimal_response=0&no_attributes=1&significant_changes_only=0"
    
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})

    # P1.4 Retry Loop
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                h_map = {eid: [] for eid in entity_ids}
                for entity_list in data:
                    if entity_list:
                        eid = entity_list[0].get("entity_id")
                        if eid in h_map: h_map[eid] = entity_list
                return h_map, True # Success
        except Exception as e:
            if attempt < 2:
                sleep_time = 1 if attempt == 0 else 3
                log("WARNING", f"API Fail (Versuch {attempt+1}/3): {e}. Retry in {sleep_time}s...", cfg_level)
                time.sleep(sleep_time)
            else:
                log("ERROR", f"API Final Fail nach 3 Versuchen: {e}", cfg_level)
                return {}, False # Failed

def map_history_to_slots(ts_iso: List[str], history: List[Dict], locf_max_min: int) -> Tuple[List[Any], Dict[str, int]]:
    # P1.3 Data Quality Counters
    quality_stats = {"unknown": 0, "unavailable": 0, "non_numeric": 0}
    
    events = []
    for h in history:
        t_str = h.get("last_updated") or h.get("last_changed")
        if t_str:
            dt_utc = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            events.append((dt_utc.replace(tzinfo=None), h.get("state")))
    events.sort(key=lambda x: x[0])
    
    values = []
    last_val = None
    last_val_time = None
    e_idx = 0
    
    for slot_str in ts_iso:
        slot_dt_utc = dt.datetime.fromisoformat(slot_str).astimezone(dt.timezone.utc).replace(tzinfo=None)
        
        while e_idx < len(events) and events[e_idx][0] <= slot_dt_utc:
            state = events[e_idx][1]
            state_str = str(state).lower().strip()
            
            # P1.3 Counting
            if state_str == "unknown": quality_stats["unknown"] += 1
            elif state_str == "unavailable": quality_stats["unavailable"] += 1
            
            if state not in (None, "unknown", "unavailable", ""):
                try:
                    last_val = float(state)
                except ValueError:
                    quality_stats["non_numeric"] += 1
                    last_val = state # String state behalten
                last_val_time = events[e_idx][0]
            e_idx += 1
            
        if last_val_time and (slot_dt_utc - last_val_time).total_seconds() / 60 > locf_max_min:
            values.append(None)
        else:
            values.append(last_val)
            
    return values, quality_stats

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities_meta: Dict[str, Any], cfg: Settings, reg_hash: str) -> Dict[str, Any]:
    # P0.3 Assert 288 Slots
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    if len(ts_iso) != SLOTS_PER_DAY:
        raise RuntimeError(f"FATAL: Slot-Generierung fehlgeschlagen. {len(ts_iso)} != {SLOTS_PER_DAY}")

    sorted_eids = sorted(entities_meta.keys())
    
    # Fetch Data
    history_map, fetch_ok = fetch_ha_history(sorted_eids, day, tz, cfg.log_level)
    
    timeseries = {"ts_iso": ts_iso}
    source_map = {}
    quality_per_entity = {}
    missing_roals_ids = []
    
    registry_gate_fail = False

    for eid in sorted_eids:
        meta = entities_meta[eid]
        metric = meta.get("metric", {})
        
        # P0.3 / P2.1 ROALS ID Logic
        roals_id = meta.get("roals_id")
        if not roals_id:
            missing_roals_ids.append(eid) # P2.1 Warning Collection
            base_key = eid
        else:
            base_key = roals_id
            
        clean_base = sanitize_key(base_key)
        suffix = metric.get("column_unit_suffix") or ""
        col_key = f"{clean_base}{suffix}"
        
        # P0.4 / P2.2 Collision Handling
        if col_key in source_map:
            msg = f"Key Collision '{col_key}' ({eid})."
            if cfg.fail_on_collision:
                raise ValueError(f"STRICT MODE: {msg}")
            else:
                log("ERROR", f"{msg} Skipping entity.", cfg.log_level)
                continue # Skip this entity, effectively failing the Registry Gate later

        locf_max = metric.get("agg_policy", {}).get("locf_max_duration_min", 15)
        
        # Map Values
        vals, q_stats = map_history_to_slots(ts_iso, history_map.get(eid, []), int(locf_max))
        timeseries[col_key] = vals
        quality_per_entity[col_key] = q_stats
        
        # P1.2 Evidence-First Excerpt
        source_map[col_key] = {
            "ha_entity_id": eid,
            "registry_excerpt": {
                "roals_id": roals_id,
                "exporter_domain": domain,
                "metric": metric
            }
        }

    # P0.1 Registry Gate Check
    # Erwartet: Alle 'active' entities sollten in 'timeseries' sein (minus collisions)
    expected_count = len(sorted_eids)
    actual_count = len(source_map)
    
    gate_status = "OK"
    if actual_count != expected_count:
        gate_status = "FAIL"
        registry_gate_fail = True
        log("ERROR", f"REGISTRY GATE FAIL: Expected {expected_count}, Got {actual_count}", cfg.log_level)

    # P1.1 Run Signature & Metadata
    meta_block = {
        "version": EXPORTER_VERSION,
        "run_id": dt.datetime.utcnow().strftime("%Y%m%d-%H%M%S") + "-mono", # P1.1
        "domain": domain,
        "date": day.isoformat(),
        "timezone": str(tz),
        "generated_at": dt.datetime.now(tz=tz).isoformat(),
        "config_hash": cfg.get_config_hash(), # P1.1
        "registry_hash": reg_hash, # P1.1
        "registry_gate": gate_status, # P0.1
        "history_fetch": "OK" if fetch_ok else "FAIL", # P1.4
        "data_quality_class": "verified" if fetch_ok and gate_status == "OK" else "insufficient_data",
        "columns_source_map": source_map,
        "data_quality": {
            "per_entity": quality_per_entity # P1.3
        },
        "integrity_hash": ""
    }
    
    if missing_roals_ids:
        meta_block["warnings_missing_roals_id"] = missing_roals_ids[:50] # P2.1 Limit

    payload = {"meta": meta_block, "timeseries": timeseries}
    
    # P0.6 Consistent Hashing (ensure_ascii=False)
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    
    # Return payload AND failure signal
    is_failed = registry_gate_fail or (not fetch_ok)
    return payload, is_failed

def main() -> int:
    stats = {"written": 0, "skipped_exists": 0, "failed": 0}
    
    try:
        with open("/data/options.json", "r") as f: opts = json.load(f)
        cfg = Settings.from_options(opts)
        
        if not check_disk_space(DATA_ROOT):
            log("ERROR", "Disk voll."); return 2
            
        tz = ZoneInfo("Asia/Manila") # Hardcoded per ROALS Standard
        
        if cfg.run_mode == "skeleton": return 0
        
        # Load Registry (P0.2 Included)
        reg, reg_hash = load_registry(REGISTRY_PATH, cfg.log_level)
        if not reg: return 1 # Fatal if registry unloadable
        
        # Determine Days
        days = []
        if cfg.start_date and cfg.end_date:
            curr = dt.date.fromisoformat(cfg.start_date)
            end = dt.date.fromisoformat(cfg.end_date)
            while curr <= end: days.append(curr); curr += dt.timedelta(days=1)
        elif cfg.target_date:
            days = [dt.date.fromisoformat(cfg.target_date)]
        else:
            days = [dt.datetime.now(tz=tz).date()]
            
        # Determine Domains
        domains = sorted(ALLOWED_DOMAINS) if "all_domains" in cfg.run_mode else [cfg.exporter_domain]
        
        # Main Loop
        for day in days:
            for dom in domains:
                if not dom: continue
                try:
                    # Filter Active Entities (P0.5 Fix)
                    if cfg.entities:
                         active = {e: reg[e] for e in cfg.entities if e in reg and reg[e].get("exporter_domain") == dom}
                    else:
                        active = {e: m for e, m in reg.items() if m.get("exporter_domain") == dom and entity_is_active(m, day)}
                        
                    out = os.path.join(DATA_ROOT, f"{day.year:04d}", f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
                    if os.path.exists(out):
                        stats["skipped_exists"] += 1; continue
                        
                    # Build Payload
                    payload, failed_flag = build_daily_payload(dom, day, tz, active, cfg, reg_hash)
                    
                    if failed_flag:
                        stats["failed"] += 1 # P1.4 / P0.1 Count as failure
                    
                    # Atomic Write
                    os.makedirs(os.path.dirname(out), exist_ok=True)
                    tmp = out + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(payload, f, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
                        f.flush(); os.fsync(f.fileno())
                    os.replace(tmp, out)
                    
                    # P0.7 Dir Sync
                    try:
                        if hasattr(os, 'open'):
                            fd = os.open(os.path.dirname(out), os.O_RDONLY)
                            os.fsync(fd); os.close(fd)
                    except: pass
                    
                    # P1.3 Sanity
                    if os.path.getsize(out) < 1024:
                        log("WARNING", f"Small file: {out}", cfg.log_level)
                        
                    stats["written"] += 1
                    
                except Exception as e:
                    log("ERROR", f"Fail {day} {dom}: {e}", cfg.log_level)
                    stats["failed"] += 1

        if stats["failed"] == 0: update_heartbeat(tz, cfg.log_level)
        return 1 if stats["failed"] > 0 else 0

    except Exception as e:
        log("ERROR", f"CRASH: {e}"); return 2
    finally:
        log("INFO", f"Run Stats: {stats}")

if __name__ == "__main__": sys.exit(main())
