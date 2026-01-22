#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — Platinum Production Release
Version: 2026.1.13-lks
Changelog: Specific Exceptions, Performance Metrics, Heartbeat Diagnostics, Post-Write Sanity.
"""

from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import re
import sys
import shutil
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# --- ZoneInfo Hardening ---
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo # type: ignore
    except ImportError:
        print("CRITICAL: zoneinfo module missing. Python 3.9+ required.")
        sys.exit(2)

# --- Constants ---
EXPORTER_VERSION = "2026.1.13-lks"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
API_TIMEOUT = 120
HISTORY_LOOKBACK_MIN = 15
ALLOWED_DOMAINS = ["budget", "cameras", "climate_eg", "climate_og", "energy", "events", "internet", "motion", "network", "prices", "security", "system", "weather"]
VALID_RUN_MODES = {"skeleton", "oneshot_today", "oneshot_date", "daily_all_domains"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    _LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if _LEVELS.get(level.upper(), 20) >= _LEVELS.get(cfg_level.upper(), 20):
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
        log_lvl = str(opts.get("log_level", "INFO")).strip().upper()
        if log_lvl not in VALID_LOG_LEVELS: log_lvl = "INFO"
        
        # Timezone Validierung (Spezifisch)
        tz_str = str(opts.get("timezone", "Asia/Manila")).strip()
        try: ZoneInfo(tz_str)
        except Exception as e: raise ValueError(f"Ungültige Timezone '{tz_str}': {e}")

        mode = str(opts.get("run_mode", "skeleton")).strip()
        if mode not in VALID_RUN_MODES: raise ValueError(f"Ungültiger run_mode: {mode}")

        target_date = opts.get("target_date")
        if mode == "oneshot_date":
            if not target_date: raise ValueError("oneshot_date benötigt target_date.")
            try: dt.date.fromisoformat(target_date)
            except ValueError: raise ValueError(f"Ungültiges target_date Format '{target_date}'. Erwarte YYYY-MM-DD.")

        return Settings(
            data_root=str(opts.get("data_root", "/share/nara_data")).strip(),
            timezone=tz_str,
            log_level=log_lvl,
            run_mode=mode,
            target_date=target_date,
            exporter_domain=opts.get("exporter_domain"),
            entities=opts.get("entities", []),
            registry_path=str(opts.get("registry_path", "/share/nara_data/registry/entity_registry.json")).strip(),
        )

# --- Robust Helpers ---
def load_registry(path: str, cfg_level: str) -> Dict[str, Any]:
    if not os.path.exists(path): return {}
    try:
        with open(path, "r", encoding="utf-8") as f: data = json.load(f)
        ENTITY_PREFIXES = ("sensor.", "binary_sensor.", "climate.", "switch.", "light.", "input_")
        return {k: v for k, v in data.items() if isinstance(v, dict) and k.startswith(ENTITY_PREFIXES)}
    except json.JSONDecodeError as e:
        log("ERROR", f"Registry JSON korrupt: {e}", cfg_level); return {}
    except OSError as e:
        log("ERROR", f"Registry Lesefehler: {e}", cfg_level); return {}
    except Exception as e:
        log("ERROR", f"Registry Unerwarteter Fehler: {e}", cfg_level); return {}

def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    try:
        since = dt.date.fromisoformat(str(meta.get("since"))) if meta.get("since") else dt.date(1970, 1, 1)
        if meta.get("until"): return since <= target < dt.date.fromisoformat(str(meta.get("until")))
        return target >= since
    except (ValueError, TypeError): return False

def check_disk_space(path: str, min_mb: int = 100) -> bool:
    try:
        if not os.path.exists(path): return True
        return (shutil.disk_usage(path).free / (1024*1024)) >= min_mb
    except OSError: return True

def update_heartbeat(tz: ZoneInfo, cfg_level: str) -> None:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token: return
    try:
        url = "http://supervisor/core/api/services/input_datetime/set_datetime"
        data = json.dumps({"entity_id": "input_datetime.nara_last_success", "datetime": dt.datetime.now(tz=tz).isoformat()}).encode()
        req = urllib.request.Request(url, data=data, headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"})
        resp = urllib.request.urlopen(req, timeout=10)
        if resp.status != 200: log("WARNING", f"Heartbeat Status {resp.status}", cfg_level)
    except urllib.error.HTTPError as e:
        if e.code == 404: log("WARNING", "Heartbeat-Entity 'input_datetime.nara_last_success' fehlt in HA.", cfg_level)
        else: log("WARNING", f"Heartbeat HTTP Error: {e}", cfg_level)
    except Exception as e: log("WARNING", f"Heartbeat Fehler: {e}", cfg_level)

# --- Core Logic ---
def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token or not entity_ids: return {eid: [] for eid in entity_ids}

    t0 = dt.datetime.now() # Performance Start
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=HISTORY_LOOKBACK_MIN)
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?filter_entity_id={urllib.parse.quote(','.join(entity_ids))}&minimal_response=0"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    
    try:
        with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            # Performance Check
            dur = (dt.datetime.now() - t0).total_seconds()
            if dur > 30: log("WARNING", f"Langsame API Antwort: {dur:.1f}s für {len(entity_ids)} Sensoren.", cfg_level)
            
            h_map = {eid: [] for eid in entity_ids}
            for entity_list in data:
                if entity_list and entity_list[0].get("entity_id") in h_map:
                    h_map[entity_list[0].get("entity_id")] = entity_list
            return h_map
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level); return {eid: [] for eid in entity_ids}

def map_history_to_slots(ts_iso: List[str], history: List[Dict], locf_max_min: int) -> List[Any]:
    events = []
    for h in history:
        t_str = h.get("last_updated") or h.get("last_changed")
        if t_str:
            dt_utc = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            events.append((dt_utc.replace(tzinfo=None), h.get("state")))
    
    events.sort(key=lambda x: x[0])
    values, last_val, last_val_time, e_idx = [], None, None, 0
    
    for slot_str in ts_iso:
        slot_dt_utc = dt.datetime.fromisoformat(slot_str).astimezone(dt.timezone.utc).replace(tzinfo=None)
        while e_idx < len(events) and events[e_idx][0] <= slot_dt_utc:
            state = events[e_idx][1]
            if state not in (None, "unknown", "unavailable", ""):
                state_clean = str(state).strip()
                if state_clean:
                    try: last_val = float(state_clean)
                    except ValueError: last_val = state_clean # Nur ValueError fangen
                    last_val_time = events[e_idx][0]
            e_idx += 1
        
        if last_val_time and (slot_dt_utc - last_val_time).total_seconds() / 60 > locf_max_min:
            values.append(None)
        else:
            values.append(last_val)
    return values

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities_meta: Dict[str, Any], cfg_level: str) -> Dict[str, Any]:
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    sorted_eids = sorted(entities_meta.keys())
    history_map = fetch_ha_history(sorted_eids, day, tz, cfg_level)
    
    timeseries, source_map = {"ts_iso": ts_iso}, {}
    for eid in sorted_eids:
        metric = entities_meta[eid].get("metric", {})
        suffix = metric.get("column_unit_suffix")
        if suffix is None: 
            log("WARNING", f"{eid}: suffix fehlt, nutze ''", cfg_level); suffix = ""
            
        locf_max = metric.get("agg_policy", {}).get("locf_max_duration_min", 15)
        if not isinstance(locf_max, (int, float)) or locf_max <= 0: locf_max = 15

        col_key = f"{re.sub(r'[^a-z0-9]+', '_', eid.lower()).strip('_')}{suffix}"
        if col_key in source_map:
            raise ValueError(f"CRITICAL: Key Collision für {col_key} ({eid} vs {source_map[col_key]['ha_entity_id']})")
            
        source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []), locf_max)
        
    payload = {"meta": {"version": EXPORTER_VERSION, "domain": domain, "date": day.isoformat(), "columns_source_map": source_map, "integrity_hash": ""}, "timeseries": timeseries}
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    return payload

def main() -> int:
    stats = {"written": 0, "skipped_exists": 0, "skipped_empty": 0, "failed": 0}
    exit_code = 0
    
    try:
        with open("/data/options.json", "r") as f: opts = json.load(f)
        c = Settings.from_options(opts)
        
        if not check_disk_space(c.data_root):
            log("ERROR", f"Disk voll: {c.data_root}"); return 2

        tz = ZoneInfo(c.timezone)
        day = dt.date.fromisoformat(c.target_date) if c.target_date else dt.datetime.now(tz=tz).date()
        reg = load_registry(c.registry_path, c.log_level)
        
        if c.run_mode == "skeleton": return 0
        domains = sorted(ALLOWED_DOMAINS) if c.run_mode == "daily_all_domains" else [c.exporter_domain]

        log("INFO", f"Exporter {EXPORTER_VERSION} Start: Mode={c.run_mode}, Day={day}", c.log_level)

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
                
                # Sanity Check
                if os.path.getsize(out) < 1024:
                    log("WARNING", f"Datei verdächtig klein: {out}", c.log_level)

                stats["written"] += 1
            except Exception as e: 
                log("ERROR", f"Fehler in {dom}: {e}"); stats["failed"] += 1

        if stats["written"] > 0: update_heartbeat(tz, c.log_level)
        exit_code = 1 if stats["failed"] > 0 else 0

    except Exception as e:
        log("ERROR", f"Globaler Absturz: {e}")
        exit_code = 2
    finally:
        log("INFO", f"Run Summary: {stats}")
    
    return exit_code

if __name__ == "__main__": sys.exit(main())
