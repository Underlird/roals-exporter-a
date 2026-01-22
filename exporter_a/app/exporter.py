{
  "name": "ROALS Exporter A",
  "version": "0.4.0",
  "slug": "roals_exporter_a",
  "description": "Truth Layer Exporter - Registry-First mit Batch-Mode Support",
  "arch": ["amd64", "aarch64", "armv7", "armhf"],
  "startup": "once",
  "boot": "manual",
  "hassio_api": true,
  "homeassistant_api": true,
  "map": ["share:rw"],
  "options": {
    "run_mode": "skeleton",
    "log_level": "INFO",
    "data_root": "/share/nara_data",
    "timezone": "Asia/Manila",
    "target_date": "",
    "start_date": "",
    "end_date": "",
    "exporter_domain": "climate_og",
    "entities": [],
    "registry_path": "/share/nara_data/registry/entity_registry.json"
  },
  "schema": {
    "run_mode": "list(skeleton|oneshot_domain|oneshot_all_domains|daily_all_domains)",
    "log_level": "list(DEBUG|INFO|WARNING|ERROR)",
    "data_root": "str",
    "timezone": "str",
    "target_date": "str?",
    "start_date": "str?",
    "end_date": "str?",
    "exporter_domain": "list(climate_og|budget|cameras|climate_eg|energy|events|internet|motion|network|prices|security|system|weather)",
    "entities": ["str?"],
    "registry_path": "str"
  }
}
exporter.py (Vereinfachte Logs)
#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — Registry-First Production Release
Version: 2026.1.17-lks

Architektur: Registry-First mit Entity Override
Run-Modes: skeleton, oneshot_domain, oneshot_all_domains, daily_all_domains
Features: Batch-Mode für Datumsbereich-Export (start_date bis end_date)
Garantien: Atomic Write, fsync Durability, Deterministic Hashing, LKS Sampling
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

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: zoneinfo module fehlt. Python 3.9+ erforderlich.", file=sys.stderr)
        sys.exit(2)

EXPORTER_VERSION = "2026.1.17-lks"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
API_TIMEOUT = 120
HISTORY_LOOKBACK_MIN = 15
ALLOWED_DOMAINS = ["budget", "cameras", "climate_eg", "climate_og", "energy", "events", "internet", "motion", "network", "prices", "security", "system", "weather"]
VALID_RUN_MODES = {"skeleton", "oneshot_domain", "oneshot_all_domains", "daily_all_domains"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if levels.get(level.upper(), 20) >= levels.get(cfg_level.upper(), 20):
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)

@dataclass(frozen=True)
class Settings:
    data_root: str
    timezone: str
    log_level: str
    run_mode: str
    target_date: Optional[str]
    start_date: Optional[str]
    end_date: Optional[str]
    exporter_domain: Optional[str]
    entities: List[str]
    registry_path: str

    @staticmethod
    def from_options(opts: Dict[str, Any]) -> "Settings":
        log_lvl = str(opts.get("log_level", "INFO")).strip().upper()
        if log_lvl not in VALID_LOG_LEVELS:
            log_lvl = "INFO"
        
        tz_str = str(opts.get("timezone", "Asia/Manila")).strip()
        try:
            ZoneInfo(tz_str)
        except Exception as e:
            raise ValueError(f"Ungueltige Timezone '{tz_str}': {e}")

        mode = str(opts.get("run_mode", "skeleton")).strip()
        if mode not in VALID_RUN_MODES:
            raise ValueError(f"Ungueltiger run_mode '{mode}'. Erlaubt: {', '.join(VALID_RUN_MODES)}")

        target_date = opts.get("target_date") if opts.get("target_date") else None
        start_date = opts.get("start_date") if opts.get("start_date") else None
        end_date = opts.get("end_date") if opts.get("end_date") else None
        
        if (start_date and not end_date) or (end_date and not start_date):
            raise ValueError("Batch-Mode erfordert sowohl start_date als auch end_date.")
        
        if start_date and end_date:
            try:
                start_dt = dt.date.fromisoformat(start_date)
                end_dt = dt.date.fromisoformat(end_date)
                if start_dt > end_dt:
                    raise ValueError(f"start_date ({start_date}) muss vor oder gleich end_date ({end_date}) sein.")
            except ValueError as e:
                raise ValueError(f"Ungueltiges Datumsformat. Erwarte YYYY-MM-DD. Details: {e}")
        
        if target_date:
            try:
                dt.date.fromisoformat(target_date)
            except ValueError:
                raise ValueError(f"Ungueltiges target_date Format '{target_date}'. Erwarte YYYY-MM-DD.")

        exporter_domain = opts.get("exporter_domain")
        
        if mode == "oneshot_domain":
            if not exporter_domain:
                raise ValueError("oneshot_domain erfordert exporter_domain.")
            if exporter_domain not in ALLOWED_DOMAINS:
                raise ValueError(f"Ungueltige exporter_domain '{exporter_domain}'.")

        return Settings(
            data_root=str(opts.get("data_root", "/share/nara_data")).strip(),
            timezone=tz_str,
            log_level=log_lvl,
            run_mode=mode,
            target_date=target_date,
            start_date=start_date,
            end_date=end_date,
            exporter_domain=exporter_domain,
            entities=opts.get("entities", []),
            registry_path=str(opts.get("registry_path", "/share/nara_data/registry/entity_registry.json")).strip(),
        )

def load_registry(path: str, cfg_level: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        log("WARNING", f"Registry nicht gefunden: {path}", cfg_level)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        entity_prefixes = ("sensor.", "binary_sensor.", "climate.", "switch.", "light.", "input_")
        return {k: v for k, v in data.items() if isinstance(v, dict) and k.startswith(entity_prefixes)}
    except json.JSONDecodeError as e:
        log("ERROR", f"Registry JSON korrupt: {e}", cfg_level)
        return {}
    except OSError as e:
        log("ERROR", f"Registry Lesefehler: {e}", cfg_level)
        return {}
    except Exception as e:
        log("ERROR", f"Registry unerwarteter Fehler: {e}", cfg_level)
        return {}

def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    try:
        since = dt.date.fromisoformat(str(meta.get("since"))) if meta.get("since") else dt.date(1970, 1, 1)
        if meta.get("until"):
            return since <= target < dt.date.fromisoformat(str(meta.get("until")))
        return target >= since
    except (ValueError, TypeError):
        return False

def check_disk_space(path: str, min_mb: int = 100) -> bool:
    try:
        if not os.path.exists(path):
            return True
        return (shutil.disk_usage(path).free / (1024 * 1024)) >= min_mb
    except OSError:
        return True

def update_heartbeat(tz: ZoneInfo, cfg_level: str) -> None:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return
    try:
        url = "http://supervisor/core/api/services/input_datetime/set_datetime"
        data = json.dumps({
            "entity_id": "input_datetime.nara_last_success",
            "datetime": dt.datetime.now(tz=tz).isoformat()
        }).encode()
        req = urllib.request.Request(url, data=data, headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })
        resp = urllib.request.urlopen(req, timeout=10)
        if resp.status != 200:
            log("WARNING", f"Heartbeat Status {resp.status}", cfg_level)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            log("WARNING", "Heartbeat-Entity fehlt in HA.", cfg_level)
        else:
            log("WARNING", f"Heartbeat HTTP Error: {e}", cfg_level)
    except Exception as e:
        log("WARNING", f"Heartbeat Fehler: {e}", cfg_level)

def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log("ERROR", "SUPERVISOR_TOKEN fehlt.", cfg_level)
        return {eid: [] for eid in entity_ids}
    
    if not entity_ids:
        return {}

    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=HISTORY_LOOKBACK_MIN)
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
    
    try:
        with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            h_map = {eid: [] for eid in entity_ids}
            for entity_list in data:
                if entity_list and len(entity_list) > 0:
                    first_eid = entity_list[0].get("entity_id")
                    if first_eid in h_map:
                        h_map[first_eid] = entity_list
            
            return h_map
            
    except urllib.error.HTTPError as e:
        log("ERROR", f"HTTP Error {e.code}: {e.reason}", cfg_level)
        return {eid: [] for eid in entity_ids}
    except urllib.error.URLError as e:
        log("ERROR", f"URL Error: {e.reason}", cfg_level)
        return {eid: [] for eid in entity_ids}
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level)
        return {eid: [] for eid in entity_ids}

def map_history_to_slots(ts_iso: List[str], history: List[Dict], locf_max_min: int) -> List[Any]:
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
            if state not in (None, "unknown", "unavailable", ""):
                state_clean = str(state).strip()
                if state_clean:
                    try:
                        last_val = float(state_clean)
                    except ValueError:
                        last_val = state_clean
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
    
    if sorted_eids:
        history_map = fetch_ha_history(sorted_eids, day, tz, cfg_level)
    else:
        history_map = {}
    
    timeseries = {"ts_iso": ts_iso}
    source_map = {}
    
    for eid in sorted_eids:
        metric = entities_meta[eid].get("metric", {})
        suffix = metric.get("column_unit_suffix")
        if suffix is None:
            suffix = ""
        
        locf_max = metric.get("agg_policy", {}).get("locf_max_duration_min", 15)
        if not isinstance(locf_max, (int, float)) or locf_max <= 0:
            locf_max = 15
        
        col_key = f"{re.sub(r'[^a-z0-9]+', '_', eid.lower()).strip('_')}{suffix}"
        if col_key in source_map:
            raise ValueError(f"Key Collision: {col_key}")
        
        source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []), locf_max)
    
    payload = {
        "meta": {
            "version": EXPORTER_VERSION,
            "domain": domain,
            "date": day.isoformat(),
            "timezone": str(tz),
            "generated_at": dt.datetime.now(tz=tz).isoformat(),
            "columns_source_map": source_map,
            "integrity_hash": ""
        },
        "timeseries": timeseries
    }
    
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    
    return payload

def main() -> int:
    stats = {"written": 0, "skipped_exists": 0, "skipped_empty": 0, "failed": 0}
    exit_code = 0
    
    try:
        with open("/data/options.json", "r") as f:
            opts = json.load(f)
        
        cfg = Settings.from_options(opts)
        
        if not check_disk_space(cfg.data_root):
            log("ERROR", f"Disk voll: {cfg.data_root}")
            return 2
        
        tz = ZoneInfo(cfg.timezone)
        
        if cfg.run_mode == "skeleton":
            log("INFO", "Skeleton Mode beendet.")
            return 0
        
        # Bestimme Datumsliste
        if cfg.start_date and cfg.end_date:
            start_dt = dt.date.fromisoformat(cfg.start_date)
            end_dt = dt.date.fromisoformat(cfg.end_date)
            days_to_process = []
            current = start_dt
            while current <= end_dt:
                days_to_process.append(current)
                current += dt.timedelta(days=1)
        elif cfg.target_date:
            days_to_process = [dt.date.fromisoformat(cfg.target_date)]
        else:
            days_to_process = [dt.datetime.now(tz=tz).date()]
        
        reg = load_registry(cfg.registry_path, cfg.log_level)
        
        # Bestimme Domains
        if cfg.run_mode == "daily_all_domains":
            domains = sorted(ALLOWED_DOMAINS)
        elif cfg.run_mode == "oneshot_all_domains":
            domains = sorted(ALLOWED_DOMAINS)
        elif cfg.run_mode == "oneshot_domain":
            domains = [cfg.exporter_domain]
        else:
            log("ERROR", f"Unbekannter run_mode: {cfg.run_mode}")
            return 1
        
        log("INFO", f"Start: {cfg.run_mode}, {len(days_to_process)} Tag(e), {len(domains)} Domain(s)", cfg.log_level)
        
        # Hauptschleife
        for day in days_to_process:
            for dom in domains:
                if not dom:
                    continue
                
                try:
                    if cfg.entities:
                        active = {eid: reg.get(eid, {"metric": {}}) for eid in cfg.entities}
                    else:
                        active = {
                            eid: meta
                            for eid, meta in reg.items()
                            if meta.get("exporter_domain") == dom and entity_is_active(meta, day)
                        }
                    
                    out = os.path.join(cfg.data_root, f"{day.year:04d}", f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
                    
                    if os.path.exists(out):
                        stats["skipped_exists"] += 1
                        continue
                    
                    if not active and cfg.run_mode == "daily_all_domains":
                        stats["skipped_empty"] += 1
                        continue
                    
                    payload = build_daily_payload(dom, day, tz, active, cfg.log_level)
                    
                    os.makedirs(os.path.dirname(out), exist_ok=True)
                    tmp = out + ".tmp"
                    with open(tmp, "w", encoding="utf-8") as f:
                        json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp, out)
                    
                    stats["written"] += 1
                    
                except Exception as e:
                    log("ERROR", f"Fehler {day.isoformat()} {dom}: {e}", cfg.log_level)
                    stats["failed"] += 1
                    exit_code = 1
        
        if stats["written"] > 0:
            update_heartbeat(tz, cfg.log_level)
        
        if stats["failed"] > 0:
            exit_code = 1
        
    except ValueError as e:
        log("ERROR", f"Konfiguration: {e}")
        exit_code = 1
    except Exception as e:
        log("ERROR", f"Absturz: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 2
    finally:
        log("INFO", f"Fertig: {stats}")
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
