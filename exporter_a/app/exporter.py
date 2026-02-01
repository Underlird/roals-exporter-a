#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Daily Truth Engine)
Version: 2026.02.05-ROALS-PLATINUM-HARDENED

Features:
- Primary: daily_truth (288-slot 5min timeseries)
- Optional: raw_snapshot (FULL forensic event dump with attributes)
- Registry-First: Enforces roals_id and domain strictness upfront
- Robustness: JSON-Safety, Exponential Backoff, clean binary detection
- Platinum: Self-describing raster & embedded data quality summary
"""

import argparse
import hashlib
import json
import logging
import os
import sys
import tempfile
import time
import requests
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Generator, Optional, Callable

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: ZoneInfo not found. Please use Python 3.9+ or install backports.zoneinfo")
        sys.exit(1)

# ROALS Constants
SLOTS_PER_DAY = 288
RASTER_MINUTES = 5
HISTORY_LOOKBACK_MIN = 30
BINARY_TRUE_VALUES = {"on", "open", "true", "detected", "home", "active", "occupied"}

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ROALS-Exporter")

def get_headers():
    token = os.getenv("SUPERVISOR_TOKEN") or os.getenv("HA_API_TOKEN")
    if not token:
        logger.error("No token found (SUPERVISOR_TOKEN or HA_API_TOKEN).")
        sys.exit(1)
    return {"Authorization": f"Bearer {token}", "content-type": "application/json"}

def calculate_integrity_hash(data: dict) -> str:
    data_copy = data.copy()
    if 'meta' in data_copy:
        meta_copy = data_copy['meta'].copy()
        meta_copy.pop('integrity_hash', None)
        data_copy['meta'] = meta_copy
    canonical = json.dumps(data_copy, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()

def calculate_file_hash(path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def write_atomic_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent), text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, str(path))
        if hasattr(os, 'open'):
            try:
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                dfd = os.open(str(path.parent), flags)
                try: os.fsync(dfd)
                finally: os.close(dfd)
            except OSError: pass
    except Exception as e:
        if tmp_name and os.path.exists(tmp_name): os.remove(tmp_name)
        raise e

def normalize_policy(policy: str) -> str:
    p = str(policy).lower().strip()
    if p in ("avg", "average"): return "mean"
    return p

# --- Helper: Binary Detection ---
def detect_binary_mode(eid: str, kind: str, profile: str) -> bool:
    """Determines if an entity should be treated as binary (0/1)."""
    if kind == "event_state":
        return True
    if eid.startswith("binary_sensor."):
        return True
    if "contact_state" in profile or "binary_state" in profile:
        return True
    return False

# --- Helper: Policy Strategies ---
def policy_max(vals): return max(vals)
def policy_min(vals): return min(vals)
def policy_sum(vals): return sum(vals)
def policy_mean(vals): return round(sum(vals) / len(vals), 4)
def policy_last(vals): return vals[-1]

POLICY_MAP: Dict[str, Callable] = {
    "max": policy_max,
    "min": policy_min,
    "sum": policy_sum,
    "mean": policy_mean,
    "last": policy_last
}

def map_events_to_slots(events: List[Dict], start_dt: datetime, raw_policy: str, is_binary: bool) -> List[Any]:
    slots = [None] * SLOTS_PER_DAY
    parsed_events = []
    
    policy_key = normalize_policy(raw_policy)
    # Default to 'last' if policy unknown
    agg_func = POLICY_MAP.get(policy_key, policy_last)

    # 1. Parse Events
    for e in events:
        state = e.get("state")
        if state in (None, "unknown", "unavailable", ""): continue
        
        ts_str = e.get("last_changed")
        if not ts_str: continue
        
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            if is_binary:
                val = 1 if str(state).lower() in BINARY_TRUE_VALUES else 0
            else:
                val = float(state)
            parsed_events.append((ts, val))
        except (ValueError, TypeError):
            continue
    
    parsed_events.sort(key=lambda x: x[0])
    
    # 2. Linear Slotting
    event_idx = 0
    num_events = len(parsed_events)
    last_known = None

    # LOCF Initialization
    while event_idx < num_events and parsed_events[event_idx][0] <= start_dt:
        last_known = parsed_events[event_idx][1]
        event_idx += 1
    
    # Pre-calc slot duration
    slot_delta = timedelta(minutes=RASTER_MINUTES)
    current_slot_end = start_dt + slot_delta

    for i in range(SLOTS_PER_DAY):
        vals_in_slot = []
        
        while event_idx < num_events and parsed_events[event_idx][0] <= current_slot_end:
            val = parsed_events[event_idx][1]
            vals_in_slot.append(val)
            last_known = val
            event_idx += 1
            
        if not vals_in_slot:
            if last_known is not None:
                slots[i] = last_known
            # Else remains None
        else:
            if is_binary:
                slots[i] = vals_in_slot[-1]
            else:
                slots[i] = agg_func(vals_in_slot)
        
        current_slot_end += slot_delta
            
    return slots

def fetch_history(session, entity_ids, start_dt, end_dt, ha_url, mode):
    all_data = defaultdict(list)
    start_iso = start_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    
    chunk_size = 50
    for i in range(0, len(entity_ids), chunk_size):
        chunk = entity_ids[i:i+chunk_size]
        url = f"{ha_url}/api/history/period/{start_iso}"
        params = {"filter_entity_id": ",".join(chunk), "end_time": end_iso}
        
        if mode == "daily_truth":
            params["minimal_response"] = "1"
            params["no_attributes"] = "1"
        
        for attempt in [1, 2, 3]:
            try:
                resp = session.get(url, params=params, timeout=60)
                if resp.status_code == 200:
                    # Robustness: Safe JSON parsing
                    data = resp.json()
                    for entity_list in data:
                        if entity_list and len(entity_list) > 0:
                            eid = entity_list[0].get("entity_id")
                            if eid:
                                all_data[eid] = entity_list
                    break
                elif resp.status_code in (401, 403):
                    logger.error("Auth failed.")
                    sys.exit(1)
                else:
                    logger.warning(f"API Error {resp.status_code}")
                    # Allow retry on 5xx
            except (json.JSONDecodeError, requests.RequestException, IndexError, KeyError) as e:
                logger.warning(f"Chunk fetch error (attempt {attempt}): {e}")
            
            if attempt < 3:
                # Exponential Backoff: 1s, 2s, 4s
                time.sleep(2 ** (attempt - 1))
            else:
                logger.error("Chunk failed after 3 retries.")
                
    return all_data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="registry.json")
    parser.add_argument("--out", default="data")
    parser.add_argument("--timezone", default="Asia/Manila")
    parser.add_argument("--mode", choices=["daily_truth", "raw_snapshot"], default="daily_truth")
    parser.add_argument("--date", help="Target Day")
    parser.add_argument("--start-date", help="Range Start")
    parser.add_argument("--end-date", help="Range End")
    parser.add_argument("--all-domains", action="store_true")
    parser.add_argument("--domain", help="Specific Domain")
    args = parser.parse_args()

    opt_path = Path("/data/options.json")
    ha_url = "http://localhost:8123"
    
    if opt_path.exists():
        ha_url = "http://supervisor/core"
        try:
            with open(opt_path) as f:
                opts = json.load(f)
                args.registry = opts.get("registry_path", args.registry)
                args.out = opts.get("data_root", args.out)
                args.timezone = opts.get("timezone", args.timezone)
                args.mode = opts.get("run_mode", args.mode)
                
                if opts.get("process_all_domains"):
                    args.all_domains = True
                else:
                    args.domain = opts.get("exporter_domain")
                
                if opts.get("start_date") and opts.get("end_date"):
                    args.start_date = opts["start_date"]
                    args.end_date = opts["end_date"]
                else:
                    args.date = opts.get("target_date")
        except Exception as e:
            logger.warning(f"Failed to read addon options: {e}")

    try:
        tz = ZoneInfo(args.timezone)
        target_dates = []
        if args.start_date and args.end_date:
            s = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            e = datetime.strptime(args.end_date, "%Y-%m-%d").date()
            curr = s
            while curr <= e:
                target_dates.append(curr)
                curr += timedelta(days=1)
            logger.info(f"Batch Mode: Processing range {s} to {e} ({len(target_dates)} days)")
        elif args.date:
            target_dates.append(datetime.strptime(args.date, "%Y-%m-%d").date())
        else:
            target_dates.append(datetime.now(tz).date() - timedelta(days=1))
    except Exception as e:
        logger.error(f"Date Error: {e}")
        sys.exit(1)

    reg_path = Path(args.registry)
    if not reg_path.exists():
        logger.error(f"Registry missing: {reg_path}")
        sys.exit(1)
    
    registry_hash = calculate_file_hash(reg_path)
    with open(reg_path) as f: raw_registry = json.load(f)
    
    valid_registry = {}
    skipped_count = 0
    for eid, meta in raw_registry.items():
        if meta.get("roals_id") and meta.get("exporter_domain"):
            valid_registry[eid] = meta
        else:
            skipped_count += 1
    
    if skipped_count > 0:
        logger.warning(f"Skipped {skipped_count} registry entries (missing roals_id/domain).")
    
    # Performance: sorted set
    available_domains = sorted(set(m["exporter_domain"] for m in valid_registry.values()))
    target_domains = []
    
    if args.all_domains:
        target_domains = available_domains
        logger.info(f"Processing ALL {len(target_domains)} domains.")
    elif args.domain:
        if args.domain in available_domains:
            target_domains = [args.domain]
        else:
            logger.warning(f"Domain '{args.domain}' empty or not in valid registry.")
    
    if not target_domains:
        logger.error("No valid domains selected.")
        sys.exit(1)

    session = requests.Session()
    session.headers.update(get_headers())
    system_id = os.getenv("ROALS_SYSTEM_ID", "porac_main")
    total_ops = len(target_dates) * len(target_domains)
    curr_op = 0

    for day in target_dates:
        start_dt = datetime.combine(day, dtime.min).replace(tzinfo=tz)
        end_dt = start_dt + timedelta(days=1)
        fetch_start = start_dt - timedelta(minutes=HISTORY_LOOKBACK_MIN)

        for dom in target_domains:
            curr_op += 1
            logger.info(f"[{curr_op}/{total_ops}] Processing {dom} for {day}...")
            
            entities = {eid: m for eid, m in valid_registry.items() if m["exporter_domain"] == dom}
            if not entities: continue
            
            try:
                raw_history = fetch_history(session, list(entities.keys()), fetch_start, end_dt, ha_url, args.mode)
                
                meta_block = {
                    "version": "2026.DT" if args.mode == "daily_truth" else "2026.RAW",
                    "mode": args.mode,
                    "domain": dom,
                    "date": str(day),
                    "timezone": args.timezone,
                    "system_id": system_id,
                    "registry_hash": registry_hash,
                    "generated_at": datetime.now(tz).isoformat(),
                    "window": {"start": start_dt.isoformat(), "end": end_dt.isoformat()},
                    "entities": {}
                }
                
                for eid, m in entities.items():
                    meta_block["entities"][eid] = {
                        "roals_id": m.get("roals_id"),
                        "area_id": m.get("area_id"),
                        "room_id": m.get("room_id"),
                        "profile": m.get("profile")
                    }

                if args.mode == "daily_truth":
                    ts_iso = [(start_dt + timedelta(minutes=5*i)).isoformat() for i in range(SLOTS_PER_DAY)]
                    timeseries = {"ts_iso": ts_iso}
                    
                    for eid, meta in entities.items():
                        metric = meta.get("metric", {})
                        # Refactored Policy Access
                        policy = metric.get("agg_policy", {}).get("primary") or meta.get("agg_policy", {}).get("primary", "last")
                        
                        kind = metric.get("kind") or meta.get("kind", "numeric")
                        profile = str(meta.get("profile", "")).lower()
                        
                        # Refactored Binary Detection
                        is_binary = detect_binary_mode(eid, kind, profile)
                        
                        timeseries[eid] = map_events_to_slots(
                            raw_history.get(eid, []), 
                            start_dt, 
                            policy, 
                            is_binary
                        )
                    
                    payload = {"meta": meta_block, "timeseries": timeseries}

                    # --- PLATINUM SUMMARY ---
                    summary = {
                        "generated_at": datetime.now(tz).isoformat(),
                        "total_entities": len(entities),
                        "columns_total": 0,
                        "worst_coverage_pct": 100.0,
                        "columns": {}
                    }

                    col_count = 0
                    for col_name, values in timeseries.items():
                        if col_name == "ts_iso": continue
                        
                        col_count += 1
                        valid_points = sum(1 for v in values if v is not None)
                        coverage = round((valid_points / SLOTS_PER_DAY) * 100, 1)
                        
                        summary["columns"][col_name] = {
                            "valid_points": valid_points,
                            "null_points": SLOTS_PER_DAY - valid_points,
                            "coverage_pct": coverage
                        }
                        
                        if coverage < summary["worst_coverage_pct"]:
                            summary["worst_coverage_pct"] = coverage

                    summary["columns_total"] = col_count
                    payload["summary"] = summary
                    # --- END SUMMARY ---

                else:
                    payload = {"meta": meta_block, "data": raw_history}

                payload["meta"]["integrity_hash"] = calculate_integrity_hash(payload)
                
                out_path = Path(args.out) / "daily" / dom / f"{day}_{dom}.json"
                write_atomic_json(out_path, payload)
                
            except Exception as e:
                logger.error(f"Failed {dom} on {day}: {e}", exc_info=True)

if __name__ == "__main__":
    main()
