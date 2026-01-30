#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Daily Truth Engine)
Version: 2026.01.31-ROALS-FINAL-PATCHED-V2

Features:
- Primary: daily_truth (288-slot 5min timeseries)
- Optional: raw_snapshot (FULL forensic event dump with attributes)
- Registry-First: Enforces roals_id and domain strictness
- Data Types: Supports Numeric (Float) and Binary State (0/1) mapping
- Durable: Atomic writes with fsync and directory sync
- Forensic: SHA-256 integrity hashing & rich metadata
- Optimization: Linear O(N) slotting algorithm
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
from typing import Any, Dict, List, Tuple, Generator, Optional

# Try importing ZoneInfo (Python 3.9+)
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
BINARY_TRUE_VALUES = {"on", "open", "true", "detected", "home", "active"}

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

# P0.1: Policy Normalization
def normalize_policy(policy: str) -> str:
    p = str(policy).lower().strip()
    if p in ("avg", "average"): return "mean"
    return p

def map_events_to_slots(events: List[Dict], start_dt: datetime, raw_policy: str, is_binary: bool) -> List[Any]:
    """
    Maps HA history events to 288 slots using optimized O(N) linear scan.
    Handles Numeric (float) and Binary (0/1).
    """
    slots = [None] * SLOTS_PER_DAY
    parsed_events = []
    
    policy = normalize_policy(raw_policy)

    # 1. Parse Events (Prepare)
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
    
    # 2. Linear Slotting (Two-Pointer Optimization P1.1)
    event_idx = 0
    num_events = len(parsed_events)
    last_known = None

    # Fast forward: Find last_known value BEFORE the first slot (LOCF initialization)
    # The start_dt is the beginning of the day. fetch_history includes LOOKBACK_MIN.
    while event_idx < num_events and parsed_events[event_idx][0] <= start_dt:
        last_known = parsed_events[event_idx][1]
        event_idx += 1

    for i in range(SLOTS_PER_DAY):
        slot_end = start_dt + timedelta(minutes=(i + 1) * RASTER_MINUTES)
        
        vals_in_slot = []
        
        # Collect all events strictly within this slot
        # Condition: start < event <= end
        # We already passed 'start' (it was 'end' of previous loop)
        while event_idx < num_events and parsed_events[event_idx][0] <= slot_end:
            val = parsed_events[event_idx][1]
            vals_in_slot.append(val)
            last_known = val # Update LOCF tracking
            event_idx += 1
            
        # Determine effective values for aggregation
        if not vals_in_slot:
            if last_known is not None:
                effective_values = [last_known]
            else:
                slots[i] = None # Gap/Missing
                continue
        else:
            effective_values = vals_in_slot

        # 3. Apply Policy
        if is_binary:
            # For binary, we stick to 'last' state usually, or max (1 if any true happened)
            slots[i] = effective_values[-1]
        else:
            if policy == "max": slots[i] = max(effective_values)
            elif policy == "min": slots[i] = min(effective_values)
            elif policy == "mean": slots[i] = round(sum(effective_values) / len(effective_values), 4)
            elif policy == "sum": slots[i] = sum(effective_values)
            else: slots[i] = effective_values[-1] # Default: last
            
    return slots

def fetch_history(session, entity_ids, start_dt, end_dt, ha_url, mode):
    """Fetches history in chunks. Adapts params based on mode (P0.3)."""
    all_data = defaultdict(list)
    start_iso = start_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    
    chunk_size = 50
    for i in range(0, len(entity_ids), chunk_size):
        chunk = entity_ids[i:i+chunk_size]
        url = f"{ha_url}/api/history/period/{start_iso}"
        
        params = {"filter_entity_id": ",".join(chunk), "end_time": end_iso}
        
        # P0.3: Raw Snapshot = Full Data. Daily Truth = Minimal.
        if mode == "daily_truth":
            params["minimal_response"] = "1"
            params["no_attributes"] = "1"
        # else: raw_snapshot gets full attributes/state objects
        
        for attempt in [1, 2, 3]:
            try:
                resp = session.get(url, params=params, timeout=60)
                if resp.status_code == 200:
                    for entity_list in resp.json():
                        if entity_list: all_data[entity_list[0]["entity_id"]] = entity_list
                    break
                elif resp.status_code in (401, 403):
                    logger.error("Auth failed.")
                    sys.exit(1)
            except Exception as e:
                if attempt == 3: logger.error(f"Chunk fetch failed: {e}")
                time.sleep(2)
                
    return all_data

def main():
    parser = argparse.ArgumentParser(description="ROALS Exporter A")
    parser.add_argument("--date", help="YYYY-MM-DD")
    parser.add_argument("--registry", default="registry.json")
    parser.add_argument("--out", default="data")
    parser.add_argument("--timezone", default="Asia/Manila")
    parser.add_argument("--system-id", default=os.getenv("ROALS_SYSTEM_ID", "porac_main"))
    parser.add_argument("--mode", choices=["daily_truth", "raw_snapshot"], default="daily_truth")
    args = parser.parse_args()

    # Add-on Option override
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
        except Exception as e:
            logger.warning(f"Failed to read addon options: {e}")

    try:
        tz = ZoneInfo(args.timezone)
    except:
        logger.error(f"Invalid timezone: {args.timezone}")
        sys.exit(1)

    day = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now(tz).date() - timedelta(days=1)
    
    start_dt = datetime.combine(day, dtime.min).replace(tzinfo=tz)
    end_dt = start_dt + timedelta(days=1) # Exclusive end
    fetch_start = start_dt - timedelta(minutes=HISTORY_LOOKBACK_MIN)

    reg_path = Path(args.registry)
    if not reg_path.exists():
        logger.error(f"Registry not found: {reg_path}")
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

    domains = set(m["exporter_domain"] for m in valid_registry.values())
    session = requests.Session()
    session.headers.update(get_headers())

    for dom in domains:
        entities = {eid: m for eid, m in valid_registry.items() if m["exporter_domain"] == dom}
        if not entities: continue

        logger.info(f"Exporting {dom} ({len(entities)} entities) in {args.mode} mode...")
        
        try:
            # Pass mode to fetch_history for P0.3
            raw_history = fetch_history(session, list(entities.keys()), fetch_start, end_dt, ha_url, args.mode)
            
            meta_block = {
                "version": "2026.DT" if args.mode == "daily_truth" else "2026.RAW",
                "mode": args.mode,
                "domain": dom,
                "date": str(day),
                "timezone": args.timezone,
                "system_id": args.system_id,
                "registry_hash": registry_hash,
                "generated_at": datetime.now(tz).isoformat(),
                "window": {
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat()
                },
                "entities": {}
            }

            for eid, m in entities.items():
                meta_block["entities"][eid] = {
                    "roals_id": m.get("roals_id"),
                    "area_id": m.get("area_id"),
                    "profile": m.get("profile")
                }

            if args.mode == "daily_truth":
                ts_iso = [(start_dt + timedelta(minutes=5*i)).isoformat() for i in range(SLOTS_PER_DAY)]
                timeseries = {"ts_iso": ts_iso}
                
                for eid, meta in entities.items():
                    metric = meta.get("metric", {})
                    policy = metric.get("agg_policy", {}).get("primary", "last")
                    kind = metric.get("kind", "numeric")
                    profile = str(meta.get("profile", "")).lower()
                    
                    # P0.2: Strict Binary Detection
                    is_binary = False
                    if (kind == "event_state" or 
                        "contact_state" in profile or 
                        "binary_state" in profile or
                        eid.startswith("binary_sensor.")):
                        is_binary = True
                    
                    timeseries[eid] = map_events_to_slots(
                        raw_history.get(eid, []), 
                        start_dt, 
                        policy, 
                        is_binary
                    )
                
                payload = {"meta": meta_block, "timeseries": timeseries}
            else:
                # Raw Snapshot with full attributes
                payload = {"meta": meta_block, "data": raw_history}

            payload["meta"]["integrity_hash"] = calculate_integrity_hash(payload)
            out_path = Path(args.out) / "daily" / dom / f"{day}.json"
            write_atomic_json(out_path, payload)
            
        except Exception as e:
            logger.error(f"Failed domain {dom}: {e}", exc_info=True)

if __name__ == "__main__":
    main()
