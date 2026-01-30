#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Truth Layer)
Version: 2026.1.30-roals-final

Purpose:
- Reads entity definitions from registry.json
- Fetches 24h history from Home Assistant (Yesterday)
- Uses Adaptive Timeouts, Backoff & Chunking
- Writes raw data to daily domain files using Atomic Writes
- Enforces ROALS metadata presence (roals_id, exporter_domain)

Usage:
  python3 exporter.py [--date YYYY-MM-DD] [--out ./data] [--timezone Europe/Berlin]
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
from typing import Any, Dict, List, Optional, Tuple, Generator

# Try importing ZoneInfo (Python 3.9+)
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: ZoneInfo not found. Please use Python 3.9+ or install backports.zoneinfo")
        sys.exit(1)

# --- CONFIGURATION ---
HA_URL = os.getenv("HA_API_URL", "http://localhost:8123").rstrip("/")
HA_TOKEN = os.getenv("HA_API_TOKEN", "")

DEFAULT_REGISTRY = Path("registry.json")
DEFAULT_OUT_DIR = Path("data")

# ROALS Policies
TIMEOUTS = [30, 60, 90]
BACKOFFS = [2, 5]
CHUNK_SIZE = 50  # Entities per API request to avoid URL length limits

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("ROALS-Exporter")


# --- HELPER FUNCTIONS ---

def get_headers() -> Dict[str, str]:
    if not HA_TOKEN:
        logger.error("HA_API_TOKEN not set in environment.")
        sys.exit(1)
    return {
        "Authorization": f"Bearer {HA_TOKEN}",
        "content-type": "application/json",
    }

def calculate_file_hash(path: Path) -> str:
    """Calculates SHA-256 hash of a file for lineage tracking."""
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def chunk_list(lst: List[Any], n: int) -> Generator[List[Any], None, None]:
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def write_json_atomic(path: Path, data: Any) -> None:
    """
    ROALS Durable Atomic Write.
    Includes Safety Polish: UnboundLocalError protection & O_DIRECTORY flag.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None # Polish 1: Init to avoid UnboundLocalError
    
    try:
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.",
            suffix=".tmp",
            dir=str(path.parent),
            text=True
        )
        
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
            
        # Atomic Move
        os.replace(tmp_name, str(path))

        # Directory Sync (Durability)
        try:
            if hasattr(os, 'open') and hasattr(os, 'fsync'):
                # Polish 2: Use O_DIRECTORY if available
                flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                dir_fd = os.open(str(path.parent), flags)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
        except OSError:
            pass 

    except Exception as e:
        logger.error(f"Failed to write {path}: {e}")
        # Polish 1: Check if tmp_name exists before cleanup
        if tmp_name and os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass
        raise

# --- CORE LOGIC ---

def fetch_history_chunked(
    session: requests.Session,
    entity_ids: List[str],
    start_iso: str,
    end_iso: str,
) -> Tuple[List[Dict] | None, str]:
    """
    Fetches history in chunks to avoid URL limits.
    Aggregates results or reports PARTIAL/FAIL.
    """
    all_history = []
    failed_chunks = 0
    total_chunks = 0
    
    base_url = f"{HA_URL}/api/history/period/{start_iso}"
    
    # Iterate over chunks
    for chunk in chunk_list(entity_ids, CHUNK_SIZE):
        total_chunks += 1
        params = {
            "filter_entity_id": ",".join(chunk),
            "end_time": end_iso,
            "minimal_response": "true", 
            "no_attributes": "true" 
        }
        
        chunk_success = False
        
        # Retry Loop for this chunk
        for attempt, timeout in enumerate(TIMEOUTS, 1):
            try:
                response = session.get(base_url, params=params, timeout=timeout)

                if response.status_code == 200:
                    try:
                        data = response.json()
                        # Flatten HA response
                        flat = [item for sublist in data for item in sublist]
                        all_history.extend(flat)
                        chunk_success = True
                        break # Success, move to next chunk
                    except ValueError:
                        logger.error(f"Chunk {total_chunks}: Invalid JSON.")
                        break # No retry on bad json

                elif response.status_code in (401, 403):
                    logger.critical("Auth failed. Aborting all.")
                    return None, "AUTH_FAIL"
                
                elif 400 <= response.status_code < 500:
                    logger.error(f"Client Error ({response.status_code}).")
                    break # No retry

                else:
                    response.raise_for_status() # 5xx -> Retry

            except requests.exceptions.RequestException as e:
                if attempt < len(TIMEOUTS):
                    time.sleep(BACKOFFS[attempt-1])
                else:
                    logger.warning(f"Chunk {total_chunks} failed after retries: {e}")
        
        if not chunk_success:
            failed_chunks += 1

    # Determine Final Status
    if failed_chunks == 0:
        return all_history, "OK"
    elif failed_chunks < total_chunks:
        return all_history, f"PARTIAL_{failed_chunks}_FAILED"
    else:
        return None, "ALL_CHUNKS_FAILED"

def load_and_validate_registry(path: Path) -> Tuple[Dict[str, Any], str]:
    """
    Loads registry and calculates hash.
    Enforces P0: entity_id, roals_id, exporter_domain required.
    """
    if not path.exists():
        logger.error(f"Registry not found: {path}")
        sys.exit(1)
        
    # Calculate Hash first (Source of Truth Lineage)
    reg_hash = calculate_file_hash(path)
    
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.error(f"Failed to read registry: {e}")
        sys.exit(1)
        
    valid_registry = {}
    skipped_count = 0
    
    for eid, meta in data.items():
        # P0: Strict Fields
        if not meta.get("roals_id") or not meta.get("exporter_domain"):
            logger.warning(f"Skipping {eid}: Missing roals_id or exporter_domain")
            skipped_count += 1
            continue
            
        valid_registry[eid] = meta
        
    if skipped_count > 0:
        logger.warning(f"Skipped {skipped_count} entities due to missing required ROALS fields.")
        
    return valid_registry, reg_hash

# --- MAIN ---

def main():
    parser = argparse.ArgumentParser(description="ROALS Exporter A (Final)")
    parser.add_argument("--date", help="Target date YYYY-MM-DD (default: yesterday)")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY), help="Path to registry.json")
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR), help="Output base directory")
    parser.add_argument("--timezone", default="Europe/Berlin", help="Local timezone (e.g. Asia/Manila)")
    parser.add_argument("--system-id", default="unknown", help="Identifier of the capture system")
    parser.add_argument("--mode", choices=["raw", "roals"], default="raw", help="Output format (raw=HA History, roals=Reserved)")
    args = parser.parse_args()

    # 1. Setup Timezone & Date
    try:
        tz = ZoneInfo(args.timezone)
    except Exception as e:
        logger.error(f"Invalid timezone '{args.timezone}': {e}")
        sys.exit(1)

    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d").date()
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        # Default: Capture yesterday relative to the timezone
        target_date = datetime.now(tz).date() - timedelta(days=1)

    # Time window (Midnight to Midnight in local TZ)
    start_dt = datetime.combine(target_date, dtime.min).replace(tzinfo=tz)
    end_dt = datetime.combine(target_date, dtime.max).replace(tzinfo=tz)
    
    # Convert to ISO (HA expects simple ISO or UTC)
    start_iso = start_dt.isoformat()
    end_iso = end_dt.isoformat()

    logger.info(f"--- Export {target_date} ({args.timezone}) ---")
    
    # 2. Load Registry
    registry, reg_hash = load_and_validate_registry(Path(args.registry))
    
    # 3. Group by Domain & Build Meta Map
    domain_map = defaultdict(list)
    entity_meta_map = {} # Context for meta block
    
    for entity_id, meta in registry.items():
        domain = meta.get("exporter_domain")
        domain_map[domain].append(entity_id)
        
        # P0: Metadata Mapping (keep it minimal but useful)
        entity_meta_map[entity_id] = {
            "roals_id": meta.get("roals_id"),
            "area_id": meta.get("area_id"), # Optional
            "profile": meta.get("profile")  # Optional
        }

    # 4. Process
    base_out_dir = Path(args.out) / target_date.strftime("%Y-%m-%d")
    base_out_dir.mkdir(parents=True, exist_ok=True)

    # Init Session
    session = requests.Session()
    session.headers.update(get_headers())

    success_count = 0
    fail_count = 0

    for domain, entities in domain_map.items():
        if not entities: continue

        logger.info(f"Domain '{domain}': Fetching {len(entities)} entities...")
        
        data, status = fetch_history_chunked(session, entities, start_iso, end_iso)
        
        # Filter entity_meta_map to only include entities in this domain
        domain_entities_meta = {e: entity_meta_map[e] for e in entities if e in entity_meta_map}

        # P0: Rich Metadata
        output_payload = {
            "meta": {
                "version": "2026.1.30",
                "system_id": args.system_id,
                "domain": domain,
                "date": str(target_date),
                "timezone": args.timezone,
                "window": {"start": start_iso, "end": end_iso},
                "generated_at": datetime.now(tz).isoformat(),
                "fetch_status": status,
                "entity_count": len(entities),
                "datapoint_count": len(data) if data else 0,
                "registry_hash": reg_hash,
                "entities": domain_entities_meta # ROALS Context
            },
            "data": data if data else []
        }
        
        out_file = base_out_dir / f"{domain}.json"
        
        try:
            write_json_atomic(out_file, output_payload)
            if "OK" in status:
                success_count += 1
            else:
                fail_count += 1
                logger.warning(f"Domain '{domain}' written with status {status}")
        except Exception as e:
            fail_count += 1
            logger.error(f"Failed to write {out_file}: {e}")

    logger.info(f"--- Finished. Success: {success_count}, Failed/Partial: {fail_count} ---")
    
    if fail_count > 0:
        sys.exit(1)
    sys.exit(0)

if __name__ == "__main__":
    main()
