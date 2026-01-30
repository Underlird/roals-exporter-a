#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Truth Layer)
Version: 2026.1.30-roals-final-tokenpatch

Purpose:
- Reads entity definitions from registry.json
- Fetches 24h history from Home Assistant (Yesterday)
- Uses Adaptive Timeouts, Backoff & Chunking
- Writes raw data to daily domain files using Atomic Writes
- Auto-detects HA Addon environment & Tokens (SUPERVISOR_TOKEN)
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
# Default URL: Lokal für Laptop, Supervisor für Add-on (wird in main() korrigiert)
HA_URL = os.getenv("HA_API_URL", "http://localhost:8123").rstrip("/")

DEFAULT_REGISTRY = Path("registry.json")
DEFAULT_OUT_DIR = Path("data")

# ROALS Policies
TIMEOUTS = [30, 60, 90]
BACKOFFS = [2, 5]
CHUNK_SIZE = 50  

# --- LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("ROALS-Exporter")


# --- HELPER FUNCTIONS ---

def get_headers() -> Dict[str, str]:
    """
    Holt das Token aus der Umgebung. 
    Unterstützt HA_API_TOKEN (Manuell/Laptop) und SUPERVISOR_TOKEN (Add-on).
    """
    token = os.getenv("HA_API_TOKEN") or os.getenv("SUPERVISOR_TOKEN")
    if not token:
        logger.error("Weder HA_API_TOKEN noch SUPERVISOR_TOKEN in Umgebung gefunden.")
        sys.exit(1)
    return {
        "Authorization": f"Bearer {token}",
        "content-type": "application/json",
    }

def calculate_file_hash(path: Path) -> str:
    sha256_hash = hashlib.sha256()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def chunk_list(lst: List[Any], n: int) -> Generator[List[Any], None, None]:
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def write_json_atomic(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None 
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent), text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, str(path))
        try:
            if hasattr(os, 'open') and hasattr(os, 'fsync'):
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
        if tmp_name and os.path.exists(tmp_name):
            try:
                os.remove(tmp_name)
            except OSError:
                pass
        raise

# --- CORE LOGIC ---

def fetch_history_chunked(session: requests.Session, entity_ids: List[str], start_iso: str, end_iso: str, url_base: str) -> Tuple[List[Dict] | None, str]:
    all_history = []
    failed_chunks = 0
    total_chunks = 0
    
    # Korrekter API-Endpoint für History
    api_url = f"{url_base}/api/history/period/{start_iso}"
    
    for chunk in chunk_list(entity_ids, CHUNK_SIZE):
        total_chunks += 1
        params = {
            "filter_entity_id": ",".join(chunk),
            "end_time": end_iso,
            "minimal_response": "true", 
            "no_attributes": "true" 
        }
        
        chunk_success = False
        for attempt, timeout in enumerate(TIMEOUTS, 1):
            try:
                response = session.get(api_url, params=params, timeout=timeout)
                if response.status_code == 200:
                    data = response.json()
                    flat = [item for sublist in data for item in sublist]
                    all_history.extend(flat)
                    chunk_success = True
                    break 
                elif response.status_code in (401, 403):
                    return None, "AUTH_FAIL"
                else:
                    response.raise_for_status()
            except Exception as e:
                if attempt < len(TIMEOUTS):
                    time.sleep(BACKOFFS[attempt-1])
        if not chunk_success:
            failed_chunks += 1

    if failed_chunks == 0: return all_history, "OK"
    return all_history, f"PARTIAL_{failed_chunks}_FAILED"

def load_and_validate_registry(path: Path) -> Tuple[Dict[str, Any], str]:
    if not path.exists():
        logger.error(f"Registry nicht gefunden: {path}")
        sys.exit(1)
    reg_hash = calculate_file_hash(path)
    data = json.loads(path.read_text(encoding="utf-8"))
    valid_registry = {k: v for k, v in data.items() if v.get("roals_id") and v.get("exporter_domain")}
    return valid_registry, reg_hash

# --- MAIN ---

def main():
    global HA_URL
    parser = argparse.ArgumentParser(description="ROALS Exporter A (Token Patch)")
    parser.add_argument("--date", help="Target date YYYY-MM-DD")
    parser.add_argument("--registry", default=str(DEFAULT_REGISTRY))
    parser.add_argument("--out", default=str(DEFAULT_OUT_DIR))
    parser.add_argument("--timezone", default="Europe/Berlin")
    parser.add_argument("--system-id", default="um890pro")
    args = parser.parse_args()

    # --- ADDON DETEKTION ---
    options_path = Path("/data/options.json")
    if options_path.exists():
        # Wir sind im Add-on Modus -> Nutze interne Supervisor-URL
        HA_URL = "http://supervisor/core"
        try:
            with open(options_path, "r") as f:
                opts = json.load(f)
            args.registry = opts.get("registry_path", args.registry)
            args.out = opts.get("data_root", args.out)
            args.timezone = opts.get("timezone", args.timezone)
            args.date = opts.get("target_date", args.date)
            logger.info(f"Add-on Mode aktiv. URL: {HA_URL}")
        except:
            pass

    try:
        tz = ZoneInfo(args.timezone)
    except:
        tz = ZoneInfo("UTC")

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else datetime.now(tz).date() - timedelta(days=1)
    start_iso = datetime.combine(target_date, dtime.min).replace(tzinfo=tz).isoformat()
    end_iso = datetime.combine(target_date, dtime.max).replace(tzinfo=tz).isoformat()

    registry, reg_hash = load_and_validate_registry(Path(args.registry))
    domain_map = defaultdict(list)
    entity_meta_map = {}
    
    for eid, meta in registry.items():
        domain_map[meta["exporter_domain"]].append(eid)
        entity_meta_map[eid] = {"roals_id": meta["roals_id"], "area_id": meta.get("area_id"), "profile": meta.get("profile")}

    base_out_dir = Path(args.out) / target_date.strftime("%Y-%m-%d")
    base_out_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(get_headers())

    for domain, entities in domain_map.items():
        logger.info(f"Exportiere Domain: {domain}")
        data, status = fetch_history_chunked(session, entities, start_iso, end_iso, HA_URL)
        
        output_payload = {
            "meta": {
                "version": "2026.1.30-patch",
                "system_id": args.system_id,
                "domain": domain,
                "date": str(target_date),
                "timezone": args.timezone,
                "generated_at": datetime.now(tz).isoformat(),
                "fetch_status": status,
                "registry_hash": reg_hash,
                "entities": {e: entity_meta_map[e] for e in entities}
            },
            "data": data or []
        }
        write_json_atomic(base_out_dir / f"{domain}.json", output_payload)

    logger.info("Export abgeschlossen.")

if __name__ == "__main__":
    main()

