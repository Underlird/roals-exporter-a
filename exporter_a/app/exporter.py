#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Final Dual-Mode)
- Erkennt automatisch Supervisor-Auth (Add-on) oder Manuelle-Auth (Laptop).
- Implementiert Adaptive Timeouts: 30s -> 60s -> 90s.
- Nutzt Durable Atomic Writes für maximale Datensicherheit.
"""

import os
import json
import logging
import sys
import tempfile
import time
import requests
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

# --- AUTH-ERKENNUNG (ROALS Standard) ---
# Falls HA_API_URL nicht gesetzt ist, wird der interne Supervisor-Pfad genutzt.
HA_URL = os.getenv("HA_API_URL", "http://supervisor/core/api").rstrip("/")
# Priorität: 1. Manueller Token (Laptop) -> 2. Supervisor Token (Add-on)
HA_TOKEN = os.getenv("HA_API_TOKEN", os.getenv("SUPERVISOR_TOKEN", ""))

# ROALS Timeout Policy: 30s -> 60s -> 90s
TIMEOUTS = [30, 60, 90]
BACKOFFS = [2, 5]

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ROALS-Exporter")

def get_headers() -> dict:
    if not HA_TOKEN:
        logger.error("Kein API-Token gefunden (weder HA_API_TOKEN noch SUPERVISOR_TOKEN).")
        sys.exit(1)
    return {"Authorization": f"Bearer {HA_TOKEN}", "content-type": "application/json"}

def write_json_atomic(path: Path, data: Any):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = ""
    try:
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent), text=True)
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_name, str(path))
    except Exception as e:
        logger.error(f"Fehler beim Schreiben von {path}: {e}")
        if os.path.exists(tmp_name): os.remove(tmp_name)
        raise

def fetch_history_batch(entity_ids: List[str], start_iso: str, end_iso: str) -> Tuple[List[Dict] | None, str]:
    url = f"{HA_URL}/api/history/period/{start_iso}"
    params = {"filter_entity_id": ",".join(entity_ids), "end_time": end_iso, "minimal_response": "true", "no_attributes": "true"}
    
    for attempt, timeout in enumerate(TIMEOUTS, 1):
        try:
            response = requests.get(url, headers=get_headers(), params=params, timeout=timeout)
            if response.status_code == 200:
                data = response.json()
                return [item for sublist in data for item in sublist], "OK"
            if response.status_code in (401, 403): return None, "AUTH_FAIL"
            if 400 <= response.status_code < 500: return None, f"HTTP_{response.status_code}"
            response.raise_for_status()
        except Exception as e:
            logger.warning(f"Versuch {attempt} fehlgeschlagen: {e}")
            if attempt < len(TIMEOUTS): time.sleep(BACKOFFS[attempt-1])
    return None, "FAIL_EXHAUSTED"

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Datum YYYY-MM-DD (Default: Gestern)")
    parser.add_argument("--registry", default="registry.json")
    parser.add_argument("--out", default="data")
    args = parser.parse_args()

    target_date = datetime.strptime(args.date, "%Y-%m-%d").date() if args.date else (datetime.now().date() - timedelta(days=1))
    start_iso = datetime.combine(target_date, datetime.min.time()).isoformat()
    end_iso = datetime.combine(target_date, datetime.max.time()).isoformat()

    if not Path(args.registry).exists():
        logger.error(f"Registry {args.registry} nicht gefunden.")
        sys.exit(1)

    registry = json.loads(Path(args.registry).read_text(encoding="utf-8"))
    
    # Gruppierung nach Domain
    domains = {}
    for eid, meta in registry.items():
        dom = meta.get("exporter_domain")
        if dom: domains.setdefault(dom, []).append(eid)

    for domain, entities in domains.items():
        logger.info(f"Exportiere Domain: {domain}")
        data, status = fetch_history_batch(entities, start_iso, end_iso)
        
        payload = {
            "meta": {"domain": domain, "date": str(target_date), "status": status, "count": len(data) if data else 0},
            "data": data or []
        }
        
        out_file = Path(args.out) / str(target_date) / f"{domain}.json"
        write_json_atomic(out_file, payload)

if __name__ == "__main__":
    main()

