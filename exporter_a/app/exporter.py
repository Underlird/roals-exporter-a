#!/usr/bin/env python3
"""
exporter.py - ROALS Exporter A (Platinum Truth Engine)
Version: V2.8 - Platinum Forensic Edition

Changes V2.7 → V2.8:
- Patch 1: meta.generated_at added (ISO timestamp) → fixes Deep Dive L2 warning
- Patch 2: ts_iso excluded from summary.columns → fixes Deep Dive L3 ghost sensor warning
- Patch 3: Version branding unified to V2.8
- Patch 4: Integrity hash covers all fields including generated_at
- Optimization: meta.window block added (start/end timestamps for Aggregator compatibility)
- Optimization: meta.registry_hash added (SHA-256 of registry for consistency tracking)
- Optimization: summary.columns includes valid_count for Deep Dive L3 cross-check
- Optimization: Windows Unicode fix for Cockpit subprocess compatibility
- Compatibility: 100% backward compatible with V2.7 consumers
"""

import argparse
import hashlib
import io
import json
import logging
import os
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Optional

import requests

# ─── Windows Unicode Fix ──────────────────────────────────────────────────────

if sys.stdout and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
elif sys.stdout and sys.stdout.encoding and sys.stdout.encoding.lower() not in ("utf-8", "utf8"):
    try:
        sys.stdout = io.TextIOWrapper(
            sys.stdout.buffer, encoding="utf-8", errors="replace", line_buffering=True,
        )
    except Exception:
        pass

# ZoneInfo Import with fallbacks
try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: ZoneInfo not available. Install 'backports.zoneinfo'")
        sys.exit(1)

# ─── ROALS Constants ──────────────────────────────────────────────────────────

VERSION = "V2.8 - Platinum Forensic Edition"
SCHEMA_VERSION = "2.8"
SLOTS_PER_DAY = 288
RASTER_MINUTES = 5
BINARY_TRUE_VALUES = {"on", "open", "true", "detected", "home", "active", "occupied"}
HISTORY_CHUNK_SIZE = 50
REQUEST_TIMEOUT = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("ROALS-Exporter")


# ─── Authentication ───────────────────────────────────────────────────────────

def get_headers() -> Dict[str, str]:
    """Get Home Assistant API headers with authentication token."""
    token = os.getenv("SUPERVISOR_TOKEN") or os.getenv("HA_API_TOKEN")
    if not token:
        logger.error("No authentication token found. Set SUPERVISOR_TOKEN or HA_API_TOKEN.")
        sys.exit(1)
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


# ─── Integrity & Atomic Write ─────────────────────────────────────────────────

def calculate_integrity_hash(data: dict) -> str:
    """Calculate SHA256 hash for data integrity verification.

    CRITICAL: This function must remain identical across Exporter, Deep Dive,
    and Aggregator. Any change here breaks all existing hashes.
    """
    d = data.copy()
    if "meta" in d:
        d["meta"] = {k: v for k, v in d["meta"].items() if k != "integrity_hash"}

    canonical = json.dumps(d, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def calculate_registry_hash(registry: dict) -> str:
    """Calculate SHA-256 hash of the registry for consistency tracking."""
    canonical = json.dumps(registry, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def write_atomic_json(path: Path, data: dict):
    """Write JSON data atomically to prevent corruption.

    Uses tempfile + os.replace for atomic operation,
    followed by fsync for durability.
    """
    path.parent.mkdir(parents=True, exist_ok=True)

    fd, tmp = tempfile.mkstemp(
        dir=str(path.parent),
        prefix=f".{path.name}.",
        suffix=".tmp",
    )

    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
            f.flush()
            os.fsync(f.fileno())

        # Atomic rename
        os.replace(tmp, str(path))

        # Directory fsync for metadata durability
        try:
            flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
            dfd = os.open(str(path.parent), flags)
            try:
                os.fsync(dfd)
            finally:
                os.close(dfd)
        except OSError:
            pass
    except Exception:
        if os.path.exists(tmp):
            os.remove(tmp)
        raise


# ─── Binary Detection ─────────────────────────────────────────────────────────

def detect_binary_mode(eid: str, kind: str, profile: str) -> bool:
    """Detect if entity should be treated as binary sensor."""
    if kind == "event_state":
        return True
    if eid.startswith("binary_sensor."):
        return True
    if profile and ("contact_state" in profile or "binary_state" in profile):
        return True
    return False


# ─── Slot Mapping ─────────────────────────────────────────────────────────────

def map_events_to_slots(
    events: List[Dict],
    start_dt: datetime,
    raw_policy: str,
    is_binary: bool,
) -> Tuple[List[Any], Dict]:
    """Map Home Assistant events to 288 5-minute time slots.

    For binary sensors, also calculates:
    - total_open_seconds: Total time in "open" state
    - toggle_count: Number of state changes

    Returns: (slots, event_stats)
    """
    slots = [None] * SLOTS_PER_DAY
    stats = {"total_open_seconds": 0, "toggle_count": 0}

    # Parse and filter events
    parsed = []
    for e in events:
        state = e.get("state")
        ts_str = e.get("last_changed")

        if state in (None, "unknown", "unavailable", "") or not ts_str:
            continue

        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))

            if is_binary:
                val = 1 if str(state).lower() in BINARY_TRUE_VALUES else 0
            else:
                val = float(state)

            parsed.append((ts, val))
        except (ValueError, TypeError):
            continue

    parsed.sort(key=lambda x: x[0])

    # Binary sensor: Calculate open duration
    if is_binary and parsed:
        stats["toggle_count"] = len(parsed)

        last_ts = start_dt
        last_val = 0  # Assume closed

        prior_events = [e for e in parsed if e[0] <= start_dt]
        if prior_events:
            last_val = prior_events[-1][1]

        end_dt = start_dt + timedelta(days=1)
        for ts, val in parsed:
            if ts > start_dt and ts < end_dt:
                if last_val == 1:
                    duration = (ts - last_ts).total_seconds()
                    stats["total_open_seconds"] += max(0, duration)

            last_ts = ts
            last_val = val

        # Handle still-open at end of day
        if last_val == 1:
            effective_end = min(end_dt, datetime.now(start_dt.tzinfo))
            if last_ts < effective_end:
                duration = (effective_end - last_ts).total_seconds()
                stats["total_open_seconds"] += max(0, duration)

    # Map to slots (LOCF - Last Observation Carried Forward)
    event_idx = 0
    last_known = None

    while event_idx < len(parsed) and parsed[event_idx][0] <= start_dt:
        last_known = parsed[event_idx][1]
        event_idx += 1

    curr_end = start_dt + timedelta(minutes=RASTER_MINUTES)
    for i in range(SLOTS_PER_DAY):
        slot_values = []

        while event_idx < len(parsed) and parsed[event_idx][0] <= curr_end:
            slot_values.append(parsed[event_idx][1])
            last_known = parsed[event_idx][1]
            event_idx += 1

        slots[i] = slot_values[-1] if slot_values else last_known

        curr_end += timedelta(minutes=RASTER_MINUTES)

    return slots, stats


# ─── History Fetching ─────────────────────────────────────────────────────────

def fetch_history(
    session: requests.Session,
    entity_ids: List[str],
    start_dt: datetime,
    end_dt: datetime,
    ha_url: str,
    mode: str,
) -> Dict[str, List[Dict]]:
    """Fetch history from Home Assistant API in batches.

    Returns dict mapping entity_id -> event list.
    """
    all_data = defaultdict(list)

    start_iso = start_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")

    total_entities = len(entity_ids)
    for i in range(0, total_entities, HISTORY_CHUNK_SIZE):
        chunk = entity_ids[i : i + HISTORY_CHUNK_SIZE]
        chunk_num = (i // HISTORY_CHUNK_SIZE) + 1
        total_chunks = (total_entities + HISTORY_CHUNK_SIZE - 1) // HISTORY_CHUNK_SIZE

        logger.debug(f"Fetching chunk {chunk_num}/{total_chunks} ({len(chunk)} entities)")

        params = {
            "filter_entity_id": ",".join(chunk),
            "end_time": end_iso,
        }

        if mode == "daily_truth":
            params.update({
                "minimal_response": "1",
                "no_attributes": "1",
            })

        try:
            resp = session.get(
                f"{ha_url}/api/history/period/{start_iso}",
                params=params,
                timeout=REQUEST_TIMEOUT,
            )

            if resp.status_code == 200:
                data = resp.json()
                for entity_list in data:
                    if entity_list and len(entity_list) > 0:
                        entity_id = entity_list[0]["entity_id"]
                        all_data[entity_id] = entity_list
            else:
                logger.warning(f"API returned status {resp.status_code} for chunk {chunk_num}")

        except requests.exceptions.Timeout:
            logger.error(f"Timeout fetching chunk {chunk_num}")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching chunk {chunk_num}: {e}")
        except Exception as e:
            logger.error(f"Unexpected error in chunk {chunk_num}: {e}")

    return dict(all_data)


# ─── Registry ─────────────────────────────────────────────────────────────────

def load_registry(registry_path: str) -> Dict[str, Dict]:
    """Load and validate registry.

    Returns dict with only valid entities (have roals_id and exporter_domain).
    """
    if not os.path.exists(registry_path):
        logger.error(f"Registry not found: {registry_path}")
        sys.exit(1)

    try:
        with open(registry_path, "r", encoding="utf-8") as f:
            raw_registry = json.load(f)
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in registry: {e}")
        sys.exit(1)
    except OSError as e:
        logger.error(f"Error loading registry: {e}")
        sys.exit(1)

    registry = {}
    for entity_id, meta in raw_registry.items():
        if meta.get("roals_id") and meta.get("exporter_domain"):
            registry[entity_id] = meta
        else:
            logger.debug(f"Skipping {entity_id}: missing roals_id or exporter_domain")

    logger.info(f"Loaded {len(registry)} valid entities from registry")
    return registry


# ─── Date & Domain Parsing ────────────────────────────────────────────────────

def parse_date_range(args) -> List:
    """Parse date arguments into list of dates.

    Supports: --start-date/--end-date (range), --date (single), default: yesterday.
    """
    dates = []

    if args.start_date and args.end_date:
        try:
            start = datetime.strptime(args.start_date, "%Y-%m-%d").date()
            end = datetime.strptime(args.end_date, "%Y-%m-%d").date()

            curr = start
            while curr <= end:
                dates.append(curr)
                curr += timedelta(days=1)

            logger.info(f"Processing date range: {args.start_date} to {args.end_date} ({len(dates)} days)")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)

    elif args.date:
        try:
            dates.append(datetime.strptime(args.date, "%Y-%m-%d").date())
            logger.info(f"Processing single date: {args.date}")
        except ValueError as e:
            logger.error(f"Invalid date format: {e}")
            sys.exit(1)

    else:
        tz = ZoneInfo(args.timezone)
        yesterday = datetime.now(tz).date() - timedelta(days=1)
        dates.append(yesterday)
        logger.info(f"Processing default (yesterday): {yesterday}")

    return dates


def determine_target_domains(args, registry: Dict) -> List[str]:
    """Determine which domains to process."""
    available = sorted(set(m["exporter_domain"] for m in registry.values()))
    logger.info(f"Available domains: {', '.join(available)}")

    targets = []

    if args.all_domains:
        targets = available
        logger.info(f"Processing ALL domains: {', '.join(targets)}")

    elif args.domain:
        if isinstance(args.domain, list):
            requested = args.domain
        else:
            requested = [d.strip() for d in str(args.domain).split(",") if d.strip()]

        targets = [d for d in requested if d in available]

        if not targets:
            logger.error(f"None of requested domains {requested} are available")
            sys.exit(1)

        logger.info(f"Processing selected domains: {', '.join(targets)}")

    if not targets:
        logger.error("No domains to process. Use --all-domains or --domain")
        sys.exit(1)

    return targets


# ─── Core Processing ──────────────────────────────────────────────────────────

def process_daily_truth(
    entities: Dict[str, Dict],
    history: Dict[str, List[Dict]],
    start_dt: datetime,
    mode: str,
) -> Dict:
    """Process daily truth mode: Generate 288-slot timeseries.

    Returns dict with timeseries and summary.
    """
    # Generate timestamp grid
    timeseries = {
        "ts_iso": [
            (start_dt + timedelta(minutes=RASTER_MINUTES * i)).isoformat()
            for i in range(SLOTS_PER_DAY)
        ]
    }

    event_metrics = {}

    for entity_id, meta in entities.items():
        policy = (
            meta.get("metric", {}).get("agg_policy", {}).get("primary")
            or meta.get("agg_policy", {}).get("primary", "last")
        )

        kind = meta.get("metric", {}).get("kind", "numeric")
        profile = str(meta.get("profile", ""))
        is_binary = detect_binary_mode(entity_id, kind, profile)

        events = history.get(entity_id, [])
        slots, stats = map_events_to_slots(events, start_dt, policy, is_binary)

        timeseries[entity_id] = slots

        if is_binary and stats["toggle_count"] > 0:
            event_metrics[entity_id] = stats

    # ── PATCH 2 (V2.8): Summary only for real entities, not ts_iso ──
    summary = {"columns": {}}
    for entity_id in entities.keys():
        slots = timeseries.get(entity_id, [])
        valid_count = sum(1 for v in slots if v is not None)
        coverage_pct = round((valid_count / SLOTS_PER_DAY) * 100, 1)

        summary["columns"][entity_id] = {
            "valid_count": valid_count,
            "coverage_pct": coverage_pct,
            "event_metrics": event_metrics.get(entity_id),
        }

    return {
        "timeseries": timeseries,
        "summary": summary,
    }


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description=f"ROALS Exporter A — {VERSION}",
    )

    # Paths
    parser.add_argument("--registry", default="registry.json", help="Path to entity_registry.json")
    parser.add_argument("--out", default="data", help="Output directory")

    # Date selection
    parser.add_argument("--date", help="Single date (YYYY-MM-DD)")
    parser.add_argument("--start-date", help="Start date for range (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for range (YYYY-MM-DD)")

    # Domain selection
    parser.add_argument("--all-domains", action="store_true", help="Process all domains")
    parser.add_argument("--domain", help="Comma-separated domain list")

    # Configuration
    parser.add_argument("--timezone", default="Asia/Manila", help="Target timezone")
    parser.add_argument("--mode", default="daily_truth", help="Export mode (daily_truth or raw)")

    args = parser.parse_args()

    # ── Home Assistant Add-on: Load options.json ──
    options_path = Path("/data/options.json")
    local_options = Path("options.json")  # Cockpit writes this for local mode
    ha_url = "http://localhost:8123"

    active_options = None
    if options_path.exists():
        logger.info("Running in Home Assistant Add-on mode")
        ha_url = "http://supervisor/core"
        active_options = options_path
    elif local_options.exists():
        logger.info("Running in local mode (options.json found)")
        active_options = local_options

    if active_options:
        try:
            with open(active_options, encoding="utf-8") as f:
                options = json.load(f)

            args.registry = options.get("registry_path", args.registry)
            args.out = options.get("data_root", args.out)
            args.timezone = options.get("timezone", args.timezone)
            args.mode = options.get("run_mode", args.mode)

            if options.get("ha_url"):
                ha_url = options["ha_url"]

            if options.get("process_all_domains"):
                args.all_domains = True
            elif options.get("exporter_domain"):
                args.domain = options.get("exporter_domain")

            if options.get("start_date") and options.get("end_date"):
                args.start_date = options["start_date"]
                args.end_date = options["end_date"]
            elif options.get("target_date"):
                args.date = options["target_date"]

        except (OSError, json.JSONDecodeError) as e:
            logger.error(f"Error loading {active_options}: {e}")
            sys.exit(1)

    # Validate timezone
    try:
        tz = ZoneInfo(args.timezone)
    except Exception as e:
        logger.error(f"Invalid timezone '{args.timezone}': {e}")
        sys.exit(1)

    # Load registry
    registry = load_registry(args.registry)
    reg_hash = calculate_registry_hash(registry)
    logger.info(f"Registry hash: {reg_hash[:16]}...")

    # Determine dates and domains
    dates = parse_date_range(args)
    target_domains = determine_target_domains(args, registry)

    # Create session
    session = requests.Session()
    session.headers.update(get_headers())

    logger.info(f"ROALS Exporter {VERSION}")
    logger.info(f"Mode: {args.mode} | TZ: {args.timezone} | Domains: {len(target_domains)} | Dates: {len(dates)}")

    files_written = 0
    files_failed = 0

    for date in dates:
        start_dt = datetime.combine(date, dtime.min).replace(tzinfo=tz)
        end_dt = start_dt + timedelta(days=1)

        for domain in target_domains:
            logger.info(f"Processing {domain} for {date}...")

            domain_entities = {
                e: m for e, m in registry.items()
                if m["exporter_domain"] == domain
            }

            if not domain_entities:
                logger.warning(f"No entities found for domain {domain}")
                continue

            logger.info(f"  {len(domain_entities)} entities in {domain}")

            # Fetch history (30 min before start to catch initial state)
            history = fetch_history(
                session,
                list(domain_entities.keys()),
                start_dt - timedelta(minutes=30),
                end_dt,
                ha_url,
                args.mode,
            )

            # ── PATCH 1 + 3 (V2.8): Full meta block with generated_at, version, window ──
            meta = {
                "version": SCHEMA_VERSION,
                "mode": args.mode,
                "domain": domain,
                "date": str(date),
                "timezone": args.timezone,
                "generated_at": datetime.now(tz).isoformat(),
                "system_id": os.getenv("HOSTNAME", "unknown"),
                "registry_hash": reg_hash,
                "window": {
                    "start": start_dt.isoformat(),
                    "end": end_dt.isoformat(),
                },
                "entities": {
                    e: {
                        "roals_id": m["roals_id"],
                        "area_id": m.get("area_id"),
                        "room_id": m.get("room_id"),
                        "profile": m.get("profile"),
                    }
                    for e, m in domain_entities.items()
                },
            }

            # Process based on mode
            if args.mode == "daily_truth":
                result = process_daily_truth(domain_entities, history, start_dt, args.mode)
                payload = {
                    "meta": meta,
                    **result,
                }
            else:
                payload = {
                    "meta": meta,
                    "raw_data": history,
                }

            # ── PATCH 4 (V2.8): Hash covers entire payload including generated_at ──
            payload["meta"]["integrity_hash"] = calculate_integrity_hash(payload)

            # Write file
            folder = "daily" if args.mode == "daily_truth" else "raw"
            output_path = Path(args.out) / folder / domain / f"{date}_{domain}.json"

            try:
                write_atomic_json(output_path, payload)
                files_written += 1
                logger.info(f"  Written: {output_path.name}")
            except Exception as e:
                files_failed += 1
                logger.error(f"  Failed to write {output_path}: {e}")

    # Final summary
    logger.info("=" * 60)
    logger.info(f"ROALS Exporter {VERSION} — Complete")
    logger.info(f"  Files written: {files_written}")
    if files_failed > 0:
        logger.warning(f"  Files failed:  {files_failed}")
    logger.info("=" * 60)

    if files_failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
