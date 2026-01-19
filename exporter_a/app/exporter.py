#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — deterministic daily JSON skeleton exporter

Run modes:
- skeleton: validates config and exits (no files written)
- oneshot_today: writes ONE domain file for today
- oneshot_date: writes ONE domain file for target_date (YYYY-MM-DD)
- daily_all_domains: writes ALL allowed domain files for target_date (default: today),
  using entity_registry.json for routing (active entities only), and ALWAYS writes all 13 files.

Core guarantees:
- Deterministic payload + deterministic hashing (SHA-256)
- 288 slots per day (5-minute raster), ts_iso always present
- Columnar timeseries with columns_source_map
- Atomic write (tmp -> fsync -> rename -> best-effort fsync(dir))
- Write-once enforcement (never overwrite an existing daily file)
"""

from __future__ import annotations

import copy
import datetime as dt
import hashlib
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore


# ----------------------------
# Constants (ROALS contract)
# ----------------------------

EXPORTER_VERSION = "2026.1"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 24 * 60 // RASTER_MINUTES  # 288

# Final allowlist: these strings become filenames: YYYY-MM-DD_<domain>.json
ALLOWED_DOMAINS: List[str] = [
    "budget",
    "cameras",
    "climate_eg",
    "climate_og",
    "energy",
    "events",
    "internet",
    "motion",
    "network",
    "prices",
    "security",
    "system",
    "weather",
]


# ----------------------------
# Minimal logging
# ----------------------------

_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}


def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    lvl = _LEVELS.get(level.upper(), 20)
    cfg = _LEVELS.get(cfg_level.upper(), 20)
    if lvl >= cfg:
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)


# ----------------------------
# Config handling (HAOS)
# ----------------------------

OPTIONS_PATH = "/data/options.json"


def load_options() -> Dict[str, Any]:
    """
    Home Assistant add-ons provide options via /data/options.json.
    """
    if not os.path.exists(OPTIONS_PATH):
        # Allow empty config for very early testing; Settings.from_options will enforce required fields per mode.
        return {}
    try:
        with open(OPTIONS_PATH, "r", encoding="utf-8") as f:
            opts = json.load(f)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in {OPTIONS_PATH}: {e}") from e
    if not isinstance(opts, dict):
        raise RuntimeError(f"{OPTIONS_PATH} must contain a JSON object.")
    return opts


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
        data_root = str(opts.get("data_root", "/share/nara_data")).strip() or "/share/nara_data"
        timezone = str(opts.get("timezone", "Asia/Manila")).strip() or "Asia/Manila"
        log_level = str(opts.get("log_level", "INFO")).strip().upper() or "INFO"
        run_mode = str(opts.get("run_mode", "skeleton")).strip()

        target_date = opts.get("target_date")
        target_date = str(target_date).strip() if target_date not in (None, "") else None

        exporter_domain = opts.get("exporter_domain")
        exporter_domain = str(exporter_domain).strip() if exporter_domain not in (None, "") else None

        entities_raw = opts.get("entities", [])
        if entities_raw is None:
            entities_raw = []
        if not isinstance(entities_raw, list):
            raise ValueError("options.entities must be a list of strings.")
        entities: List[str] = []
        for x in entities_raw:
            if not isinstance(x, str):
                raise ValueError("options.entities must contain only strings.")
            s = x.strip()
            if s:
                entities.append(s)

        registry_path = opts.get("registry_path")
        registry_path = str(registry_path).strip() if registry_path not in (None, "") else None

        strict_registry = bool(opts.get("strict_registry", True))

        return Settings(
            data_root=data_root,
            timezone=timezone,
            log_level=log_level,
            run_mode=run_mode,
            target_date=target_date,
            exporter_domain=exporter_domain,
            entities=entities,
            registry_path=registry_path,
            strict_registry=strict_registry,
        )


# ----------------------------
# Sanitizing / canonicalization
# ----------------------------

_DOMAIN_RE = re.compile(r"^[a-z0-9_]+$")


def sanitize_domain(domain: str) -> str:
    d = domain.strip().lower()
    d = re.sub(r"[^a-z0-9_]+", "_", d)
    d = re.sub(r"_+", "_", d).strip("_")
    return d


def validate_domain_or_raise(domain: str) -> str:
    d = sanitize_domain(domain)
    if d not in ALLOWED_DOMAINS:
        raise ValueError(
            f"Invalid exporter_domain={domain!r} (sanitized={d!r}). "
            f"Allowed domains: {', '.join(ALLOWED_DOMAINS)}"
        )
    if not _DOMAIN_RE.match(d):
        raise ValueError(f"Sanitized domain is not valid: {d!r}")
    return d


def sanitize_entity_id(entity_id: str) -> str:
    return entity_id.strip()


def entity_to_column_key(entity_id: str) -> str:
    """
    Deterministic, file-safe column name derived from HA entity_id.
    Example: sensor.system_monitor_arbeitsspeicherauslastung -> sensor_system_monitor_arbeitsspeicherauslastung
    """
    s = entity_id.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# ----------------------------
# Date/time utilities
# ----------------------------

def get_zoneinfo(tz_name: str) -> ZoneInfo:
    if ZoneInfo is None:
        raise RuntimeError("zoneinfo is not available in this Python build.")
    try:
        return ZoneInfo(tz_name)
    except Exception as e:
        raise RuntimeError(f"Invalid timezone {tz_name!r}: {e}") from e


def today_in_tz(tz: ZoneInfo) -> dt.date:
    return dt.datetime.now(tz=tz).date()


def parse_target_date(target_date: Optional[str], tz: ZoneInfo) -> dt.date:
    if not target_date:
        return today_in_tz(tz)
    try:
        return dt.date.fromisoformat(target_date)
    except ValueError as e:
        raise ValueError(f"target_date must be YYYY-MM-DD, got {target_date!r}") from e


def build_ts_iso_for_date(day: dt.date, tz: ZoneInfo) -> List[str]:
    start = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    out: List[str] = []
    for i in range(SLOTS_PER_DAY):
        out.append((start + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat())
    return out


# ----------------------------
# Deterministic hashing
# ----------------------------

def canonical_dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def compute_integrity_hash(payload_without_hash: Dict[str, Any]) -> str:
    s = canonical_dumps(payload_without_hash)
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def attach_integrity_hash(payload: Dict[str, Any]) -> Dict[str, Any]:
    cloned = copy.deepcopy(payload)
    meta = cloned.get("meta", {})
    if isinstance(meta, dict):
        meta["integrity_hash"] = ""
    h = compute_integrity_hash(cloned)
    payload["meta"]["integrity_hash"] = h
    return payload


# ----------------------------
# Atomic write (write-once)
# ----------------------------

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def atomic_write_json_write_once(path: str, payload: Dict[str, Any]) -> None:
    """
    Write-once enforcement + atomic durability.
    - If final path exists: hard-fail
    - Write to tmp in same directory, fsync file, rename, best-effort fsync(dir)
    """
    if os.path.exists(path):
        raise FileExistsError(f"write-once violation: output already exists: {path}")

    directory = os.path.dirname(path)
    ensure_dir(directory)

    tmp_path = f"{path}.tmp.{os.getpid()}"
    data = canonical_dumps(payload)  # deterministic on disk
    encoded = data.encode("utf-8")

    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(encoded)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, path)  # atomic on same filesystem

        # fsync directory entry for durability (best-effort, not all FS support this)
        try:
            dir_fd = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except OSError:
            pass

    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass


# ----------------------------
# Registry handling (routing only)
# ----------------------------

def load_registry(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    """
    Active if since <= target < until (until exclusive). until null/missing => active.
    """
    since_raw = meta.get("since")
    until_raw = meta.get("until")

    # Parse 'since' (default: 1970-01-01)
    if since_raw in (None, "", "null"):
        since = dt.date(1970, 1, 1)
    else:
        try:
            since = dt.date.fromisoformat(str(since_raw).strip())
        except Exception:
            return False

    # Parse 'until' (None/empty = active forever)
    if until_raw in (None, "", "null"):
        return target >= since

    try:
        until = dt.date.fromisoformat(str(until_raw).strip())
    except Exception:
        return False

    return since <= target < until


def select_entities_for_domain_from_registry(
    registry: Dict[str, Any],
    domain: str,
    target: dt.date,
    strict: bool,
    cfg_level: str,
) -> List[str]:
    """
    Returns active entity_ids for this domain, sorted deterministically.
    Enforces:
    - exporter_domain must be valid and in allowlist
    - registry values must be objects/dicts
    - lifecycle gate since/until
    """
    selected: List[str] = []

    for eid, meta in registry.items():
        if not isinstance(eid, str):
            if strict:
                raise ValueError("Registry keys must be entity_id strings.")
            log("WARNING", f"Registry key is not a string, skipping: {eid!r}", cfg_level)
            continue

        if not isinstance(meta, dict):
            if strict:
                raise ValueError(f"Registry entry for {eid!r} must be an object.")
            log("WARNING", f"Registry entry for {eid!r} is not an object, skipping.", cfg_level)
            continue

        dom_raw = meta.get("exporter_domain")
        if dom_raw in (None, ""):
            if strict:
                raise ValueError(f"Registry entry for {eid!r} missing exporter_domain.")
            continue

        try:
            dom = validate_domain_or_raise(str(dom_raw))
        except Exception as e:
            if strict:
                raise
            log("WARNING", f"Invalid exporter_domain for {eid!r}: {e}. Skipping.", cfg_level)
            continue

        if dom != domain:
            continue

        if not entity_is_active(meta, target):
            continue

        selected.append(sanitize_entity_id(eid))

    selected = sorted(set(selected))
    return selected


# ----------------------------
# Daily payload construction
# ----------------------------

def build_daily_payload(
    exporter_domain: str,
    day: dt.date,
    tz: ZoneInfo,
    tz_name: str,
    entities: List[str],
) -> Dict[str, Any]:
    """
    Skeleton payload:
    - timeseries.ts_iso always present
    - for each entity, create a column of length 288 filled with null (data fill is Phase 2)
    - columns_source_map maps column_key -> { ha_entity_id: original }
    """
    ts_iso = build_ts_iso_for_date(day, tz)

    entities_sorted = sorted(set([sanitize_entity_id(e) for e in entities if e.strip()]))

    columns_source_map: Dict[str, Any] = {}
    timeseries: Dict[str, Any] = {"ts_iso": ts_iso}

    # Deterministic collision handling:
    # If multiple entity_ids collapse to the same base column key, all of them get a stable hash suffix.
    col_to_entities: Dict[str, List[str]] = {}
    for eid in entities_sorted:
        base = entity_to_column_key(eid)
        col_to_entities.setdefault(base, []).append(eid)

    for base_col, eids in sorted(col_to_entities.items()):
        eids_sorted = sorted(eids)
        if len(eids_sorted) == 1:
            eid = eids_sorted[0]
            columns_source_map[base_col] = {"ha_entity_id": eid}
            timeseries[base_col] = [None] * SLOTS_PER_DAY
        else:
            for eid in eids_sorted:
                suffix = hashlib.sha256(eid.encode("utf-8")).hexdigest()[:8]
                col_unique = f"{base_col}_{suffix}"
                columns_source_map[col_unique] = {"ha_entity_id": eid}
                timeseries[col_unique] = [None] * SLOTS_PER_DAY

    generated_at = dt.datetime.now(tz=tz).isoformat()

    payload: Dict[str, Any] = {
        "events": [],
        "meta": {
            "version": EXPORTER_VERSION,
            "domain": exporter_domain,
            "date": day.isoformat(),
            "timezone": tz_name,
            "generated_at": generated_at,
            "integrity_hash": "",
            "columns_source_map": columns_source_map,
        },
        "timeseries": timeseries,
    }

    return attach_integrity_hash(payload)


def build_output_path(data_root: str, day: dt.date, exporter_domain: str) -> str:
    y = f"{day.year:04d}"
    m = f"{day.month:02d}"
    filename = f"{day.isoformat()}_{exporter_domain}.json"
    return os.path.join(data_root, y, m, filename)


# ----------------------------
# Mode execution
# ----------------------------

def run_skeleton(cfg: Settings, tz: ZoneInfo) -> None:
    log("INFO", "run_mode=skeleton: validating configuration (no output files).", cfg.log_level)

    _ = today_in_tz(tz)

    if cfg.exporter_domain:
        _ = validate_domain_or_raise(cfg.exporter_domain)

    if cfg.target_date:
        _ = dt.date.fromisoformat(cfg.target_date)

    if cfg.registry_path:
        if not os.path.exists(cfg.registry_path):
            raise FileNotFoundError(f"registry_path not found: {cfg.registry_path}")
        reg = load_registry(cfg.registry_path)
        if not isinstance(reg, dict):
            raise ValueError("Registry must be a JSON object.")

    log("INFO", "skeleton validation OK.", cfg.log_level)


def run_oneshot(cfg: Settings, tz: ZoneInfo, tz_name: str, day: dt.date) -> None:
    if not cfg.exporter_domain:
        raise ValueError("exporter_domain is required for oneshot_* modes.")
    if not cfg.entities:
        raise ValueError("entities must be a non-empty list for oneshot_* modes.")

    domain = validate_domain_or_raise(cfg.exporter_domain)
    entities = sorted(set(cfg.entities))

    payload = build_daily_payload(domain, day, tz, tz_name, entities)
    out_path = build_output_path(cfg.data_root, day, domain)

    log("INFO", f"Writing oneshot daily file: {out_path}", cfg.log_level)
    atomic_write_json_write_once(out_path, payload)
    log("INFO", f"oneshot wrote: {out_path}", cfg.log_level)


def run_daily_all_domains(cfg: Settings, tz: ZoneInfo, tz_name: str, day: dt.date) -> None:
    if not cfg.registry_path:
        raise ValueError("registry_path is required for run_mode=daily_all_domains.")
    if not os.path.exists(cfg.registry_path):
        raise FileNotFoundError(f"registry_path not found: {cfg.registry_path}")

    reg = load_registry(cfg.registry_path)
    if not isinstance(reg, dict):
        raise ValueError("Registry must be a JSON object.")

    log("INFO", f"daily_all_domains: registry_path={cfg.registry_path}", cfg.log_level)
    log("INFO", f"daily_all_domains: target_date={day.isoformat()} tz={tz_name}", cfg.log_level)

    # ALWAYS write all 13 domain files (even if empty), for maximum auditability.
    for domain in sorted(ALLOWED_DOMAINS):
        entities = select_entities_for_domain_from_registry(
            registry=reg,
            domain=domain,
            target=day,
            strict=cfg.strict_registry,
            cfg_level=cfg.log_level,
        )

        payload = build_daily_payload(domain, day, tz, tz_name, entities)
        out_path = build_output_path(cfg.data_root, day, domain)

        log("INFO", f"Writing {domain} ({len(entities)} entities): {out_path}", cfg.log_level)
        atomic_write_json_write_once(out_path, payload)

    log("INFO", "daily_all_domains completed successfully.", cfg.log_level)


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    try:
        opts = load_options()
        cfg = Settings.from_options(opts)

        tz = get_zoneinfo(cfg.timezone)
        tz_name = cfg.timezone

        mode = cfg.run_mode.strip()

        if mode == "skeleton":
            run_skeleton(cfg, tz)
            return 0

        if mode == "oneshot_today":
            day = today_in_tz(tz)
            run_oneshot(cfg, tz, tz_name, day)
            return 0

        if mode == "oneshot_date":
            day = parse_target_date(cfg.target_date, tz)
            run_oneshot(cfg, tz, tz_name, day)
            return 0

        if mode == "daily_all_domains":
            day = parse_target_date(cfg.target_date, tz)
            run_daily_all_domains(cfg, tz, tz_name, day)
            return 0

        raise ValueError(
            f"Invalid run_mode={mode!r}. Supported: skeleton, oneshot_today, oneshot_date, daily_all_domains."
        )

    except Exception as e:
        # Fail loud, fail fast (ROALS)
        lvl = "INFO"
        try:
            o = load_options()
            lvl = str(o.get("log_level", "INFO")).upper()
        except Exception:
            pass
        log("ERROR", f"Exporter failed: {type(e).__name__}: {e}", lvl)
        return 2


if __name__ == "__main__":
    sys.exit(main())
```0
