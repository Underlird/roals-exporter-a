import json
import os
import hashlib
import tempfile
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import copy as copy_module

RASTER_POINTS = 288
STEP_MINUTES = 5


# -----------------------------
# Canonical hashing (deterministic)
# -----------------------------
def _canonical_bytes(payload: dict) -> bytes:
    canonical = copy_module.deepcopy(payload)
    if "meta" not in canonical or not isinstance(canonical["meta"], dict):
        canonical["meta"] = {}
    # Hash must ignore integrity_hash field itself
    canonical["meta"]["integrity_hash"] = None
    s = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return s.encode("utf-8")


def calculate_integrity_hash(payload: dict) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


# -----------------------------
# Timebase (288 * 5min)
# -----------------------------
def generate_timebase(target_date: date, tz: ZoneInfo) -> list[str]:
    if not isinstance(tz, ZoneInfo):
        raise TypeError(f"tz must be ZoneInfo, got {type(tz).__name__}")
    base_dt = datetime.combine(target_date, time.min, tzinfo=tz)
    return [(base_dt + timedelta(minutes=STEP_MINUTES * i)).isoformat() for i in range(RASTER_POINTS)]


# -----------------------------
# Sanitizers
# -----------------------------
def sanitize_domain(domain: str) -> str:
    if not domain or not isinstance(domain, str):
        raise ValueError(f"Invalid domain: {domain!r}")
    d = domain.strip()
    if not d:
        raise ValueError("Invalid domain: empty after strip")
    if "/" in d or "\\" in d or ".." in d or d.startswith("."):
        raise ValueError(f"domain contains forbidden characters: {d!r}")
    return d


def sanitize_entity_id(entity_id: str) -> str:
    if not entity_id or not isinstance(entity_id, str):
        raise ValueError(f"Invalid entity_id: {entity_id!r}")
    e = entity_id.strip()
    if not e:
        raise ValueError("Invalid entity_id: empty after strip")
    # no path characters
    if "/" in e or "\\" in e or ".." in e:
        raise ValueError(f"entity_id contains forbidden path characters: {e!r}")
    return e


def entity_to_column(entity_id: str) -> str:
    e = sanitize_entity_id(entity_id)
    # Minimal deterministic mapping for now. (Later: unit-suffixed columns.)
    return e.replace(".", "_")


# -----------------------------
# Validation
# -----------------------------
def validate_payload(payload: dict) -> None:
    if not isinstance(payload, dict):
        raise ValueError("payload must be dict")

    meta = payload.get("meta")
    if meta is None or not isinstance(meta, dict):
        raise ValueError("meta missing or not dict")

    ts = payload.get("timeseries")
    if ts is None or not isinstance(ts, dict):
        raise ValueError("timeseries missing or not dict")

    if "ts_iso" not in ts:
        raise ValueError("timeseries.ts_iso missing")

    if not isinstance(ts["ts_iso"], list):
        raise ValueError("timeseries.ts_iso must be list")

    if len(ts["ts_iso"]) != RASTER_POINTS:
        raise ValueError(f"ts_iso length mismatch: {len(ts['ts_iso'])} != {RASTER_POINTS}")

    for col, values in ts.items():
        if not isinstance(values, list):
            raise ValueError(f"timeseries column not list: {col}")
        if len(values) != RASTER_POINTS:
            raise ValueError(f"timeseries column length mismatch: {col} = {len(values)} != {RASTER_POINTS}")

    # MUST: columns_source_map validation
    csm = meta.get("columns_source_map")
    if csm is None:
        raise ValueError("meta.columns_source_map missing")
    if not isinstance(csm, dict):
        raise ValueError("meta.columns_source_map must be dict")

    timeseries_cols = set(ts.keys()) - {"ts_iso"}
    csm_cols = set(csm.keys())

    if timeseries_cols != csm_cols:
        missing = sorted(timeseries_cols - csm_cols)
        extra = sorted(csm_cols - timeseries_cols)
        raise ValueError(f"columns_source_map mismatch: missing={missing}, extra={extra}")

    # syntactic JSON validity
    json.dumps(payload, ensure_ascii=False)


# -----------------------------
# Atomic write (write-once, durable)
# -----------------------------
def atomic_write_json(filepath: str, payload: dict) -> None:
    target_dir = os.path.dirname(filepath)
    os.makedirs(target_dir, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=target_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())

        # atomic commit
        os.replace(tmp_path, filepath)

        # fsync directory AFTER rename to persist directory entry
        dir_fd = os.open(target_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

    except Exception:
        # best-effort cleanup of tmp
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        finally:
            raise


# -----------------------------
# Payload builder (currently "empty" timeseries; real ingest comes later)
# -----------------------------
def build_empty_daily(domain: str, target_date: date, tz: ZoneInfo, tz_name: str, entities: list[str]) -> dict:
    domain = sanitize_domain(domain)

    if not isinstance(entities, list):
        raise ValueError("entities must be list[str]")

    ts_iso = generate_timebase(target_date, tz)

    payload = {
        "meta": {
            "version": "2026.1",
            "domain": domain,
            "date": target_date.isoformat(),
            "timezone": tz_name,
            "generated_at": datetime.now(tz).isoformat(),
            "integrity_hash": None,
            "columns_source_map": {}
        },
        "timeseries": {
            "ts_iso": ts_iso
        },
        # Multi-day event bridging via root_event_id is implemented in the event pipeline (not in skeleton).
        "events": []
    }

    # Missing ≠ Null: only create columns for declared entities.
    # LOCF/gap-handling is implemented later in the ingest/pipeline stage (not in skeleton).
    for raw_ent in entities:
        if raw_ent is None or not isinstance(raw_ent, str):
            raise ValueError(f"Invalid entity in entities list: {raw_ent!r}")

        col = entity_to_column(raw_ent)  # sanitizes internally

        payload["timeseries"][col] = [None] * RASTER_POINTS

        # Data provenance: store the source identifier as provided (trim cosmetic whitespace only).
        payload["meta"]["columns_source_map"][col] = {"ha_entity_id": raw_ent.strip()}

    # MUST: validate first, then hash (hash only for valid payloads)
    validate_payload(payload)
    payload["meta"]["integrity_hash"] = calculate_integrity_hash(payload)
    return payload


# -----------------------------
# Options
# -----------------------------
def load_options() -> dict:
    path = "/data/options.json"
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                obj = json.load(f)
                if obj is None:
                    return {}
                if not isinstance(obj, dict):
                    raise ValueError("options.json must contain a JSON object")
                return obj
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON in {path}: {e}")
    return {}


# -----------------------------
# Main
# -----------------------------
def main() -> None:
    opt = load_options()

    data_root = opt.get("data_root", "/share/nara_data")
    tz_name = opt.get("timezone", "Asia/Manila")
    run_mode = opt.get("run_mode", "skeleton")
    log_level = opt.get("log_level", "INFO")

    VALID_LOG_LEVELS = {"INFO", "DEBUG", "WARNING", "ERROR"}
    VALID_RUN_MODES = {"skeleton", "oneshot_today", "oneshot_date"}

    if log_level not in VALID_LOG_LEVELS:
        raise ValueError(f"Invalid log_level: {log_level}. Must be one of {sorted(VALID_LOG_LEVELS)}")

    if run_mode not in VALID_RUN_MODES:
        raise ValueError(f"Unknown run_mode: {run_mode}. Must be one of {sorted(VALID_RUN_MODES)}")

    entities = opt.get("entities", [])
    if entities is None:
        entities = []
    if not isinstance(entities, list):
        raise ValueError("options.entities must be a list of entity_ids (strings)")

    tz = ZoneInfo(tz_name)

    if run_mode == "skeleton":
        print("ROALS Exporter A – skeleton build OK")
        return

    domain = "system"  # for now fixed, later domain activation will select per-domain

    if run_mode == "oneshot_today":
        target = datetime.now(tz).date()
        payload = build_empty_daily(domain, target, tz, tz_name, entities)

        out_path = os.path.join(
            data_root,
            str(target.year),
            f"{target.month:02d}",
            f"{target.isoformat()}_{domain}.json"
        )

        atomic_write_json(out_path, payload)
        print(f"ROALS Exporter A – oneshot_today wrote: {out_path}")
        return

    if run_mode == "oneshot_date":
        target_date_str = (opt.get("target_date") or "").strip()
        if not target_date_str:
            raise ValueError("run_mode 'oneshot_date' requires target_date (YYYY-MM-DD)")

        try:
            target = datetime.strptime(target_date_str, "%Y-%m-%d").date()
        except ValueError:
            raise ValueError(f"Invalid target_date format: {target_date_str}. Use YYYY-MM-DD")

        payload = build_empty_daily(domain, target, tz, tz_name, entities)

        out_path = os.path.join(
            data_root,
            str(target.year),
            f"{target.month:02d}",
            f"{target.isoformat()}_{domain}.json"
        )

        atomic_write_json(out_path, payload)
        print(f"ROALS Exporter A – oneshot_date wrote: {out_path}")
        return

    # Should never be reached (guarded above)
    raise ValueError(f"Unknown run_mode: {run_mode}")


if __name__ == "__main__":
    main()
