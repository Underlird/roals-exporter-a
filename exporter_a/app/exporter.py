import json
import os
import hashlib
import tempfile
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo


RASTER_POINTS = 288
STEP_MINUTES = 5


def _canonical_bytes(payload: dict) -> bytes:
    # Hash über Payload ohne meta.integrity_hash (Regel)
    copy = json.loads(json.dumps(payload))  # deterministic deep copy
    meta = copy.get("meta", {})
    meta["integrity_hash"] = None
    copy["meta"] = meta
    s = json.dumps(copy, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return s.encode("utf-8")


def calculate_integrity_hash(payload: dict) -> str:
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def generate_timebase(target_date: date, tz: ZoneInfo) -> list[str]:
    base_dt = datetime.combine(target_date, time.min, tzinfo=tz)
    return [(base_dt + timedelta(minutes=STEP_MINUTES * i)).isoformat() for i in range(RASTER_POINTS)]


def validate_payload(payload: dict) -> None:
    ts = payload.get("timeseries", {})
    if "ts_iso" not in ts:
        raise ValueError("timeseries.ts_iso missing")

    if len(ts["ts_iso"]) != RASTER_POINTS:
        raise ValueError(f"ts_iso length mismatch: {len(ts['ts_iso'])} != {RASTER_POINTS}")

    for col, values in ts.items():
        if not isinstance(values, list):
            raise ValueError(f"timeseries column not list: {col}")
        if len(values) != RASTER_POINTS:
            raise ValueError(f"timeseries column length mismatch: {col} = {len(values)} != {RASTER_POINTS}")

    # syntaktische JSON-Validität (jq empty äquivalent)
    json.dumps(payload, ensure_ascii=False)


def atomic_write_json(filepath: str, payload: dict) -> None:
    target_dir = os.path.dirname(filepath)
    os.makedirs(target_dir, exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(dir=target_dir, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())

        dir_fd = os.open(target_dir, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)

        os.replace(tmp_path, filepath)  # atomic commit
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        finally:
            raise


def build_empty_daily(domain: str, target_date: date, tz: ZoneInfo, tz_name: str) -> dict:
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
            "ts_iso": ts_iso,
            "demo_value": [None] * RASTER_POINTS
        },
        "events": []
    }

    payload["meta"]["integrity_hash"] = calculate_integrity_hash(payload)
    validate_payload(payload)
    return payload


def load_options() -> dict:
    path = "/data/options.json"
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def main() -> None:
    opt = load_options()

    data_root = opt.get("data_root", "/share/nara_data")
    tz_name = opt.get("timezone", "Asia/Manila")
    run_mode = opt.get("run_mode", "skeleton")

    tz = ZoneInfo(tz_name)

    if run_mode == "skeleton":
        print("ROALS Exporter A – skeleton build OK")
        return

    if run_mode == "oneshot_today":
        today = datetime.now(tz).date()
        payload = build_empty_daily("system", today, tz, tz_name)

        out_path = os.path.join(
            data_root,
            str(today.year),
            f"{today.month:02d}",
            f"{today.isoformat()}_system.json"
        )

        atomic_write_json(out_path, payload)
        print(f"ROALS Exporter A – oneshot_today wrote: {out_path}")
        return

    raise ValueError(f"Unknown run_mode: {run_mode}")


if __name__ == "__main__":
    main()
