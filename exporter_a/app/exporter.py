#!/usr/bin/env python3
import argparse, hashlib, json, logging, os, sys, tempfile, time, requests
from collections import defaultdict
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Any, Dict, List, Tuple, Generator, Optional, Callable
try:
    from zoneinfo import ZoneInfo
except ImportError:
    print("CRITICAL: ZoneInfo not found."); sys.exit(1)

SLOTS_PER_DAY, RASTER_MINUTES = 288, 5
BINARY_TRUE_VALUES = {"on", "open", "true", "detected", "home", "active", "occupied"}
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ROALS-Exporter")

def get_headers():
    token = os.getenv("SUPERVISOR_TOKEN") or os.getenv("HA_API_TOKEN")
    return {"Authorization": f"Bearer {token}", "content-type": "application/json"}

def calculate_integrity_hash(data: dict) -> str:
    d = data.copy()
    if 'meta' in d: d['meta'] = {k:v for k,v in d['meta'].items() if k != 'integrity_hash'}
    return hashlib.sha256(json.dumps(d, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def write_atomic_json(path: Path, data: dict):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=str(path.parent), text=True)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, str(path))

def detect_binary_mode(eid: str, kind: str, profile: str) -> bool:
    return kind == "event_state" or eid.startswith("binary_sensor.") or "contact_state" in profile or "binary_state" in profile

def map_events_to_slots(events: List[Dict], start_dt: datetime, raw_policy: str, is_binary: bool) -> Tuple[List[Any], Dict]:
    slots, stats = [None] * SLOTS_PER_DAY, {"total_open_seconds": 0, "toggle_count": 0}
    parsed = []
    for e in events:
        state, ts_str = e.get("state"), e.get("last_changed")
        if state in (None, "unknown", "unavailable", "") or not ts_str: continue
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
            val = (1 if str(state).lower() in BINARY_TRUE_VALUES else 0) if is_binary else float(state)
            parsed.append((ts, val))
        except: continue
    parsed.sort(key=lambda x: x[0])
    if is_binary and parsed:
        stats["toggle_count"] = len(parsed)
        last_ts, last_val = start_dt, 0
        prior = [e for e in parsed if e[0] <= start_dt]
        if prior: last_val = prior[-1][1]
        for ts, val in [e for e in parsed if e[0] > start_dt]:
            if start_dt < ts < (start_dt + timedelta(days=1)):
                if last_val == 1: stats["total_open_seconds"] += max(0, (ts - last_ts).total_seconds())
            last_ts, last_val = ts, val
        if last_val == 1:
            eff_end = min(start_dt + timedelta(days=1), datetime.now(start_dt.tzinfo))
            stats["total_open_seconds"] += max(0, (eff_end - last_ts).total_seconds())
    event_idx, last_known = 0, None
    while event_idx < len(parsed) and parsed[event_idx][0] <= start_dt:
        last_known = parsed[event_idx][1]; event_idx += 1
    curr_end = start_dt + timedelta(minutes=5)
    for i in range(SLOTS_PER_DAY):
        vals = []
        while event_idx < len(parsed) and parsed[event_idx][0] <= curr_end:
            vals.append(parsed[event_idx][1]); last_known = parsed[event_idx][1]; event_idx += 1
        slots[i] = last_known if not vals else vals[-1]
        curr_end += timedelta(minutes=5)
    return slots, stats

def fetch_history(session, entity_ids, start_dt, end_dt, ha_url, mode):
    all_data = defaultdict(list)
    s_iso = start_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    e_iso = end_dt.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z")
    for i in range(0, len(entity_ids), 50):
        chunk = entity_ids[i:i+50]
        params = {"filter_entity_id": ",".join(chunk), "end_time": e_iso}
        if mode == "daily_truth": params.update({"minimal_response": "1", "no_attributes": "1"})
        try:
            resp = session.get(f"{ha_url}/api/history/period/{s_iso}", params=params, timeout=60)
            if resp.status_code == 200:
                for elist in resp.json():
                    if elist: all_data[elist[0]["entity_id"]] = elist
        except: continue
    return all_data

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--registry", default="registry.json"); parser.add_argument("--out", default="data")
    parser.add_argument("--timezone", default="Asia/Manila"); parser.add_argument("--mode", default="daily_truth")
    parser.add_argument("--date"); parser.add_argument("--start-date"); parser.add_argument("--end-date")
    parser.add_argument("--all-domains", action="store_true"); parser.add_argument("--domain")
    args = parser.parse_args()
    opt_path, ha_url = Path("/data/options.json"), "http://localhost:8123"
    if opt_path.exists():
        ha_url = "http://supervisor/core"
        with open(opt_path) as f:
            o = json.load(f)
            args.registry, args.out, args.timezone, args.mode = o.get("registry_path", args.registry), o.get("data_root", args.out), o.get("timezone", args.timezone), o.get("run_mode", args.mode)
            if o.get("process_all_domains"): args.all_domains = True
            else: args.domain = o.get("exporter_domain")
            if o.get("start_date") and o.get("end_date"): args.start_date, args.end_date = o["start_date"], o["end_date"]
            else: args.date = o.get("target_date")
    tz = ZoneInfo(args.timezone)
    dates = []
    if args.start_date and args.end_date:
        curr = datetime.strptime(args.start_date, "%Y-%m-%d").date()
        while curr <= datetime.strptime(args.end_date, "%Y-%m-%d").date(): dates.append(curr); curr += timedelta(days=1)
    elif args.date: dates.append(datetime.strptime(args.date, "%Y-%m-%d").date())
    else: dates.append(datetime.now(tz).date() - timedelta(days=1))
    with open(args.registry) as f: raw_reg = json.load(f)
    reg = {e: m for e, m in raw_reg.items() if m.get("roals_id") and m.get("exporter_domain")}
    avail = sorted(set(m["exporter_domain"] for m in reg.values()))
    targets = avail if args.all_domains else []
    if not args.all_domains and args.domain:
        req = args.domain if isinstance(args.domain, list) else [d.strip() for d in str(args.domain).split(",") if d.strip()]
        targets = [d for d in req if d in avail]
    if not targets: sys.exit(1)
    sess = requests.Session(); sess.headers.update(get_headers())
    for d in dates:
        s_dt = datetime.combine(d, dtime.min).replace(tzinfo=tz)
        for dom in targets:
            ents = {e: m for e, m in reg.items() if m["exporter_domain"] == dom}
            if not ents: continue
            hist = fetch_history(sess, list(ents.keys()), s_dt - timedelta(minutes=30), s_dt + timedelta(days=1), ha_url, args.mode)
            meta = {"version": "2026.DT", "mode": args.mode, "domain": dom, "date": str(d), "entities": {e: {"roals_id": m["roals_id"], "area_id": m["area_id"], "room_id": m["room_id"], "profile": m["profile"]} for e, m in ents.items()}}
            if args.mode == "daily_truth":
                ts = {"ts_iso": [(s_dt + timedelta(minutes=5*i)).isoformat() for i in range(SLOTS_PER_DAY)]}
                metrics = {}
                for e, m in ents.items():
                    pol = m.get("metric", {}).get("agg_policy", {}).get("primary") or m.get("agg_policy", {}).get("primary", "last")
                    is_b = detect_binary_mode(e, m.get("metric", {}).get("kind", "numeric"), str(m.get("profile", "")))
                    slots, st = map_events_to_slots(hist.get(e, []), s_dt, pol, is_b)
                    ts[e] = slots
                    if is_b and st["toggle_count"] > 0: metrics[e] = st
                p = {"meta": meta, "timeseries": ts, "summary": {"columns": {e: {"coverage_pct": round((sum(1 for v in ts[e] if v is not None)/288)*100, 1), "event_metrics": metrics.get(e)} for e in ents}}}
                p["meta"]["integrity_hash"] = calculate_integrity_hash(p)
                write_atomic_json(Path(args.out) / "daily" / dom / f"{d}_{dom}.json", p)

if __name__ == "__main__":
    main()
