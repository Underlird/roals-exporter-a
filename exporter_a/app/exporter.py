#!/usr/bin/env python3
"""
ROALS Exporter A (Truth Layer) — Registry-First Production Release
Version: 2026.1.15-lks

Architektur: Registry-First mit Entity Override
Run-Modes: skeleton, oneshot_domain, oneshot_all_domains, daily_all_domains
Garantien: Atomic Write, fsync Durability, Deterministic Hashing, LKS Sampling
Audit-Policy: Oneshot schreibt immer Dateien (auch bei 0 Entities)
Bugfix: oneshot_all_domains erfordert keine exporter_domain mehr
"""

from __future__ import annotations
import datetime as dt
import hashlib
import json
import os
import re
import sys
import shutil
import urllib.request
import urllib.parse
import urllib.error
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    from zoneinfo import ZoneInfo
except ImportError:
    try:
        from backports.zoneinfo import ZoneInfo
    except ImportError:
        print("CRITICAL: zoneinfo module fehlt. Python 3.9+ erforderlich.", file=sys.stderr)
        sys.exit(2)

EXPORTER_VERSION = "2026.1.15-lks"
RASTER_MINUTES = 5
SLOTS_PER_DAY = 288
API_TIMEOUT = 120
HISTORY_LOOKBACK_MIN = 15
ALLOWED_DOMAINS = ["budget", "cameras", "climate_eg", "climate_og", "energy", "events", "internet", "motion", "network", "prices", "security", "system", "weather"]
VALID_RUN_MODES = {"skeleton", "oneshot_domain", "oneshot_all_domains", "daily_all_domains"}
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}

def log(level: str, msg: str, cfg_level: str = "INFO") -> None:
    """
    Schreibt Log-Nachrichten mit Zeitstempel auf stdout.
    Nachrichten werden nur ausgegeben wenn ihr Level >= dem konfigurierten Level ist.
    """
    levels = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40}
    if levels.get(level.upper(), 20) >= levels.get(cfg_level.upper(), 20):
        now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()
        print(f"{now} [{level.upper()}] {msg}", flush=True)

@dataclass(frozen=True)
class Settings:
    """
    Konfiguration des Exporters aus Home Assistant Add-on Options.
    Immutable Dataclass für Thread-Safety und Debugging.
    """
    data_root: str
    timezone: str
    log_level: str
    run_mode: str
    target_date: Optional[str]
    exporter_domain: Optional[str]
    entities: List[str]
    registry_path: str

    @staticmethod
    def from_options(opts: Dict[str, Any]) -> "Settings":
        """
        Factory-Methode die Add-on Options validiert und Settings-Objekt erstellt.
        Wirft ValueError bei ungültigen Konfigurationen.
        """
        log_lvl = str(opts.get("log_level", "INFO")).strip().upper()
        if log_lvl not in VALID_LOG_LEVELS:
            log_lvl = "INFO"
        
        # Timezone validieren durch Versuch sie zu laden
        tz_str = str(opts.get("timezone", "Asia/Manila")).strip()
        try:
            ZoneInfo(tz_str)
        except Exception as e:
            raise ValueError(f"Ungueltige Timezone '{tz_str}': {e}")

        # Run-Mode muss einer der vier erlaubten Modi sein
        mode = str(opts.get("run_mode", "skeleton")).strip()
        if mode not in VALID_RUN_MODES:
            raise ValueError(f"Ungueltiger run_mode '{mode}'. Erlaubt: {', '.join(VALID_RUN_MODES)}")

        target_date = opts.get("target_date")
        
        # Target Date muss ISO 8601 Format haben wenn gesetzt
        if mode in ("oneshot_domain", "oneshot_all_domains") and target_date:
            try:
                dt.date.fromisoformat(target_date)
            except ValueError:
                raise ValueError(f"Ungueltiges target_date Format '{target_date}'. Erwarte YYYY-MM-DD.")

        exporter_domain = opts.get("exporter_domain")
        
        # KRITISCH: Domain ist NUR für oneshot_domain erforderlich
        # oneshot_all_domains verarbeitet alle Domains, egal was in UI steht
        if mode == "oneshot_domain":
            if not exporter_domain:
                raise ValueError("oneshot_domain erfordert exporter_domain. Bitte Domain in UI auswaehlen.")
            if exporter_domain not in ALLOWED_DOMAINS:
                raise ValueError(f"Ungueltige exporter_domain '{exporter_domain}'. Erlaubt: {', '.join(ALLOWED_DOMAINS)}")

        return Settings(
            data_root=str(opts.get("data_root", "/share/nara_data")).strip(),
            timezone=tz_str,
            log_level=log_lvl,
            run_mode=mode,
            target_date=target_date,
            exporter_domain=exporter_domain,
            entities=opts.get("entities", []),
            registry_path=str(opts.get("registry_path", "/share/nara_data/registry/entity_registry.json")).strip(),
        )

def load_registry(path: str, cfg_level: str) -> Dict[str, Any]:
    """
    Lädt die Entity Registry JSON und filtert auf gültige Entity-Typen.
    Gibt leeres Dict zurück bei Fehlern statt abzubrechen.
    """
    if not os.path.exists(path):
        log("WARNING", f"Registry nicht gefunden: {path}", cfg_level)
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Nur Entities mit diesen Präfixen sind für Export relevant
        entity_prefixes = ("sensor.", "binary_sensor.", "climate.", "switch.", "light.", "input_")
        return {k: v for k, v in data.items() if isinstance(v, dict) and k.startswith(entity_prefixes)}
    except json.JSONDecodeError as e:
        log("ERROR", f"Registry JSON korrupt: {e}", cfg_level)
        return {}
    except OSError as e:
        log("ERROR", f"Registry Lesefehler: {e}", cfg_level)
        return {}
    except Exception as e:
        log("ERROR", f"Registry unerwarteter Fehler: {e}", cfg_level)
        return {}

def entity_is_active(meta: Dict[str, Any], target: dt.date) -> bool:
    """
    Prüft ob eine Entity am Zieldatum aktiv war basierend auf since/until Feldern.
    Entities ohne since-Datum gelten als seit 1970 aktiv (Backwards-Kompatibilität).
    """
    try:
        since = dt.date.fromisoformat(str(meta.get("since"))) if meta.get("since") else dt.date(1970, 1, 1)
        if meta.get("until"):
            # Entity hat Enddatum: Prüfe ob target im Gültigkeitsfenster liegt
            return since <= target < dt.date.fromisoformat(str(meta.get("until")))
        # Keine until-Grenze: Entity ist ab since-Datum unbegrenzt gültig
        return target >= since
    except (ValueError, TypeError):
        # Ungültige Datumsformate werden als inaktiv behandelt
        return False

def check_disk_space(path: str, min_mb: int = 100) -> bool:
    """
    Prüft ob mindestens min_mb freier Speicherplatz verfügbar ist.
    Gibt True zurück wenn Pfad nicht existiert (noch keine Dateien geschrieben).
    """
    try:
        if not os.path.exists(path):
            return True
        return (shutil.disk_usage(path).free / (1024 * 1024)) >= min_mb
    except OSError:
        return True

def update_heartbeat(tz: ZoneInfo, cfg_level: str) -> None:
    """
    Aktualisiert input_datetime.nara_last_success in Home Assistant.
    Best-Effort: Fehler werden geloggt aber brechen Export nicht ab.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        return
    try:
        url = "http://supervisor/core/api/services/input_datetime/set_datetime"
        data = json.dumps({
            "entity_id": "input_datetime.nara_last_success",
            "datetime": dt.datetime.now(tz=tz).isoformat()
        }).encode()
        req = urllib.request.Request(url, data=data, headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        })
        resp = urllib.request.urlopen(req, timeout=10)
        if resp.status != 200:
            log("WARNING", f"Heartbeat Status {resp.status}", cfg_level)
    except urllib.error.HTTPError as e:
        if e.code == 404:
            log("WARNING", "Heartbeat-Entity 'input_datetime.nara_last_success' fehlt in HA.", cfg_level)
        else:
            log("WARNING", f"Heartbeat HTTP Error: {e}", cfg_level)
    except Exception as e:
        log("WARNING", f"Heartbeat Fehler: {e}", cfg_level)

def fetch_ha_history(entity_ids: List[str], day: dt.date, tz: ZoneInfo, cfg_level: str) -> Dict[str, List[Dict]]:
    """
    Holt History-Daten von Home Assistant Supervisor API für gegebene Entities und Tag.
    Nutzt ALLE wichtigen API-Parameter um vollständige Daten zu bekommen:
    - significant_changes_only=0: ALLE State-Changes, nicht nur signifikante
    - minimal_response=0: Vollständige Response mit allen Feldern
    - no_attributes=1: Attribute weglassen für Performance
    
    Gibt Dict[entity_id -> List[Event]] zurück.
    """
    token = os.environ.get("SUPERVISOR_TOKEN")
    if not token:
        log("ERROR", "SUPERVISOR_TOKEN fehlt. API-Zugriff unmoeglich.", cfg_level)
        return {eid: [] for eid in entity_ids}
    
    if not entity_ids:
        return {}

    t0 = dt.datetime.now()
    
    # Zeitfenster: Tag mit 15min Lookback für Initial-State
    start_dt = dt.datetime.combine(day, dt.time(0, 0), tzinfo=tz) - dt.timedelta(minutes=HISTORY_LOOKBACK_MIN)
    end_dt = dt.datetime.combine(day, dt.time(23, 59, 59), tzinfo=tz)
    
    # API erwartet UTC-Timestamps im ISO 8601 Format mit Z-Suffix
    start_iso = start_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    end_iso = end_dt.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    
    # KRITISCHE Parameter: Müssen als Strings übergeben werden
    params = {
        "filter_entity_id": ",".join(entity_ids),
        "end_time": end_iso,
        "minimal_response": "0",              # Vollständige Response
        "no_attributes": "1",                 # Attribute nicht laden
        "significant_changes_only": "0"       # ALLE Changes, nicht gefiltert
    }
    
    url = f"http://supervisor/core/api/history/period/{urllib.parse.quote(start_iso)}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    })
    
    log("DEBUG", f"History API Request fuer {len(entity_ids)} Entities", cfg_level)
    
    try:
        with urllib.request.urlopen(req, timeout=API_TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            
            # Performance-Monitoring: Warne bei langsamen Responses
            dur = (dt.datetime.now() - t0).total_seconds()
            if dur > 30:
                log("WARNING", f"Langsame API Antwort: {dur:.1f}s fuer {len(entity_ids)} Sensoren", cfg_level)
            
            # API gibt Liste von Listen zurück: [[events_entity1], [events_entity2], ...]
            total_items = sum(len(el) for el in data if isinstance(el, list))
            log("DEBUG", f"API returned {total_items} total items in {len(data)} lists", cfg_level)
            
            # Mapping aufbauen: entity_id -> events
            h_map = {eid: [] for eid in entity_ids}
            for entity_list in data:
                if entity_list and len(entity_list) > 0:
                    first_eid = entity_list[0].get("entity_id")
                    if first_eid in h_map:
                        h_map[first_eid] = entity_list
            
            # Debug-Output pro Entity
            for eid, events in h_map.items():
                log("DEBUG", f"Entity {eid}: {len(events)} events geladen", cfg_level)
            
            return h_map
            
    except urllib.error.HTTPError as e:
        log("ERROR", f"HTTP Error {e.code}: {e.reason}", cfg_level)
        return {eid: [] for eid in entity_ids}
    except urllib.error.URLError as e:
        log("ERROR", f"URL Error: {e.reason}", cfg_level)
        return {eid: [] for eid in entity_ids}
    except Exception as e:
        log("ERROR", f"API Fehler: {e}", cfg_level)
        return {eid: [] for eid in entity_ids}

def map_history_to_slots(ts_iso: List[str], history: List[Dict], locf_max_min: int) -> List[Any]:
    """
    Last-Known-State Sampling: Mappt History-Events auf 288 5-Minuten-Slots.
    
    Algorithmus:
    1. Extrahiere Timestamps und States aus History
    2. Sortiere Events chronologisch
    3. Für jeden Slot: Finde letzten bekannten gültigen State vor/bei Slot-Zeit
    4. LOCF-Policy: Wenn letzter State zu alt (> locf_max_min), schreibe None
    
    Das garantiert deterministische Werte auch bei unregelmäßigen Sensor-Updates.
    """
    events = []
    for h in history:
        t_str = h.get("last_updated") or h.get("last_changed")
        if t_str:
            # Normalisiere auf naive UTC datetime für einfacheren Vergleich
            dt_utc = dt.datetime.fromisoformat(t_str.replace("Z", "+00:00")).astimezone(dt.timezone.utc)
            events.append((dt_utc.replace(tzinfo=None), h.get("state")))
    
    events.sort(key=lambda x: x[0])
    
    values = []
    last_val = None
    last_val_time = None
    e_idx = 0
    
    for slot_str in ts_iso:
        # Slot-Zeit auch auf naive UTC normalisieren
        slot_dt_utc = dt.datetime.fromisoformat(slot_str).astimezone(dt.timezone.utc).replace(tzinfo=None)
        
        # Vorwärts durch Events bis wir über Slot-Zeit hinaus sind
        while e_idx < len(events) and events[e_idx][0] <= slot_dt_utc:
            state = events[e_idx][1]
            # Filtere ungültige States
            if state not in (None, "unknown", "unavailable", ""):
                state_clean = str(state).strip()
                if state_clean:
                    try:
                        last_val = float(state_clean)
                    except ValueError:
                        # Nicht-numerische States (z.B. "on", "off") als String behalten
                        last_val = state_clean
                    last_val_time = events[e_idx][0]
            e_idx += 1
        
        # LOCF Policy: Wenn letzter State zu alt, schreibe None statt veralteten Wert
        if last_val_time and (slot_dt_utc - last_val_time).total_seconds() / 60 > locf_max_min:
            values.append(None)
        else:
            values.append(last_val)
    
    return values

def build_daily_payload(domain: str, day: dt.date, tz: ZoneInfo, entities_meta: Dict[str, Any], cfg_level: str) -> Dict[str, Any]:
    """
    Baut das tägliche JSON-Payload mit 288 Slots für eine Domain.
    
    Struktur:
    - meta: Version, Domain, Datum, Integrity-Hash
    - timeseries: ts_iso + eine Spalte pro Entity
    
    Integrity Hash wird über das gesamte Payload (ohne Hash selbst) berechnet.
    """
    # 288 Slots von Mitternacht bis 23:55 in 5-Minuten-Schritten
    start_dt = dt.datetime(day.year, day.month, day.day, 0, 0, 0, tzinfo=tz)
    ts_iso = [(start_dt + dt.timedelta(minutes=RASTER_MINUTES * i)).isoformat() for i in range(SLOTS_PER_DAY)]
    
    sorted_eids = sorted(entities_meta.keys())
    
    # Hole History-Daten für alle Entities in einem API-Call
    if sorted_eids:
        history_map = fetch_ha_history(sorted_eids, day, tz, cfg_level)
    else:
        history_map = {}
    
    timeseries = {"ts_iso": ts_iso}
    source_map = {}
    
    for eid in sorted_eids:
        metric = entities_meta[eid].get("metric", {})
        suffix = metric.get("column_unit_suffix")
        if suffix is None:
            log("WARNING", f"{eid}: suffix fehlt, nutze ''", cfg_level)
            suffix = ""
        
        # LOCF Max Duration aus Registry-Metric
        locf_max = metric.get("agg_policy", {}).get("locf_max_duration_min", 15)
        if not isinstance(locf_max, (int, float)) or locf_max <= 0:
            locf_max = 15
        
        # Column Key: normalisierte entity_id + unit suffix
        col_key = f"{re.sub(r'[^a-z0-9]+', '_', eid.lower()).strip('_')}{suffix}"
        if col_key in source_map:
            # Key-Collision würde zu Datenverlust führen - harter Fehler
            raise ValueError(f"CRITICAL: Key Collision fuer {col_key} ({eid} vs {source_map[col_key]['ha_entity_id']})")
        
        source_map[col_key] = {"ha_entity_id": eid}
        timeseries[col_key] = map_history_to_slots(ts_iso, history_map.get(eid, []), locf_max)
    
    payload = {
        "meta": {
            "version": EXPORTER_VERSION,
            "domain": domain,
            "date": day.isoformat(),
            "timezone": str(tz),
            "generated_at": dt.datetime.now(tz=tz).isoformat(),
            "columns_source_map": source_map,
            "integrity_hash": ""
        },
        "timeseries": timeseries
    }
    
    # Deterministic Hash: Sortierte Keys, keine Spaces, UTF-8
    s = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    payload["meta"]["integrity_hash"] = hashlib.sha256(s.encode()).hexdigest()
    
    return payload

def main() -> int:
    """
    Hauptlogik des Exporters.
    
    Flow:
    1. Lade und validiere Konfiguration
    2. Lade Registry
    3. Bestimme zu verarbeitende Domains basierend auf run_mode
    4. Für jede Domain: Filtere aktive Entities, hole Daten, schreibe JSON
    5. Update Heartbeat bei Erfolg
    
    Exit Codes:
    0: Erfolg
    1: Teilweise Fehler (einige Domains fehlgeschlagen)
    2: Kritischer Fehler (Konfiguration, Disk voll, etc.)
    """
    stats = {"written": 0, "skipped_exists": 0, "skipped_empty": 0, "failed": 0}
    exit_code = 0
    
    try:
        # Lade Add-on Options aus Home Assistant
        with open("/data/options.json", "r") as f:
            opts = json.load(f)
        
        cfg = Settings.from_options(opts)
        
        # Pre-Flight Check: Disk Space
        if not check_disk_space(cfg.data_root):
            log("ERROR", f"Disk voll: {cfg.data_root}")
            return 2
        
        tz = ZoneInfo(cfg.timezone)
        # Nutze target_date wenn gesetzt, sonst heutiges Datum in Timezone
        day = dt.date.fromisoformat(cfg.target_date) if cfg.target_date else dt.datetime.now(tz=tz).date()
        
        # Skeleton Mode: Tue nichts, beende erfolgreich
        if cfg.run_mode == "skeleton":
            log("INFO", "Skeleton Mode beendet.")
            return 0
        
        # Lade Entity Registry
        reg = load_registry(cfg.registry_path, cfg.log_level)
        
        # Bestimme Domains basierend auf Run-Mode
        if cfg.run_mode == "daily_all_domains":
            domains = sorted(ALLOWED_DOMAINS)
        elif cfg.run_mode == "oneshot_all_domains":
            domains = sorted(ALLOWED_DOMAINS)
        elif cfg.run_mode == "oneshot_domain":
            domains = [cfg.exporter_domain]
        else:
            log("ERROR", f"Unbekannter run_mode: {cfg.run_mode}")
            return 1
        
        log("INFO", f"Exporter {EXPORTER_VERSION} Start: Mode={cfg.run_mode}, Day={day}, Domains={len(domains)}", cfg.log_level)
        
        # Verarbeite jede Domain separat
        for dom in domains:
            if not dom:
                continue
            
            try:
                # Entity-Auswahl: Entweder Override aus Config oder Registry-Filter
                if cfg.entities:
                    log("INFO", f"Domain {dom}: Entity-Override aktiv ({len(cfg.entities)} Entities)", cfg.log_level)
                    active = {eid: reg.get(eid, {"metric": {}}) for eid in cfg.entities}
                else:
                    # Registry-First: Filtere auf Domain und Aktivitätsdatum
                    active = {
                        eid: meta
                        for eid, meta in reg.items()
                        if meta.get("exporter_domain") == dom and entity_is_active(meta, day)
                    }
                    log("INFO", f"Domain {dom}: Registry-First Routing ({len(active)} aktive Entities)", cfg.log_level)
                
                # Zieldatei-Pfad: /share/nara_data/YYYY/MM/YYYY-MM-DD_domain.json
                out = os.path.join(cfg.data_root, f"{day.year:04d}", f"{day.month:02d}", f"{day.isoformat()}_{dom}.json")
                
                # Write-Once Policy: Existierende Dateien nicht überschreiben
                if os.path.exists(out):
                    log("WARNING", f"Datei existiert bereits: {out}", cfg.log_level)
                    stats["skipped_exists"] += 1
                    continue
                
                # Daily Mode: Überspringe leere Domains stillschweigend
                if not active and cfg.run_mode == "daily_all_domains":
                    log("INFO", f"Domain {dom}: Keine aktiven Entities, ueberspringe", cfg.log_level)
                    stats["skipped_empty"] += 1
                    continue
                
                # Oneshot Mode: Schreibe auch leere Dateien für Audit-Trail
                if not active and cfg.run_mode in ("oneshot_domain", "oneshot_all_domains"):
                    log("WARNING", f"Domain {dom}: Keine aktiven Entities, schreibe Audit-Datei mit ts_iso only", cfg.log_level)
                
                log("INFO", f"Verarbeite {dom} ({len(active)} Entities)...", cfg.log_level)
                payload = build_daily_payload(dom, day, tz, active, cfg.log_level)
                
                # Atomic Write: Schreibe in .tmp, dann rename
                os.makedirs(os.path.dirname(out), exist_ok=True)
                tmp = out + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(payload, f, separators=(",", ":"), sort_keys=True)
                    f.flush()
                    os.fsync(f.fileno())  # Force to disk
                os.replace(tmp, out)  # Atomic rename
                
                # Sanity Check: Sehr kleine Dateien sind verdächtig
                if os.path.getsize(out) < 1024:
                    log("WARNING", f"Datei verdaechtig klein: {out} ({os.path.getsize(out)} Bytes)", cfg.log_level)
                
                log("INFO", f"SUCCESS: {out}", cfg.log_level)
                stats["written"] += 1
                
            except Exception as e:
                log("ERROR", f"Fehler in {dom}: {e}", cfg.log_level)
                import traceback
                traceback.print_exc()
                stats["failed"] += 1
                exit_code = 1
        
        # Update Heartbeat nur bei mindestens einem erfolgreichen Write
        if stats["written"] > 0:
            update_heartbeat(tz, cfg.log_level)
        
        # Exit Code 1 wenn Fehler aufgetreten, sonst 0
        if stats["failed"] > 0:
            exit_code = 1
        
    except ValueError as e:
        # Konfigurationsfehler: User-freundliche Fehlermeldung
        log("ERROR", f"Konfigurationsfehler: {e}")
        exit_code = 1
    except Exception as e:
        # Unerwartete Fehler: Full Traceback für Debugging
        log("ERROR", f"Globaler Absturz: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 2
    finally:
        # Statistik immer ausgeben für Transparenz
        log("INFO", f"Run Summary: {stats}")
    
    return exit_code

if __name__ == "__main__":
    sys.exit(main())
