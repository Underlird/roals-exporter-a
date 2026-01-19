# ROALS Exporter A (Truth Layer)

Deterministic daily JSON skeleton exporter for Home Assistant (HAOS Add-on).

Core principles:
- Truth Layer only: no semantics, no interpretation, no heuristics.
- Write-once: same day + same domain file must never be overwritten.
- Deterministic outputs: stable ordering, stable hashing, stable file layout.
- 5-minute raster: 288 slots per day.

## Output location

Exporter writes into `data_root` using this structure:

`{data_root}/{YYYY}/{MM}/{YYYY-MM-DD}_{domain}.json`

Default:
- `data_root = /share/nara_data`

Example:
- `/share/nara_data/2026/01/2026-01-19_system.json`

## Run modes

### 1) `skeleton`
Validates configuration and exits.
- No files written.
- Use for installation tests and config sanity checks.

Required:
- none (safe defaults)

---

### 2) `oneshot_today`
Writes exactly ONE domain file for today (in configured timezone).
- Enforces write-once.

Required:
- `exporter_domain`
- `entities`

Optional:
- `target_date` must be empty (today)

---

### 3) `oneshot_date`
Writes exactly ONE domain file for a specific date.
- Enforces write-once.

Required:
- `exporter_domain`
- `entities`
- `target_date` (format: `YYYY-MM-DD`)

---

### 4) `daily_all_domains`
Writes ALL 13 domain files for the target date (default: today).
- Uses `entity_registry.json` for routing (registry-based entity grouping).
- Enforces write-once (fails loud if any output file already exists).
- Always writes 13 files/day (even if a domain has zero active entities).

Required:
- `registry_path`

Optional:
- `target_date` (empty = today)
- `strict_registry` (default: true)

## Domain allowlist (13 domains)

Exactly these domains are allowed (must match file naming):

budget  
cameras  
climate_eg  
climate_og  
energy  
events  
internet  
motion  
network  
prices  
security  
system  
weather  

Any other domain value is rejected.

## Registry (routing only)

Registry is a manual, human-owned meaning catalog.  
Exporter A uses it **only for technical routing** in `daily_all_domains`.

Default path:
- `/share/nara_data/registry/entity_registry.json`

Minimal format:

```json
{
  "sensor.example_temperature": {
    "exporter_domain": "climate_og",
    "area": "og_schlafzimmer",
    "since": "2026-01-17",
    "until": null,
    "replaced_by": null,
    "notes": "optional"
  }
}
