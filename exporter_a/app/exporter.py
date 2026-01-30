{
  "name": "ROALS Exporter A",
  "version": "0.7.1",
  "slug": "roals_exporter_a",
  "description": "ROALS Truth Layer - Daily Truth (288 Slots) mit Batch- & History-Support",
  "arch": ["amd64", "aarch64", "armv7", "armhf"],
  "startup": "once",
  "boot": "manual",
  "hassio_api": true,
  "homeassistant_api": true,
  "map": ["share:rw"],
  "options": {
    "run_mode": "daily_truth",
    "process_all_domains": false,
    "exporter_domain": "climate_og",
    "target_date": "",
    "start_date": "",
    "end_date": "",
    "log_level": "INFO",
    "timezone": "Asia/Manila",
    "data_root": "/share/nara_data",
    "registry_path": "/share/nara_data/registry/entity_registry.json"
  },
  "schema": {
    "run_mode": "list(daily_truth|raw_snapshot)",
    "process_all_domains": "bool",
    "exporter_domain": "list(climate_og|budget|cameras|climate_eg|energy|events|internet|motion|network|prices|security|system|weather)",
    "target_date": "str?",
    "start_date": "str?",
    "end_date": "str?",
    "log_level": "list(DEBUG|INFO|WARNING|ERROR)",
    "timezone": "str",
    "data_root": "str",
    "registry_path": "str"
  }
}

