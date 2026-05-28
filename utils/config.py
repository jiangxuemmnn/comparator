"""Configuration loader for comparator tool."""

import os
import yaml


DEFAULT_CONFIG = {
    "databases": {},
    "comparison": {
        "tables": [],
        "exclude_tables": [],
        "schema": "public",
        "chunk_size": 10000,
        "parallel": 4,
    },
    "rpo": {
        "marker_schema": "public",
    },
    "workload": {
        "concurrency": 10,
        "duration": 300,
    },
    "schedule": {
        "interval": 0,
        "report_dir": "./reports",
    },
    "logging": {
        "level": "INFO",
        "file": "./logs/comparator.log",
    },
}


def load_config(path: str) -> dict:
    """Load YAML config file, merging with defaults."""
    if not os.path.exists(path):
        raise FileNotFoundError("Config file not found: %s" % path)

    with open(path, "r", encoding="utf-8") as f:
        user_config = yaml.safe_load(f) or {}

    config = _deep_merge(DEFAULT_CONFIG, user_config)

    _validate(config)
    return config


def _deep_merge(base: dict, override: dict) -> dict:
    """Recursively merge override into base dict."""
    result = base.copy()
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _validate(config: dict):
    """Validate config has required fields."""
    if not config.get("databases"):
        raise ValueError("At least one database node must be configured in 'databases'")
    if "schedule" not in config:
        raise ValueError("Missing 'schedule' section in config")
    interval = config["schedule"].get("interval", 0)
    if not isinstance(interval, int) or interval < 0:
        raise ValueError("schedule.interval must be a non-negative integer")


def generate_template(path: str):
    """Generate a default config template file with comments.

    Copies from the bundled template file (config.yaml in project root).
    """
    import os as _os
    _template = _os.path.join(_os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))), "config.yaml.example")
    if _os.path.exists(_template):
        with open(_template, "r", encoding="utf-8") as src:
            content = src.read()
    else:
        # Fallback: dump without comments
        content = (
            "# Database Consistency Comparator - Configuration\n"
            + yaml.dump(DEFAULT_CONFIG, default_flow_style=False, allow_unicode=True)
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
