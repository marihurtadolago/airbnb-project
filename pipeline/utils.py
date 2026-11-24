"""Shared helpers used by every pipeline module.

This module keeps three things in one place so that the rest of the code
does not repeat them:
    1. the project folders (raw, curated, rejected, evidence, logs, warehouse),
    2. reading the source configuration (config/sources.json),
    3. a common logger that writes to the console and to logs/.
"""

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

# Root of the repository (the folder that contains pipeline/).
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CONFIG_FILE = PROJECT_ROOT / "config" / "sources.json"
DATA_DIR = PROJECT_ROOT / "data"
EVIDENCE_DIR = PROJECT_ROOT / "evidence"
LOGS_DIR = PROJECT_ROOT / "logs"
WAREHOUSE_FILE = PROJECT_ROOT / "warehouse" / "airbnb.duckdb"


def load_config() -> dict:
    """Read config/sources.json and return it as a dictionary."""
    with open(CONFIG_FILE, encoding="utf-8") as f:
        return json.load(f)


def snapshot_dirs(snapshot_date: str) -> dict:
    """Return the raw, curated and rejected folders of one snapshot.

    Every snapshot gets its own folders (for example data/raw/2026-09-17/),
    so different snapshots never overwrite each other. Folders are created
    if they do not exist yet.
    """
    dirs = {
        layer: DATA_DIR / layer / snapshot_date
        for layer in ("raw", "curated", "rejected")
    }
    for path in dirs.values():
        path.mkdir(parents=True, exist_ok=True)
    return dirs


def get_logger(name: str) -> logging.Logger:
    """Return a logger that writes to the console and to logs/pipeline_<date>.log.

    All modules share the same log file per day, so one run can be followed
    from start to end. Times are in UTC, like the pipeline schedule.
    """
    logger = logging.getLogger(name)
    if logger.handlers:  # already configured: avoid duplicated lines
        return logger

    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOGS_DIR / f"pipeline_{datetime.now(timezone.utc):%Y-%m-%d}.log"

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    formatter.converter = lambda *_: datetime.now(timezone.utc).timetuple()

    for handler in (logging.StreamHandler(), logging.FileHandler(log_file, encoding="utf-8")):
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    logger.setLevel(logging.INFO)
    return logger
