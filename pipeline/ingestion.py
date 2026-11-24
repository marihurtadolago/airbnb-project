"""Ingestion: download the source files and check them before processing.

Pipeline tasks covered here (see docs/orchestration_plan.md, section 5):
    task 2  download_files  download the four Inside Airbnb files to data/raw/<snapshot>/
    task 4  validate_raw    check that every raw file exists, is not empty and
                            has the columns the rest of the pipeline needs

Task 3 (store_raw, upload to Azure) lives in this module too, but it is added
together with pipeline/cloud.py.

Run it on its own with:
    python -m pipeline.ingestion            # uses default_snapshot_date from the config
    python -m pipeline.ingestion 2026-09-17
"""

import csv
import gzip
import sys
import time
from pathlib import Path

import requests

from pipeline.utils import get_logger, load_config, snapshot_dirs

log = get_logger("ingestion")

# Columns each file must contain. Only the columns used later in the pipeline
# are listed, so extra columns added by Inside Airbnb do not break the run.
REQUIRED_COLUMNS = {
    "listings": {"id", "host_id", "neighbourhood_cleansed", "neighbourhood_group_cleansed",
                 "room_type", "accommodates", "price"},
    "calendar": {"listing_id", "date", "available", "minimum_nights", "maximum_nights"},
    "reviews": {"listing_id", "id", "date"},
    "neighbourhoods": {"neighbourhood_group", "neighbourhood"},
}

CHUNK_SIZE = 1024 * 1024  # download in 1 MB pieces, so large files never fill memory
TIMEOUT_SECONDS = 60      # maximum wait for the server to answer


def raw_file_path(raw_dir: Path, relative_path: str) -> Path:
    """Local path of a raw file, keeping its original name (e.g. listings.csv.gz)."""
    return raw_dir / Path(relative_path).name


def download_file(url: str, target: Path) -> int:
    """Download one file safely and return its size in bytes.

    The file is first written as <name>.part and only renamed when the download
    has finished. If the download fails halfway, no incomplete file is left with
    the final name, so later steps can never read a broken file by mistake.
    """
    part_file = target.with_name(target.name + ".part")
    with requests.get(url, stream=True, timeout=TIMEOUT_SECONDS) as response:
        response.raise_for_status()  # stop on 404, 500, ... instead of saving an error page
        with open(part_file, "wb") as f:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                f.write(chunk)
    part_file.replace(target)  # atomic rename: the file appears complete or not at all
    return target.stat().st_size


def download_files(snapshot_date: str) -> dict:
    """Task 2: download the four source files of one snapshot.

    Raw files are never overwritten: if a file is already present, it is kept and
    the download is skipped. This makes the task safe to run again after a failure.
    Returns {dataset: local path}.
    """
    config = load_config()
    raw_dir = snapshot_dirs(snapshot_date)["raw"]
    downloaded = {}

    for dataset, relative_path in config["files"].items():
        target = raw_file_path(raw_dir, relative_path)
        if target.exists():
            log.info("%s: already in raw storage, skipping download (%s)", dataset, target.name)
        else:
            url = f"{config['base_url']}/{snapshot_date}/{relative_path}"
            log.info("%s: downloading %s", dataset, url)
            start = time.perf_counter()
            size = download_file(url, target)
            log.info("%s: saved %s (%.1f MB in %.1f s)",
                     dataset, target.name, size / 1e6, time.perf_counter() - start)
        downloaded[dataset] = target

    return downloaded


def read_header(path: Path) -> list:
    """Return the column names of a CSV file, compressed (.gz) or not."""
    opener = gzip.open if path.suffix == ".gz" else open
    with opener(path, "rt", encoding="utf-8", newline="") as f:
        return next(csv.reader(f))


def validate_raw(snapshot_date: str) -> dict:
    """Task 4: quality gate on the raw files.

    Checks that each file exists, is not empty and contains the required columns.
    Bad files will not fix themselves, so this task does not retry: it raises an
    error and stops the run. Returns a summary per dataset for the evidence report.
    """
    config = load_config()
    raw_dir = snapshot_dirs(snapshot_date)["raw"]
    summary, problems = {}, []

    for dataset, relative_path in config["files"].items():
        path = raw_file_path(raw_dir, relative_path)

        if not path.exists():
            problems.append(f"{dataset}: file not found ({path.name})")
            continue
        if path.stat().st_size == 0:
            problems.append(f"{dataset}: file is empty ({path.name})")
            continue

        missing = REQUIRED_COLUMNS[dataset] - set(read_header(path))
        if missing:
            problems.append(f"{dataset}: missing columns {sorted(missing)}")
            continue

        summary[dataset] = {"file": path.name, "size_mb": round(path.stat().st_size / 1e6, 1)}
        log.info("%s: raw file OK (%s MB, required columns present)",
                 dataset, summary[dataset]["size_mb"])

    if problems:
        for problem in problems:
            log.error("validate_raw: %s", problem)
        raise ValueError(f"Raw validation failed for snapshot {snapshot_date}: {problems}")

    log.info("validate_raw: all %d raw files passed", len(summary))
    return summary


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else load_config()["default_snapshot_date"]
    download_files(date)
    validate_raw(date)
