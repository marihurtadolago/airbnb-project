"""Integration: clean, validate and combine the four datasets with DuckDB.

Pipeline tasks covered here (see docs/orchestration_plan.md, section 5):
    task 5  transform_clean    select and type the needed columns; valid rows go to
                               data/curated/<snapshot>/, invalid rows to
                               data/rejected/<snapshot>/ with the rejection reason
    task 6  aggregate_all      build listing_summary: availability and review activity
                               per listing, aggregated separately and then joined
    task 7  validate_curated   quality gate on the curated files (unique keys,
                               every listing and neighbourhood matches its reference)

All transformations are SQL queries executed by DuckDB. DuckDB reads the compressed
CSV files directly and writes Parquet, so large files (8.4 million calendar rows)
are never loaded into Python memory.

Each task reads its inputs from files and writes its outputs to files, so the
tasks can run as separate Airflow tasks.

Run it on its own with:
    python -m pipeline.integration            # uses default_snapshot_date
    python -m pipeline.integration 2026-09-17
"""

import json
import sys
import time

import duckdb

from pipeline.utils import EVIDENCE_DIR, get_logger, load_config, snapshot_dirs

log = get_logger("integration")

ROOM_TYPES = ("Entire home/apt", "Private room", "Shared room", "Hotel room")


def raw_csv(raw_dir, name: str) -> str:
    """SQL expression that reads one raw CSV file with every column as text.

    Reading everything as text means no row is lost because of an unexpected
    value: types are converted afterwards with TRY_CAST, and rows that cannot be
    converted are rejected with a clear reason instead of stopping the load.
    """
    path = next(raw_dir.glob(f"{name}.csv*"))  # listings.csv.gz or neighbourhoods.csv
    return f"read_csv('{path}', header = true, all_varchar = true)"


def write_split(con, table: str, curated_dir, rejected_dir) -> dict:
    """Write valid rows to curated/<table>.parquet and invalid rows to rejected/.

    The temporary table must have a reject_reason column, NULL for valid rows.
    Returns the counts used in the evidence report.
    """
    con.execute(f"""
        COPY (SELECT * EXCLUDE (reject_reason) FROM {table} WHERE reject_reason IS NULL)
        TO '{curated_dir / table}.parquet' (FORMAT parquet)""")
    con.execute(f"""
        COPY (SELECT * FROM {table} WHERE reject_reason IS NOT NULL)
        TO '{rejected_dir / table}.parquet' (FORMAT parquet)""")

    total, = con.execute(f"SELECT count(*) FROM {table}").fetchone()
    reasons = dict(con.execute(f"""
        SELECT reject_reason, count(*) FROM {table}
        WHERE reject_reason IS NOT NULL GROUP BY 1 ORDER BY 2 DESC""").fetchall())
    rejected = sum(reasons.values())
    log.info("%s: %d rows read, %d valid, %d rejected %s",
             table, total, total - rejected, rejected, reasons or "")
    return {"rows_read": total, "rows_valid": total - rejected,
            "rows_rejected": rejected, "reject_reasons": reasons}


def transform_clean(snapshot_date: str) -> dict:
    """Task 5: clean each dataset and split it into curated and rejected rows.

    Order matters: neighbourhoods first (reference for listings), then listings
    (reference for calendar and reviews).
    """
    dirs = snapshot_dirs(snapshot_date)
    raw, cur, rej = dirs["raw"], dirs["curated"], dirs["rejected"]
    con = duckdb.connect()  # in-memory database, only used as a processing engine
    counts = {}

    # Neighbourhoods: reference table of 128 (district, neighbourhood) pairs.
    con.execute(f"""
        CREATE TABLE neighbourhoods AS
        SELECT trim(neighbourhood_group) AS district,
               trim(neighbourhood)       AS neighbourhood,
               CASE
                 WHEN district IS NULL OR neighbourhood IS NULL THEN 'missing name'
                 WHEN row_number() OVER (PARTITION BY district, neighbourhood) > 1
                   THEN 'duplicate pair'
               END AS reject_reason
        FROM {raw_csv(raw, 'neighbourhoods')}""")
    counts["neighbourhoods"] = write_split(con, "neighbourhoods", cur, rej)

    # Listings: only the columns needed for the analysis are kept.
    # Host names, descriptions, URLs and other personal or free-text fields are dropped.
    # price arrives as text like "$1,234.00"; an empty price is kept as NULL, not rejected.
    con.execute(f"""
        CREATE TABLE listings AS
        WITH typed AS (
            SELECT TRY_CAST(id AS BIGINT)            AS listing_id,
                   TRY_CAST(host_id AS BIGINT)       AS host_id,
                   trim(neighbourhood_group_cleansed) AS district,
                   trim(neighbourhood_cleansed)      AS neighbourhood,
                   room_type,
                   TRY_CAST(accommodates AS INTEGER) AS accommodates,
                   TRY_CAST(replace(replace(price, '$', ''), ',', '') AS DECIMAL(10, 2)) AS price
            FROM {raw_csv(raw, 'listings')}
        )
        SELECT t.*,
               CASE
                 WHEN t.listing_id IS NULL THEN 'missing or invalid id'
                 WHEN count(*) OVER (PARTITION BY t.listing_id) > 1 THEN 'duplicate id'
                 WHEN n.neighbourhood IS NULL THEN 'neighbourhood not in reference table'
                 WHEN t.room_type NOT IN {ROOM_TYPES} THEN 'unknown room_type'
                 WHEN t.accommodates IS NULL OR t.accommodates <= 0 THEN 'invalid accommodates'
               END AS reject_reason
        FROM typed t
        LEFT JOIN (SELECT * FROM neighbourhoods WHERE reject_reason IS NULL) n
               USING (district, neighbourhood)""")
    counts["listings"] = write_split(con, "listings", cur, rej)
    con.execute("CREATE TABLE valid_ids AS SELECT listing_id FROM listings WHERE reject_reason IS NULL")

    # Calendar: one row per listing and day. available arrives as 't' / 'f'.
    con.execute(f"""
        CREATE TABLE calendar AS
        WITH typed AS (
            SELECT TRY_CAST(listing_id AS BIGINT)     AS listing_id,
                   TRY_CAST(date AS DATE)             AS date,
                   CASE available WHEN 't' THEN true WHEN 'f' THEN false END AS available,
                   TRY_CAST(minimum_nights AS BIGINT) AS minimum_nights,
                   TRY_CAST(maximum_nights AS BIGINT) AS maximum_nights
            FROM {raw_csv(raw, 'calendar')}
        )
        SELECT t.*,
               CASE
                 WHEN t.listing_id IS NULL OR t.date IS NULL THEN 'missing or invalid key'
                 WHEN v.listing_id IS NULL THEN 'listing not in curated listings'
                 WHEN t.available IS NULL THEN 'invalid available value'
                 WHEN t.minimum_nights IS NULL OR t.maximum_nights IS NULL
                   OR t.minimum_nights <= 0 OR t.minimum_nights > t.maximum_nights
                   THEN 'invalid minimum/maximum nights'
                 WHEN count(*) OVER (PARTITION BY t.listing_id, t.date) > 1
                   THEN 'duplicate listing and date'
               END AS reject_reason
        FROM typed t LEFT JOIN valid_ids v USING (listing_id)""")
    counts["calendar"] = write_split(con, "calendar", cur, rej)

    # The snapshot label (e.g. 2026-09-17) is not the collection date: Inside Airbnb
    # collects the data over several days afterwards. Reviews are checked against
    # the latest collection date found in the listings file (last_scraped).
    last_collected, = con.execute(f"""
        SELECT max(TRY_CAST(last_scraped AS DATE)) FROM {raw_csv(raw, 'listings')}""").fetchone()
    log.info("data collected up to %s (snapshot label %s)", last_collected, snapshot_date)

    # Reviews: only listing_id, review id and date are read.
    # Reviewer id, reviewer name and comments are never loaded (privacy).
    # The key is (listing_id, id) because some review ids repeat across listings.
    con.execute(f"""
        CREATE TABLE reviews AS
        WITH typed AS (
            SELECT TRY_CAST(listing_id AS BIGINT) AS listing_id,
                   TRY_CAST(id AS BIGINT)         AS review_id,
                   TRY_CAST(date AS DATE)         AS review_date
            FROM {raw_csv(raw, 'reviews')}
        )
        SELECT t.*,
               CASE
                 WHEN t.listing_id IS NULL OR t.review_id IS NULL THEN 'missing or invalid key'
                 WHEN t.review_date IS NULL THEN 'missing or invalid date'
                 WHEN t.review_date > DATE '{last_collected}' THEN 'date after data collection'
                 WHEN v.listing_id IS NULL THEN 'listing not in curated listings'
                 WHEN count(*) OVER (PARTITION BY t.listing_id, t.review_id) > 1
                   THEN 'duplicate listing and review id'
               END AS reject_reason
        FROM typed t LEFT JOIN valid_ids v USING (listing_id)""")
    counts["reviews"] = write_split(con, "reviews", cur, rej)

    con.close()
    return counts


def aggregate_all(snapshot_date: str) -> dict:
    """Task 6: build listing_summary, one row per curated listing.

    Calendar and reviews are aggregated SEPARATELY and only then joined to the
    listings. Joining the detailed tables directly would repeat every review once
    per calendar day and multiply the counts.
    """
    cur = snapshot_dirs(snapshot_date)["curated"]
    con = duckdb.connect()
    con.execute(f"""
        COPY (
            WITH availability AS (
                SELECT listing_id,
                       count(*)                                  AS calendar_days,
                       round(avg(CAST(available AS INTEGER)), 4) AS availability_rate
                FROM '{cur}/calendar.parquet' GROUP BY listing_id
            ),
            review_activity AS (
                SELECT listing_id,
                       count(*)         AS review_count,
                       min(review_date) AS first_review,
                       max(review_date) AS last_review
                FROM '{cur}/reviews.parquet' GROUP BY listing_id
            )
            SELECT l.listing_id, l.district, l.neighbourhood, l.room_type,
                   l.accommodates, l.price,
                   a.calendar_days, a.availability_rate,
                   coalesce(r.review_count, 0) AS review_count,
                   r.first_review, r.last_review
            FROM '{cur}/listings.parquet' l
            LEFT JOIN availability    a USING (listing_id)
            LEFT JOIN review_activity r USING (listing_id)
        ) TO '{cur}/listing_summary.parquet' (FORMAT parquet)""")
    rows, = con.execute(f"SELECT count(*) FROM '{cur}/listing_summary.parquet'").fetchone()
    con.close()
    log.info("listing_summary: %d rows written", rows)
    return {"listing_summary_rows": rows}


def validate_curated(snapshot_date: str) -> dict:
    """Task 7: quality gate on the curated files.

    Every check is a query that counts problems; a result other than 0 stops the
    run. Bad data will not fix itself, so this task does not retry.
    """
    cur = snapshot_dirs(snapshot_date)["curated"]
    p = lambda name: f"'{cur}/{name}.parquet'"  # path of a curated file
    checks = {
        "listings: duplicate listing_id":
            f"SELECT count(*) - count(DISTINCT listing_id) FROM {p('listings')}",
        "calendar: duplicate (listing_id, date)":
            f"SELECT count(*) - count(DISTINCT (listing_id, date)) FROM {p('calendar')}",
        "reviews: duplicate (listing_id, review_id)":
            f"SELECT count(*) - count(DISTINCT (listing_id, review_id)) FROM {p('reviews')}",
        "calendar: listing_id not in listings":
            f"SELECT count(*) FROM {p('calendar')} WHERE listing_id NOT IN (SELECT listing_id FROM {p('listings')})",
        "reviews: listing_id not in listings":
            f"SELECT count(*) FROM {p('reviews')} WHERE listing_id NOT IN (SELECT listing_id FROM {p('listings')})",
        "listings: neighbourhood not in reference":
            f"SELECT count(*) FROM {p('listings')} l ANTI JOIN {p('neighbourhoods')} n USING (district, neighbourhood)",
        "listing_summary: row count differs from listings":
            f"SELECT abs((SELECT count(*) FROM {p('listing_summary')}) - (SELECT count(*) FROM {p('listings')}))",
    }
    con = duckdb.connect()
    results = {name: con.execute(sql).fetchone()[0] for name, sql in checks.items()}
    con.close()

    failed = {name: n for name, n in results.items() if n != 0}
    for name, n in results.items():
        (log.error if n else log.info)("validate_curated: %s -> %d", name, n)
    if failed:
        raise ValueError(f"Curated validation failed for snapshot {snapshot_date}: {failed}")
    log.info("validate_curated: all %d checks passed", len(checks))
    return results


def write_evidence(snapshot_date: str, report: dict) -> None:
    """Save the run results as evidence/<snapshot>_integration_report.json."""
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    path = EVIDENCE_DIR / f"{snapshot_date}_integration_report.json"
    path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    log.info("evidence report saved to %s", path.relative_to(EVIDENCE_DIR.parent))


if __name__ == "__main__":
    date = sys.argv[1] if len(sys.argv) > 1 else load_config()["default_snapshot_date"]
    start = time.perf_counter()
    report = {"snapshot_date": date,
              "transform_clean": transform_clean(date),
              "aggregate_all": aggregate_all(date),
              "validate_curated": validate_curated(date)}
    report["duration_seconds"] = round(time.perf_counter() - start, 1)
    write_evidence(date, report)
