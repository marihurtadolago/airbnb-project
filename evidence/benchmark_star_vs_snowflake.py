"""Benchmark: star schema versus snowflake schema for availability by neighbourhood.

Supports decision 3.3 in docs/data_model.md. It builds two small DuckDB
warehouses from the curated Parquet files of one snapshot, which differ only in
whether fact_availability contains neighbourhood_key (star) or not (snowflake),
and runs the same query on both: average availability by neighbourhood.

Requires the curated files (run pipeline.ingestion and pipeline.integration first).
Run it with:
    python evidence/benchmark_star_vs_snowflake.py            # snapshot 2026-09-17
    python evidence/benchmark_star_vs_snowflake.py 2026-09-17

Results are written to evidence/benchmark_star_vs_snowflake_<snapshot>.json.
Timings depend on the machine; the comparison between both designs is what matters.
"""

import json
import platform
import statistics
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent.parent
RUNS = 9  # timed runs per design; the median is reported

DIMENSIONS = [
    """CREATE TABLE dim_neighbourhood AS
       SELECT row_number() OVER (ORDER BY district, neighbourhood)::SMALLINT AS neighbourhood_key,
              district, neighbourhood
       FROM '{cur}/neighbourhoods.parquet'""",
    """CREATE TABLE dim_listing AS
       SELECT l.listing_id, n.neighbourhood_key, l.room_type, l.accommodates, l.price
       FROM '{cur}/listings.parquet' l JOIN dim_neighbourhood n USING (district, neighbourhood)""",
]
DESIGNS = {
    "snowflake": {
        "fact": """CREATE TABLE fact_availability AS
                   SELECT listing_id, date, available, minimum_nights, maximum_nights
                   FROM '{cur}/calendar.parquet' ORDER BY listing_id, date""",
        "query": """SELECT n.district, n.neighbourhood, avg(CAST(f.available AS INTEGER))
                    FROM fact_availability f
                    JOIN dim_listing l USING (listing_id)
                    JOIN dim_neighbourhood n ON n.neighbourhood_key = l.neighbourhood_key
                    GROUP BY ALL""",
    },
    "star": {
        "fact": """CREATE TABLE fact_availability AS
                   SELECT c.listing_id, l.neighbourhood_key, c.date, c.available,
                          c.minimum_nights, c.maximum_nights
                   FROM '{cur}/calendar.parquet' c JOIN dim_listing l USING (listing_id)
                   ORDER BY c.listing_id, c.date""",
        "query": """SELECT n.district, n.neighbourhood, avg(CAST(f.available AS INTEGER))
                    FROM fact_availability f
                    JOIN dim_neighbourhood n USING (neighbourhood_key)
                    GROUP BY ALL""",
    },
}


def run(snapshot_date: str) -> dict:
    cur = ROOT / "data" / "curated" / snapshot_date
    results = {}
    with tempfile.TemporaryDirectory() as tmp:
        for name, design in DESIGNS.items():
            db_file = Path(tmp) / f"{name}.duckdb"
            con = duckdb.connect(str(db_file))
            for sql in DIMENSIONS + [design["fact"]]:
                con.execute(sql.format(cur=cur))
            rows, = con.execute("SELECT count(*) FROM fact_availability").fetchone()
            con.execute("CHECKPOINT")
            con.execute(design["query"]).fetchall()  # warm-up run, not timed
            timings = []
            for _ in range(RUNS):
                start = time.perf_counter()
                con.execute(design["query"]).fetchall()
                timings.append((time.perf_counter() - start) * 1000)
            con.close()
            results[name] = {
                "fact_rows": rows,
                "database_size_mb": round(db_file.stat().st_size / 1e6, 1),
                "query_median_ms": round(statistics.median(timings), 1),
                "query_runs_ms": [round(t, 1) for t in timings],
            }
    return {
        "snapshot_date": snapshot_date,
        "measured_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "duckdb_version": duckdb.__version__,
        "machine": f"{platform.system()} {platform.machine()}",
        "query": "average availability by neighbourhood",
        "results": results,
    }


if __name__ == "__main__":
    snapshot = sys.argv[1] if len(sys.argv) > 1 else "2026-09-17"
    report = run(snapshot)
    out = ROOT / "evidence" / f"benchmark_star_vs_snowflake_{snapshot}.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    for name, r in report["results"].items():
        print(f"{name:9s}  size {r['database_size_mb']} MB  median {r['query_median_ms']} ms")
    print(f"saved to {out.relative_to(ROOT)}")
