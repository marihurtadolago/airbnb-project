# Checkpoint 2 status update (25 November 2026)

**Project:** Madrid Short-Term Rental Observatory. Four related Inside Airbnb datasets for Madrid (listings, calendar, reviews, neighbourhoods), snapshot 2026-09-17.

**Focus of this checkpoint:** architecture, ETL and monitoring.

## Summary

The core of the pipeline works end to end on a laptop. It downloads the four files, checks them, cleans and integrates about ten million rows with DuckDB, and writes curated Parquet files. Every run is logged, and a failed run sends an alert to the team. The warehouse schema is designed and its loading step is the next piece of work.

| Checkpoint item | Status | Where to look |
|---|---|---|
| Lifecycle mapping | Done | [docs/lifecycle.md](lifecycle.md) |
| Architecture and cluster design | Done | [docs/architecture.md](architecture.md) |
| Working ingestion and integration | Working locally. The Azure copy is pending | `pipeline/ingestion.py`, `pipeline/integration.py`, [evidence report](../evidence/2026-09-17_integration_report.json) |
| Data warehouse model | In progress: star schema defined and tested, load pending | [docs/data_model.md](data_model.md), `sql/create_tables.sql` |
| Orchestration plan | Done. The DAG and the production job are not written yet | [docs/orchestration_plan.md](orchestration_plan.md) |
| Logging and alerting design | Designed and implemented | [docs/logging_alerting.md](logging_alerting.md), `pipeline/monitoring.py`, `pipeline/alerts.py` |

## Evidence from the latest run

| Step | Result |
|---|---|
| Download of the four files (about 206 MB) | 109 s |
| Raw validation | 4 of 4 files passed |
| Cleaning and integration | 8,438,629 calendar rows, 1,524,765 reviews, 23,109 listings and 128 neighbourhoods. 0 rows rejected |
| Curated validation | 7 of 7 checks passed |
| Duration of cleaning, integration and validation | about 9 s |

Each run gets a run id that appears on every log line, and one summary line is added to `logs/runs.jsonl`. A run that was forced to fail (a snapshot date that does not exist) produced a CRITICAL alert naming the failed task. In that test the alert was only written to the log, because no team webhook was configured yet. Delivery to a webhook was tested with a local test server.

## Main decisions since Checkpoint 1

- **DuckDB as the only engine**, for both transformation and the warehouse. It reads the compressed CSV files directly, so we need no cluster at this size.
- **Star schema with daily availability grain** and a `snapshot_date` on every fact row, so that new snapshots can be added without losing history.
- **Scheduled Azure container job for production.** Airflow is kept for local development, so no machine has to stay on.
- **Alerts through a chat webhook**, and only when someone needs to act: no alert on retries that succeed or on days without a new snapshot.
- **Privacy by design:** reviewer names, reviewer ids and comments are never read from the source files.

## Next steps (towards the presentation on 21 December)

1. Load the warehouse (`load_warehouse`, `mark_processed`) and build `dim_date`.
2. Upload raw files and logs to Azure Blob Storage with the Python SDK.
3. Write the Airflow DAG, `run_daily.py` and the script that creates the scheduled Azure job.
4. Build the dashboard and derive the first insights.
5. Write the scalability analysis (×10 to ×10,000), the cloud migration cost, the data consumers design, the AI and privacy reflections, and the AI usage disclosure.

## Open questions

- Which dashboard tool to use: Looker Studio (needs exported data) or Streamlit (reads DuckDB directly).
- Whether to keep `host_id`, hash it or remove it (to be decided in the privacy reflection).
